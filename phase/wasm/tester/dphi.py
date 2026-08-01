# phase.wasm.tester.dphi
import sys
import json
import asyncio
from typing import Tuple

from arch.topos.tunnel.factory import TunnelFactory

from watcher.dphi.scheme.scenario.sandbox import SandboxScenarios
from watcher.dphi.scheme.scenario.ledger import LedgerScenarios
from watcher.dphi.scheme.scenario.a2a import A2AScenarios
from watcher.dphi.scheme.scenario.ecosystem import EcosystemScenarios
from watcher.dphi.scheme.scenario.anchor import AnchorScenarios

from phase.bind.resolver import resolve_path
from phase.runtime.daemon.task.supervisor import TaskSupervisor
from phase.runtime.daemon.task.wasm import WasmTaskerDaemon
from watcher.dphi.broker import WasmBroker

from watcher.plane.emitter import get_emitter, flow_scope
from phase.wasm.auditor import CanonicalProofAuditor

log = get_emitter("tester.dphi")

class WasmTester:
    def __init__(self, wasm_module_path: str, sandbox_root: str, suites: list[str] = None):
        self.wasm_module_path = wasm_module_path
        self.sandbox_root = sandbox_root
        self.rupture_confirmed = False
        self.last_error_context = ""
        self.auditors = [] 
        self.suites = suites or ["all"]
        self.test_execution_hash = None

    async def _await_rupture(self) -> None:
        """@desc: Polls for a Collapse signal in the background while tests are actively running"""
        try:
            while not self.rupture_confirmed:
                for auditor in self.auditors:
                    if getattr(auditor, 'is_collapsed', False) or getattr(auditor, 'is_exhausted', False):
                        self.rupture_confirmed = True
                        self.last_error_context = f"Auditor '{auditor.__class__.__name__}' detected fatal system collapse."
                        log.crit(f"[FATAL] {self.last_error_context}")
                        return
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass # 정상적인 태스크 취소 흐름

    async def _run_all_suites(self, broker: WasmBroker) -> int:
        """@desc: Sequentially executes only the selected scenario suites within an asynchronous context"""
        total_fails = 0
        run_all = "all" in self.suites
        
        if run_all or "sandbox" in self.suites:
            log.info("\n>>> [PHASE 1] Starting Sandbox & Compute Engine Tests <<<")
            sandbox_suite = SandboxScenarios(broker)
            await sandbox_suite.run_all()
            total_fails += sandbox_suite.fail_count
            
        if run_all or "ledger" in self.suites:
            log.info("\n>>> [PHASE 2] Starting Ledger & Blockchain Consensus Tests <<<")
            ledger_suite = LedgerScenarios(broker)
            await ledger_suite.run_all()
            total_fails += ledger_suite.fail_count

        if run_all or "a2a" in self.suites:
            log.info("\n>>> [PHASE 3] Starting A2A Monetization Tests <<<")
            a2a_suite = A2AScenarios(broker)
            await a2a_suite.run_all()
            total_fails += a2a_suite.fail_count

        if run_all or "ecosystem" in self.suites:
            log.info("\n>>> [PHASE 4] Starting Ecosystem OS Tests <<<")
            ecosystem_suite = EcosystemScenarios(broker)
            await ecosystem_suite.run_all()
            total_fails += ecosystem_suite.fail_count
            
        if run_all or "anchor" in self.suites:
            log.info("\n>>> [PHASE 5] Starting Anchor & Alignment Lifecycle Tests <<<")
            anchor_suite = AnchorScenarios(broker)
            await anchor_suite.run_all()
            total_fails += anchor_suite.fail_count
            
        return total_fails

    async def execute(self) -> Tuple[bool, str]:
        log.info("\n--- [START] Orchestrating Distributed WASM Environment (Delegated) ---")
        supervisor = None
        tunnel = None
        proof_auditor = CanonicalProofAuditor()
        self.auditors.append(proof_auditor)
        proof_auditor.attach()
        
        try:
            tunnel = await TunnelFactory.get_default()
            log.info("[SYSTEM] Initializing TaskSupervisor and WasmTaskerDaemon...")
            supervisor = TaskSupervisor(source="WasmTester")
            
            tasker_daemon = WasmTaskerDaemon(
                tunnel=tunnel, 
                supervisor=supervisor, 
                default_wasm_path=self.wasm_module_path
            )
            
            test_stream = "wasm:execute:stream:tester_isolated"
            test_control = "wasm:control:req:tester_isolated"
            
            tasker_daemon.topic = test_stream
            tasker_daemon.control_channel = test_control
            tasker_daemon.group_name = "wasm_tester_group"
            
            supervisor.mount_daemon(tasker_daemon)
            await asyncio.sleep(1) 
            
            log.info("[SYSTEM] Initializing WasmBroker & Scenarios...")
            broker = WasmBroker(
                request_stream=test_stream, 
                timeout=5.0,
                target_auditor=proof_auditor
            )
            broker.control_channel = test_control
            
            with flow_scope(phase="TEST_EXECUTION", flow_id=proof_auditor.flow_id):
                observer_task = asyncio.create_task(self._await_rupture())
                scenario_task = asyncio.create_task(self._run_all_suites(broker))
                
                done, pending = await asyncio.wait(
                    [observer_task, scenario_task], 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # [개선] 보류 중인(Pending) 백그라운드 태스크의 안전한 취소 및 정리 보장
                for task in pending:
                    task.cancel()
                    
                # 취소된 태스크들이 리소스를 완전히 반환할 때까지 대기
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                    
                if self.rupture_confirmed:
                    return False, self.last_error_context

                if scenario_task in done:
                    total_fails = scenario_task.result()
                    if total_fails > 0:
                        err_msg = f"Test suites failed with {total_fails} total errors. Likely Semantic Hang, Trap, or Signature Mismatch."
                        return False, err_msg
                    
                    log.info("\n[SYSTEM] Generating Canonical Proof for entire test run...")
                    canonical_payload = proof_auditor.generate_payload()
                    self.canonical_events_captured = len(proof_auditor.canonical_records)
                    
                    if not canonical_payload or canonical_payload == "[]":
                        err_msg = "[Ledger] Canonical payload is empty. Structural projection failed."
                        log.error(err_msg)
                        return False, err_msg
                    
                    try:
                        proof_res = await broker.invoke("compute_root_fingerprint", canonical_payload)
                        if getattr(proof_res, 'success', False):
                            self.test_execution_hash = json.loads(proof_res.output).get("fingerprint")
                            log.info(f"[Ledger] Test Execution Proof successfully sealed! Hash: {self.test_execution_hash}")
                        else:
                            error_msg = getattr(proof_res.error, 'message', 'Unknown Error') if hasattr(proof_res, 'error') else 'Unknown Error'
                            log.error(f"[Ledger] Failed to seal test execution: {error_msg}")
                    except Exception as proof_err:
                        log.error(f"[Ledger] Exception during proof generation: {proof_err}")
                        
                    return True, ""
                    
        except Exception as e:
            log.error(f"[FATAL] Test orchestration crashed: {e}", exc_info=True)
            return False, str(e)
            
        finally:
            if proof_auditor:
                proof_auditor.detach()
            if supervisor:
                log.info("[SYSTEM] Tearing down Sandbox (Shutting down Supervisor)...")
                await supervisor.shutdown()
                
            if tunnel:
                # KernelReactor 라이프사이클에 맞춘 안전한 터널 종료
                if hasattr(tunnel.state_store, 'aclose'):
                    await tunnel.state_store.aclose()
                elif hasattr(tunnel.state_store, 'close'):
                    await tunnel.state_store.close()
            await TunnelFactory.close_all()