# watcher.flare.tester
import json
import asyncio
from typing import Tuple, Dict, Type, Any, Optional

from xphi.watcher.wasm.auditor import CanonicalProofAuditor
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.watcher.tracer.bound import BaseStreamAuditor
from xphi.watcher.flare.tracer import FlareInfraMixin, WranglerDevAuditor, WranglerTailAuditor

log = get_emitter("flare.tester")

class FlareTester(FlareInfraMixin):
    """
    @desc: Meta-tester orchestrating dynamic CF Worker provisioning (pysand.ts mutation) 
           and parallel execution of test suites via FlareFacade.
    """
    def __init__(self, target_name: str = "dphi-edge-sandbox", mode: str = "dev", timeout: int = 120, suites: Dict[str, Type] = None):
        self.worker_name = target_name
        self.mode = mode
        self.timeout = timeout
        self.suites = suites or {}
        self.keep_workspace = False  # Flag to preserve workspace for diagnostics
        
        self.workspace = None
        self.auditor: Optional[BaseStreamAuditor] = None
        self.proof_auditor: Optional[CanonicalProofAuditor] = None
        
        self.rupture_confirmed = False
        self.last_error_context = ""
        self.test_execution_hash = None
        self.suite_runners: Dict[str, Any] = {}

    async def _await_rupture(self) -> None:
        """@desc: Monitors V8 sandbox for physical collapse (Error 1102) or logical panic."""
        try:
            while not self.rupture_confirmed:
                if self.proof_auditor and (getattr(self.proof_auditor, 'is_collapsed', False) or getattr(self.proof_auditor, 'is_exhausted', False)):
                    self.rupture_confirmed = True
                    self.last_error_context = "CanonicalProofAuditor detected fatal logic collapse."
                    log.crit(f"[FATAL] {self.last_error_context}")
                    return
                
                if self.auditor and getattr(self.auditor, 'hit_cpu_limit', False):
                    self.rupture_confirmed = True
                    self.last_error_context = "V8 Kinetic Trap (Error 1102) Triggered! Boundary defense is fully functional."
                    log.crit(f"[V8_RUPTURE] {self.last_error_context}")
                    return
                    
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    async def _run_all_suites(self, broker: Any) -> int:
        total_fails = 0
        for suite_name, suite_cls in self.suites.items():
            log.info(f"\n>>> [PHASE] Starting Flare Edge Test Suite: {suite_name.upper()} <<<")
            try:
                suite_instance = suite_cls(broker)
                self.suite_runners[suite_name] = suite_instance
                await suite_instance.run_all()
                total_fails += suite_instance.fail_count
            except Exception as e:
                log.error(f"[ERROR] Suite '{suite_name}' crashed unexpectedly: {e}", exc_info=True)
                total_fails += 1
        return total_fails

    async def execute(self, broker: Any) -> Tuple[bool, str]:
        log.info(f"\n--- [START] Orchestrating Cloudflare Edge Hologram ({self.mode.upper()}) ---")
        
        self.proof_auditor = CanonicalProofAuditor()
        self.proof_auditor.attach()
        
        # Inject proof auditor into broker dynamically
        if hasattr(broker, 'target_auditor'):
            broker.target_auditor = self.proof_auditor
        
        try:
            # 1. Provision Edge runtime dynamically
            await self.provision_workspace(self.worker_name)
            
            # 2. Attach V8 Observer & await runtime
            if self.mode == "dev":
                log.info("[SYSTEM] Igniting Local Hologram & Dev Auditor...")
                self.auditor = WranglerDevAuditor(self.workspace, boundary=None)
                self.auditor.attach()
                for _ in range(15):
                    if getattr(self.auditor, 'is_ready', False): break
                    await asyncio.sleep(1)
                    
            elif self.mode == "deploy":
                await self.deploy_to_global_edge()
                log.info("[SYSTEM] Igniting Global Tail Auditor...")
                self.auditor = WranglerTailAuditor(self.worker_name, boundary=None)
                self.auditor.attach()
                await asyncio.sleep(2)
            
            # [FIX] Automated Self-Diagnosis: Extract and report buffered logs upon timeout
            if not getattr(self.auditor, 'is_ready', False):
                last_logs = getattr(self.auditor, 'startup_logs', [])
                if last_logs:
                    log_dump = "\n  │ ".join(last_logs)
                    error_msg = f"Edge Runtime failed to materialize in time.\n  │ [Wrangler Output Dump]\n  │ {log_dump}"
                else:
                    error_msg = "Edge Runtime failed to materialize. (No logs emitted. Is 'npx' installed and accessible in PATH?)"
                
                raise TimeoutError(error_msg)

            # 3. Parallel execution: Foreground (Tests) vs Background (Observation)
            with flow_scope(phase="TEST_EXECUTION", flow_id=self.proof_auditor.flow_id):
                observer_task = asyncio.create_task(self._await_rupture())
                scenario_task = asyncio.create_task(self._run_all_suites(broker))
                
                done, pending = await asyncio.wait(
                    [observer_task, scenario_task], 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in pending: task.cancel()
                if pending: await asyncio.gather(*pending, return_exceptions=True)
                
                # Check background rupture flag first
                if self.rupture_confirmed:
                    return False, self.last_error_context

                # Evaluate logical test results
                if scenario_task in done:
                    total_fails = scenario_task.result()
                    if total_fails > 0:
                        return False, f"Logical execution failed with {total_fails} errors."
                    
                    log.info("\n[SYSTEM] Generating Canonical Proof for Edge execution...")
                    canonical_payload = self.proof_auditor.generate_payload()
                    
                    if not canonical_payload or canonical_payload == "[]":
                        return False, "[Ledger] Canonical payload is empty."
                    
                    proof_res = await broker.invoke("compute_root_fingerprint", canonical_payload)
                    if getattr(proof_res, 'success', False):
                        self.test_execution_hash = json.loads(proof_res.output).get("fingerprint")
                        log.info(f"[Ledger] Edge Execution Proof sealed! Hash: {self.test_execution_hash}")
                        return True, ""
                    else:
                        return False, "[Ledger] Failed to seal test execution at Edge."
                    
        except Exception as e:
            log.error(f"[FATAL] Flare Edge Orchestration crashed: {e}", exc_info=True)
            return False, str(e)
            
        finally:
            self.proof_auditor.detach()
            if self.auditor: self.auditor.detach()
            
            if self.mode == "deploy":
                await self.destroy_global_edge(self.worker_name)
                
            # Check keep_workspace flag before dismantling to allow diagnostics
            if getattr(self, 'keep_workspace', False):
                log.warning(f"🛑 [DIAGNOSTICS] Workspace preserved at {self.workspace} for post-mortem analysis.")
            else:
                log.info("[SYSTEM] Dismantling Edge Hologram Workspace...")
                await self.teardown_workspace()