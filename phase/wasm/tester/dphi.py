# phase.wasm.tester.dphi
import sys
import asyncio
from typing import Tuple

from arch.topos.bound.tunnel import TunnelFactory

from phase.wasm.resolver.scenario.sandbox import SandboxScenarios
from phase.wasm.resolver.scenario.ledger import LedgerScenarios
from phase.wasm.resolver.scenario.a2a import A2AScenarios
from phase.wasm.resolver.scenario.ecosystem import EcosystemScenarios
from phase.wasm.resolver.scenario.anchor import AnchorScenarios

from phase.bind.resolver import resolve_path
from phase.runtime.task.supervisor import TaskSupervisor
from phase.runtime.task.wasm import WasmTaskerDaemon
from phase.wasm.broker import WasmBroker

from watcher.plane.emitter import get_emitter

log = get_emitter("tester.dphi")
NEXUS_ROOT = resolve_path("nexus")

class WasmTester:
    """
    @desc: Handles the orchestration of WASM environment setup, worker execution,
           and multi-dimensional scenario testing with live observation.
    """
    def __init__(self, wasm_module_path: str, sandbox_root: str, suites: list[str] = None):
        self.wasm_module_path = wasm_module_path
        self.sandbox_root = sandbox_root
        self.rupture_confirmed = False
        self.last_error_context = ""
        self.auditors = [] 
        self.suites = suites or ["all"]

    async def _await_rupture(self) -> None:
        """
        @desc: Polls for a Collapse signal in the background while tests are actively running.
        """
        while not self.rupture_confirmed:
            for auditor in self.auditors:
                if getattr(auditor, 'is_collapsed', False) or getattr(auditor, 'is_exhausted', False):
                    self.rupture_confirmed = True
                    self.last_error_context = f"Auditor '{auditor.__class__.__name__}' detected fatal system collapse."
                    log.crit(f"[FATAL] {self.last_error_context}")
                    return
            await asyncio.sleep(0.5)

    async def _run_all_suites(self, broker: WasmBroker) -> int:
        """
        @desc: Sequentially executes only the selected scenario suites within an asynchronous context.
        """
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
        """
        @desc: Constructs the test sandbox and executes the scenarios asynchronously.
        """
        log.info("\n--- [START] Orchestrating Distributed WASM Environment (Delegated) ---")
        if str(NEXUS_ROOT) not in sys.path:
            sys.path.insert(0, str(NEXUS_ROOT))
            
        supervisor = None
        tunnel = None
        
        try:
            tunnel = await TunnelFactory.get_default()
            log.info("[SYSTEM] Initializing TaskSupervisor and WasmTaskerDaemon...")
            supervisor = TaskSupervisor(source="WasmTester")
            
            tasker_daemon = WasmTaskerDaemon(
                tunnel=tunnel, 
                supervisor=supervisor, 
                default_wasm_path=self.wasm_module_path
            )
            
            # Isolate the test-specific stream to fundamentally block interference from phantom/zombie daemons.
            test_stream = "wasm:execute:stream:tester_isolated"
            test_control = "wasm:control:req:tester_isolated"
            
            tasker_daemon.topic = test_stream
            tasker_daemon.control_channel = test_control
            tasker_daemon.group_name = "wasm_tester_group"
            
            supervisor.mount_daemon(tasker_daemon)
            await asyncio.sleep(1) 
            
            log.info("[SYSTEM] Initializing WasmBroker & Scenarios...")
            # The broker also routes its commands to the isolated test stream.
            broker = WasmBroker(request_stream=test_stream, timeout=5.0)
            broker.control_channel = test_control
            
            observer_task = asyncio.create_task(self._await_rupture())
            
            # Create the scenario task directly in the event loop without thread delegation.
            scenario_task = asyncio.create_task(self._run_all_suites(broker))
            
            done, pending = await asyncio.wait(
                [observer_task, scenario_task], 
                return_when=asyncio.FIRST_COMPLETED
            )
            
            for task in pending:
                task.cancel()
                
            if self.rupture_confirmed:
                return False, self.last_error_context

            if scenario_task in done:
                total_fails = scenario_task.result()
                if total_fails > 0:
                    err_msg = f"Test suites failed with {total_fails} total errors. Likely Semantic Hang, Trap, or Signature Mismatch."
                    return False, err_msg
                
            return True, ""
            
        except Exception as e:
            log.error(f"[FATAL] Test orchestration crashed: {e}", exc_info=True)
            return False, str(e)
            
        finally:
            if supervisor:
                log.info("[SYSTEM] Tearing down Sandbox (Shutting down Supervisor)...")
                await supervisor.shutdown()
                
            if tunnel:
                if hasattr(tunnel.state_store, 'aclose'):
                    await tunnel.state_store.aclose()
                elif hasattr(tunnel.state_store, 'close'):
                    await tunnel.state_store.close()
            await TunnelFactory.close_all()