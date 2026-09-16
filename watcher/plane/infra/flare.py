# xphi.watcher.plane.infra.flare
## @lineage: xphi.watcher.plane.phase.flare
"""
@desc: 
- Edge V8 Sandbox Orchestrator
- Provisions Cloudflare Workers (V8 Isolates) and observes WASM traps, CPU limits, and crashes
"""
import json
import asyncio
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, Type, Tuple, List

from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.arch.dev.tracer.base import BaseStreamAuditor, SystemBound, log_streamer
from xphi.arch.dev.wasm.auditor import CanonicalProofAuditor

TIME_ROOT = resolve_path("time")
EDGE_WORKSPACE_ROOT = resolve_path("flaretime")

log = get_emitter("edge.sandbox.controller")

class WranglerSandboxAuditor(BaseStreamAuditor):
    """@desc: Observes standard I/O from local V8 Isolate (Wrangler) processes."""
    def __init__(self, workspace: Path, port: int, worker_name: str, boundary: SystemBound = None):
        super().__init__(target=f"wrangler_dev_{port}", boundary=boundary, delay=1)
        self.workspace = workspace
        self.port = port
        self.inspector_port = port + 500
        self.worker_name = worker_name
        self.log = get_emitter(f"auditor.v8_sandbox.{worker_name}", phase="telemetry")
        
        self.hit_cpu_limit = False
        self.is_ready = False
        self.startup_logs: List[str] = []

    @log_streamer(["npx", "wrangler", "dev", "--port", "{port}", "--inspector-port", "{inspector_port}", "--local"], cwd="{workspace}")
    async def run_stream(self, line: str) -> None:
        if not line: return
        line_stripped = line.strip()
        
        self.startup_logs.append(line_stripped)
        if len(self.startup_logs) > 50: self.startup_logs.pop(0)
        
        if f"Ready on http://127.0.0.1:{self.port}" in line or f"Ready on http://localhost:{self.port}":
            self.is_ready = True
            self.log.info(f"  [{self.worker_name.upper()}] ⚡ V8 Isolate Provisioned (Port {self.port} / Debug {self.inspector_port}).")
        elif "1102" in line or "CPU time limit exceeded" in line:
            self.hit_cpu_limit = True
            self.log.warning(f"  [{self.worker_name.upper()}] [RESOURCE_EXHAUSTION] Hard CPU Time Limit (Error 1102) Exceeded!")
        elif "error" in line.lower() or "exception" in line.lower() or "trap" in line.lower():
            self.log.error(f"  [{self.worker_name.upper()}_FAULT] {line_stripped}")
        else:
            self.log.debug(f"  [{self.worker_name.upper()}_STREAM] {line_stripped}")


class FlareController:
    """@desc: Orchestrates Dual V8 Isolates (WASM Gateway & Python Native) and monitors for execution limits."""
    def __init__(self, target_name: str = "xphi-edge-sandbox", mode: str = "dev", timeout: int = 120, suites: Dict[str, Type] = None):
        self.worker_name = target_name
        self.mode = mode
        self.timeout = timeout
        self.suites = suites or {}
        self.keep_workspace = False  
        
        self.workspace = Path("/tmp/xphi_edge_workspace")
        self.gateway_dir = self.workspace / "gateway_node"
        self.python_dir = self.workspace / "python_node"
        
        self.auditor_gateway: Optional[WranglerSandboxAuditor] = None
        self.auditor_python: Optional[WranglerSandboxAuditor] = None
        self.proof_auditor = CanonicalProofAuditor()
        
        self.crash_confirmed = False
        self.last_error_context = ""
        self.suite_runners: Dict[str, Any] = {}

    def _provision_microservices(self):
        time_root = Path(TIME_ROOT)
        edge_root = Path(EDGE_WORKSPACE_ROOT)

        # 1. Setup Gateway Node (TypeScript + WASM)
        self.gateway_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(edge_root / "router.ts", self.gateway_dir / "index.ts")
        
        # 엄밀한 WASM 타겟 바인딩 (gateway, dphi, dvm)
        wasm_targets = ["gateway.wasm", "dphi.wasm", "dvm.wasm"]
        for wasm in wasm_targets:
            if (time_root / wasm).exists():
                shutil.copy2(time_root / wasm, self.gateway_dir / wasm)
                
        gateway_toml = f"""
name = "xphi-gateway-node"
main = "index.ts"
compatibility_date = "2024-01-01"

[[rules]]
type = "CompiledWasm"
globs = ["**/*.wasm"]
fallthrough = true

# Service Binding: Gateway can route requests to the internal Python Engine
[[services]]
binding = "PYTHON_ENGINE"
service = "xphi-python-node"
"""
        (self.gateway_dir / "wrangler.toml").write_text(gateway_toml.strip())

        # 2. Setup Native Python Engine Worker
        self.python_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(edge_root / "engine.py", self.python_dir / "index.py")
        
        python_toml = f"""
name = "xphi-python-node"
main = "index.py"
compatibility_date = "2024-03-20"
compatibility_flags = ["python_workers"]
"""
        (self.python_dir / "wrangler.toml").write_text(python_toml.strip())
        log.info("📦 Edge Microservices Provisioned: WASM Gateway Node & Native Python Engine.")

    async def _await_crash_or_limit(self) -> None:
        """Monitors for intentional CPU Limits (Error 1102) or fatal WASM Traps during tests"""
        try:
            while not self.crash_confirmed:
                if self.proof_auditor and (getattr(self.proof_auditor, 'is_collapsed', False) or getattr(self.proof_auditor, 'is_exhausted', False)):
                    self.crash_confirmed = True
                    self.last_error_context = "CanonicalProofAuditor detected fatal execution crash (OOM/Trap)."
                    log.critical(f"[FATAL_CRASH] {self.last_error_context}")
                    return
                
                # Check both workers for V8 Isolate CPU/Memory limits
                gateway_limit = self.auditor_gateway and getattr(self.auditor_gateway, 'hit_cpu_limit', False)
                python_limit = self.auditor_python and getattr(self.auditor_python, 'hit_cpu_limit', False)
                
                if gateway_limit or python_limit:
                    self.crash_confirmed = True
                    self.last_error_context = "Resource Exhaustion Triggered! V8 CPU limits enforced successfully."
                    log.critical(f"[RESOURCE_LIMIT_TRAP] {self.last_error_context}")
                    return
                    
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    async def _run_all_suites(self, broker: Any) -> int:
        """Executes all injected test scenarios"""
        total_fails = 0
        for suite_name, suite_cls in self.suites.items():
            log.info(f"\n>>> [PHASE] Starting Edge Integration Suite: {suite_name.upper()} <<<")
            try:
                suite_instance = suite_cls(broker)
                self.suite_runners[suite_name] = suite_instance
                await suite_instance.run_all()
                total_fails += getattr(suite_instance, 'fail_count', 0)
            except Exception as e:
                log.error(f"[ERROR] Suite '{suite_name}' crashed unexpectedly: {e}", exc_info=True)
                total_fails += 1
        return total_fails

    async def execute(self, broker: Any) -> Tuple[bool, str]:
        log.info(f"\n--- [START] Orchestrating Edge V8 Sandbox Environment ({self.mode.upper()}) ---")
        self.proof_auditor.attach()
        if hasattr(broker, 'target_auditor'):
            broker.target_auditor = self.proof_auditor
        
        try:
            if self.workspace.exists(): shutil.rmtree(self.workspace)
            self._provision_microservices()
            
            if self.mode == "dev":
                log.info("[SYSTEM] Starting Dual V8 Isolate Workers (WASM Gateway & Python Engine)...")
                
                # Python Engine Boot (Port 8788, Debug 9288)
                self.auditor_python = WranglerSandboxAuditor(self.python_dir, 8788, "python_engine")
                self.auditor_python.attach()
                
                # Gateway Engine Boot (Port 8787, Debug 9287)
                self.auditor_gateway = WranglerSandboxAuditor(self.gateway_dir, 8787, "wasm_gateway")
                self.auditor_gateway.attach()
                
                # Wait for both workers to be ready
                for _ in range(20):
                    if getattr(self.auditor_gateway, 'is_ready', False) and getattr(self.auditor_python, 'is_ready', False):
                        break
                    await asyncio.sleep(1)
                    
                if not (self.auditor_gateway.is_ready and self.auditor_python.is_ready):
                    raise TimeoutError("V8 Sandbox Workers failed to start within the timeout period.")

            with flow_scope(phase="TEST_EXECUTION", flow_id=self.proof_auditor.flow_id):
                observer_task = asyncio.create_task(self._await_crash_or_limit())
                scenario_task = asyncio.create_task(self._run_all_suites(broker))
                
                done, pending = await asyncio.wait(
                    [observer_task, scenario_task], 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in pending: task.cancel()
                if pending: await asyncio.gather(*pending, return_exceptions=True)
                
                if self.crash_confirmed:
                    return False, self.last_error_context
                    
                if scenario_task in done:
                    total_fails = scenario_task.result()
                    if total_fails > 0:
                        return False, f"Logical execution failed with {total_fails} errors."
                    
                    log.info("\n[SYSTEM] Generating Canonical Execution Trace Hash...")
                    canonical_payload = self.proof_auditor.generate_payload()
                    
                    if not canonical_payload or canonical_payload == "[]":
                        return False, "[Telemetry] Canonical payload is empty."
                    
                    proof_res = await broker.invoke("compute_root_fingerprint", canonical_payload)
                    if getattr(proof_res, 'success', False):
                        self.test_execution_hash = json.loads(proof_res.output).get("fingerprint")
                        log.info(f"[Telemetry] Edge Execution Proof sealed! Hash: {self.test_execution_hash}")
                        return True, ""
                    else:
                        return False, "[Telemetry] Failed to seal test execution trace at Edge."

        except Exception as e:
            log.error(f"[FATAL] Sandbox Orchestration failed: {e}")
            return False, str(e)
        finally:
            self.proof_auditor.detach()
            if self.auditor_gateway: self.auditor_gateway.detach()
            if self.auditor_python: self.auditor_python.detach()
            if not self.keep_workspace and self.workspace.exists():
                shutil.rmtree(self.workspace)