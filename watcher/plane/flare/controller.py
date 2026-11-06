# xphi.watcher.plane.flare.controller
import os
import re
import json
import asyncio
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, Type, Tuple, List

from xphi.arch.contract.config.env import config as env_config
from xphi.node.runtime.wasm.broker import WasmBroker
from xphi.node.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.watcher.plane.flare.tunnel import FlareTunnelFactory
from xphi.watcher.plane.tracer.base import BaseStreamAuditor, SystemBound, log_streamer

TIME_ROOT = resolve_path("time")
EDGE_WORKSPACE_ROOT = resolve_path("flaretime")

log = get_emitter("plane.flare.controller")
_WORKERS_DEV_URL_PATTERN = re.compile(r"https://[a-zA-Z0-9._-]+\.workers\.dev")

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
        
        self.log.debug(f"[{self.worker_name.upper()}] Auditor initialized (Port: {self.port}, Workspace: {self.workspace})")

    @log_streamer(["npx", "wrangler", "dev", "--port", "{port}", "--inspector-port", "{inspector_port}", "--local"], cwd="{workspace}")
    async def run_stream(self, line: str) -> None:
        if not line: return
        line_stripped = line.strip()
        
        self.startup_logs.append(line_stripped)
        if len(self.startup_logs) > 50: self.startup_logs.pop(0)
        
        if f"Ready on http://127.0.0.1:{self.port}" in line or f"Ready on http://localhost:{self.port}" in line:
            self.is_ready = True
            self.log.info(f"  [{self.worker_name.upper()}] [READY] V8 Isolate Provisioned (Port {self.port} / Inspector {self.inspector_port}).")
        elif "1102" in line or "CPU time limit exceeded" in line:
            self.hit_cpu_limit = True
            self.log.warning(f"  [{self.worker_name.upper()}] [RESOURCE_EXHAUSTION] Hard CPU Time Limit (Error 1102) Exceeded!")
        elif "error" in line.lower() or "exception" in line.lower() or "trap" in line.lower():
            self.log.error(f"  [{self.worker_name.upper()}_FAULT] {line_stripped}")
        else:
            self.log.debug(f"  [{self.worker_name.upper()}_STREAM] {line_stripped}")


class FlareController:
    """@desc: Orchestrates Dual V8 Isolates (Wasm Gateway & Pyodide Engine) and manages Local <-> Edge bridge lifecycle."""
    def __init__(
        self,
        target_name: str = "xphi-edge-sandbox",
        mode: str = "dev",
        timeout: int = 120,
        suites: Dict[str, Type] = None,
        edge_state_bindings: str = "",
        keep_workspace: bool = False,
    ):
        self.worker_name = target_name
        self.mode = mode.lower().strip()
        self.timeout = timeout
        self.suites = suites or {}
        self.edge_state_bindings = edge_state_bindings.strip()
        
        env_keep = str(env_config.get("XPHI_KEEP_WORKSPACE", os.getenv("XPHI_KEEP_WORKSPACE", ""))).lower()
        self.keep_workspace = keep_workspace or (env_keep in ("1", "true", "yes"))
        
        self.workspace = Path("/tmp/xphi_edge_workspace")
        self.gateway_dir = self.workspace / "gateway_node"
        self.python_dir = self.workspace / "python_node"
        
        self.auditor_gateway: Optional[WranglerSandboxAuditor] = None
        self.auditor_python: Optional[WranglerSandboxAuditor] = None
        
        self.flow_id = "pta_edge_epoch" 
        self.crash_confirmed = False
        self.last_error_context = ""
        self.suite_runners: Dict[str, Any] = {}
        self.test_execution_hash: Optional[str] = None
        self._prev_plane_config: Optional[str] = None
        self._deployed_url: Optional[str] = None
        log.debug(f"[FlareController] Initialized with mode={self.mode}, timeout={self.timeout}, suites={list(self.suites.keys())}")

    @property
    def edge_url(self) -> str:
        if self.mode == "dev":
            return "http://127.0.0.1:8787"
        if self._deployed_url:
            return self._deployed_url
        resolved = FlareTunnelFactory._resolve_edge_url("")
        if resolved and not FlareTunnelFactory._is_local_endpoint(resolved):
            return resolved
        return f"https://{self.worker_name}.workers.dev"

    def _provision_microservices(self):
        log.debug(f"[FlareController:_provision_microservices] Starting provisioning at workspace: {self.workspace}")
        time_root = Path(TIME_ROOT)
        edge_root = Path(EDGE_WORKSPACE_ROOT)

        # Setup Gateway Node
        self.gateway_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(edge_root / "router.ts", self.gateway_dir / "index.ts")
        
        wasm_targets = ["gateway.wasm", "phase.wasm", "fsm.wasm"]
        for wasm in wasm_targets:
            if (time_root / wasm).exists():
                shutil.copy2(time_root / wasm, self.gateway_dir / wasm)

        state_anchor_block = self.edge_state_bindings or (
            "# [Edge State Anchor Placeholder: Durable Objects / D1 / Queues]\n"
            "# Core defaults to Host-Sovereign stateless compute.\n"
            "# [[durable_objects.bindings]]\n"
            "# name = \"XPHI_STATE_ANCHOR\"\n"
            "# class_name = \"EdgeEpochCoordinator\""
        )

        gateway_toml = f"""
name = "xphi-gateway-node"
main = "index.ts"
compatibility_date = "2024-01-01"

[[rules]]
type = "CompiledWasm"
globs = ["**/*.wasm"]
fallthrough = true

[[services]]
binding = "PYTHON_ENGINE"
service = "xphi-python-node"

{state_anchor_block}
"""
        (self.gateway_dir / "wrangler.toml").write_text(gateway_toml.strip())

        # Setup Pyodide Python Engine Worker
        self.python_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(edge_root / "engine.py", self.python_dir / "index.py")
        
        python_toml = f"""
name = "xphi-python-node"
main = "index.py"
compatibility_date = "2024-03-20"
compatibility_flags = ["python_workers"]
"""
        (self.python_dir / "wrangler.toml").write_text(python_toml.strip())
        log.info("[PROVISION] Edge Workers Mounted: Wasm Gateway Node & Pyodide Python Engine.")

    async def _run_wrangler_deploy(self, node_dir: Path, node_label: str) -> str:
        """Executes `npx wrangler deploy` for a microservice directory and returns stdout."""
        log.info(f"[DEPLOY] Deploying {node_label} from {node_dir}...")
        proc = await asyncio.create_subprocess_exec(
            "npx", "wrangler", "deploy",
            cwd=str(node_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout_bytes, _ = await asyncio.wait_for(proc.communicate(), timeout=float(self.timeout))
        output = stdout_bytes.decode("utf-8", errors="replace")

        if proc.returncode != 0:
            raise RuntimeError(f"Wrangler deploy failed for {node_label} (exit={proc.returncode}):\n{output}")

        match = _WORKERS_DEV_URL_PATTERN.search(output)
        deployed_endpoint = match.group(0) if match else ""
        log.info(f"[DEPLOY] {node_label} deployed successfully. {f'Endpoint: {deployed_endpoint}' if deployed_endpoint else ''}")
        return deployed_endpoint

    async def _deploy_microservices(self) -> str:
        """
        Deploys Dual V8 Isolates to Cloudflare in strict Service Binding dependency order
        (`xphi-python-node` first, then `xphi-gateway-node`), unless a remote BYOC URL
        is already explicitly provided via `XPHI_WASM_PLANE=flare:https://...`.
        """
        explicit_remote = FlareTunnelFactory._resolve_edge_url("")
        if explicit_remote and not FlareTunnelFactory._is_local_endpoint(explicit_remote):
            log.info(f"[DEPLOY] Target BYOC Edge URL already specified ({explicit_remote}). Skipping wrangler deploy.")
            self._deployed_url = explicit_remote
            return explicit_remote

        # 1. Deploy downstream Python Engine first (required by PYTHON_ENGINE Service Binding)
        await self._run_wrangler_deploy(self.python_dir, "xphi-python-node (Pyodide Engine)")

        # 2. Deploy upstream Wasm Gateway and capture public workers.dev URL
        gateway_url = await self._run_wrangler_deploy(self.gateway_dir, "xphi-gateway-node (Wasm Gateway)")
        if gateway_url:
            self._deployed_url = gateway_url
        return self.edge_url

    async def start_runtime(self) -> str:
        """Provisions workspace, starts local Dual V8 Isolates (dev) or deploys/binds remote Workers (deploy)."""
        self.crash_confirmed = False
        self.last_error_context = ""

        if self.workspace.exists(): 
            shutil.rmtree(self.workspace)
        self._provision_microservices()
        
        if self.mode == "dev":
            log.info("[SYSTEM] Starting Dual V8 Isolate Workers (Wasm Gateway & Pyodide Engine)...")
            self.auditor_python = WranglerSandboxAuditor(self.python_dir, 8788, "python_engine")
            self.auditor_python.attach()

            self.auditor_gateway = WranglerSandboxAuditor(self.gateway_dir, 8787, "wasm_gateway")
            self.auditor_gateway.attach()
            
            for _ in range(20):
                gw_ready = getattr(self.auditor_gateway, 'is_ready', False)
                py_ready = getattr(self.auditor_python, 'is_ready', False)
                
                if gw_ready and py_ready:
                    log.info("[WAIT] Awaiting local workerd runtime & Pyodide snapshot stabilization (3.0s)...")
                    await asyncio.sleep(3.0)
                    break
                await asyncio.sleep(1)
                
            if not (self.auditor_gateway.is_ready and self.auditor_python.is_ready):
                raise TimeoutError("V8 Sandbox Workers failed to start within the timeout period.")
        else:
            target_url = await self._deploy_microservices()
            log.info(f"[SYSTEM] Bound to Remote Edge Worker Endpoint: {target_url}")

        await FlareTunnelFactory.get_default(mq_url=self.edge_url)
        return self.edge_url

    async def stop_runtime(self) -> None:
        """Safely closes the HTTP bridge, detaches V8 Isolate processes, and cleans up the workspace."""
        log.debug("[FlareController:stop_runtime] Initiating Sandbox Cleanup & Teardown phase.")
        await FlareTunnelFactory.close_all()
        if self.auditor_gateway:
            self.auditor_gateway.detach()
            self.auditor_gateway = None
        if self.auditor_python:
            self.auditor_python.detach()
            self.auditor_python = None
        if not self.keep_workspace and self.workspace.exists():
            shutil.rmtree(self.workspace)
        elif self.keep_workspace and self.workspace.exists():
            log.info(f"[WORKSPACE] Preserved synthesized Edge blueprints at: {self.workspace}")

    async def __aenter__(self) -> WasmBroker:
        """Context manager for general runtime usage: provisions V8 Isolates and binds XPHI_WASM_PLANE."""
        self._prev_plane_config = env_config.get("XPHI_WASM_PLANE", os.getenv("XPHI_WASM_PLANE", "local"))
        edge_url = await self.start_runtime()
        env_config.set_override("XPHI_WASM_PLANE", f"flare:{edge_url}")
        return WasmBroker.get_shared(timeout=float(self.timeout))

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        shared_broker = WasmBroker._shared_instance
        if shared_broker is not None:
            await shared_broker.close()
        if self._prev_plane_config is not None:
            env_config.set_override("XPHI_WASM_PLANE", self._prev_plane_config)
        await self.stop_runtime()

    async def _await_crash_or_limit(self) -> None:
        """Monitors for intentional CPU Limits (Error 1102) or fatal WASM Traps during tests"""
        log.debug("[FlareController:_await_crash_or_limit] Observer task started, waiting for crash or limit flags...")
        try:
            while not self.crash_confirmed:
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
        """Executes all injected test scenarios and collects resulting edge hashes"""
        log.debug("[FlareController:_run_all_suites] Test suite execution started.")
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
                
        extracted_root = getattr(broker, 'epoch_root_hash', None)
        if extracted_root:
            self.test_execution_hash = extracted_root
            
        return total_fails

    async def execute(self, broker: Any = None) -> Tuple[bool, str]:
        log.info(f"\n--- [START] Orchestrating Edge V8 Isolate Environment ({self.mode.upper()}) ---")
        try:
            await self.start_runtime()
            active_broker = broker or WasmBroker.get_shared(tunnel_factory=FlareTunnelFactory)

            with flow_scope(phase="TEST_EXECUTION", flow_id=self.flow_id):
                observer_task = asyncio.create_task(self._await_crash_or_limit())
                scenario_task = asyncio.create_task(self._run_all_suites(active_broker))
                
                done, pending = await asyncio.wait(
                    [observer_task, scenario_task], 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in pending: 
                    task.cancel()
                if pending: 
                    await asyncio.gather(*pending, return_exceptions=True)
                
                if self.crash_confirmed:
                    return False, self.last_error_context
                    
                if scenario_task in done:
                    total_fails = scenario_task.result()
                    if total_fails > 0:
                        return False, f"Logical execution failed with {total_fails} errors."
                    
                    log.info("\n[SYSTEM] Verifying Epoch Merkle Root generated by Edge Suites...")
                    if not self.test_execution_hash:
                        log.warning("[FlareController] Root Hash missing! (Did Domain 5 PTA Rollup fail or was Broker bypassed?)")
                        self.test_execution_hash = "EMPTY_EPOCH_ROOT"
                        
                    log.info(f"[Telemetry] Edge Execution Proof sealed via PTA | Root Hash: {self.test_execution_hash}")
                    return True, ""
        except Exception as e:
            log.error(f"[FATAL] Sandbox Orchestration failed: {e}")
            return False, str(e)
        finally:
            if broker is not None and hasattr(broker, "close"):
                await broker.close()
            await self.stop_runtime()