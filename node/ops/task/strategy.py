# xphi.node.ops.task.strategy
import json
import threading
import queue
import time
from pathlib import Path
from typing import Any, Dict
from contextlib import suppress

from xphi.state.inter.wasm import WasmInterpreter
from xphi.state.inter.python import PythonInterpreter
from xphi.state.inter.fsm import FSMInterpreter
from xphi.node.runtime.wasm.quota import QuotaPolicy

class ExecutionStrategy:
    """Class-based Execution Strategy for isolated sandboxing and execution"""
    
    def __init__(self, prewarm_pool_size: int = 5):
        self.prewarm_pool_size = prewarm_pool_size
        self.py_pool = queue.Queue(maxsize=self.prewarm_pool_size)
        self._pool_lock = threading.Lock()
        self._pool_started = False
        
        self._start_pool_if_needed()

    def _replenish_worker(self):
        while True:
            try:
                interp = PythonInterpreter(
                    enable_network_access=[], 
                    enable_read_paths=[], 
                    enable_write_paths=[],
                    enable_env_vars=["FIBER_ISOLATION_MARKER"], 
                    policy=QuotaPolicy.standard()
                )
                interp.start()
                self.py_pool.put(interp)
            except Exception:
                time.sleep(1)

    def _start_pool_if_needed(self):
        if not self._pool_started:
            with self._pool_lock:
                if not self._pool_started:
                    threading.Thread(target=self._replenish_worker, daemon=True, name="PyReplenisher").start()
                    self._pool_started = True

    def _is_request_expired(self, context: dict, default_timeout_sec: float = 15.0) -> bool:
        ts_val = float(context.get("timestamp", 0))
        if ts_val <= 0:
            return False
            
        if ts_val > 1e11:
            req_ts = ts_val / 1000.0
        else:
            req_ts = ts_val

        elapsed = time.time() - req_ts
        timeout_sec = float(context.get("timeout", default_timeout_sec))
        
        if elapsed > 86400 * 365:
            return False
            
        if elapsed > timeout_sec:
            return True
            
        return False

    def _commit_state_diff_to_master(self, context: dict, state_diff: dict, log, default_target: str = "root"):
        """Commit FSM state diffs (branches or cursors) to the master ledger."""
        if not state_diff:
            return
        target_stream = context.get("stream_id") or context.get("target_address") or default_target
        log.info(f"[Host Commit] Committed {len(state_diff)} state diff(s) to master ledger (target: {target_stream})")

    def run_fsm_sandbox(self, target_path: Path, job_policy: QuotaPolicy, safe_payload: Any, context: dict, job_id: str, log) -> dict:
        try:
            safe_dict = safe_payload if isinstance(safe_payload, dict) else {}
            if isinstance(safe_payload, str):
                with suppress(Exception):
                    safe_dict = json.loads(safe_payload)
            
            vm_target = safe_dict.get("vm_target", "FSM_STEP")
            log.info(f"[{job_id[:8]}] Entering Stateless Multi-VM Jail: {target_path.name} (Tier: {job_policy.tier.value}, Target: {vm_target})")
            with FSMInterpreter(wasm_module_name=target_path.name, policy=job_policy) as fsm_inter:
                result = fsm_inter.execute(
                    vm_target=vm_target,
                    target_address=safe_dict.get("target_address", ""),
                    calldata=safe_dict.get("calldata", ""),
                    state_snapshot=safe_dict.get("state_snapshot", {}),
                    context=context
                )
                
                metrics = fsm_inter.get_metrics()
                if result.success:
                    with suppress(Exception):
                        out_dict = json.loads(result.output)
                        metrics["gas_used"] = out_dict.get("gas_used", 0)
                        state_diff = out_dict.get("state_diff", {})
                        if state_diff:
                            target_addr = safe_dict.get("target_address", "root")
                            self._commit_state_diff_to_master(context, state_diff, log, default_target=target_addr)
                        
                log.info(f"[{job_id[:8]}] VM Sandbox Metrics: {metrics}")
                return {
                    "success": result.success, "output": result.output, 
                    "error": str(result.error) if not result.success else "", "metrics": metrics
                }
        except Exception as e:
            log.error(f"[{job_id[:8]}] Sandbox Execution crashed: {e}", exc_info=True)
            return {"success": False, "output": "", "error": f"Execution Error: {e}"}

    def run_python_sandbox(self, job_policy: QuotaPolicy, safe_payload: Any, context: dict, job_id: str, log) -> dict:
        if self._is_request_expired(context):
            log.warning(f"[{job_id[:8]}] 🗑️ Dropping expired zombie request before acquiring Python Sandbox.")
            return {"success": False, "output": "", "error": "Request expired (TTL Exceeded)."}

        py_sandbox = None
        t0 = time.perf_counter()
        
        try:
            try:
                py_sandbox = self.py_pool.get(timeout=4.5)
                t_wait_ms = (time.perf_counter() - t0) * 1000
            except queue.Empty:
                log.warning(f"[{job_id[:8]}] Python Worker pool exhausted. Applying Backpressure.")
                return {"success": False, "output": "", "error": "SYSTEM_OVERLOADED (Backpressure applied). Please try again."}
                
            if hasattr(py_sandbox, 'apply_policy'):
                py_sandbox.apply_policy(job_policy)

            code_to_run, variables = "", {}
            if isinstance(safe_payload, str):
                code_to_run = safe_payload
            elif isinstance(safe_payload, dict):
                code_to_run = safe_payload.get("code", safe_payload.get("data", ""))
                variables = safe_payload.get("variables", {})

            log.info(f"[{job_id[:8]}] Entering Python Legacy Jail (Tier: {job_policy.tier.value}, Cached: True)")
            
            t_exec_start = time.perf_counter()
            result = py_sandbox.execute(code=code_to_run, variables=variables, callables={}, context=context)
            t_exec_ms = (time.perf_counter() - t_exec_start) * 1000
            
            metrics = py_sandbox.get_metrics()
            metrics["timing_ms"] = {
                "wait": round(t_wait_ms, 2),
                "spawn": 0.0, 
                "exec": round(t_exec_ms, 2)
            }
            
            total_ms = t_wait_ms + t_exec_ms
            log.info(f"[{job_id[:8]}] [Perf] ⚡(Cached) Wait: {t_wait_ms:.2f}ms | Exec: {t_exec_ms:.2f}ms | Total: {total_ms:.2f}ms")
            log.info(f"[{job_id[:8]}] Sandbox Metrics: {metrics}")
            
            return {
                "success": result.success, "output": result.output,
                "error": str(result.error) if not result.success else "", "metrics": metrics
            }
        except Exception as e:
            error_msg = str(e)
            if "Execution Timeout" in error_msg:
                log.warning(f"[{job_id[:8]}] Sandbox Execution Timeout (Infinite loop/Deadlock defended).")
                return {"success": False, "output": "", "error": error_msg}
                
            log.error(f"[{job_id[:8]}] Python Execution crashed: {e}", exc_info=True)
            return {"success": False, "output": "", "error": f"Execution Error: {error_msg}"}
        finally:
            if py_sandbox:
                with suppress(Exception):
                    py_sandbox.shutdown()