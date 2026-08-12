# kernel.phase.daemon.task.strategy
import json
import threading
import queue
import time
from pathlib import Path
from typing import Any, Dict
from contextlib import suppress

from kernel.bind.inter.wasm import WasmInterpreter
from kernel.bind.inter.python import PythonInterpreter
from kernel.bind.inter.dvm import DvmInterpreter
from kernel.dphi.cgroup import CgroupPolicy
from kernel.dphi.method import DphiMethod

PREWARM_POOL_SIZE = 4
_py_pool = queue.Queue(maxsize=PREWARM_POOL_SIZE)
_pool_lock = threading.Lock()
_pool_started = False

def _replenish_worker():
    """백그라운드에서 끊임없이 Deno 프로세스를 구동하여 큐에 적재합니다."""
    while True:
        try:
            interp = PythonInterpreter(
                enable_network_access=[], 
                enable_read_paths=[], 
                enable_write_paths=[], 
                enable_env_vars=[], 
                policy=CgroupPolicy.standard()
            )
            interp.start() # Deno 프로세스를 즉시 구동 (Cold Start 선행)
            _py_pool.put(interp) # 큐가 꽉 차면 Blocking. 워커가 꺼내가면 즉시 재생성.
        except Exception:
            time.sleep(1) # 오류 발생 시 스로틀링

def _start_pool_if_needed():
    global _pool_started
    if not _pool_started:
        with _pool_lock:
            if not _pool_started:
                threading.Thread(target=_replenish_worker, daemon=True, name="PyReplenisher").start()
                _pool_started = True


# =====================================================================
# [Execution Strategies]
# =====================================================================

def validate_intent_checkpoint(payload: dict, exec_data: Any, context: dict, job_id: str, core_wasm_path: Path, log) -> Any:
    try:
        with WasmInterpreter(str(core_wasm_path), policy=CgroupPolicy.system()) as wasm_gate:
            validation_res = wasm_gate.invoke(DphiMethod.VALIDATE_INTENT, json.dumps(payload), context=context)
            if not validation_res.success:
                if "not registered" in str(validation_res.error) or "not found" in str(validation_res.error):
                    log.debug(f"[{job_id[:8]}] '{DphiMethod.VALIDATE_INTENT}' missing in Core WASM. Bypassing checkpoint.")
                    return exec_data
                else:
                    log.error(f"[{job_id[:8]}] WASM Gateway crashed: {validation_res.error}")
                    return {"success": False, "output": "", "error": f"Gateway Fault: {validation_res.error}"}
            
            val_data = json.loads(validation_res.output)
            if not val_data.get("is_valid", True):
                error_code = val_data.get('error_code', 'UNAUTHORIZED_INTENT')
                log.warning(f"[{job_id[:8]}] 🔒 Checkpoint Denied: {error_code}")
                return {"success": False, "output": "", "error": f"Security Policy Violation: {error_code}"}
            
            return val_data.get("safe_payload", exec_data)
    except Exception as e:
        log.error(f"[{job_id[:8]}] Validation checkpoint error: {e}")
        return {"success": False, "output": "", "error": f"Checkpoint Error: {e}"}


def run_dvm_sandbox(target_path: Path, job_policy: CgroupPolicy, safe_payload: Any, context: dict, job_id: str, log) -> dict:
    try:
        with DvmInterpreter(wasm_module_name=target_path.name, policy=job_policy) as dvm_sandbox:
            safe_dict = safe_payload if isinstance(safe_payload, dict) else {}
            if isinstance(safe_payload, str):
                with suppress(Exception):
                    safe_dict = json.loads(safe_payload)
            
            vm_target = safe_dict.get("vm_target", "EVM")
            log.info(f"[{job_id[:8]}] 🔓 Entering Multi-VM Jail: {target_path.name} (Tier: {job_policy.tier.value}, Target: {vm_target})")
            result = dvm_sandbox.execute(
                vm_target=vm_target,
                target_address=safe_dict.get("target_address", ""),
                calldata=safe_dict.get("calldata", ""),
                state_snapshot=safe_dict.get("state_snapshot", {}),
                context=context
            )
            
            metrics = dvm_sandbox.get_metrics()
            if result.success:
                with suppress(Exception):
                    out_dict = json.loads(result.output)
                    metrics["gas_used"] = out_dict.get("gas_used", 0)
                    
            log.info(f"[{job_id[:8]}] 📊 EVM Sandbox Metrics: {metrics}")
            return {
                "success": result.success, "output": result.output, 
                "error": str(result.error) if not result.success else "", "metrics": metrics
            }
    except Exception as e:
        log.error(f"[{job_id[:8]}] DVM Execution crashed: {e}", exc_info=True)
        return {"success": False, "output": "", "error": f"Execution Error: {e}"}


def run_python_sandbox(job_policy: CgroupPolicy, safe_payload: Any, context: dict, job_id: str, log) -> dict:
    _start_pool_if_needed()
    
    py_sandbox = None
    is_fallback = False
    
    # [시간 측정 시작]
    t0 = time.perf_counter()
    
    try:
        try:
            # 1. 큐 대기 (풀에서 예열된 인스턴스 획득)
            py_sandbox = _py_pool.get(timeout=0.5)
            t_wait_ms = (time.perf_counter() - t0) * 1000
            t_spawn_ms = 0.0
        except queue.Empty:
            # 2. 풀 고갈 시 On-demand 생성 (Cold Start)
            t_spawn_start = time.perf_counter()
            log.warning(f"[{job_id[:8]}] Pre-warm pool empty. Spawning on-demand (Spike Detected).")
            py_sandbox = PythonInterpreter(
                enable_network_access=[], enable_read_paths=[], 
                enable_write_paths=[], enable_env_vars=[], policy=job_policy
            )
            py_sandbox.start()
            t_spawn_ms = (time.perf_counter() - t_spawn_start) * 1000
            t_wait_ms = (time.perf_counter() - t0) * 1000
            is_fallback = True
            
        # 런타임 정책 적용 (Pool에서 가져온 경우 객체의 정책을 덮어씀)
        if not is_fallback and hasattr(py_sandbox, 'apply_policy'):
            py_sandbox.apply_policy(job_policy)

        code_to_run, variables = "", {}
        if isinstance(safe_payload, str):
            code_to_run = safe_payload
        elif isinstance(safe_payload, dict):
            code_to_run = safe_payload.get("code", safe_payload.get("data", ""))
            variables = safe_payload.get("variables", {})

        log.info(f"[{job_id[:8]}] 🔓 Entering Python Legacy Jail (Tier: {job_policy.tier.value}, Cached: {not is_fallback})")
        
        # 3. 순수 코드 실행 시간 측정 (IPC + Deno Execution)
        t_exec_start = time.perf_counter()
        result = py_sandbox.execute(code=code_to_run, variables=variables, callables={}, context=context)
        t_exec_ms = (time.perf_counter() - t_exec_start) * 1000
        
        metrics = py_sandbox.get_metrics()
        
        # [시각적 지표 추가] 타이밍 메트릭 삽입
        metrics["timing_ms"] = {
            "wait": round(t_wait_ms, 2),
            "spawn": round(t_spawn_ms, 2),
            "exec": round(t_exec_ms, 2)
        }
        
        total_ms = t_wait_ms + t_exec_ms
        status_icon = "⚡(Cached)" if not is_fallback else "🐌(ColdStart)"
        log.info(f"[{job_id[:8]}] ⏱️ [Perf] {status_icon} Wait: {t_wait_ms:.2f}ms | Spawn: {t_spawn_ms:.2f}ms | Exec: {t_exec_ms:.2f}ms | Total: {total_ms:.2f}ms")
        log.info(f"[{job_id[:8]}] 📊 Sandbox Metrics: {metrics}")
        
        return {
            "success": result.success, "output": result.output,
            "error": str(result.error) if not result.success else "", "metrics": metrics
        }
    except Exception as e:
        error_msg = str(e)
        # [테스트 크래시 방어] 타임아웃은 정상적인 샌드박스 차단 기작이므로 WARNING으로 격하
        if "Execution Timeout" in error_msg:
            log.warning(f"[{job_id[:8]}] Sandbox Execution Timeout (Infinite loop/Deadlock defended).")
            return {"success": False, "output": "", "error": error_msg}
            
        log.error(f"[{job_id[:8]}] Python Execution crashed: {e}", exc_info=True)
        return {"success": False, "output": "", "error": f"Execution Error: {error_msg}"}
    finally:
        # [Dispose] 메모리 누수 및 상태 오염 방지를 위해 1회 사용 후 무조건 파기
        if py_sandbox:
            with suppress(Exception):
                py_sandbox.shutdown()


def run_pure_wasm(target_path: Path, target_func: str, job_policy: CgroupPolicy, exec_data: Any, context: dict, job_id: str, log) -> dict:
    """순수 WASM Core Kernel 비즈니스 로직 실행"""
    try:
        with WasmInterpreter(str(target_path), policy=job_policy) as wasm_runner:
            log.debug(f"[{job_id[:8]}] Bypassing Jail. Direct WASM Kernel logic: {target_func} via {target_path.name}")
            exec_data_str = json.dumps(exec_data) if isinstance(exec_data, dict) else str(exec_data)
            
            result = wasm_runner.invoke(target_func, exec_data_str, context=context)
            
            metrics = wasm_runner.get_metrics()
            log.info(f"[{job_id[:8]}] 📊 WASM Metrics: {metrics}")
            
            return {
                "success": result.success, "output": result.output if result.success else "",
                "error": str(result.error) if not result.success else "", "metrics": metrics
            }
    except Exception as e:
        log.error(f"[{job_id[:8]}] WASM Kernel logic crashed: {e}", exc_info=True)
        return {"success": False, "output": "", "error": str(e)}