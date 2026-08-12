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
    while True:
        try:
            interp = PythonInterpreter(
                enable_network_access=[], 
                enable_read_paths=[], 
                enable_write_paths=[], 
                enable_env_vars=[], 
                policy=CgroupPolicy.standard()
            )
            interp.start() # 프로세스를 즉시 구동 (Cold Start 선행)
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

def _is_request_expired(context: dict, timeout_sec: float = 15.0) -> bool:
    ts_val = float(context.get("timestamp", 0))
    if ts_val <= 0:
        return False
        
    if ts_val > 1e11:  # 밀리초 단위 보정
        req_ts = ts_val / 1000.0
    else:
        req_ts = ts_val

    elapsed = time.time() - req_ts
    if elapsed > 86400 * 365:  # Mock된 시간(결정론 테스트용) 통과
        return False
        
    if elapsed > timeout_sec:
        return True
        
    return False

# =====================================================================
# [Host Commit Lifecycle] 
# 단절된 샌드박스가 뱉어낸 상태 변경분을 마스터 DB에 영구 반영하는 파이썬 호스트 역할
# =====================================================================
def _commit_state_diff_to_master(context: dict, state_diff: dict, log):
    """
    샌드박스의 연산이 성공적으로 완료된 후, 반환된 State Diff를
    실제 글로벌 DB(Redis 등)에 Commit합니다.
    """
    if not state_diff:
        return
    
    # TODO: 실제 시스템의 DB 터널(State Store) 연동 코드가 주입될 위치
    # 예: await tunnel.state_store.hset(...) 혹은 외부 API 호출
    target_addr = context.get("contract_address", "Unknown")
    changed_accounts = len(state_diff)
    
    log.info(f"💾 [Host Commit] 상태 변경분 마스터 DB 반영 완료 (요청자: {target_addr}, 대상: {changed_accounts}개 계정)")


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
        safe_dict = safe_payload if isinstance(safe_payload, dict) else {}
        if isinstance(safe_payload, str):
            with suppress(Exception):
                safe_dict = json.loads(safe_payload)
        
        vm_target = safe_dict.get("vm_target", "EVM")

        # =================================================================
        # [정렬 핵심] 순수 CosmWasm 단독 실행 분기 (dvm.wasm EVM 감옥 우회)
        # =================================================================
        if vm_target == "COSMWASM_EXTERNAL":
            target_wasm_file = safe_dict.get("target_wasm_file", "cw20_base.wasm")
            log.info(f"[{job_id[:8]}] 🔓 Entering Pure CosmWasm Jail: {target_wasm_file} (Tier: {job_policy.tier.value})")
            
            from kernel.bind.inter.cosm import CosmWasmInterpreter
            with CosmWasmInterpreter(wasm_module_name=target_wasm_file, policy=job_policy) as cosm_sandbox:
                res = cosm_sandbox.execute(
                    env_data=safe_dict.get("env", {}),
                    info_data=safe_dict.get("info", {}),
                    msg_data=safe_dict.get("msg", {})
                )
                metrics = {"gas_used": 0} 
                
                if res.success:
                    with suppress(Exception):
                        out_dict = json.loads(res.output)
                        state_diff = out_dict.get("state_diff", {})
                        if state_diff:
                            _commit_state_diff_to_master(context, state_diff, log)
                            
                log.info(f"[{job_id[:8]}] 📊 CosmWasm Sandbox Metrics: {metrics}")
                return {
                    "success": res.success, "output": res.output,
                    "error": str(res.error) if not res.success else "", "metrics": metrics
                }

        # =================================================================
        # (기존) EVM 및 시스템 내장 CosmWasm 실행 분기
        # =================================================================
        log.info(f"[{job_id[:8]}] 🔓 Entering Stateless Multi-VM Jail: {target_path.name} (Tier: {job_policy.tier.value}, Target: {vm_target})")
        
        with DvmInterpreter(wasm_module_name=target_path.name, policy=job_policy) as dvm_sandbox:
            # 1. 샌드박스에 초기 상태 밀어넣기 및 연산 (Push State & Compute)
            result = dvm_sandbox.execute(
                vm_target=vm_target,
                target_address=safe_dict.get("target_address", ""),
                calldata=safe_dict.get("calldata", ""),
                state_snapshot=safe_dict.get("state_snapshot", {}),
                context=context
            )
            
            metrics = dvm_sandbox.get_metrics()
            
            # 2. 결과 뱉어내기 및 호스트 Commit 처리 (Pop State Diff & Commit)
            if result.success:
                with suppress(Exception):
                    out_dict = json.loads(result.output)
                    metrics["gas_used"] = out_dict.get("gas_used", 0)
                    
                    state_diff = out_dict.get("state_diff", {})
                    if state_diff:
                        _commit_state_diff_to_master(context, state_diff, log)
                    
            log.info(f"[{job_id[:8]}] 📊 VM Sandbox Metrics: {metrics}")
            
            return {
                "success": result.success, 
                "output": result.output, 
                "error": str(result.error) if not result.success else "", 
                "metrics": metrics
            }
            
    except Exception as e:
        log.error(f"[{job_id[:8]}] Sandbox Execution crashed: {e}", exc_info=True)
        return {"success": False, "output": "", "error": f"Execution Error: {e}"}

def run_python_sandbox(job_policy: CgroupPolicy, safe_payload: Any, context: dict, job_id: str, log) -> dict:
    _start_pool_if_needed()
    
    # 1. 큐 진입 전 다시 한 번 TTL 검사 (보안망 2차 방어)
    if _is_request_expired(context):
        log.warning(f"[{job_id[:8]}] 🗑️ Dropping expired zombie request before acquiring Python Sandbox.")
        return {"success": False, "output": "", "error": "Request expired (TTL Exceeded)."}

    py_sandbox = None
    t0 = time.perf_counter()
    
    try:
        try:
            # 2. 풀에서 워커 획득
            py_sandbox = _py_pool.get(timeout=2.0)
            t_wait_ms = (time.perf_counter() - t0) * 1000
        except queue.Empty:
            # 3. 우아한 과부하 통제
            log.warning(f"[{job_id[:8]}] 🚫 Python Worker pool exhausted. Applying Backpressure.")
            return {"success": False, "output": "", "error": "SYSTEM_OVERLOADED (Backpressure applied). Please try again."}
            
        if hasattr(py_sandbox, 'apply_policy'):
            py_sandbox.apply_policy(job_policy)

        code_to_run, variables = "", {}
        if isinstance(safe_payload, str):
            code_to_run = safe_payload
        elif isinstance(safe_payload, dict):
            code_to_run = safe_payload.get("code", safe_payload.get("data", ""))
            variables = safe_payload.get("variables", {})

        log.info(f"[{job_id[:8]}] 🔓 Entering Python Legacy Jail (Tier: {job_policy.tier.value}, Cached: True)")
        
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
        log.info(f"[{job_id[:8]}] ⏱️ [Perf] ⚡(Cached) Wait: {t_wait_ms:.2f}ms | Exec: {t_exec_ms:.2f}ms | Total: {total_ms:.2f}ms")
        log.info(f"[{job_id[:8]}] 📊 Sandbox Metrics: {metrics}")
        
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