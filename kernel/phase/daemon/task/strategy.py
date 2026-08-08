# kernel.phase.daemon.task.strategy
import json
from pathlib import Path
from typing import Any, Dict
from contextlib import suppress

from kernel.bind.inter.wasm import WasmInterpreter
from kernel.bind.inter.python import PythonInterpreter
from kernel.bind.inter.dvm import DvmInterpreter
from kernel.dphi.cgroup import CgroupPolicy
from kernel.dphi.method import DphiMethod  # [ADD] DphiMethod Enum 임포트

def validate_intent_checkpoint(payload: dict, exec_data: Any, context: dict, job_id: str, core_wasm_path: Path, log) -> Any:
    """Core dphi.wasm을 통한 인텐트 유효성 및 보안 검증"""
    try:
        with WasmInterpreter(str(core_wasm_path), policy=CgroupPolicy.system()) as wasm_gate:
            # [MODIFIED] 하드코딩된 "validate_intent" 문자열 대신 Enum 사용
            validation_res = wasm_gate.invoke(DphiMethod.VALIDATE_INTENT, json.dumps(payload), context=context)
            
            if not validation_res.success:
                if "not registered" in str(validation_res.error) or "not found" in str(validation_res.error):
                    # [MODIFIED] 로그 메시지에도 Enum 반영
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
    """Multi-VM (EVM/SVM) Rust 샌드박스 실행"""
    try:
        with DvmInterpreter(wasm_module_name=target_path.name, policy=job_policy) as dvm_sandbox:
            safe_dict = safe_payload if isinstance(safe_payload, dict) else {}
            if isinstance(safe_payload, str):
                with suppress(Exception):
                    safe_dict = json.loads(safe_payload)
            
            vm_target = safe_dict.get("vm_target", "EVM")
            log.info(f"[{job_id[:8]}] 🔓 Entering Multi-VM Jail: {target_path.name} (Tier: {job_policy.tier.value}, Target: {vm_target})")
            result = dvm_sandbox.execute(
                vm_target=vm_target, # 누락되었던 필수 위치 인자
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
                "success": result.success,
                "output": result.output,
                "error": str(result.error) if not result.success else "",
                "metrics": metrics
            }
    except Exception as e:
        log.error(f"[{job_id[:8]}] DVM Execution crashed: {e}", exc_info=True)
        return {"success": False, "output": "", "error": f"Execution Error: {e}"}

def run_python_sandbox(job_policy: CgroupPolicy, safe_payload: Any, context: dict, job_id: str, log) -> dict:
    """Python/Deno Legacy Jail 실행"""
    try:
        with PythonInterpreter(enable_network_access=None, policy=job_policy) as py_sandbox:
            code_to_run, variables = "", {}
            if isinstance(safe_payload, str):
                code_to_run = safe_payload
            elif isinstance(safe_payload, dict):
                code_to_run = safe_payload.get("code", safe_payload.get("data", ""))
                variables = safe_payload.get("variables", {})

            host_capabilities = {"system_ping": lambda: "pong_from_host"}
            
            log.info(f"[{job_id[:8]}] 🔓 Entering Python Legacy Jail (Tier: {job_policy.tier.value})")
            
            result = py_sandbox.execute(
                code=code_to_run, 
                variables=variables,
                callables=host_capabilities,
                context=context
            )
            
            metrics = py_sandbox.get_metrics()
            log.info(f"[{job_id[:8]}] 📊 Sandbox Metrics: {metrics}")
            
            return {
                "success": result.success,
                "output": result.output,
                "error": str(result.error) if not result.success else "",
                "metrics": metrics
            }
    except Exception as e:
        log.error(f"[{job_id[:8]}] Python Execution crashed: {e}", exc_info=True)
        return {"success": False, "output": "", "error": f"Execution Error: {e}"}

def run_pure_wasm(target_path: Path, target_func: str, job_policy: CgroupPolicy, exec_data: Any, context: dict, job_id: str, log) -> dict:
    """순수 WASM Core Kernel 비즈니스 로직 실행"""
    try:
        with WasmInterpreter(str(target_path), policy=job_policy) as wasm_runner:
            log.debug(f"[{job_id[:8]}] Bypassing Jail. Direct WASM Kernel logic: {target_func} via {target_path.name} (Tier: {job_policy.tier.value})")
            
            exec_data_str = json.dumps(exec_data) if isinstance(exec_data, dict) else str(exec_data)
                
            result = wasm_runner.invoke(target_func, exec_data_str, context=context)
            metrics = wasm_runner.get_metrics()
            
            log.info(f"[{job_id[:8]}] 📊 WASM Metrics: {metrics}")
            
            return {
                "success": result.success,
                "output": result.output if result.success else "",
                "error": str(result.error) if not result.success else "",
                "metrics": metrics
            }
    except Exception as e:
        log.error(f"[{job_id[:8]}] WASM Kernel logic crashed: {e}", exc_info=True)
        return {"success": False, "output": "", "error": str(e)}