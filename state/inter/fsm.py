# xphi.state.inter.fsm
import json
import threading
import os
from pathlib import Path
from typing import Any, Dict, Union, Optional
from contextlib import suppress

try:
    import wasmtime
except ImportError:
    wasmtime = None

from xphi.arch.contract.interpreter import ExecutionError, ExecutionResult
from xphi.node.space.bind.resolver import resolve_path
from xphi.node.runtime.wasm.quota import WasmQuotaManager, QuotaPolicy, Tier

from xphi.state.inter.wasm import WasmInterpreter
from xphi.watcher.plane.emitter import get_emitter

TIME_ROOT = resolve_path("time")
log = get_emitter("inter.fsm", phase="SYSTEM")

_GLOBAL_ENGINE = None
_GLOBAL_MODULE_CACHE = {}
_CACHE_LOCK = threading.Lock()


def get_cached_module(wasm_path: str, quota_manager: WasmQuotaManager):
    global _GLOBAL_ENGINE, _GLOBAL_MODULE_CACHE
    
    with _CACHE_LOCK:
        if _GLOBAL_ENGINE is None:
            config = wasmtime.Config()
            quota_manager.apply_to_config(config)
            _GLOBAL_ENGINE = wasmtime.Engine(config)
            
        if wasm_path not in _GLOBAL_MODULE_CACHE:
            log.info(f"⚙️ [AOT Compile] Compiling {Path(wasm_path).name} (Only once per process)...")
            if not os.path.exists(wasm_path):
                raise FileNotFoundError(f"WASM Artifact not found: {wasm_path}")
            _GLOBAL_MODULE_CACHE[wasm_path] = wasmtime.Module.from_file(_GLOBAL_ENGINE, wasm_path)
            log.info(f"✅ [AOT Compile] {Path(wasm_path).name} cached successfully.")
            
        return _GLOBAL_ENGINE, _GLOBAL_MODULE_CACHE[wasm_path]


class FSMInterpreter:
    def __init__(
        self,
        wasm_module_name: str = "fsm.wasm",
        policy: Optional[QuotaPolicy] = None,
    ) -> None:
        if wasmtime is None:
            raise ImportError("The 'wasmtime' package is required. Please install it.")
            
        self.wasm_module_path = str(Path(TIME_ROOT) / wasm_module_name)
        self.policy = policy or QuotaPolicy.standard()
        self._execution_count = 0

        # Wasmtime Core
        self.engine = None
        self.store = None
        self.module = None
        self.instance = None
        self.memory = None
        
        # Wasm Exports (ABI)
        self._wasm_alloc = None
        self._wasm_dealloc = None
        self._wasm_execute_router = None
        self.cg = WasmQuotaManager(policy_name=f"fsm-vm-worker-{id(self)}", policy=self.policy)
        
        self._ensure_engine_started()

    def _ensure_engine_started(self) -> None:
        if self.instance is not None:
            return
             
        try:
            self.engine, self.module = get_cached_module(self.wasm_module_path, self.cg)
            
            wasi_config = wasmtime.WasiConfig()
            wasi_config.inherit_stdout()
            wasi_config.inherit_stderr()
            
            self.store = wasmtime.Store(self.engine)
            self.store.set_wasi(wasi_config)
            self.cg.apply_to_store(self.store)
            
            linker = wasmtime.Linker(self.engine)
            linker.define_wasi()

            # Host Escape Hatch: Cross-VM Bridge (fsm.wasm -> phase.wasm)
            def invoke_native_vm_callback(input_ptr: int) -> int:
                result_bytes = self._read_c_string(input_ptr)
                json_str = result_bytes.decode('utf-8')
                native_result = {"success": False, "revert_reason": "Unknown Host Bridge Target"}
                
                try:
                    payload = json.loads(json_str)
                    vm_target = payload.get("vm_target", "UNKNOWN").upper()
                    if vm_target == "PHASE":
                        log.info("[Host Bridge] Cross-VM Call: fsm.wasm -> phase.wasm")
                        dphi_method = payload.get("method", "evaluate_tension")
                        dphi_context = payload.get("context", {"injected_anchor": 1, "injected_tick": 0})
                        dphi_payload = payload.get("payload", {})
                        
                        phase_wasm_path = str(Path(TIME_ROOT) / "phase.wasm")
                        with WasmInterpreter(phase_wasm_path, policy=QuotaPolicy.system()) as dphi_kernel:
                            res = dphi_kernel.invoke(dphi_method, json.dumps(dphi_payload), context=dphi_context)
                            
                            if res.success:
                                residue = json.loads(res.output)
                                native_result = {
                                    "success": residue.get("success", False),
                                    "gas_used": int(residue.get("gas_used", 0)),
                                    "output": residue.get("data", "{}"),
                                    "revert_reason": residue.get("error")
                                }
                            else:
                                native_result = {"success": False, "revert_reason": f"XPHI Kernel Panic: {res.error}"}
                except Exception as e:
                    log.error(f"Cross-VM Execution Failed: {e}", exc_info=True)
                    native_result = {"success": False, "revert_reason": f"Host Bridge Error: {str(e)}"}
                    
                res_bytes = json.dumps(native_result).encode('utf-8') + b'\x00'
                res_ptr = self._wasm_alloc(self.store, len(res_bytes))
                self.memory.write(self.store, res_bytes, res_ptr)
                return res_ptr

            func_type = wasmtime.FuncType([wasmtime.ValType.i32()], [wasmtime.ValType.i32()])
            linker.define_func("env", "invoke_native_vm", func_type, invoke_native_vm_callback)

            self.instance = linker.instantiate(self.store, self.module)
            self.memory = self.instance.exports(self.store)["memory"]
            
            exports = self.instance.exports(self.store)
            self._wasm_alloc = exports.get("alloc")
            self._wasm_dealloc = exports.get("dealloc")
            self._wasm_execute_router = exports.get("execute_router")
            
            if not all([self._wasm_alloc, self._wasm_dealloc, self._wasm_execute_router]):
                raise ExecutionError("fsm.wasm missing required exports: 'alloc', 'dealloc', or 'execute_router'")
        except Exception as e:
            raise ExecutionError(f"Failed to initialize fsm.wasm engine: {e}")

    def _read_c_string(self, ptr: int, chunk_size: int = 256) -> bytearray:
        result_bytes = bytearray()
        curr_ptr = ptr
        while True:
            chunk = self.memory.read(self.store, curr_ptr, curr_ptr + chunk_size)
            null_idx = chunk.find(b'\x00')
            if null_idx != -1:
                result_bytes.extend(chunk[:null_idx])
                break
            result_bytes.extend(chunk)
            curr_ptr += chunk_size
        return result_bytes

    @staticmethod
    def _resolve_fsm_target(vm_target: str, calldata: Any) -> str:
        target = (vm_target or "").upper()
        if target in ("FSM_COMPILE", "FSM_STEP", "FSM_SANDBOX", "HOST_BRIDGE"):
            return target
        if target == "COMPILE":
            return "FSM_COMPILE"
        if target == "STEP":
            return "FSM_STEP"
        if isinstance(calldata, dict):
            if "_m" in calldata and "g" in calldata:
                return "FSM_COMPILE"
            if "op" in calldata and "curr_node" in calldata:
                return "FSM_STEP"
        return "FSM_STEP"

    def execute(
        self,
        vm_target: str,
        target_address: str,
        calldata: Union[Dict[str, Any], str],
        state_snapshot: Union[Dict[str, Any], str],
        context: Union[Dict[str, Any], str, None] = None
    ) -> ExecutionResult:
        """strategy.py의 run_fsm_sandbox 호출 시그니처와 100% 동일한 엔트리포인트"""
        self._execution_count += 1
        
        # 0. Type Sanitization (str -> dict 자동 변환)
        if isinstance(calldata, str) and calldata.strip():
            with suppress(Exception):
                calldata = json.loads(calldata)
        calldata = calldata if isinstance(calldata, dict) else {}

        if isinstance(state_snapshot, str) and state_snapshot.strip():
            with suppress(Exception):
                state_snapshot = json.loads(state_snapshot)
        state_snapshot = state_snapshot if isinstance(state_snapshot, dict) else {}

        if isinstance(context, str) and context.strip():
            with suppress(Exception):
                context = json.loads(context)
        context = context if isinstance(context, dict) else {}

        # 1. Fuel (gas_limit) 결정: context["fuel"]이 있으면 최우선 적용, 없으면 QuotaPolicy 기본값 사용
        default_quota = self.policy.cpu_fuel_quota if self.policy.tier == Tier.STANDARD else 30_000_000
        fuel_limit = int(context.get("fuel", calldata.get("fuel", default_quota)))

        # 2. Rust 라우터 타겟 결정 ("EVM" 기본값 유입 시에도 안전하게 FSM 타겟으로 변환)
        resolved_target = self._resolve_fsm_target(vm_target, calldata)

        inner_payload = {
            "target_address": target_address or context.get("stream_id", "root"),
            "calldata": calldata,
            "gas_limit": fuel_limit,
            "state_snapshot": state_snapshot,
        }

        unified_input = {
            "vm_target": resolved_target,
            "payload": inner_payload,
        }
        
        log.info(f"[fsm.wasm] Routing {resolved_target} on '{target_address}' (Fuel Limit: {fuel_limit})")
        
        try:
            self._ensure_engine_started()
            
            payload_bytes = json.dumps(unified_input).encode('utf-8') + b'\x00'
            req_len = len(payload_bytes)
            
            code_ptr = self._wasm_alloc(self.store, req_len)
            self.memory.write(self.store, payload_bytes, code_ptr)
            res_ptr = 0
            res_len = 0
            
            try:
                res_ptr = self._wasm_execute_router(self.store, code_ptr)
                if res_ptr == 0:
                    raise ExecutionError("WASM Execution returned null pointer.")
                    
                result_bytes = self._read_c_string(res_ptr)
                res_len = len(result_bytes) + 1
                result_str = result_bytes.decode('utf-8')
                
            finally:
                if code_ptr:
                    self._wasm_dealloc(self.store, code_ptr, req_len)
                if res_ptr and res_len > 0:
                    with suppress(Exception):
                        self._wasm_dealloc(self.store, res_ptr, res_len)
                    
            result_data = json.loads(result_str)
            success = bool(result_data.get("success", False))
            gas_used = int(result_data.get("gas_used", 0))  # strategy.py 호환을 위해 int 유지
            output_data = result_data.get("output", "{}")
            revert_reason = result_data.get("revert_reason")
            
            response_payload = {
                "success": success,
                "gas_used": gas_used,
                "state_root": result_data.get("state_root", "0x0"),
                "output": output_data,
                "logs": result_data.get("logs", []),
                "state_diff": result_data.get("state_diff", {}),
                "revert_reason": revert_reason,
            }
            
            if success:
                return ExecutionResult(success=True, output=json.dumps(response_payload))
            else:
                return ExecutionResult(
                    success=False,
                    output=json.dumps(response_payload),
                    error=ExecutionError(revert_reason or "FSM Execution Reverted"),
                )
            
        except Exception as e:
            return ExecutionResult(success=False, error=ExecutionError(f"fsm Fatal: {str(e)}"))

    def get_metrics(self) -> dict:
        if not self.store or not self.memory:
            return {}
        return self.cg.inspect_metrics(self.store, self.memory)

    def shutdown(self) -> None:
        self.engine = None
        self.store = None
        self.module = None
        self.instance = None
        self.memory = None
        
        self._wasm_alloc = None
        self._wasm_dealloc = None
        self._wasm_execute_router = None

    def __enter__(self) -> "FSMInterpreter":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown()