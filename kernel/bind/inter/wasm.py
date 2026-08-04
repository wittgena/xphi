# kernel.bind.inter.wasm
## @lineage: kernel.inter.wasm
## @lineage: kernel.dphi.wasm.inter.wasm
## @lineage: phase.wasm.inter.wasm
## @lineage: phase.runtime.inter.wasm
"""@desc: Local interpreter for secure Python code execution using Wasmtime/RustPython"""
import functools
import inspect
import json
import keyword
import os
import threading
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Mapping

try:
    import wasmtime
except ImportError:
    wasmtime = None

from kernel.bind.inter.protocol import PRIMITIVE_TYPES, ExecutionError, ProtocolError, ExecutionResult
from kernel.bind.resolver import find_current_self, get_invoker, resolve_path
from watcher.plane.emitter import get_emitter
from kernel.dphi.cgroup import WasmCgroup, CgroupPolicy

TIME_ROOT = resolve_path("time")
LARGE_VAR_THRESHOLD = 100 * 1024 * 1024

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

class WasmInterpreter:
    def __init__(
        self,
        wasm_module_path: str = "dphi.wasm",
        enable_read_paths: list[PathLike | str] | None = None,
        enable_write_paths: list[PathLike | str] | None = None,
        enable_env_vars: list[str] | None = None,
        sync_files: bool = True,
        policy: CgroupPolicy | None = None,
    ) -> None:
        if wasmtime is None:
            raise ImportError("The 'wasmtime' module is required. Please install it.")
        
        self.wasm_module_path = wasm_module_path
        self.enable_read_paths = enable_read_paths or []
        self.enable_write_paths = enable_write_paths or []
        self.enable_env_vars = enable_env_vars or []
        self.sync_files = sync_files

        self._request_id = 0
        self._owner_thread: int | None = None
        
        self.engine = None
        self.store = None
        self.module = None
        self.instance = None
        self.memory = None
        
        self._wasm_alloc = None
        self._wasm_dealloc = None
        self._wasm_invoke = None
        
        self._invoke_shared = None
        self._get_ptr = None
        self._get_size = None
        self.shared_ptr = None
        self.shared_size = 0
        
        self.valid_methods = set()
        cg_policy = policy or CgroupPolicy.standard()
        self.cg = WasmCgroup(cgroup_name=f"worker-{id(self)}", policy=cg_policy)
        
        self._ensure_engine_started()

    def _check_thread_ownership(self) -> None:
        current_thread = threading.current_thread().ident
        if self._owner_thread is None:
            self._owner_thread = current_thread
        elif self._owner_thread != current_thread:
            raise RuntimeError("WasmInterpreter is not thread-safe. Instantiate per thread.")

    def _ensure_engine_started(self) -> None:
        if self.instance is not None:
             return
             
        try:
            config = wasmtime.Config()
            self.cg.apply_to_config(config)
            
            self.engine = wasmtime.Engine(config)
            self.module = wasmtime.Module.from_file(self.engine, self.wasm_module_path)
            
            wasi_config = wasmtime.WasiConfig()
            wasi_config.inherit_stdout()
            wasi_config.inherit_stderr()
            
            for path in self.enable_read_paths:
                wasi_config.preopen_dir(str(path), f"/sandbox/read/{os.path.basename(str(path))}")
            for path in self.enable_write_paths:
                 wasi_config.preopen_dir(str(path), f"/sandbox/write/{os.path.basename(str(path))}")
            
            env = []
            for var in self.enable_env_vars:
                 if var in os.environ:
                     env.append((var, os.environ[var]))
            wasi_config.env = env
            
            self.store = wasmtime.Store(self.engine)
            self.store.set_wasi(wasi_config)
            
            self.cg.apply_to_store(self.store)
            
            linker = wasmtime.Linker(self.engine)
            linker.define_wasi()

            self.instance = linker.instantiate(self.store, self.module)
            self.memory = self.instance.exports(self.store)["memory"]
            
            exports = self.instance.exports(self.store)
            
            self._wasm_alloc = exports.get("alloc")
            self._wasm_dealloc = exports.get("dealloc")
            self._wasm_invoke = exports.get("invoke_wasm")
            
            if not self._wasm_alloc or not self._wasm_dealloc or not self._wasm_invoke:
                raise ProtocolError("WASM module missing legacy memory exports ('alloc', 'dealloc', 'invoke_wasm')")

            self._get_ptr = exports.get("get_shared_buffer_ptr")
            self._get_size = exports.get("get_shared_buffer_size")
            self._invoke_shared = exports.get("invoke_shared")
            
            if self._get_ptr and self._get_size and self._invoke_shared:
                self.shared_ptr = self._get_ptr(self.store)
                self.shared_size = self._get_size(self.store)
            else:
                log.warning("Shared Buffer APIs not found in WASM. Running in Legacy-only mode.")

            registry_path = Path(TIME_ROOT) / "registry.json"
            if registry_path.exists():
                try:
                    with open(registry_path, "r", encoding="utf-8") as f:
                        reg_data = json.load(f)
                        self.valid_methods = set(reg_data.get("methods", []))
                except Exception as e:
                    log.warning(f"Failed to load registry.json: {e}")

        except Exception as e:
            raise ProtocolError(f"Failed to initialize Wasmtime engine: {e}")

    def _to_json_compatible(self, value: Any) -> Any: ...
    def _serialize_value(self, value: Any) -> str: ...
    def _inject_variables(self, code: str, variables: Mapping[str, Any]) -> str: ...

    # [수정됨] context 파라미터 추가
    def _run_wasm_function(self, target_func_name: str, payload: Any, context: dict | None = None) -> str:
        self._ensure_engine_started()
        
        if self.valid_methods and target_func_name not in self.valid_methods:
            raise ExecutionError(f"Method '{target_func_name}' is not registered in Wasm API.")
        
        actual_payload = payload
        if isinstance(payload, str):
            try:
                actual_payload = json.loads(payload)
            except json.JSONDecodeError:
                pass

        context = context or {}
        if "timestamp" not in context:
            context["timestamp"] = 0
        if "seed" not in context:
            context["seed"] = "dphi_secure_fallback_seed"

        routed_request = {
            "method": target_func_name,
            "context": context,
            "payload": actual_payload
        }
        
        payload_bytes = json.dumps(routed_request).encode('utf-8')
        req_len = len(payload_bytes)
        if self.shared_ptr is not None and req_len <= self.shared_size:
            self.memory.write(self.store, payload_bytes, self.shared_ptr)
            res_len = self._invoke_shared(self.store, req_len)
            
            if res_len == 0:
                raise ExecutionError("WASM shared buffer execution failed (buffer overflow or critical error).")
                
            result_bytes = self.memory.read(self.store, self.shared_ptr, self.shared_ptr + res_len)
            return result_bytes.decode('utf-8')
            
        else:
            code_ptr = self._wasm_alloc(self.store, req_len)
            res_ptr = None
            res_len_legacy = None
            
            try:
                self.memory.write(self.store, payload_bytes, code_ptr)
                result_packed = self._wasm_invoke(self.store, code_ptr, req_len)
                
                res_ptr = result_packed >> 32
                res_len_legacy = result_packed & 0xFFFFFFFF
                result_bytes = self.memory.read(self.store, res_ptr, res_ptr + res_len_legacy)
                return result_bytes.decode('utf-8')
            finally:
                if code_ptr is not None:
                    self._wasm_dealloc(self.store, code_ptr, req_len)
                if res_ptr is not None and res_len_legacy is not None:
                    self._wasm_dealloc(self.store, res_ptr, res_len_legacy)

    # [수정됨] context 파라미터 연동
    def invoke(self, target_func: str, payload: str, context: dict | None = None) -> ExecutionResult:
        self._check_thread_ownership()
        try:
            result_str = self._run_wasm_function(target_func, payload, context=context)
            result_data = json.loads(result_str)
            
            if result_data.get("success", False):
                return ExecutionResult(
                    success=True, 
                    output=json.dumps(result_data.get("data", {}))
                )
            else:
                error_msg = result_data.get("error", "Unknown WASM Validation Failed")
                return ExecutionResult(success=False, error=ExecutionError(error_msg))
                
        except Exception as e:
            return ExecutionResult(success=False, error=ExecutionError(f"WASM Invoke Failed: {e}"))

    # [수정됨] context 파라미터 시그니처 추가 및 연동
    def execute(
        self,
        code: str,
        variables: Mapping[str, Any] | None = None,
        callables: Mapping[str, Callable[..., Any]] | None = None,
        context: dict | None = None,
    ) -> ExecutionResult:
        self._check_thread_ownership()
        variables = variables or {}
        
        try:
            injected_code = self._inject_variables(code, variables)
        except ExecutionError as e:
            return ExecutionResult(success=False, error=e)

        try:
             result_str = self._run_wasm_function("execute_code", injected_code, context=context)
             result_data = json.loads(result_str)
             
             if result_data.get("success", False):
                 inner_data = result_data.get("data", {})
                 actual_output = inner_data.get("output", "")
                 return ExecutionResult(success=True, output=actual_output)
             else:
                 error_msg = result_data.get("error", "Unknown execution error")
                 return ExecutionResult(success=False, error=ExecutionError(error_msg))
        except Exception as e:
             return ExecutionResult(success=False, error=ExecutionError(f"WASM Execution Failed: {e}"))

    def get_metrics(self) -> dict:
        if not self.store or not self.memory:
            return {}
        return self.cg.inspect_metrics(self.store, self.memory)

    def start(self) -> None:
        self._ensure_engine_started()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.shutdown()

    # [수정됨] context 파라미터 연동
    def __call__(
        self, 
        code: str, 
        variables=None, 
        callables=None, 
        context: dict | None = None
    ) -> ExecutionResult:
        return self.execute(code, variables, callables, context)

    def shutdown(self) -> None:
        self.engine = None
        self.store = None
        self.module = None
        self.instance = None
        self.memory = None
        
        self._wasm_alloc = None
        self._wasm_dealloc = None
        self._wasm_invoke = None
        self._invoke_shared = None
        self._get_ptr = None
        self._get_size = None
        self.shared_ptr = None
        self.shared_size = 0
        self.valid_methods.clear()
        self._owner_thread = None