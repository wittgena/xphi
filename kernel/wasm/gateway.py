# xphi.kernel.wasm.gateway
import json
import threading
import ctypes
import time
from typing import Any, Dict, List
from pathlib import Path

try:
    import wasmtime
except ImportError:
    wasmtime = None

from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("wasm.gateway")

class GatewayRuptureError(Exception):
    pass

class MemoryBoundaryError(GatewayRuptureError):
    pass

class GatewayWasm:
    _engine = None
    _module = None
    _compile_lock = threading.Lock()
    
    MAX_INPUT_SIZE = 1024 * 1024 
    
    def __init__(self, wasm_filename: str = "gateway.wasm"):
        if wasmtime is None:
            raise ImportError("wasmtime is strictly required.")
            
        self._tls = threading.local()
        self._initialize_module(wasm_filename)

    @classmethod
    def _initialize_module(cls, wasm_filename: str):
        """JIT/AOT compilation occurs exactly once during application bootstrap (Thread-Safe)."""
        with cls._compile_lock:
            if cls._engine is None:
                config = wasmtime.Config()
                cls._engine = wasmtime.Engine(config)
            
            if cls._module is None:
                wasm_path = str(resolve_path("time") / wasm_filename)
                if not Path(wasm_path).exists():
                    raise FileNotFoundError(f"Gateway WASM not found: {wasm_path}")
                cls._module = wasmtime.Module.from_file(cls._engine, wasm_path)

    def _get_local_instance(self) -> Any:
        """Instantiates and caches a WASM module instance per thread via TLS."""
        if not hasattr(self._tls, "is_initialized"):
            self._tls.store = wasmtime.Store(self._engine)
            linker = wasmtime.Linker(self._engine)
            
            self._tls.instance = linker.instantiate(self._tls.store, self._module)
            self._tls.memory = self._tls.instance.exports(self._tls.store)["memory"]
            
            exports = self._tls.instance.exports(self._tls.store)
            self._tls.alloc = exports.get("alloc")
            self._tls.execute_gateway = exports.get("execute_gateway")
            self._tls.dealloc_c_string = exports.get("dealloc_c_string") 
            
            self._tls.input_ptr = self._tls.alloc(self._tls.store, self.MAX_INPUT_SIZE)
            self._tls.is_initialized = True
            
        return self._tls

    def _read_c_string_fast(self, tls: Any, ptr: int) -> str:
        try:
            host_base_addr = ctypes.cast(tls.memory.data_ptr(tls.store), ctypes.c_void_p).value
            target_addr = host_base_addr + ptr
            raw_bytes = ctypes.string_at(target_addr)
            return raw_bytes.decode('utf-8', errors='replace')
        except Exception as e:
            raise GatewayRuptureError(f"C-String direct read failed: {e}")

    def invoke_raw_ffi(self, payload_str: str) -> str:
        payload_bytes = payload_str.encode('utf-8') + b'\0'
        req_len = len(payload_bytes)
        
        if req_len > self.MAX_INPUT_SIZE:
            raise MemoryBoundaryError(f"Payload exceeds boundary ({self.MAX_INPUT_SIZE}B).")

        tls = self._get_local_instance()
        res_ptr = None
        
        try:
            tls.memory.write(tls.store, payload_bytes, tls.input_ptr)
            res_ptr = tls.execute_gateway(tls.store, tls.input_ptr)
            if res_ptr == 0:
                raise GatewayRuptureError("Gateway Panic: Null pointer returned.")
            return self._read_c_string_fast(tls, res_ptr)
            
        finally:
            if res_ptr:
                try:
                    tls.dealloc_c_string(tls.store, res_ptr)
                except Exception as e:
                    log.error(f"❌ [Gateway] C-String deallocation failed: {e}")

    def evaluate_intent(self, dimension: int, base_friction: float, raw_payload: str, state_vector: List[int]) -> Dict[str, Any]:
        """Routes payload to the Computing Parser."""
        req_payload = {
            "target_module": "intent_parser",
            "dimension": dimension,
            "base_friction": base_friction,
            "raw_payload": raw_payload,
            "state_vector": state_vector
        }
        return self._safe_invoke(req_payload)

    def execute_transaction_fsm(self, fsm_state: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
        """Routes and evaluates the Transaction FSM state."""
        req_payload = {
            "target_module": "transaction_fsm",
            "fsm_state": fsm_state,
            "event": event
        }
        return self._safe_invoke(req_payload)

    def execute_clearing_fsm(self, fsm_state: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
        """Routes and evaluates the Clearing FSM state."""
        req_payload = {
            "target_module": "clearing_fsm",
            "fsm_state": fsm_state,
            "event": event
        }
        return self._safe_invoke(req_payload)

    def _safe_invoke(self, payload_dict: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # 1. Serialize payload and measure size
            json_str = json.dumps(payload_dict, separators=(',', ':'))
            payload_size_kb = len(json_str.encode('utf-8')) / 1024.0
            
            target_module = payload_dict.get("target_module", "unknown")
            log.info(f"⚡ [WASM Gateway] Invoking module '{target_module}' ({payload_size_kb:.2f} KB)")
            
            # 2. Measure execution time and invoke FFI
            start_time = time.perf_counter()
            res_str = self.invoke_raw_ffi(json_str)
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            
            # 3. Parse and log receipt
            receipt = json.loads(res_str)
            if receipt.get("success"):
                log.info(f"✅ [WASM Gateway] Execution successful: '{target_module}' ({elapsed_ms:.2f} ms)")
            else:
                log.warning(f"⚠️ [WASM Gateway] Execution reverted: '{target_module}' (Reason: {receipt.get('revert_reason')})")
                
            return receipt
            
        except MemoryBoundaryError as mbe:
            log.warning(f"🛡️ [Circuit Breaker Triggered] {mbe}")
            return {"success": False, "revert_reason": "Memory Boundary Exceeded"}
        except GatewayRuptureError as gre:
            log.warning(f"⚠️ [Gateway Rupture] {gre}")
            return {"success": False, "revert_reason": str(gre)}
        except Exception as e:
            log.error(f"❌ [Gateway Fatal] {e}")
            return {"success": False, "revert_reason": "Internal Execution Error"}