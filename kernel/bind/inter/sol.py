# kernel.bind.inter.sol
"""@desc: Local interpreter for secure Solidity/EVM execution using Wasmtime and drevm.wasm"""
import json
import threading
import os
from pathlib import Path
from typing import Any, Dict, Union

try:
    import wasmtime
except ImportError:
    wasmtime = None

from kernel.bind.inter.protocol import ExecutionError, ExecutionResult
from kernel.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter
from kernel.dphi.cgroup import WasmCgroup, CgroupPolicy, Tier

TIME_ROOT = resolve_path("time")
log = get_emitter("inter.sol", phase="SYSTEM")

class SolInterpreter:
    def __init__(
        self,
        wasm_module_name: str = "drevm.wasm",
        policy: CgroupPolicy | None = None,
    ) -> None:
        if wasmtime is None:
            raise ImportError("The 'wasmtime' package is required. Please install it.")
            
        self.wasm_module_path = str(Path(TIME_ROOT) / wasm_module_name)
        self.policy = policy or CgroupPolicy.standard()
        self._owner_thread: int | None = None
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
        self._wasm_execute_evm = None
        
        self.cg = WasmCgroup(cgroup_name=f"evm-worker-{id(self)}", policy=self.policy)
        
        self._ensure_engine_started()

    def _check_thread_ownership(self) -> None:
        current_thread = threading.current_thread().ident
        if self._owner_thread is None:
            self._owner_thread = current_thread
        elif self._owner_thread != current_thread:
            raise RuntimeError("SolInterpreter is not thread-safe. Instantiate per thread.")

    def _ensure_engine_started(self) -> None:
        if self.instance is not None:
             return
             
        try:
            config = wasmtime.Config()
            self.cg.apply_to_config(config)
            
            self.engine = wasmtime.Engine(config)
            
            if not os.path.exists(self.wasm_module_path):
                raise FileNotFoundError(f"WASM Artifact not found: {self.wasm_module_path}")
                
            self.module = wasmtime.Module.from_file(self.engine, self.wasm_module_path)
            
            wasi_config = wasmtime.WasiConfig()
            wasi_config.inherit_stdout()
            wasi_config.inherit_stderr()
            
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
            self._wasm_execute_evm = exports.get("execute_evm")
            
            if not all([self._wasm_alloc, self._wasm_dealloc, self._wasm_execute_evm]):
                raise ExecutionError("drevm.wasm missing required exports: 'alloc', 'dealloc', or 'execute_evm'")

        except Exception as e:
            raise ExecutionError(f"Failed to initialize drevm.wasm engine: {e}")

    def execute(
        self,
        target_address: str,
        calldata: str,
        state_snapshot: Union[Dict[str, Any], str],
        context: Union[Dict[str, Any], str, None] = None
    ) -> ExecutionResult:
        self._check_thread_ownership()
        self._execution_count += 1
        
        # 0. Type Sanitization (상태 스냅샷 및 컨텍스트 파싱)
        if isinstance(state_snapshot, str):
            try: state_snapshot = json.loads(state_snapshot)
            except: state_snapshot = {}
        state_snapshot = state_snapshot or {}

        if isinstance(context, str):
            try: context = json.loads(context)
            except: context = {}
        context = context or {}

        # 1. 컨텍스트에서 트랜잭션 및 블록 환경 변수 동적 추출
        caller = context.get("caller") or context.get("caller_address")
        value = str(context.get("value", "0"))
        
        block_info = context.get("block", {})
        block_context = None
        if block_info:
            block_context = {
                "timestamp": int(block_info.get("timestamp", 0)),
                "block_number": int(block_info.get("block_number", 0)),
                "coinbase": block_info.get("coinbase"),
                "chain_id": int(block_info.get("chain_id", 1)) if block_info.get("chain_id") else None
            }

        # 2. Payload Assembly (drevm.wasm 이 요구하는 확장된 EvmInput 구조체 형태)
        gas_limit = self.policy.cpu_fuel_quota if self.policy.tier == Tier.STANDARD else 30_000_000
        
        evm_input = {
            "target_address": target_address,
            "calldata": calldata,
            "gas_limit": gas_limit,
            "state_snapshot": state_snapshot
        }

        # WASM 측의 Option<T> 역직렬화를 위해 값이 있을 때만 필드 추가
        if caller:
            evm_input["caller_address"] = caller
        if value and value != "0":
            evm_input["value"] = value
        if block_context:
            evm_input["block_context"] = block_context
        
        log.info(f"[drevm.wasm] Executing TX on {target_address} (Gas Limit: {gas_limit})")
        
        try:
            self._ensure_engine_started()
            
            # 3. Rust CStr 포맷을 맞추기 위해 JSON 문자열 끝에 널 바이트('\x00') 추가
            payload_bytes = json.dumps(evm_input).encode('utf-8') + b'\x00'
            req_len = len(payload_bytes)
            
            # 4. WASM Memory 에 할당(Alloc) 후 기록(Write)
            code_ptr = self._wasm_alloc(self.store, req_len)
            self.memory.write(self.store, payload_bytes, code_ptr)
            
            try:
                # 5. EVM 실행 (순수 Rust 엔진 연산)
                res_ptr = self._wasm_execute_evm(self.store, code_ptr)
                
                if res_ptr == 0:
                    raise ExecutionError("WASM EVM Execution returned null pointer.")
                    
                # 6. WASM 메모리에서 널 바이트('\x00')를 만날 때까지 결과 읽어오기 (C-String Read)
                chunk_size = 256
                result_bytes = bytearray()
                curr_ptr = res_ptr
                
                while True:
                    chunk = self.memory.read(self.store, curr_ptr, curr_ptr + chunk_size)
                    null_idx = chunk.find(b'\x00')
                    if null_idx != -1:
                        result_bytes.extend(chunk[:null_idx])
                        break
                    result_bytes.extend(chunk)
                    curr_ptr += chunk_size
                    
                result_str = result_bytes.decode('utf-8')
                
            finally:
                # 7. 파이썬이 할당했던 입력 메모리 공간 해제
                if code_ptr is not None:
                    self._wasm_dealloc(self.store, code_ptr, req_len)
                    
            # 8. drevm.wasm 의 결과(EvmOutput) 파싱 및 표준화
            result_data = json.loads(result_str)
            success = result_data.get("success", False)
            gas_used = str(result_data.get("gas_used", 0))
            output_hex = result_data.get("output", "0x")
            revert_reason = result_data.get("revert_reason")

            response_payload = {
                "success": success,
                "gas_used": gas_used,
                "state_root": "0x0000000000000000000000000000000000000000000000000000000000000000",
                "output": output_hex,
                "logs": [], 
                "revert_reason": revert_reason
            }

            return ExecutionResult(success=success, output=json.dumps(response_payload))

        except Exception as e:
            return ExecutionResult(success=False, error=ExecutionError(f"drevm Fatal: {str(e)}"))

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
        self._wasm_execute_evm = None
        self._owner_thread = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.shutdown()