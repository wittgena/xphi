# watcher.kernel.wasm.auditor
## @lineage: watcher.auditor.wasm
## @lineage: ops.watcher.tracer.wasm.auditor
import asyncio
import struct
from typing import Union, Any

from watcher.tracer.bound import BaseStreamAuditor, BaseBoundary
from watcher.plane.emitter import get_emitter

class WasmTelemetryAuditor(BaseStreamAuditor):
    def __init__(self, interpreter, boundary: Union[BaseBoundary, Any]):
        super().__init__(target="wasm_memory", boundary=boundary, delay=0)
        self.interpreter = interpreter
        self.log = get_emitter("auditor.wasm_telemetry", phase="agent")
        
        self.max_depth = 0
        self.health_flag = 0
        self.is_collapsed = False
        self.poll_interval = 0.1

    async def run_stream(self) -> None:
        while not self.is_collapsed:
            try:
                store = getattr(self.interpreter, 'store', None)
                instance = getattr(self.interpreter, 'instance', None)
                if not store or not instance:
                    await asyncio.sleep(self.poll_interval)
                    continue

                get_telemetry = instance.exports(store).get("get_telemetry_ptr")
                memory = instance.exports(store).get("memory")
                
                if get_telemetry and memory:
                    ptr = get_telemetry(store)
                    raw_data = memory.read(store, ptr, ptr + 12)
                    recursion_depth, max_type_depth, health_flag = struct.unpack("<III", raw_data)
                    
                    self.max_depth = max_type_depth
                    self.health_flag = health_flag
                    
                    if health_flag == 2:  # 2 = Collapsed
                        self.is_collapsed = True
                        self.log.crit(f"  [FATAL] WASM Semantic Collapse Confirmed! (Depth: {max_type_depth})")
                        break
            except Exception:
                pass
            await asyncio.sleep(self.poll_interval)

class WasmEntropyAuditor(BaseStreamAuditor):
    """@desc: [Energy Axis] Wasmtime 엔진의 Fuel(가스) 소모량을 실시간 관측"""
    def __init__(self, interpreter, boundary: Union[BaseBoundary, Any]):
        super().__init__(target="wasm_fuel", boundary=boundary, delay=0)
        self.interpreter = interpreter
        self.log = get_emitter("auditor.wasm_entropy", phase="agent")
        self.total_fuel_used = 0
        self.is_exhausted = False
        self.poll_interval = 0.5

    async def run_stream(self) -> None:
        while not self.is_exhausted:
            try:
                store = getattr(self.interpreter, 'store', None)
                if store:
                    fuel_consumed = store.fuel_consumed()
                    if fuel_consumed is not None:
                        self.total_fuel_used = fuel_consumed
                        cg_policy = getattr(getattr(self.interpreter, 'cg', None), 'policy', None)
                        fuel_limit = cg_policy.fuel_limit if cg_policy else 10_000_000
                        
                        if fuel_consumed >= fuel_limit:
                            self.is_exhausted = True
                            self.log.crit(f"  [ENTROPY FATAL] Fuel Exhausted: {fuel_consumed}/{fuel_limit}")
                            break
            except Exception:
                pass
            await asyncio.sleep(self.poll_interval)