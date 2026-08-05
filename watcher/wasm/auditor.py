# watcher.wasm.auditor
## @lineage: kernel.dphi.wasm.auditor
import asyncio
import struct
import json
from typing import Union, Any, Dict, List

from watcher.tracer.bound import BaseStreamAuditor, BaseBoundary
from watcher.plane.emitter import get_emitter
from arch.contract.event.next import next_id

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


class CanonicalProofAuditor(BaseStreamAuditor):
    """
    @desc: [Cryptographic Axis] Meta-Boundary(Tracer/Tester)로부터 직접 상태를 투영받아
           결정론적 상태 전이(Canonical Log)를 추출하고 연산 증명(Proof)을 생성합니다.
           (실행계의 로깅에 의존하는 자기 참조 모순을 제거한 순수 관측기)
    """
    def __init__(self, boundary: Union[BaseBoundary, Any] = None):
        super().__init__(target="canonical_proof", boundary=boundary, delay=0)
        self.log = get_emitter("auditor.canonical_proof", phase="agent")
        
        # [복원됨] WasmTester가 분산 컨텍스트를 열 때 사용할 고유 세션 ID
        # 로깅 인터셉터 용도가 아니라, 전체 테스트(Orchestration) 공간의 통일된 Trace ID 역할을 합니다.
        self.flow_id = next_id()
        
        self.canonical_records: List[Dict[str, Any]] = []
        
        # WasmTester 생명주기에 맞추기 위한 상태 플래그
        self.is_collapsed = False
        self.is_exhausted = False

    def project_state(self, action: str, metrics: dict) -> None:
        """
        @desc: Meta-Boundary(Tester)에서 실행계(Daemon)의 물리적 결과를 투영받습니다.
               로깅을 가로채지 않고 명시적인 객체 결합(Projection)을 통해 증명을 수집합니다.
        """
        if not metrics:
            return
            
        fuel = metrics.get("fuel_consumed")
        
        # 물리적 자원 소모(Fuel)가 기록된 유의미한 연산만 증명 대상에 포함
        if fuel is not None:
            record = {
                "action": action,
                "tier": metrics.get("tier", "UNKNOWN"),
                "fuel": fuel,
                "mem_usage": metrics.get("mem_usage_bytes"),
                "mem_peak": metrics.get("mem_peak_bytes")
            }
            # None 값 필터링 (결정론적 해싱을 위해 스키마를 정규화)
            clean_record = {k: v for k, v in record.items() if v is not None}
            self.canonical_records.append(clean_record)

    def generate_payload(self) -> str:
        """
        @desc: 추출된 이벤트들을 결정론적(Deterministic) JSON 구조로 직렬화합니다.
               이 페이로드의 Hash가 외부 스마트 컨트랙트에 제출될 state_root 역할을 합니다.
        """
        return json.dumps(self.canonical_records, sort_keys=True, separators=(',', ':'))

    async def run_stream(self) -> None:
        """
        @desc: 명시적인 Projection 호출 기반의 컴포넌트이므로 능동적인 Polling이 필요 없습니다.
               BaseStreamAuditor의 라이프사이클(Task) 유지를 위해 idle 상태로 대기합니다.
        """
        try:
            while not (self.is_collapsed or self.is_exhausted):
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        finally:
            self.canonical_records.clear()