# arch.executor.dynamics
from __future__ import annotations
import asyncio
from arch.contract.event.next import next_id, next_phase_id, parse_id, parse_phase_id 
from typing import List, Dict, Optional, Any
from arch.executor.base import BaseExecutor
from arch.contract.registry import registry

class PhaseField(type(BaseExecutor)):
    """@phase.bound: 클래스 생성 시점에 고유한 Snowflake ID 부여"""
    def __new__(mcs, name, bases, namespace):
        # 4글자 해시 대신 Snowflake ID를 사용하여 글로벌 고유성 확보
        namespace['bound_id'] = f"bound.{next_id()}"
        return super().__new__(mcs, name, bases, namespace)

class XeCont(BaseExecutor, metaclass=PhaseField):
    """
    @entity: autonomous phase carrier
    @flow: Snowflake로 선후관계를, PhaseId로 상태 벡터를 기록
    """
    def __init__(self, bound, ex: str = "void", origin: str = "void"):
        super().__init__()
        ## trace_id를 Snowflake로 변경하여 시간순 정렬 가능하게 함
        self.trace_id = next_id() 
        self.phase_id = 0 # 현재 위상 상태 저장
        self.ex = ex
        self.origin = origin
        self.bound = bound

    async def execute(self, psi: Any) -> List[Any]:
        ## step.1: Ψ → ∂Φ (흡수 및 위상 업데이트)
        batch_payload = [{"payload": psi.symbol}]
        self.bound.absorb(batch_payload)
        
        # 현재 bound의 물리량으로부터 Phase ID 갱신
        self.phase_id = next_phase_id(
            topo=int(getattr(self.bound, 'topology', 0)), 
            press=int(getattr(self.bound, 'pressure', 0))
        )

        ## step.2: τ evaluation
        decision = self.bound.evaluate()
        
        if decision == "DEPOSIT":
            # Rupture(단절) 발생 시 Phase ID의 Epoch를 전환하여 계보를 분리
            self.phase_id = next_phase_id(
                topo=int(self.bound.topology), 
                press=int(self.bound.pressure), 
                rupture=True
            )
            
            self.bound.commit()
            ext_base = self._ext__()
            
            ## 로그 출력 시에도 정렬 가능한 ID 사용
            print(f"\n[Rupture] {self.trace_id} (Phase:{hex(self.phase_id)}) -> {ext_base.trace_id}")
            self.ex = ext_base.ex
            self.origin = ext_base.trace_id
        else:
            # Saturation(포화) 상태 로그
            pass

        ## step.3: ID 주입 (psi 이벤트에 현재의 Snowflake와 Phase 정보를 바인딩)
        psi.event_id = next_id()
        psi.phase_id = self.phase_id
        return [psi]

    def _ext__(self) -> 'XeCont':
        return XeCont(
            bound=self.bound,
            ex=f"Base.bind(inversion.overflow.{self.ex})",
            origin=self.trace_id
        )

class LoopCarrier(BaseExecutor):
    """
    @role: self-driven Ψ loop generator
    @flow: 각 Tick마다 Snowflake ID를 새로 생성하여 정밀한 순서 보장
    """
    def __init__(self, xe: XeCont, max_ticks: int = 100, interval: float = 0.1):
        super().__init__()
        self.xe = xe
        self.tick = 0
        self.max_ticks = max_ticks
        self.interval = interval

    async def execute(self, psi: Any) -> List[Any]:
        out = []
        ## 현재 틱(psi) 처리 (위상 장 흡수 및 평가)
        xe_out = await self.xe.execute(psi)
        out.extend(xe_out) ## 현재 결과는 Actuator로 보내서 Surface 업데이트

        ## 다음 틱(Tick) 생성 및 재귀적 발행
        if self.tick < self.max_ticks:
            await asyncio.sleep(self.interval)
            next_psi = psi.__class__(
                event_id=next_id(), 
                parent_id=getattr(psi, "event_id", None),
                source_id="loop.carrier",
                scope=getattr(psi, "scope", "GLOBAL"),
                carrier=getattr(psi, "carrier", None),
                phase_id=getattr(self.xe, 'phase_id', 0),
                tick=self.tick + 1,
                context=getattr(psi, "context", {}).copy()
            )
            
            ## 핵심 해결책: 리턴(out.append)하지 않고, Node의 Bus로 직접 밀어넣어 재순환시킴
            if hasattr(self, "node") and self.node:
                await self.node.bus.publish(next_psi)
            else:
                ## 폴백: 노드가 바인딩되지 않았다면 기존처럼 리턴 (테스트용)
                out.append(next_psi) 
            self.tick += 1
        return out

class SystemBuilder:
    """
    @role: Declarative JSON Config -> Topological Bound Assembly
    @flow: contract.registry의 create_component를 활용하여 객체를 동적 생성 및 바인딩
    """
    @classmethod
    def build(cls, config: Dict[str, Any]) -> Any:
        ## Registry를 통한 컴포넌트 자동 인스턴스화
        ## config에 있는 "type"과 "params"는 create_component가 자동으로 추출/병합해줍니다.
        kernel = registry.create_component("kernel", config.get("kernel", {}))
        field = registry.create_component("field", config.get("field", {}))
        watcher = registry.create_component("watcher", config.get("watcher", {}))
        regime = registry.create_component("regime", config.get("regime", {}))
        
        ## Ator(노드 에이전트) 목록 생성
        ators = []
        for ator_cfg in config.get("ators", []):
            ## config 내의 params 외에 id, initial_state 등 1뎁스 변수들을 kwargs로 주입
            ator = registry.create_component(
                "ator", 
                ator_cfg, 
                node_id=ator_cfg.get("id"),
                initial_state=ator_cfg.get("initial_state")
            )
            ators.append(ator)

        ## 위상 공간(Topos Field)에 생성된 컴포넌트들을 바인딩 (의존성 주입)
        field.bind_kernel(kernel)
        field.bind_ators(ators)
        field.bind_watcher(watcher)
        field.bind_regime(regime)

        ## 3. XeCont가 다룰 수 있는 통합 Bound 객체(field) 반환
        return field

class DynamicsExecutor(BaseExecutor):
    """
    @role: System Boundary와 Phase Carrier(XeCont) 사이의 파사드 (Facade)
    @flow: 조립된 Bound를 바탕으로 시간/위상 축(XeCont)을 래핑하여 실행 제어
    """
    def __init__(self, config_dict: Dict[str, Any]):
        super().__init__()
        self.config_dict = config_dict
        
        ## SystemBuilder로 선언적 시스템 완벽 조립
        self.bound = SystemBuilder.build(self.config_dict)
        
        ## XeCont (상태 벡터 및 위상/스노우플레이크 캐리어) 래핑 및 Bound 주입
        self._xe = XeCont(bound=self.bound, ex="dynamics.init", origin="system.boot")

    @property
    def phase_id(self) -> int:
        """LoopCarrier가 다음 이벤트를 생성할 때 참조하는 인과적 상태 해시 노출"""
        return getattr(self._xe, 'phase_id', 0)

    async def execute(self, psi: Any) -> List[Any]:
        """@flow: 외부의 LoopCarrier로부터 틱(Tick)을 받아 내부 _xe로 전달 (Psi 흡수 및 전이)"""
        return await self._xe.execute(psi)