# cognitive.flow.dynamics.executor
## @lineage: cognitive.dynamics.executor
## @lineage: topos.dynamics.executor
from __future__ import annotations
import json
import asyncio
from arch.model.event.psi import PsiEvent
from arch.model.event.next import next_id, next_phase_id, parse_id, parse_phase_id 
from typing import List, Dict, Optional, Any
from phase.bind.executor import BaseExecutor
from arch.flow.executor import _FlowExecutor
from arch.contract.registry.unified import registry
from arch.model.manifold.cont import XeCont

class SystemBuilder:
    """
    @role: Declarative JSON Config -> Topological Bound Assembly
    @flow: contract.registry의 create_component를 활용하여 객체를 동적 생성 및 바인딩
    """
    @classmethod
    def build(cls, config: Dict[str, Any]) -> Any:
        kernel = registry.create_component("kernel", config.get("kernel", {}))
        field = registry.create_component("field", config.get("field", {}))
        watcher = registry.create_component("watcher", config.get("watcher", {}))
        regime = registry.create_component("regime", config.get("regime", {}))
        
        ators = []
        for ator_cfg in config.get("ators", []):
            ator = registry.create_component(
                "ator", 
                ator_cfg, 
                node_id=ator_cfg.get("id"),
                initial_state=ator_cfg.get("initial_state")
            )
            ators.append(ator)

        field.bind_kernel(kernel)
        field.bind_ators(ators)
        field.bind_watcher(watcher)
        field.bind_regime(regime)

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

    @property
    def states(self) -> Dict[str, Any]:
        """@flow: 외부 관찰자(Treg Circuit Breaker 등)가 내부 Field(bound)의 개별 노드 Tension 상태를 실시간으로 읽을 수 있도록 허용"""
        ## bound(Field 객체)에 states가 존재하면 반환, 혹시 모를 XeCont 래핑 구조를 위한 fallback 포함
        if hasattr(self.bound, 'states'):
            return self.bound.states
        return getattr(self._xe, 'states', {})

    async def execute(self, psi: Any) -> List[Any]:
        """@flow: 외부의 LoopCarrier로부터 틱(Tick)을 받아 내부 _xe로 전달 (Psi 흡수 및 전이)"""
        return await self._xe.execute(psi)

# class DynamicsExecutor(_FlowExecutor):
#     def __init__(self, config_dict: dict):
#         self.config_dict = config_dict
#         # 1. 내부 엔진 조립 (기본 기능)
#         self.bound = SystemBuilder.build(self.config_dict)
#         self._xe = XeCont(bound=self.bound, ex="dynamics.init", origin="system.boot")
        
#         # 2. 부모(_FlowExecutor)의 기능을 활성화 (자신을 스트림 공급자로 등록)
#         super().__init__(flow_instance=self, completion_signal=asyncio.Event())

#     async def execute(self, psi: PsiEvent) -> list:
#         """
#         이 메서드가 '기존 방식'과 'Flow 방식'을 가르는 판별기입니다.
#         """
#         # Flow CLI에서 보낸 응답 채널이 있는가?
#         response_channel = psi.context.get("response_channel")
        
#         if response_channel:
#             # [Flow 모드] 부모 클래스의 execute를 실행하여 실시간 중계 시작
#             # 부모는 내부적으로 아래의 'stream()' 제너레이터를 호출합니다.
#             return await super().execute(psi) 
#         else:
#             # [레거시 모드] 기존 방식대로 1개 틱만 처리하고 리스트 반환
#             # LoopCarrier는 이 반환값을 받고 예전처럼 아무 일 없었다는 듯 돌아갑니다.
#             return await self._xe.execute(psi)

#     async def stream(self):
#         """
#         부모 클래스가 실시간 중계를 위해 내부적으로 호출하는 제너레이터입니다.
#         """
#         count = 0
#         max_ticks = self.config_dict.get("runtime", {}).get("max_ticks", 100)
        
#         while count < max_ticks:
#             count += 1
#             # 1틱 실행 (내부 엔진 직접 제어)
#             await self._xe.step_execute() 
            
#             # 부모가 Redis로 쏠 수 있게 데이터 공급
#             yield FlowEvent(
#                 phase="DYNAMICS_EVOLUTION",
#                 psi=type('obj', (object,), {'name': f"tick-{count}"}),
#                 phi=type('obj', (object,), {'name': f"vix-{self.bound.get_vix():.2f}"}),
#                 boundary=type('obj', (object,), {'name': self.bound.__class__.__name__}),
#                 surface=type('obj', (object,), {'name': "stable"}) if count == max_ticks else None
#             )

#     @property
#     def states(self):
#         return self.bound.states if hasattr(self.bound, 'states') else {}

#     @property
#     def phase_id(self):
#         return getattr(self._xe, 'phase_id', 0)