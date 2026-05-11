# topos.dynamics.executor
from __future__ import annotations
import asyncio
from phase.runtime.contract.event.next import next_id, next_phase_id, parse_id, parse_phase_id 
from typing import List, Dict, Optional, Any
from phase.executor.base import BaseExecutor
from phase.runtime.contract.registry.unified import registry
from topos.dynamics.xe import XeCont

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