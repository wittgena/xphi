# kernel.phase.runtime.executor.dynamics
"""@desc: System Boundary Facade and Declarative Assembly Engine"""
from __future__ import annotations
import json
import asyncio
from typing import List, Dict, Optional, Any

from xphi.arch.contract.event.psi import PsiEvent
from xphi.arch.contract.event.next import next_id, next_phase_id, parse_id, parse_phase_id 
from xphi.arch.contract.executor import BaseExecutor
from xphi.arch.contract.registry.unified import registry
from xphi.kernel.phase.runtime.flow.cont import XeCont

class SystemBuilder:
    """
    @role: Declarative JSON Config -> Topological Bound Assembly
    @flow: Utilizes the unified registry to dynamically instantiate and bind components.
    """
    @classmethod
    def build(cls, config: Dict[str, Any]) -> Any:
        kernel = registry.create_component(config.get("kernel", {}))
        field = registry.create_component(config.get("field", {}))
        watcher = registry.create_component(config.get("watcher", {}))
        regime = registry.create_component(config.get("regime", {}))
        
        ators = []
        for ator_cfg in config.get("ators", []):
            ator = registry.create_component(
                ator_cfg, 
                ator_id=ator_cfg.get("id"),
                state=ator_cfg.get("initial_state", "NORMAL")
            )
            ators.append(ator)

        # 조립된 컴포넌트들을 Field(네트워크 토폴로지)에 바인딩
        field.bind_kernel(kernel)
        field.bind_ators(ators)
        field.bind_watcher(watcher)
        field.bind_regime(regime)

        return field


class DynamicsExecutor(BaseExecutor):
    """
    @role: Facade between the System Boundary and Phase Carrier (XeCont)
    @flow: Wraps the assembled bound with a temporal/topological carrier for execution control
    """
    def __init__(self, config_dict: Dict[str, Any]):
        super().__init__()
        self.config_dict = config_dict
        
        # SystemBuilder를 통해 선언적 시스템 완벽 조립
        self.bound = SystemBuilder.build(self.config_dict)
        
        # XeCont (상태 벡터 및 위상/스노우플레이크 캐리어) 래핑 및 Bound 주입
        self._xe = XeCont(bound=self.bound, ex="dynamics.init", origin="system.boot")

    @property
    def phase_id(self) -> int:
        """LoopCarrier가 다음 이벤트를 생성할 때 참조하는 인과적 상태 해시(Phase Hash)"""
        return getattr(self._xe, 'phase_id', 0)

    @property
    def states(self) -> Dict[str, Any]:
        """
        @flow: 외부 관찰자(Treg Circuit Breaker 등)가 내부 Field(bound)의 
        개별 노드 상태를 실시간으로 읽을 수 있도록 허용
        """
        # bound(Field 객체)에 states가 존재하면 반환, 혹시 모를 XeCont 래핑 구조를 위한 fallback 포함
        if hasattr(self.bound, 'states'):
            return self.bound.states
        return getattr(self._xe, 'states', {})

    async def execute(self, psi: Any) -> List[Any]:
        """@flow: 외부의 LoopCarrier로부터 틱(Tick)을 받아 내부 _xe로 전달 (Psi 흡수 및 전이)"""
        return await self._xe.execute(psi)