# watcher.field.legacy.ator
from typing import Dict, Any
from contract.registry import contract
from resonance.interface import IPhaseAtor
from bridge.psi import PsiEvent

@contract.ator("phase.legacy.ator")
class PhaseLegacyAtor(IPhaseAtor):
    """
    @role: 위상 공간(Field)의 각 노드를 대리하는 에이전트
    """
    
    # 레지스트리(SystemBuilder)가 주입하는 파라미터(**kwargs)를 수용하도록 변경
    def __init__(self, **kwargs):
        # kwargs에서 필드 추출 (SystemBuilder에서 명시적으로 넘겨준 값들)
        self._id = kwargs.get("node_id", "unknown")
        self._initial_state = kwargs.get("initial_state", "NORMAL")
        self.reflector_boost = kwargs.get("reflector_boost", 0.0)
        
        # 내부 상태
        self._current_state = {"status": self._initial_state}

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> Dict[str, Any]:
        return self._current_state

    def set_state(self, new_state: str) -> None:
        self._current_state["status"] = new_state

    async def react(self, event: PsiEvent, field: Any, bus: Any) -> None:
        """이벤트(Psi)에 반응하여 위상 장(Field)이나 외부 버스에 개입하는 로직"""
        # 현재 노드의 상태(NORMAL vs REFLECTOR)에 따른 상호작용
        pass