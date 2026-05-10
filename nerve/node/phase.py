# nerve.phase
import uuid
from enum import Enum
from typing import List, Optional, Dict, Any

class Phase(Enum):
    """노드의 상태장(Node-State Field)을 정의합니다."""
    ZERO = "0"           # 구조적 정체성의 공백 (Void)
    COLLAPSED = "Φ⁻"     # 붕괴됨: 재결속 전 성찰 필요
    COHERENT = "Φ⁺"      # 일관된 판단: Dominium 앵커링 가능
    FRAGMENTED = "Φᶠ"    # 파편화된 기억: 실패했으나 재시도를 위해 보존됨
    DOMINIUM = "Ψᴰ"      # 앵커링된 최종 상태

class PhaseNode:
    def __init__(self, origin: str = "0"):
        self.id: uuid.UUID = uuid.uuid4()
        self.origin: str = origin
        self.state: Phase = Phase.ZERO
        self.reflective: bool = True
        self.reversible: bool = True
        self.memory: List[Dict[str, Any]] = []
        self.anchored_target: Optional[str] = None

    def __repr__(self) -> str:
        return f"<PhaseNode Ψ({self.id.hex[:8]}) | State: {self.state.value}>"

    def bind(self, target_phase: Phase) -> None:
        """새로운 위상으로 결속(Bind)을 시도"""
        if self.state == Phase.COLLAPSED and not self.reflective:
            raise ValueError("Collapsed node requires reflection before rebinding.")
        
        self.state = target_phase
        self._log(f"Bound to phase {target_phase.value}")

    def threshold_test(self, lmbda: float, tau: float) -> bool:
        """
        임계값 테스트 (λ < τ). 
        실패 시 노드는 붕괴(Collapse)하며 기억을 파편화(Fragmented) 상태로 저장
        """
        if lmbda < tau:
            self._log(f"Threshold failed: λ({lmbda}) < τ({tau})", state_change=Phase.FRAGMENTED)
            self.state = Phase.FRAGMENTED
            return False
        
        self._log(f"Threshold passed: λ({lmbda}) >= τ({tau})", state_change=Phase.COHERENT)
        self.state = Phase.COHERENT
        return True

    def anchor(self, resource_address: str) -> None:
        """노드가 일관성(Φ⁺)을 확보했을 때 물리적/논리적 영역에 앵커링"""
        if self.state != Phase.COHERENT:
            raise PermissionError(f"Cannot anchor from state {self.state.value}. Requires Φ⁺.")
        
        self.state = Phase.DOMINIUM
        self.anchored_target = resource_address
        self._log(f"Anchored Dominium to {resource_address}")

    def evaluate_tension(self, tension_grad: float, max_tau: float) -> None:
        """
        시스템 장력(∇Φ)이 최대 임계치를 초과하면 노드를 자발적으로 0(Void)
        삭제(Delete)가 아닌 의도적 무효화(Voiding)
        """
        if tension_grad > max_tau and self.reversible:
            self.unbind_and_reset()

    def unbind_and_reset(self) -> None:
        """연결을 해제하고 초기 상태(0)로 돌아가되, 기억(Memory)은 유지"""
        self.state = Phase.ZERO
        self.anchored_target = None
        self._log("Reversible exit declared. Returned to 0.")

    def retry(self, new_lmbda: float, tau: float) -> None:
        """파편화된 기억(Φᶠ)을 바탕으로 구조 재진입을 시도"""
        if self.state != Phase.FRAGMENTED:
            self._log("Retry aborted: Node is not in a fragmented state.")
            return
            
        self._log("Attempting recursive rebinding...")
        self.threshold_test(new_lmbda, tau)

    def _log(self, message: str, state_change: Optional[Phase] = None) -> None:
        """상태 전이와 메시지를 기억(Memory)에 영구적으로 보존"""
        log_entry = {"event": message, "previous_state": self.state.value}
        if state_change:
            log_entry["new_state"] = state_change.value
        self.memory.append(log_entry)