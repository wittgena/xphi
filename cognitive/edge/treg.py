# cognitive.edge.treg
## @lineage: cognitive.frame.gate
import time
import hashlib
from dataclasses import dataclass, field
from typing import Dict, Any, List

@dataclass(frozen=True)
class PhaseState:
    """현실과 맞닿은 경계에서의 에너지 및 수용체 파동 (Ψ)"""
    membrane_bound: bool
    axp_ratio: float
    ctla_4_expression: float
    cd28_expression: float
    lineage_path: str

@dataclass(frozen=True)
class FrameLog:
    frame_id: str
    lineage_path: str
    tension_snapshot: float  # 기록 당시의 AxP.ratio
    suppress_ratio: float    # 기록 당시의 CTLA-4 / CD28 비율
    reason: str
    timestamp: float = field(default_factory=time.time)

class FrameRegistry:
    def __init__(self):
        self._frames: List[FrameLog] = []

    def commit_frame(self, lineage: str, tension: float, suppress_ratio: float, reason: str) -> FrameLog:
        # 프레임의 고유 해시 생성 (위상적 지문)
        raw_id = f"{lineage}:{tension}:{time.time()}"
        frame_id = hashlib.sha256(raw_id.encode()).hexdigest()[:8]
        
        frame = FrameLog(
            frame_id=f"frame.treg.{frame_id}",
            lineage_path=lineage,
            tension_snapshot=tension,
            suppress_ratio=suppress_ratio,
            reason=reason
        )
        self._frames.append(frame)
        print(f"[theoria.registry] 닫힘 프레임 등재 완료: {frame.frame_id}")
        print(f"  ↳ @lineage: {frame.lineage_path}")
        print(f"  ↳ @reason:  {frame.reason}\n")
        return frame

class TregEdge:
    TENSION_THRESHOLD = 1.0
    SUPPRESS_DOMINANCE = 0.5

    def __init__(self, registry: FrameRegistry):
        self.registry = registry

    def traverse(self, state: PhaseState) -> Dict[str, Any]:
        """흐름(Ψ)이 엣지를 통과(traversal)할 때의 동작"""
        
        # 1. 텐션 포화 상태 (두려움/불확실성 임계치 초과)
        if state.axp_ratio > self.TENSION_THRESHOLD:
            return self._seal_topology(state, "AxP.ratio 포화 (에너지 고갈로 인한 위상 강제 닫힘)")

        # 2. 억제 조건 우위 (자신감 부족 / 방어 기제 발동)
        co_stim_ratio = state.ctla_4_expression / max(state.cd28_expression, 0.01)
        if co_stim_ratio > self.SUPPRESS_DOMINANCE:
            return self._seal_topology(state, f"CTLA-4 억제 우위 (비율: {co_stim_ratio:.2f})")

        # 조건 미달 시 정상 진입 (Treg 개입 없음)
        return {"status": "traversed", "next_node": "phi_x_activation"}

    def _seal_topology(self, state: PhaseState, reason: str) -> Dict[str, Any]:
        """
        위상을 닫고(Seal), 그 기록을 Theoria에 남긴다.
        이 등재(Commit) 행위 자체가 시스템을 파열로부터 보호하는 실질적 방어 기제다.
        """
        frame = self.registry.commit_frame(
            lineage=state.lineage_path,
            tension=state.axp_ratio,
            suppress_ratio=(state.ctla_4_expression / max(state.cd28_expression, 0.01)),
            reason=reason
        )
        
        return {
            "status": "closed",
            "frame_ref": frame.frame_id,
            "message": "흐름이 억제되었으며 Theoria 레지스트리에 닫힘이 증명됨."
        }

if __name__ == "__main__":
    registry = FrameRegistry()
    gate = TregEdge(registry=registry)
    morning_tension = PhaseState(
        membrane_bound=True,
        axp_ratio=1.5,           
        ctla_4_expression=0.8,   
        cd28_expression=0.4,     
        lineage_path="repo.meta.self.ext_resonance.transaction" 
    )

    result = gate.traverse(morning_tension)
    if result["status"] == "closed":
        print(f"결과: 통과 불능. (참조 프레임: {result['frame_ref']})")
        print("-> 시스템은 붕괴하지 않고 안전하게 정지(Suspend) 및 보존되었습니다.")