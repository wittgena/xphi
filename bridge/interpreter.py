# bridge.interpreter
"""
@desc: Phase Interference Reflection (PIR)

@overview:
- PIR defines a closed loop where Ψ carriers are evaluated against a bounded Φ structure
- Structure evolves only through constrained δ (anchoring)

@flow:
Ψ (carrier)
 → ∂Φ (domain boundary resolve)
 → Φ′ (phase transition evaluation)
 → {θ resonance | θ interference}
 → δ (optional structural anchoring)
 → Φ⁺ (anchored structure)
"""
import __future__
import time
from dataclasses import dataclass, field
from typing import Dict, Tuple, FrozenSet, Optional, Any, Union
from enum import Enum
from model.event import LogEvent
from plane.emitter import get_emitter
from bridge.pir import PsiCarrier, PhaseField

log = get_emitter("bridge.interpreter")

@dataclass(frozen=True)
class AnchoredIR:
    """Φ⁺ (Anchored Structure)"""
    version: int
    # prefixes: FrozenSet[str]
    # transitions: Dict[Tuple[str, str], str]
    recept_boundaries: FrozenSet[str]
    resonance_map: Dict[str, str]

class AnchorFlow:
    """@flow: bootstrap (계약 기반 경계 형성) → revise (동적 확장)"""
    
    @staticmethod
    def bootstrap(recepts: Optional[FrozenSet[str]] = None) -> AnchoredIR:
        """@desc: 노드의 Contract(recept)를 기반으로 초기 인식 경계를 생성"""
        ## 아무 계약도 주입되지 않으면, 최소한의 생존(제어) 시그널만 인식
        if not recepts:
            recepts = frozenset({"system:signal", "system:ping"})

        log.trace(f"[bootstrap] Constructing boundary with recepts: {recepts}")
        resonance_map = AnchorFlow._synthesize_resonance(recepts)
        return AnchoredIR(
            version=1,
            recept_boundaries=recepts,
            resonance_map=resonance_map
        )

    @staticmethod
    def revise(anchor: AnchoredIR, new_recept: str) -> AnchoredIR:
        """δ structural anchoring: 런타임 중 새로운 수신 계약이 발견되면 경계를 확장"""
        log.trace(f"[δ] revise → expanding boundary for {new_recept}")

        new_boundaries = set(anchor.recept_boundaries)
        new_boundaries.add(new_recept)
        frozen_boundaries = frozenset(new_boundaries)
        new_resonance_map = AnchorFlow._synthesize_resonance(frozen_boundaries)
        return AnchoredIR(
            version=anchor.version + 1,
            recept_boundaries=frozen_boundaries,
            resonance_map=new_resonance_map
        )

    @staticmethod
    def _synthesize_resonance(boundaries: FrozenSet[str]) -> Dict[str, str]:
        """유입된 심볼(Event source)을 처리 방식(Action)으로 매핑 - 이분법적 라우팅만 수행"""
        resonance_map: Dict[str, str] = {}
        for bound in boundaries:
            resonance_map[bound] = "RESONANCE:SPAWN"
        
        ## 경계에 없는 것은 모두 무시(Interference)
        resonance_map["UNKNOWN"] = "INTERFERENCE:DROP"
        return resonance_map

class PhaseInterpreter:
    """@bridge: Stateless Logic + Legacy Interface"""
    def __init__(self, anchor: AnchoredIR, field: PhaseField = PhaseField.COHERENT):
        self.anchor = anchor
        self.current_field = field
        self._last_action = "PHASE_IDLE"

    @property
    def phase(self) -> str:
        """레거시 로거(Logger)가 참조할 수 있도록 속성 제공"""
        return self._last_action

    def _resolve_symbol(self, tag: str) -> str:
        # 신규(recept_boundaries)와 구형(prefixes) 모두 지원하도록 브릿지
        boundaries = getattr(self.anchor, 'recept_boundaries', 
                             getattr(self.anchor, 'prefixes', []))
        for bound in boundaries:
            if tag.startswith(bound):
                return bound
        return "UNKNOWN"

    def process(self, carrier: PsiCarrier) -> dict:
        """@return: Dispatcher가 기대하는 딕셔너리 구조로 반환"""
        ## 필드 검증 (Gating)
        # if carrier.target_field != self.current_field:
        #     return {"action": "INTERFERENCE:FIELD_MISMATCH", "phase": self.phase}

        ## 경계 해석 (Resolve)
        symbol = self._resolve_symbol(carrier.tag)
        
        ## 공명 확인 (Resonance Check) - resonance_map이 없으면 기본적으로 SPAWN으로 간주하는 폴백(Fallback) 배치
        res_map = getattr(self.anchor, 'resonance_map', {})
        action = res_map.get(symbol, "RESONANCE:SPAWN" if symbol != "UNKNOWN" else "INTERFERENCE:DROP")
        
        ## 상태를 저장하는 대신, 마지막 액션을 기록 (로그용)
        self._last_action = f"PHASE_ACTIVE::{symbol}" if "RESONANCE" in action else "PHASE_IDLE"

        ## 레거시 시스템(Dispatcher/Actuator)이 이해할 수 있는 딕셔너리 반환
        return {
            "psi": carrier.symbol,
            "action": action,
            "phase": self.phase,
            "version": getattr(self.anchor, 'version', 0),
            "resonance": action.startswith("RESONANCE")
        }