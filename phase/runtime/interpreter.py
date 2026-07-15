# phase.runtime.interpreter
import __future__
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Tuple, FrozenSet, Optional, Any, Union
from enum import Enum
from arch.contract.event.next import LogEvent
from watcher.plane.emitter import get_emitter
from arch.contract.event.psi import PsiCarrier, PhaseField

log = get_emitter("node.interpreter")

class PhaseAction(str, Enum):
    SPAWN = "RESONANCE:SPAWN"
    DROP = "INTERFERENCE:DROP"
    FIELD_MISMATCH = "INTERFERENCE:FIELD_MISMATCH"

@dataclass(frozen=True)
class PhaseJudgment:
    """@contract: NodeInterpreter가 판단한 결과의 불변 구조체 (dict 반환 대체)"""
    psi_symbol: str
    action: PhaseAction
    phase: str
    version: int
    is_resonance: bool

@dataclass(frozen=True)
class AnchoredIR:
    """Φ⁺ (Anchored Structure)"""
    version: int
    recept_boundaries: FrozenSet[str]
    resonance_map: Dict[str, PhaseAction] # 매직 스트링 대신 Enum 타입 사용

class AnchorFlow:
    @staticmethod
    def bootstrap(recepts: Optional[FrozenSet[str]] = None) -> AnchoredIR:
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
        log.trace(f"[δ] revise → expanding boundary for {new_recept}")
        new_boundaries = frozenset(anchor.recept_boundaries | {new_recept})
        new_resonance_map = AnchorFlow._synthesize_resonance(new_boundaries)
        
        return AnchoredIR(
            version=anchor.version + 1,
            recept_boundaries=new_boundaries,
            resonance_map=new_resonance_map
        )

    @staticmethod
    def _synthesize_resonance(boundaries: FrozenSet[str]) -> Dict[str, PhaseAction]:
        resonance_map: Dict[str, PhaseAction] = {}
        for bound in boundaries:
            resonance_map[bound] = PhaseAction.SPAWN
            
        resonance_map["UNKNOWN"] = PhaseAction.DROP
        return resonance_map

class NodeInterpreter:
    def __init__(self, anchor: AnchoredIR, field: PhaseField = PhaseField.COHERENT):
        self.anchor = anchor
        self.current_field = field
        self._current_phase = "PHASE_IDLE"

    @property
    def phase(self) -> str:
        return self._current_phase

    def _resolve_symbol(self, tag: str) -> str:
        boundaries = getattr(self.anchor, 'recept_boundaries', [])
        for bound in boundaries:
            if tag.startswith(bound):
                return bound
        return "UNKNOWN"

    def process(self, carrier: PsiCarrier) -> PhaseJudgment:
        symbol = self._resolve_symbol(carrier.tag)
        res_map = getattr(self.anchor, 'resonance_map', {})
        action = res_map.get(symbol, PhaseAction.SPAWN if symbol != "UNKNOWN" else PhaseAction.DROP)
        is_resonance = (action == PhaseAction.SPAWN)
        self._current_phase = f"PHASE_ACTIVE::{symbol}" if is_resonance else "PHASE_IDLE"
        return PhaseJudgment(
            psi_symbol=carrier.symbol,
            action=action,
            phase=self.phase,
            version=getattr(self.anchor, 'version', 0),
            is_resonance=is_resonance
        )