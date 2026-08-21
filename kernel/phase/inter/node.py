# kernel.phase.inter.node
## @lineage: kernel.bind.inter.node
import json
import __future__
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Tuple, FrozenSet, Optional, Any, Union
from enum import Enum

from arch.contract.event.next import LogEvent
from arch.contract.event.psi import PsiCarrier, PhaseField
from watcher.plane.emitter import get_emitter
from kernel.dphi.broker import DphiBroker, DphiMethod

log = get_emitter("inter.node")

class PhaseAction(str, Enum):
    SPAWN = "RESONANCE:SPAWN"
    DROP = "INTERFERENCE:DROP"
    FIELD_MISMATCH = "INTERFERENCE:FIELD_MISMATCH"

@dataclass(frozen=True)
class PhaseJudgment:
    """@contract: NodeInterpreter가 판단한 결과의 불변 구조체"""
    psi_symbol: str
    action: PhaseAction
    phase: str
    version: int
    is_resonance: bool

@dataclass(frozen=True)
class AnchoredIR:
    version: int
    recept_boundaries: FrozenSet[str]

class AnchorFlow:
    @staticmethod
    def bootstrap(recepts: Optional[FrozenSet[str]] = None) -> AnchoredIR:
        if not recepts:
            recepts = frozenset({"system:signal", "system:ping"})
        log.trace(f"[bootstrap] Constructing boundary with recepts: {recepts}")
        return AnchoredIR(
            version=1,
            recept_boundaries=recepts
        )

    @staticmethod
    def revise(anchor: AnchoredIR, new_recept: str) -> AnchoredIR:
        log.trace(f"[δ] revise → expanding boundary for {new_recept}")
        new_boundaries = frozenset(anchor.recept_boundaries | {new_recept})
        return AnchoredIR(
            version=anchor.version + 1,
            recept_boundaries=new_boundaries
        )

class NodeInterpreter:
    def __init__(self, broker: DphiBroker, anchor: AnchoredIR, field: PhaseField = PhaseField.COHERENT):
        self.broker = broker
        self.anchor = anchor
        self.current_field = field
        self._current_phase = "PHASE_IDLE"

    @property
    def phase(self) -> str:
        return self._current_phase

    async def process(self, carrier: PsiCarrier, context: Optional[dict] = None) -> PhaseJudgment:
        phase_root = {
            "kind": "ANCHOR",
            "name": "anchor_root",
            "content": json.dumps(list(self.anchor.recept_boundaries))
        }
        
        evolution_ctx = {
            "phase_root": phase_root,
            "external_rules": []
        }

        payload = {
            "evolution_ctx": evolution_ctx,
            "intent_action": "EVALUATE_CARRIER", 
            "intent_payload": {
                "tag": carrier.tag,
                "symbol": carrier.symbol,
                "field": self.current_field.value
            }
        }
        result = await self.broker.invoke(
            target_func=DphiMethod.EXECUTE_TRANSITION,
            payload=json.dumps(payload),
            context=context
        )
        if not result.success:
            log.error(f"[NodeInterpreter] WASM Transition Failed: {result.error}")
            return PhaseJudgment(
                psi_symbol=carrier.symbol,
                action=PhaseAction.DROP,
                phase="PHASE_ERROR",
                version=self.anchor.version,
                is_resonance=False
            )

        try:
            trans_result = json.loads(result.output)
            is_authorized = trans_result.get("is_authorized", False)
            residues = trans_result.get("all_residues") or []

            if is_authorized:
                action = PhaseAction.SPAWN
                self._current_phase = f"PHASE_ACTIVE::{carrier.symbol}"
            else:
                action = PhaseAction.DROP
                self._current_phase = "PHASE_IDLE"

            # 잔여물(Residue)을 통해 오류/충돌 여부를 세밀하게 판단
            for residue in residues:
                if residue.get("kind") == "ERROR":
                    action = PhaseAction.FIELD_MISMATCH
                    self._current_phase = "PHASE_ERROR"

            return PhaseJudgment(
                psi_symbol=carrier.symbol,
                action=action,
                phase=self._current_phase,
                version=self.anchor.version,
                is_resonance=is_authorized
            )
            
        except json.JSONDecodeError as e:
            log.error(f"[NodeInterpreter] Invalid TransitionResult payload: {e}")
            return PhaseJudgment(
                psi_symbol=carrier.symbol,
                action=PhaseAction.DROP,
                phase="PHASE_ERROR",
                version=self.anchor.version,
                is_resonance=False
            )