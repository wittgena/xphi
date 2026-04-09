# bridge.pir
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

log = get_emitter("interface.pir")

## resonance (Ψ behavior)
class CarrierType(str, Enum):
    """resonance (Ψ behavior)"""
    RECURSIVE = "recursive"      # self-referential resonance
    MODULATORY = "modulatory"    # indirect / delayed resonance
    FIXED = "fixed"              # constrained / invariant resonance
    DIFFUSE = "diffuse"          # distributed resonance

## Φ Field
class PhaseField(str, Enum):
    LOCAL = "local"
    COHERENT = "coherent"
    INTERFERENCE = "interference"
    EVALUATION = "evaluation"

## Ψ Carrier
@dataclass(frozen=True)
class PsiCarrier:
    kind: str
    tag: str
    payload: Any
    carrier_type: Optional[CarrierType] = CarrierType.FIXED
    target_field: Optional[PhaseField] = PhaseField.LOCAL

    def symbol(self) -> str:
        """boundary routing symbol (∂Φ input)"""
        return f"{self.kind}:{self.tag}"

@dataclass
class PsiEvent:
    """minimal unit of phase resonance"""
    event_id: str  
    parent_id: Optional[str]
    source_id: str
    scope: str
    tick: int
    
    carrier: PsiCarrier
    context: Dict[str, Any] = field(default_factory=dict)

    @property
    def payload(self) -> str:
        """기존 시스템과의 호환성을 위해 property로 제공"""
        return self.carrier.payload

    @property
    def event_type(self) -> str:
        """기존 시스템과의 호환성을 위해 property로 제공"""
        return self.carrier.kind
        
    @property
    def symbol(self) -> str:
        """라우팅 심볼 위임"""
        return self.carrier.symbol()

    @property
    def tag(self) -> str:
        """tag 위임"""
        return self.carrier.tag

    @property
    def kind(self) -> str:
        """kind 위임"""
        return self.carrier.kind

PsiType = Union[PsiCarrier, PsiEvent]
