# bridge.interface.pir
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
from bridge.interface.event import LogEvent
from bridge.plane.emitter import get_emitter

log = get_emitter("interface.pir")

@dataclass(frozen=True)
class PsiSignal:
    """Ψ (Signal - legacy flat carrier for boundary)"""
    kind: str
    tag: str
    payload: Any

    def symbol(self) -> str:
        """
        @role:
        boundary routing symbol (∂Φ input)
        """
        return f"{self.kind}:{self.tag}"

## resonance (Ψ behavior)
class CarrierType(str, Enum):
    """resonance (Ψ behavior)"""
    RECURSIVE = "recursive"      # self-referential resonance
    MODULATORY = "modulatory"    # indirect / delayed resonance
    FIXED = "fixed"              # constrained / invariant resonance
    DIFFUSE = "diffuse"          # distributed resonance


## Dynamical Axes
class TemporalScale(str, Enum):
    FAST = "fast"
    MEDIUM = "medium"
    SLOW = "slow"


class SpatialScope(str, Enum):
    LOCAL = "local"
    REGIONAL = "regional"
    GLOBAL = "global"

class Persistence(str, Enum):
    TRANSIENT = "transient"
    STABLE = "stable"
    LONG = "long"

## Φ Field
class PhaseField(str, Enum):
    LOCAL = "local"
    COHERENT = "coherent"
    INTERFERENCE = "interference"
    EVALUATION = "evaluation"

## Symbol Projection (non-structural)
CARRIER_SYMBOL = {
    CarrierType.RECURSIVE: "Ψˡ",
    CarrierType.MODULATORY: "Ψʰ",
    CarrierType.FIXED: "Ψᵃ",
    CarrierType.DIFFUSE: "Ψᶜ⁻",
}

FIELD_SYMBOL = {
    PhaseField.COHERENT: "Φᶜ",
    PhaseField.INTERFERENCE: "Φᶜ⁻",
    PhaseField.EVALUATION: "Φᵗ",
}

## Ψ Carrier
@dataclass(frozen=True)
class PsiCarrier(PsiSignal):
    carrier_type: Optional[CarrierType] = None
    target_field: Optional[PhaseField] = None
    temporal: Optional[TemporalScale] = None
    spatial: Optional[SpatialScope] = None
    persistence: Optional[Persistence] = None

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

PsiType = Union[PsiSignal, PsiCarrier, PsiEvent]

class PsiFactory:
    """# Factory (mapping lock)"""
    @staticmethod
    def create_recursive(kind: str, tag: str) -> PsiCarrier:
        return PsiCarrier(
            kind=kind,
            tag=tag,
            carrier_type=CarrierType.RECURSIVE,
            target_field=PhaseField.COHERENT,
            temporal=TemporalScale.FAST,
            spatial=SpatialScope.LOCAL,
            persistence=Persistence.STABLE,
        )

    @staticmethod
    def create_modulatory(
        kind: str,
        tag: str,
        payload = None,
        target: PhaseField = None,
    ) -> PsiCarrier:
        if target not in (PhaseField.LOCAL, PhaseField.INTERFERENCE):
            raise ValueError("MODULATORY must target LOCAL or DISTRIBUTED")

        return PsiCarrier(
            kind=kind,
            tag=tag,
            payload=payload,
            carrier_type=CarrierType.MODULATORY,
            target_field=target,
            temporal=TemporalScale.SLOW,
            spatial=SpatialScope.GLOBAL,
            persistence=Persistence.LONG,
        )

    @staticmethod
    def create_fixed(kind: str, tag: str, payload = None) -> PsiCarrier:
        return PsiCarrier(
            kind=kind,
            tag=tag,
            payload=payload,
            carrier_type=CarrierType.FIXED,
            target_field=PhaseField.LOCAL,
            temporal=TemporalScale.FAST,
            spatial=SpatialScope.LOCAL,
            persistence=Persistence.TRANSIENT,
        )

    @staticmethod
    def create_diffuse(kind: str, tag: str, payload = None) -> PsiCarrier:
        return PsiCarrier(
            kind=kind,
            tag=tag,
            carrier_type=CarrierType.DIFFUSE,
            target_field=PhaseField.INTERFERENCE,
            temporal=TemporalScale.MEDIUM,
            spatial=SpatialScope.GLOBAL,
            persistence=Persistence.LONG,
        )

@dataclass(frozen=True)
class PsiDomain:
    """∂Φ Boundary"""
    prefixes: FrozenSet[str]

    def resolve(self, carrier: PsiCarrier) -> str:
        """
        @phase:
        - ∂Φ: boundary classification

        @rule:
        - uses tag prefix only (no interpretation)
        """
        for prefix in self.prefixes:
            if carrier.tag.startswith(prefix):
                log.trace(f"[domain] resolve -> {prefix}")
                return prefix

        log.trace(f"[domain] UNKNOWN -> {carrier.tag}")
        return "UNKNOWN"


@dataclass(frozen=True)
class PhaseIR:
    """Φ′ (Phase Transition Kernel)"""
    transitions: Dict[Tuple[str, str], str]

    def next_phase(self, phase: str, symbol: str) -> str:
        """@phase: deterministic transition"""
        next_p = (
            self.transitions.get((phase, symbol))
            or self.transitions.get((phase, "UNKNOWN"))
            or phase
        )
        log.trace(f"[phase] {phase} --({symbol})--> {next_p}")
        return next_p

@dataclass(frozen=True)
class AnchoredIR:
    """Φ⁺ (Anchored Structure)"""
    version: int
    domain: PsiDomain
    ir: PhaseIR

class AnchorFlow:
    """@flow: bootstrap → synthesize → revise"""
    @staticmethod
    def bootstrap(initial_prefixes: Optional[FrozenSet[str]] = None) -> AnchoredIR:
        """@phase: initial Φ⁺ construction"""
        if initial_prefixes is None:
            initial_prefixes = frozenset({
                "psi:",
                "delta:",
                "execution:",
                "xor:",
                "loop:",
                "theoria:",
            })

        log.trace(f"[bootstrap] prefixes={initial_prefixes}")
        domain = PsiDomain(initial_prefixes)
        ir = AnchorFlow._synthesize(domain)
        anchor = AnchoredIR(
            version=1,
            domain=domain,
            ir=ir,
        )

        log.trace(f"[bootstrap] anchor version={anchor.version}")
        return anchor

    @staticmethod
    def revise(anchor: AnchoredIR, new_prefix: str) -> AnchoredIR:
        """@phase: δ structural anchoring"""
        log.trace(f"[δ] revise → add prefix {new_prefix}")

        new_prefixes = set(anchor.domain.prefixes)
        new_prefixes.add(new_prefix)

        new_domain = PsiDomain(frozenset(new_prefixes))
        new_ir = AnchorFlow._synthesize(new_domain)

        new_anchor = AnchoredIR(
            version=anchor.version + 1,
            domain=new_domain,
            ir=new_ir,
        )

        log.trace(f"[δ] new anchor version={new_anchor.version}")
        return new_anchor

    @staticmethod
    def _synthesize(domain: PsiDomain) -> PhaseIR:
        """@phase: Φ′ generation from ∂Φ"""
        transitions: Dict[Tuple[str, str], str] = {}

        for prefix in domain.prefixes:
            transitions[("PHASE_IDLE", prefix)] = f"PHASE_ACTIVE::{prefix}"
            transitions[(f"PHASE_ACTIVE::{prefix}", prefix)] = f"PHASE_ACTIVE::{prefix}"

        transitions[("PHASE_IDLE", "UNKNOWN")] = "PHASE_IDLE"

        log.trace(f"[IR] synthesized with {len(domain.prefixes)} prefixes")
        return PhaseIR(transitions)

class PhaseInterpreter:
    """
    @role:
    - Ψ processing under Φ constraint

    @invariant:
    - field gating first
    - no interpretation before constraint
    """
    def __init__(self, anchor: AnchoredIR, field: PhaseField = PhaseField.COHERENT):
        self.anchor = anchor
        self.phase = "PHASE_IDLE"
        self.current_field = field

    def process(self, carrier: PsiCarrier, mediator=None) -> AnchoredIR:
        log.trace(f"Processing {CARRIER_SYMBOL[carrier.carrier_type]}")

        ## Φ gating
        if carrier.target_field != self.current_field:
            log.trace(f"[field] Bypass: {carrier.target_field} != {self.current_field}")
            return self.anchor

        ## ∂Φ resolve
        symbol = self.anchor.domain.resolve(carrier)

        ## θ resonance
        if symbol != "UNKNOWN":
            self.phase = self.anchor.ir.next_phase(self.phase, symbol)
            log.trace(f"[θ] resonance via {carrier.carrier_type}")
            return self.anchor

        ## θ interference
        log.trace("[θ] interference")

        if carrier.carrier_type != CarrierType.RECURSIVE:
            log.trace(f"[θ] ignore: {carrier.carrier_type}")
            return self.anchor

        # if mediator is None:
        #     return self.anchor
        return self.anchor

        # candidate = mediator.interpret(carrier)
        # if not self.policy_gate(candidate, carrier.collapse_risk):
        #     return self.anchor
        # ## δ anchoring
        # self.anchor = AnchorFlow.revise(self.anchor, candidate)
        # self.phase = self.anchor.ir.next_phase(self.phase, candidate)
        # return self.anchor

    def policy_gate(self, candidate: str) -> bool:
        """@phase:pre-δ constraint"""
        if not candidate.endswith(":"):
            return False
        return True