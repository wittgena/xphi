# xphi.kernel.node.fsm.edge
from enum import Enum, auto
from dataclasses import dataclass
from typing import Any, Dict

class EdgePhaseState(Enum):
    INIT = auto()
    COMPUTING = auto()
    COMPLIANCE_CHECKING = auto()
    SETTLING = auto()
    COMPLETED = auto()
    FAILED = auto()

@dataclass
class StartIntentEvent:
    client_id: str
    action: str
    max_fuel: int
    payload: Any
    signature: str

@dataclass
class ComputePhaseCompletedEvent:
    audit_receipt: Dict[str, Any]
    cost_usd: float

@dataclass
class CompliancePhaseCompletedEvent:
    otlp_hash: str

@dataclass
class SettlementPhaseCompletedEvent:
    tx_hash: str

@dataclass
class PhaseFailedEvent:
    reason: str

@dataclass
class RunComputePhaseCmd:
    client_id: str
    action: str
    max_fuel: int
    payload: Any
    signature: str

@dataclass
class RunCompliancePhaseCmd:
    audit_receipt: Dict[str, Any]

@dataclass
class RunSettlementPhaseCmd:
    client_id: str
    cost_usd: float

@dataclass
class FinishWorkflowCmd:
    tx_hash: str

@dataclass
class HaltWorkflowCmd:
    reason: str

class EdgePhaseFSM:
    def __init__(self):
        self.state = EdgePhaseState.INIT
        self.context: Dict[str, Any] = {}

    def apply(self, event: Any) -> Any:
        if isinstance(event, PhaseFailedEvent):
            self.state = EdgePhaseState.FAILED
            return HaltWorkflowCmd(reason=event.reason)

        if self.state == EdgePhaseState.INIT and isinstance(event, StartIntentEvent):
            self.state = EdgePhaseState.COMPUTING
            self.context["client_id"] = event.client_id
            return RunComputePhaseCmd(
                client_id=event.client_id, 
                action=event.action,
                max_fuel=event.max_fuel, 
                payload=event.payload,
                signature=event.signature
            )
        elif self.state == EdgePhaseState.COMPUTING and isinstance(event, ComputePhaseCompletedEvent):
            self.state = EdgePhaseState.COMPLIANCE_CHECKING
            self.context["audit_receipt"] = event.audit_receipt
            self.context["cost_usd"] = event.cost_usd
            return RunCompliancePhaseCmd(audit_receipt=event.audit_receipt)
        elif self.state == EdgePhaseState.COMPLIANCE_CHECKING and isinstance(event, CompliancePhaseCompletedEvent):
            self.state = EdgePhaseState.SETTLING
            return RunSettlementPhaseCmd(
                client_id=self.context["client_id"],
                cost_usd=self.context.get("cost_usd", 0.0)
            )
        elif self.state == EdgePhaseState.SETTLING and isinstance(event, SettlementPhaseCompletedEvent):
            self.state = EdgePhaseState.COMPLETED
            return FinishWorkflowCmd(tx_hash=event.tx_hash)

        self.state = EdgePhaseState.FAILED
        return HaltWorkflowCmd(reason=f"Invalid Event {event.__class__.__name__} at {self.state.name}")