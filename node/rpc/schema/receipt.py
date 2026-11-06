# xphi.node.rpc.schema.receipt
from enum import Enum
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional

class EdgeState(str, Enum):
    INTENT_VALIDATED = "INTENT_VALIDATED"
    EXECUTION_SUCCESS = "EXECUTION_SUCCESS"
    PROOF_GENERATED = "PROOF_GENERATED"
    SEALED_AND_COMMITTED = "SEALED_AND_COMMITTED"
    INTENT_ACCEPTED = "INTENT_ACCEPTED"
    RECEIPT_GENERATED = "RECEIPT_GENERATED"
    SUCCESS = "success"
    ERROR = "error"

class EdgeHeader(str, Enum):
    # Compute & Execution Proof
    CANONICAL_HASH = "X-XPHI-Canonical-Hash"
    STATE = "X-Edge-State"
    ERROR_DETAIL = "X-Error-Detail"

    # Node Identity & Transport Attestation
    CONTENT_HASH = "X-Dphi-Content-Hash"
    SIGNATURE = "X-Dphi-Signature"
    TIMESTAMP = "X-Dphi-Timestamp"
    SIGNER = "X-Dphi-Signer"
    ROOT_SIGNATURE = "X-Dphi-Root-Signature"

    # Control Plane, FSM & Metering Context
    TENANT = "X-Fiber-Tenant"
    CHECKPOINT = "X-Fiber-Checkpoint"
    STREAM = "X-Fiber-Stream"
    HITL_TICKET = "X-Fiber-HITL-Ticket"
    FUEL_RECEIPT = "X-Fiber-Fuel-Receipt"
    EDGE_TOKEN = "X-Fiber-Edge-Token"

class IntentValidationRequest(BaseModel):
    requester_id: str = Field(..., description="Requester agent ID")
    responder_id: str = Field(..., description="Responder agent ID")
    action: str = Field(..., description="Target action name")
    max_fuel_budget: int = Field(..., ge=0, le=4294967295, description="Max fuel budget (u32)")
    signature: Optional[str] = None
    sig_algo: str = "ECDSA_SECP256K1"

class ExecuteComputeRequest(BaseModel):
    code: str = Field(..., description="Python script to execute in WASM sandbox")
    variables: Dict[str, Any] = Field(default_factory=dict, description="Injected variable context")

class ProofGenerationRequest(BaseModel):
    execution_hash: str = Field(..., description="Deterministic execution hash")
    fuel_consumed: int = Field(..., ge=0, le=4294967295, description="Consumed fuel (u32)")
    verification_seed: str = Field(..., description="Random verification seed")

class IntentValidationResponse(BaseModel):
    status: EdgeState
    clearance: Dict[str, Any]

class ExecuteComputeResponse(BaseModel):
    status: EdgeState
    output: str

class ProofGenerationResponse(BaseModel):
    status: EdgeState
    zk_receipt: Dict[str, Any]

class TradeIngressRequest(BaseModel):
    client_id: str = Field(..., description="Client ID")
    action: str = Field(..., description="Trade action (e.g., buy, sell, swap)")

class TradeIngressResponse(BaseModel):
    status: EdgeState
    session: Dict[str, Any] = Field(..., description="Issued session info")

class ClearingReceiptRequest(BaseModel):
    entangled_state: Dict[str, Any] = Field(default_factory=dict, description="Matched entangled state data")
    signatures: List[str] = Field(default_factory=list, description="Participant signatures")
    cost_metrics: Dict[str, Any] = Field(default_factory=dict, description="Settlement cost and gas metrics")

class ClearingReceiptResponse(BaseModel):
    status: EdgeState
    rollup_payload: Dict[str, Any] = Field(..., description="External payload for EVM/Rollup")

class EpochInitPayload(BaseModel):
    ts: int = Field(..., description="Ingress timestamp (ms)")
    topo: int = Field(..., description="Sequencer topology ID")
    press: int = Field(..., description="Allocated fuel limit")
    rupture: bool = Field(..., description="Network rupture flag")
    injected_intent: TradeIngressRequest = Field(..., description="Injected raw intent")

class ParityTripletSchema(BaseModel):
    topos_id: str = Field(..., description="Topology ID")
    phase_id: int = Field(ge=0, le=4294967295, description="Rust u32 constraint")
    nexus_id: int = Field(ge=0, le=4294967295, description="Rust u32 constraint")

class AnchorProposalRequest(BaseModel):
    receptor_id: str = Field(..., description="Receptor agent ID")
    proposed_parity: ParityTripletSchema = Field(..., description="Proposed parity triplet")
    parent_nexus_id: int = Field(default=0, ge=0, le=4294967295, description="Parent nexus ID")
    self_parent_state: str = Field(..., description="Parent state hash")
    repos: Dict[str, str] = Field(default_factory=dict, description="State repository map")
    signers: List[str] = Field(default_factory=list, description="Signer list")
    signatures: List[str] = Field(default_factory=list, description="Cryptographic signature list")
    timestamp: float = Field(..., description="Proposal timestamp")

class AnchorSealResponse(BaseModel):
    status: EdgeState = Field(..., description="Consensus status receipt")
    nexus_id: int = Field(..., description="Finalized nexus ID")
    commit_hash: str = Field(..., description="Deterministic commit hash")
    receipt: Dict[str, Any] = Field(default_factory=dict, description="Sealed receipt details")