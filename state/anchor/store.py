# xphi.state.anchor.store
import time
import json
import hashlib
from enum import Enum
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field, asdict
from pydantic import BaseModel, Field

from xphi.node.space.bind.resolver import resolve_path
from xphi.node.runtime.wasm.broker import DphiBroker  
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.state.anchor.driver import AnchorDriverFactory
from xphi.state.anchor.mempool import MempoolRelay
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("anchor.store", phase="KERNEL")

ANCHOR_DB_PATH = resolve_path("adb")

class AnchorRole(Enum):
    SEALER = "SEALER"        # Acquired physical lock; can invoke WASM and seal states
    PROPOSER = "PROPOSER"    # Read-Only mode

def deterministic_hash(data: Dict[str, Any]) -> str:
    canonical_bytes = StateAdapter.to_canonical_bytes(data)
    return hashlib.sha256(canonical_bytes).hexdigest()

class LogicStream(BaseModel):
    """Ψ_open - The external request flowing into the system."""
    id: str
    action: str = "default_action"
    payload: Any  
    metadata: Dict[str, Any] = Field(default_factory=dict)

@dataclass
class ToposBlob:
    """OOB Log / Residue representation from Phase transitions or System Anomalies."""
    action: str
    from_state: str
    to_state: str
    tension: float
    details: str
    timestamp: float = field(default_factory=time.time)

@dataclass
class PhaseCommit:
    """Sealed kernel snapshot verified by Phase or Multi Signature"""
    stream_id: str
    executable_payload: Any
    tension_at_seal: float
    blob_hashes: List[str]
    parent_hash: Optional[str] = None
    sealed_at: float = field(default_factory=time.time)

class SealedKernel(BaseModel):
    """Ω_knot - The mathematically invariant, executable closed boundary."""
    kernel_id: str
    stream_id: str
    executable_payload: Any
    tension_at_seal: float
    signature: str 

class AnchorStore:
    _instance = None

    def __new__(cls, path=ANCHOR_DB_PATH):
        if cls._instance is None:
            cls._instance = super(AnchorStore, cls).__new__(cls)
            cls._instance._initialize_consensus_node(path)
        return cls._instance

    def _initialize_consensus_node(self, target_path: str):
        """Proof of Lock: Determines LEADER/FOLLOWER role and binds system handlers."""
        self.db = AnchorDriverFactory.mount(str(target_path))
        if self.db.is_sealer:
            self.role = AnchorRole.SEALER
            self.broker = DphiBroker.get_shared()
            self.relay = MempoolRelay(self)
            self.relay.start_sealer_consumer()
            log.info(f"[AnchorStore] Acquired physical lock. Operating as {self.role.value}. WASM Kernel mounted.")
        else:
            self.role = AnchorRole.PROPOSER
            self.broker = None 
            self.relay = MempoolRelay(self)
            log.info(f"[AnchorStore] Lock held by another node. Operating as {self.role.value} (Read-Only/Proposer).")

    def _handle_warden_anomaly(self, action: str, details: str) -> None:
        blob = ToposBlob(
            action=f"SYS_WARDEN_GUARD::{action}", 
            from_state="host.python.sandbox", 
            to_state="host.os.kernel",
            tension=1.0, 
            details=details
        )
        self.save_transition(blob)

    def _put_object(self, obj_type: str, data: Dict[str, Any]) -> str:
        obj_hash = deterministic_hash(data)
        if self.role == AnchorRole.SEALER:
            key = f"{obj_type}:{obj_hash}".encode('utf-8')
            if key not in self.db:
                self.db[key] = json.dumps(data).encode('utf-8')
            return obj_hash
        return self.relay.propose_object_sync(obj_type, obj_hash, data)

    def save_transition(self, blob: ToposBlob) -> str:
        return self._put_object("blob", asdict(blob))

    def update_head(self, stream_id: str, commit_hash: str) -> None:
        if self.role == AnchorRole.SEALER:
            key = f"ref:{stream_id}".encode('utf-8')
            self.db[key] = commit_hash.encode('utf-8')
        else:
            self.relay.propose_head_sync(stream_id, commit_hash)

    def get_head_hash(self, stream_id: str) -> Optional[str]:
        key = f"ref:{stream_id}".encode('utf-8')
        if key in self.db:
            return self.db[key].decode('utf-8')
        return None

    def _save_kernel_unsafe(self, commit: PhaseCommit) -> str:
        return self._put_object("commit", asdict(commit))

    def seal_system_epoch(self, commit: PhaseCommit, signatures: List[str], threshold: int = 1) -> str:
        from xphi.arch.bound.adapter.pta import NodeSigner

        if not signatures:
            error_msg = "System Epoch Seal Rejected: No signatures provided."
            log.critical(f"[KernelStore: PRIVILEGE] {error_msg}")
            self._handle_warden_anomaly("kernel.privilege_escalation_attempt", error_msg)
            raise PermissionError(error_msg)

        signer = NodeSigner.get_instance()
        canonical_bytes = StateAdapter.to_canonical_bytes(asdict(commit))
        valid_count = sum(1 for sig in signatures if signer.verify_signature(canonical_bytes, sig))

        if valid_count < threshold:
            error_msg = f"Multi-Sig Threshold Failed: {valid_count}/{threshold} valid signatures."
            log.critical(f"[KernelStore: PRIVILEGE] {error_msg}")
            self._handle_warden_anomaly("kernel.privilege_escalation_attempt", error_msg)
            raise PermissionError(error_msg)

        log.info(f"[KernelStore: PRIVILEGE] Multi-Sig Verified ({valid_count}/{threshold}). Authorized direct ledger commit.")
        return self._save_kernel_unsafe(commit)

    async def propose_and_seal(self, stream: LogicStream) -> Optional[SealedKernel]:
        if self.role == AnchorRole.PROPOSER:
            return await self.relay.propose_stream_async(stream)

        self.relay.ensure_sealer_consumer()

        parent_hash = self.get_head_hash(stream.id) or "genesis"
        final_root = stream.payload 
        tension_at_seal = 0.0

        blob_hashes = [self.save_transition(ToposBlob(stream.action, "logic.stream", "sealed.kernel", 0.0, f"[TRANSITION] {stream.id}"))]
        commit = PhaseCommit(
            stream_id=stream.id,
            executable_payload=final_root,
            tension_at_seal=tension_at_seal,
            blob_hashes=blob_hashes,
            parent_hash=parent_hash
        )

        signature = self._save_kernel_unsafe(commit)
        self.update_head(stream.id, signature)
        log.info(f"[AnchorStore:LEADER] Epoch Sealed. Stream: {stream.id} | Commit: {signature[:8]}")

        return SealedKernel(
            kernel_id=f"ker_{stream.id}",
            stream_id=stream.id,
            executable_payload=final_root,
            tension_at_seal=tension_at_seal,
            signature=signature
        )

    def close(self):
        if hasattr(self, 'relay') and self.relay is not None:
            self.relay.stop_sealer_consumer()
        if hasattr(self, 'db') and self.db is not None:
            self.db.close()
            role_msg = self.role.value if hasattr(self, 'role') else "UNKNOWN"
            log.info(f"[AnchorStore] DB closed cleanly. ({role_msg})")
        AnchorStore._instance = None