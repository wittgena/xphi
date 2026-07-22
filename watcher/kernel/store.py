# watcher.kernel.store
"""
@module: watcher.kernel.store (Absorbed kernel.compiler)
@desc: 
- Consensus-aware Merkle Store & Unified Entry Gateway for dphi.wasm Kernel.
- Stripped of all "fake intelligence" (Python-side state evaluation).
- Acts strictly as an I/O pipeline: Ingress Stream -> FOLLOWER(Mempool) or LEADER(WASM FFI -> RocksDB).
"""
import time
import json
import hashlib
import asyncio
from enum import Enum
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field, asdict
from pydantic import BaseModel, Field
from rocksdict import Rdict, Options, AccessType

from watcher.plane.emitter import get_emitter
from phase.bind.resolver import resolve_path
from arch.topos.bound.tunnel import TunnelFactory
from phase.wasm.broker import WasmBroker  

log = get_emitter("kernel.store.consensus", phase="KERNEL")

LEDGER_DB_PATH = resolve_path("ledger")

class LedgerRole(Enum):
    LEADER = "SEALER"        # Acquired physical lock; can invoke WASM and seal states
    FOLLOWER = "PROPOSER"    # Read-Only mode; proposes raw streams to Mempool

def deterministic_hash(data: Dict[str, Any]) -> str:
    """Generates a deterministic integrity hash (SHA-256) via sorted serialization (JCS-like)."""
    serialized = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()

class LogicStream(BaseModel):
    """@desc: Ψ_open - The external request flowing into the system."""
    id: str
    action: str = "default_action"
    payload: Any  
    metadata: Dict[str, Any] = Field(default_factory=dict)

@dataclass
class ToposBlob:
    """OOB Log / Residue representation from WASM transitions."""
    action: str
    from_state: str
    to_state: str
    tension: float
    details: str
    timestamp: float = field(default_factory=time.time)

@dataclass
class KernelCommit:
    """Sealed kernel snapshot verified by WASM."""
    stream_id: str
    executable_payload: Any
    tension_at_seal: float
    blob_hashes: List[str]
    parent_hash: Optional[str] = None
    sealed_at: float = field(default_factory=time.time)

class SealedKernel(BaseModel):
    """@desc: Ω_knot - The mathematically invariant, executable closed boundary."""
    kernel_id: str
    stream_id: str
    executable_payload: Any
    tension_at_seal: float
    signature: str  # The deterministic Merkle Commit hash 

# -------------------------------------------------------------
# The Unified Kernel Store (The Spinal Cord)
# -------------------------------------------------------------

class KernelStore:
    """
    @desc: Unified Physical Ledger & WASM Gateway.
           All streams must pass through `propose_and_seal` to enter the WASM Universe.
    """
    _instance = None

    def __new__(cls, path=LEDGER_DB_PATH):
        if cls._instance is None:
            cls._instance = super(KernelStore, cls).__new__(cls)
            cls._instance._initialize_consensus_node(path)
        return cls._instance

    def _initialize_consensus_node(self, target_path: str):
        """Proof of Lock: Determines LEADER/FOLLOWER role."""
        opt = Options()
        opt.create_if_missing(True)
        
        try:
            # 1. Attempt Proof of Lock (Requires Write Access)
            self.db = Rdict(str(target_path), opt)
            self.role = LedgerRole.LEADER
            self.broker = None
            
            # [ARCHITECTURE SHIFT] Only LEADER mounts the WASM Engine
            self.wasm = WasmBroker() 
            log.info(f"[Ledger] Acquired physical lock. Operating as {self.role.value}. WASM Kernel mounted.")
            
        except Exception as e:
            error_msg = str(e).lower()
            if "lock" in error_msg or "temporarily unavailable" in error_msg:
                # Lock Acquisition Failed -> Transition to FOLLOWER mode
                self.role = LedgerRole.FOLLOWER
                self.wasm = None # FOLLOWER does not execute WASM
                log.info(f"[Ledger] Lock held by another node. Operating as {self.role.value} (Read-Only/Proposer).")
                
                ro_opt = Options()
                self.db = Rdict(str(target_path), ro_opt, access_type=AccessType.read_only())
                self.broker = TunnelFactory.get_sync()
            else:
                raise 

    # --- Low-Level Persistence (Leader vs Follower routing) ---

    def _put_object(self, obj_type: str, data: Dict[str, Any]) -> str:
        """Routes to either physical disk write (LEADER) or Mempool proposal (FOLLOWER)."""
        obj_hash = deterministic_hash(data)
        
        if self.role == LedgerRole.LEADER:
            key = f"{obj_type}:{obj_hash}".encode('utf-8')
            if key not in self.db:
                self.db[key] = json.dumps(data).encode('utf-8')
            return obj_hash
        else:
            payload = {"type": obj_type, "hash": obj_hash, "data": json.dumps(data)}
            try:
                self.broker.stream_produce(f"ledger:mempool:{obj_type}s", payload)
                log.debug(f"[Ledger:FOLLOWER] Proposed {obj_type} {obj_hash[:8]} to mempool.")
            except Exception as e:
                log.error(f"[Ledger:FOLLOWER] Mempool propose failed: {e}")
            return obj_hash

    def save_transition(self, blob: ToposBlob) -> str:
        """Used by Warden for out-of-band synchronous anomaly logging."""
        return self._put_object("blob", asdict(blob))

    def update_head(self, stream_id: str, commit_hash: str) -> None:
        """Updates the latest kernel pointer (Anchor Ref) for a logic stream."""
        if self.role == LedgerRole.LEADER:
            key = f"ref:{stream_id}".encode('utf-8')
            self.db[key] = commit_hash.encode('utf-8')
        else:
            payload = {"type": "head_update", "stream_id": stream_id, "commit_hash": commit_hash}
            try:
                self.broker.stream_produce("ledger:mempool:heads", payload)
            except Exception:
                pass

    def get_head_hash(self, stream_id: str) -> Optional[str]:
        """Reads the Anchor Hash (Readable by both LEADER and FOLLOWER)."""
        key = f"ref:{stream_id}".encode('utf-8')
        if key in self.db:
            return self.db[key].decode('utf-8')
        return None

    # --- High-Level Pipeline (Replacing compiler.compile_kernel) ---

    async def propose_and_seal(self, stream: LogicStream) -> Optional[SealedKernel]:
        """
        @desc: The Unified Execution Gateway. 
               Delegates ALL logic, validation, and tension calculation to `dphi.wasm`.
        """
        if self.role == LedgerRole.FOLLOWER:
            # [FOLLOWER FLOW] No authority to compute or seal. Forward raw stream to LEADER.
            stream_data = {"id": stream.id, "action": stream.action, "payload": stream.payload, "metadata": stream.metadata}
            self.broker.stream_produce("ledger:mempool:logic_streams", stream_data)
            log.info(f"[Ledger:FOLLOWER] Delegated stream {stream.id} to Mempool.")
            return None

        # [LEADER FLOW] Construct Theoria Pressure & Invoke WASM

        # 1. Retrieve the Anchor (Time & State Context)
        parent_hash = self.get_head_hash(stream.id) or "genesis"

        # 2. Assemble TransitionPayload (Matching dphi.wasm schema)
        # Note: We wrap the stream into a primitive StateNode (CORE) to begin evolution.
        transition_payload = {
            "intent_action": stream.action,
            "intent_payload": stream.payload,
            "evolution_ctx": {
                "phase_root": {
                    "name": f"root_{stream.id}",
                    "kind": "CORE",
                    "content": json.dumps(stream.metadata),
                    "ref_target": parent_hash,
                    "children": {}
                },
                "external_rules": [] # Optional topological mutation rules could be passed here
            }
        }

        # 3. Delegate to WASM Kernel (The Absolute Authority)
        log.debug(f"[Ledger:LEADER] Injecting stream {stream.id} into dphi.wasm (Anchor: {parent_hash[:8]})")
        payload_json = json.dumps(transition_payload)
        
        # Async invocation to prevent blocking the event loop during WASM compute
        wasm_res = await self.wasm.invoke("execute_transition", payload_json)

        if not wasm_res.success:
            log.error(f"[Ledger:WASM] FATAL: Kernel panicked or rejected execution. {wasm_res.error}")
            return None

        trans_result = json.loads(wasm_res.output)

        # 4. Check Spatial Fence Authorization
        if not trans_result.get("is_authorized", False):
            err_msg = trans_result.get("error_msg", "Spatial Fence Denied")
            log.warning(f"[Ledger:WASM] REJECTED: {err_msg}")
            
            # Record the rejection as a Blob (Out-of-band audit)
            await asyncio.to_thread(
                self.save_transition, 
                ToposBlob(action="wasm.reject", from_state="logic.stream", to_state="fragmented", tension=1.0, details=err_msg)
            )
            return None

        # 5. Extract Residues & Final Root (Evolution Completed)
        final_root = trans_result.get("final_root")
        all_residues = trans_result.get("all_residues", [])
        
        # WASM determines the complexity. We translate residue count/types to topological tension (τ)
        tension_at_seal = len(all_residues) * 0.1 

        # 6. Translate WASM Residues into ToposBlobs for immutable ledger history
        blob_hashes = []
        for residue in all_residues:
            blob = ToposBlob(
                action="wasm.mutation",
                from_state="logic.loop",
                to_state="sealed.kernel",
                tension=tension_at_seal,
                details=f"[{residue['kind']}] {residue['msg']}"
            )
            b_hash = await asyncio.to_thread(self.save_transition, blob)
            blob_hashes.append(b_hash)

        # 7. Commit & Seal to Physical Ledger
        commit = KernelCommit(
            stream_id=stream.id,
            executable_payload=final_root,
            tension_at_seal=tension_at_seal,
            blob_hashes=blob_hashes,
            parent_hash=parent_hash
        )

        signature = await asyncio.to_thread(self.save_kernel, commit)
        await asyncio.to_thread(self.update_head, stream.id, signature)

        log.info(f"[Ledger:LEADER] ✨ Epoch Sealed by WASM. Stream: {stream.id} | Commit: {signature[:8]}")

        return SealedKernel(
            kernel_id=f"ker_{stream.id}",
            stream_id=stream.id,
            executable_payload=final_root,
            tension_at_seal=tension_at_seal,
            signature=signature
        )

    def close(self):
        if hasattr(self, 'db') and self.db is not None:
            self.db.close()
            role_msg = self.role.value if hasattr(self, 'role') else "UNKNOWN"
            log.info(f"[KernelStore] Ledger DB closed cleanly. ({role_msg})")