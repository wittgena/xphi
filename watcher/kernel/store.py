# watcher.kernel.store
"""@desc: Consensus-aware Merkle Store for Execution Kernel Ledger"""
import time
import json
import hashlib
from enum import Enum
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field, asdict
from rocksdict import Rdict, Options, AccessType

from watcher.plane.emitter import get_emitter
from phase.bind.resolver import resolve_path
from arch.topos.bound.sandbox.tunnel import TunnelFactory

log = get_emitter("kernel.store.consensus")

LEDGER_DB_PATH = resolve_path("ledger")

class LedgerRole(Enum):
    LEADER = "SEALER"        # Acquired physical lock; holds write-access to the ledger
    FOLLOWER = "PROPOSER"    # Failed to acquire lock; operates in Read-Only mode and proposes to Mempool

def deterministic_hash(data: Dict[str, Any]) -> str:
    """Generates a deterministic integrity hash (SHA-256) via sorted serialization."""
    serialized = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()

@dataclass
class ToposBlob:
    """Single state transition record (Analogous to a Git Blob)."""
    action: str
    from_state: str
    to_state: str
    tension: float
    details: str
    timestamp: float = field(default_factory=time.time)

@dataclass
class KernelCommit:
    """Sealed kernel snapshot breaching the tension threshold (Analogous to a Git Commit)."""
    stream_id: str
    executable_payload: Any
    tension_at_seal: float
    blob_hashes: List[str]
    parent_hash: Optional[str] = None
    sealed_at: float = field(default_factory=time.time)


class KernelStore:
    """Dedicated Content-Addressable Storage for the topological ledger (Consensus architecture applied)."""
    _instance = None

    def __new__(cls, path=LEDGER_DB_PATH):
        if cls._instance is None:
            cls._instance = super(KernelStore, cls).__new__(cls)
            cls._instance._initialize_consensus_node(path)
        return cls._instance

    def _initialize_consensus_node(self, target_path: str):
        """Proof of Lock: Determines LEADER/FOLLOWER role based on physical file lock acquisition."""
        opt = Options()
        opt.create_if_missing(True)
        
        try:
            # 1. Attempt Proof of Lock (Requires Write Access)
            self.db = Rdict(str(target_path), opt)
            self.role = LedgerRole.LEADER
            self.broker = None  # LEADER writes directly to DB; no immediate need for a broker
            log.info(f"[Ledger] Acquired physical lock. Operating as {self.role.value}.")
            
        except Exception as e:
            error_msg = str(e).lower()
            if "lock" in error_msg or "temporarily unavailable" in error_msg:
                ## Lock Acquisition Failed: Transition to FOLLOWER mode gracefully
                self.role = LedgerRole.FOLLOWER
                log.info(f"[Ledger] Lock held by another phase. Operating as {self.role.value} (Read-Only).")
                
                ## Mount RocksDB in Read-Only mode (Allows reading despite another process holding the Write Lock)
                ro_opt = Options()
                self.db = Rdict(str(target_path), ro_opt, access_type=AccessType.read_only())
                
                ## Acquire Sync Tunnel to propose to Redis Stream from synchronous environments (e.g., CPython Audit Hook)
                self.broker = TunnelFactory.get_sync()
            else:
                ## Re-raise critical I/O errors (e.g., insufficient permissions, corrupted DB)
                raise 

    def _put_object(self, obj_type: str, data: Dict[str, Any]) -> str:
        """Routes to either physical disk write (LEADER) or Mempool proposal (FOLLOWER) based on role."""
        obj_hash = deterministic_hash(data)
        
        if self.role == LedgerRole.LEADER:
            # LEADER: Commit immediately to disk
            key = f"{obj_type}:{obj_hash}".encode('utf-8')
            if key not in self.db:
                self.db[key] = json.dumps(data).encode('utf-8')
            return obj_hash
            
        else:
            # FOLLOWER: Propose transaction to Mempool (Redis Stream)
            payload = {
                "type": obj_type,
                "hash": obj_hash,
                "data": json.dumps(data)
            }
            try:
                # Synchronous call via get_sync() broker
                self.broker.stream_produce("ledger:mempool:blobs", payload)
                log.debug(f"[Ledger:FOLLOWER] Proposed {obj_type} {obj_hash[:8]} to mempool.")
            except Exception as e:
                log.error(f"[Ledger:FOLLOWER] Mempool propose failed: {e}")
            return obj_hash

    def save_transition(self, blob: ToposBlob) -> str:
        """Saves or proposes a state transition as a Blob."""
        return self._put_object("blob", asdict(blob))

    def save_kernel(self, commit: KernelCommit) -> str:
        """Saves or proposes a sealed kernel as a Commit."""
        return self._put_object("commit", asdict(commit))

    def update_head(self, stream_id: str, commit_hash: str) -> None:
        """Updates the latest kernel pointer (Ref) for a specific logic stream."""
        if self.role == LedgerRole.LEADER:
            key = f"ref:{stream_id}".encode('utf-8')
            self.db[key] = commit_hash.encode('utf-8')
        else:
            payload = {
                "type": "head_update",
                "stream_id": stream_id,
                "commit_hash": commit_hash
            }
            try:
                self.broker.stream_produce("ledger:mempool:heads", payload)
            except Exception as e:
                log.error(f"[Ledger:FOLLOWER] Head update propose failed: {e}")

    def get_head_hash(self, stream_id: str) -> Optional[str]:
        """Returns the latest kernel hash for the stream (Readable by both LEADER and FOLLOWER)."""
        key = f"ref:{stream_id}".encode('utf-8')
        if key in self.db:
            return self.db[key].decode('utf-8')
        return None
        
    def close(self):
        """Closes the RocksDB instance and releases resources/lock."""
        if hasattr(self, 'db') and self.db is not None:
            self.db.close()
            role_msg = self.role.value if hasattr(self, 'role') else "UNKNOWN"
            log.info(f"[KernelStore] Ledger DB closed cleanly. ({role_msg})")