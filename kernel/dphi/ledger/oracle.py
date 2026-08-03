# kernel.dphi.ledger.oracle
import json
from typing import Any, Dict, Optional, List, Tuple
from rocksdict import Rdict, Options, AccessType

from kernel.phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

log = get_emitter("kernel.ledger.reader", phase="KERNEL")
LEDGER_DB_PATH = resolve_path("ledger")

class LedgerOracle:
    def __init__(self, path: str = LEDGER_DB_PATH):
        ro_opt = Options()
        try:
            self.db = Rdict(str(path), ro_opt, access_type=AccessType.read_only())
            log.info("[LedgerOracle] Mounted ledger DB in Read-Only mode for proof verification.")
        except Exception as e:
            log.error(f"[LedgerOracle] Failed to mount ledger DB: {e}")
            raise

    def close(self):
        if hasattr(self, 'db') and self.db is not None:
            self.db.close()

    def _get_raw_object(self, obj_type: str, obj_hash: str) -> Optional[Dict[str, Any]]:
        key = f"{obj_type}:{obj_hash}".encode('utf-8')
        if key in self.db:
            return json.loads(self.db[key].decode('utf-8'))
        return None

    def get_stream_head(self, stream_id: str) -> Optional[str]:
        key = f"ref:{stream_id}".encode('utf-8')
        if key in self.db:
            return self.db[key].decode('utf-8')
        return None

    def get_snapshot_proof(self, commit_hash: str) -> Optional[Dict[str, Any]]:
        return self._get_raw_object("commit", commit_hash)

    def get_blob_evidence(self, blob_hash: str) -> Optional[Dict[str, Any]]:
        return self._get_raw_object("blob", blob_hash)

    def verify_kernel_lineage(self, start_commit_hash: str, depth: int = 5) -> Dict[str, Any]:
        chain_trace = []
        current_hash = start_commit_hash
        traversed_depth = 0

        while current_hash and current_hash != "genesis" and traversed_depth < depth:
            snapshot = self.get_snapshot_proof(current_hash)
            if not snapshot:
                log.warning(f"[LedgerOracle] Lineage ruptured at {current_hash[:8]}")
                return {
                    "is_valid": False, 
                    "rupture_hash": current_hash, 
                    "trace": chain_trace
                }
            
            chain_trace.append(current_hash)
            current_hash = snapshot.get("parent_hash")
            traversed_depth += 1

        return {
            "is_valid": True,
            "reached_genesis": current_hash == "genesis",
            "trace": chain_trace
        }

    def verify_billing_continuity(self, current_continuity_proof: str, expected_previous_nexus: str) -> bool:
        epoch_state = self.get_snapshot_proof(current_continuity_proof) 
        if not epoch_state:
            log.error(f"[LedgerOracle:Billing] Proof not found: {current_continuity_proof}")
            return False

        recorded_previous = epoch_state.get("previous_nexus")
        if recorded_previous != expected_previous_nexus:
            log.error(f"[LedgerOracle:Billing] XOR Linkage Mismatch. Expected: {expected_previous_nexus}, Found: {recorded_previous}")
            return False
            
        return True