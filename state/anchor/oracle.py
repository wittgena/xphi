# xphi.state.anchor.oracle
import json
from typing import Any, Dict, Optional, List
from rocksdict import Rdict, Options, AccessType

from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.wasm.broker import DphiBroker, DphiMethod
from xphi.arch.bound.adapter.state import StateAdapter

log = get_emitter("anchor.oracle", phase="KERNEL")
ANCHOR_DB_PATH = resolve_path("adb")

class AnchorOracle:
    def __init__(self, broker: DphiBroker, path: str = ANCHOR_DB_PATH):
        self.broker = broker
        self.path = str(path)
        ro_opt = Options()
        try:
            self.db = Rdict(self.path, ro_opt, access_type=AccessType.read_only())
            log.info("[AnchorOracle] Mounted Anchor DB in Read-Only mode. Canonical WASM Broker attached.")
        except Exception as e:
            log.error(f"[AnchorOracle] Failed to mount ledger DB: {e}")
            raise

    def close(self):
        if hasattr(self, 'db') and self.db is not None:
            self.db.close()

    def _get_raw_object(self, obj_type: str, obj_hash: str) -> Optional[Dict[str, Any]]:
        key = f"{obj_type}:{obj_hash}".encode('utf-8')
        if key in self.db:
            return json.loads(self.db[key].decode('utf-8'))
        return None

    # =====================================================================
    # 💡 [신규 추가] UTXO 상태 수화 (State Hydration & Balance Query)
    # =====================================================================

    async def get_unspent_outputs(self, owner: str, asset_type: str = "fuel") -> List[Dict[str, Any]]:
        """
        @desc: RocksDB에서 특정 소유자(owner)의 아직 소모되지 않은(Unspent) UTXO 목록을 스캔하여 반환합니다.
               이는 PtaAdapter나 TenantEco가 결제 청사진(Coin Selection)을 짤 때 사용됩니다.
        """
        # Prefix 규칙: utxo:{owner}:{asset_type}:
        prefix = f"utxo:{owner}:{asset_type}:".encode('utf-8')
        unspent_outputs = []
        
        try:
            # RocksDB의 Prefix Iteration을 통해 해당 테넌트의 자산만 O(K) 속도로 빠르게 스캔
            iterator = self.db.iter()
            iterator.seek(prefix)
            
            while iterator.valid():
                key_bytes = iterator.key()
                if not key_bytes.startswith(prefix):
                    break
                    
                output_data = json.loads(iterator.value().decode('utf-8'))
                
                # 키 구조 파싱: utxo:{owner}:{asset_type}:{tx_hash}:{idx}
                key_str = key_bytes.decode('utf-8')
                key_parts = key_str.split(':')
                
                if len(key_parts) >= 5:
                    tx_hash = key_parts[3]
                    output_idx = int(key_parts[4])
                    
                    unspent_outputs.append({
                        "pointer": {"tx_hash": tx_hash, "output_index": output_idx},
                        "output": output_data
                    })
                
                iterator.next()
                
        except Exception as e:
            log.error(f"[AnchorOracle] Failed to scan UTXOs for owner '{owner}': {e}", exc_info=True)
            
        return unspent_outputs

    async def get_balance(self, owner: str, asset_type: str = "fuel") -> int:
        """
        @desc: 오라클이 직접 DB를 스캔하여 확정된 물리적 잔고(Confirmed Balance)를 계산하여 반환합니다.
        """
        outputs = await self.get_unspent_outputs(owner, asset_type)
        confirmed_balance = sum(item["output"].get("amount", 0) for item in outputs)
        
        log.debug(f"[AnchorOracle] Confirmed balance for {owner[:12]}... -> {confirmed_balance} {asset_type}")
        return confirmed_balance

    # =====================================================================
    # 기존 원장 무결성 검증 (Nexus Observe & Lineage)
    # =====================================================================

    async def observe_nexus(self, epoch_hash: str) -> Dict[str, Any]:
        snapshot = self._get_raw_object("commit", epoch_hash)
        if not snapshot:
            raise ValueError(f"Epoch not found in Anchor Store: {epoch_hash}")

        entangled_state = snapshot.get("entangled_state", {})
        if not entangled_state.get("has_contention", False):
            return {"epoch_hash": epoch_hash, "resolved_state": snapshot, "is_collapsed": False}

        raw_phase_root = snapshot.get("phase_root")
        if raw_phase_root:
            phase_root = StateAdapter.build_core_node(
                name=raw_phase_root.get("name", "root"), 
                content=raw_phase_root.get("content", ""), 
                children=raw_phase_root.get("children")
            )
        else:
            phase_root = StateAdapter.build_core_node("root", snapshot.get("parent_hash", "genesis"))

        evo_ctx = StateAdapter.build_evolution_context(phase_root=phase_root)
        transition_payload = StateAdapter.build_transition_payload(
            intent_action="COLLAPSE_ENTANGLEMENT",
            intent_payload=entangled_state,
            evolution_ctx=evo_ctx
        )

        canonical_payload = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
        log.info(f"[AnchorOracle] Injecting canonical entanglement into WASM for collapse. Epoch: {epoch_hash[:8]}")
        exec_result = await self.broker.invoke(
            target_func=DphiMethod.EXECUTE_TRANSITION,
            payload=canonical_payload,
            tier="KERNEL"
        )

        if not exec_result.success:
            log.error(f"[AnchorOracle] WASM Collapse Failed: {exec_result.error.msg}")
            raise RuntimeError(f"WASM State Transition Failed: {exec_result.error.msg}")

        collapsed_state = json.loads(exec_result.output)
        return {
            "epoch_hash": epoch_hash,
            "parent_hash": snapshot.get("parent_hash"),
            "resolved_state": collapsed_state.get("final_root"),
            "residues": collapsed_state.get("all_residues", []),
            "is_collapsed": True
        }

    async def verify_kernel_lineage(self, start_commit_hash: str, depth: int = 5) -> Dict[str, Any]:
        """StateAdapter를 통해 ParityTriplet을 조립하여 WASM 엔진에서 무결성을 검증합니다."""
        chain_trace = []
        current_hash = start_commit_hash
        traversed_depth = 0

        while current_hash and current_hash != "genesis" and traversed_depth < depth:
            snapshot = self._get_raw_object("commit", current_hash)
            if not snapshot:
                return {"is_valid": False, "rupture_hash": current_hash, "trace": chain_trace}
            
            parity_req = StateAdapter.build_parity_triplet(
                topos_id=str(snapshot.get("topos_id", "0")),
                phase_id=int(snapshot.get("phase_id", 0)),
                nexus_id=int(snapshot.get("nexus_id", 0))
            )
            
            canonical_parity = StateAdapter.to_canonical_bytes(parity_req).decode('utf-8')
            res = await self.broker.invoke(DphiMethod.VERIFY_PARITY, canonical_parity)
            if not res.success:
                log.warning(f"[AnchorOracle] WASM Parity verification crashed at {current_hash[:8]}")
                return {"is_valid": False, "rupture_hash": current_hash, "trace": chain_trace}

            output = json.loads(res.output)
            is_valid = output.get("is_valid", False)
            if not is_valid and "recovered_missing" in output:
                log.warning(f"[AnchorOracle] ⚠️ Parity fractured but recovered via XOR at {current_hash[:8]}")
                is_valid = True

            if not is_valid:
                log.warning(f"[AnchorOracle] Parity invalid at {current_hash[:8]}")
                return {"is_valid": False, "rupture_hash": current_hash, "trace": chain_trace}

            chain_trace.append(current_hash)
            current_hash = snapshot.get("parent_hash")
            traversed_depth += 1

        return {"is_valid": True, "reached_genesis": current_hash == "genesis", "trace": chain_trace}