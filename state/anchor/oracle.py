# xphi.state.anchor.oracle
import json
from typing import Any, Dict, Optional, List
from rocksdict import Rdict, Options, AccessType

from xphi.node.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter
from xphi.node.runtime.wasm.broker import DphiBroker, DphiMethod
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.state.anchor.store import PhaseStore, AnchorRole

log = get_emitter("anchor.oracle", phase="ANCHOR")
ANCHOR_DB_PATH = resolve_path("adb")


class AnchorOracle:
    def __init__(self, broker: Optional[DphiBroker] = None, path: str = ANCHOR_DB_PATH):
        self.broker = broker or DphiBroker.get_shared()
        self.path = str(path)
        try:
            self.store = PhaseStore(self.path)
            self.db = self.store.db
            log.info("[AnchorOracle] Mounted Anchor DB via PhaseStore. Canonical WASM Broker attached.")
        except Exception as e:
            log.error(f"[AnchorOracle] Failed to mount ledger DB: {e}")
            raise

    def close(self):
        # DB 핸들의 생명주기는 PhaseStore 싱글톤에서 일괄 관리함
        pass

    def _sync_readonly_view(self) -> None:
        """PROPOSER(Read-Only) 모드일 때 SEALER가 디스크에 기록한 최신 변경분을 동기화합니다."""
        if getattr(self.store, "role", None) != AnchorRole.PROPOSER:
            return
        try:
            if hasattr(self.db, "try_catch_up_with_primary"):
                self.db.try_catch_up_with_primary()
            else:
                self.store.db.close()
                self.store.db = Rdict(self.path, Options(), access_type=AccessType.read_only())
                self.db = self.store.db
        except Exception as e:
            log.debug(f"[AnchorOracle] Read-only view sync skipped: {e}")

    def _get_raw_object(self, obj_type: str, obj_hash: str) -> Optional[Dict[str, Any]]:
        if not obj_hash:
            return None
        self._sync_readonly_view()
        key = f"{obj_type}:{obj_hash}".encode('utf-8')
        if key in self.db:
            return json.loads(self.db[key].decode('utf-8'))
        return None

    @staticmethod
    def _extract_tx_from_commit(commit_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """PhaseCommit의 executable_payload 내부에서 PTA 트랜잭션 딕셔너리(tx_hash, inputs, outputs)를 추출합니다."""
        if not isinstance(commit_data, dict):
            return None
        payload = commit_data.get("executable_payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                return None
        if not isinstance(payload, dict):
            return None

        if "tx_hash" in payload and "outputs" in payload:
            return payload

        for key in ("intent_payload", "tx", "content"):
            sub = payload.get(key)
            if isinstance(sub, str):
                try:
                    sub = json.loads(sub)
                except Exception:
                    continue
            if isinstance(sub, dict) and "tx_hash" in sub and "outputs" in sub:
                return sub

        return None

    def find_commit_hash_by_tx(self, target_tx_hash: str) -> Optional[str]:
        """특정 tx_hash를 담고 있는 PhaseCommit의 해시(signature)를 commit: 스캔으로 역추적합니다."""
        self._sync_readonly_view()
        prefix = b"commit:"
        try:
            iterator = self.db.iter()
            iterator.seek(prefix)
            while iterator.valid():
                key_bytes = iterator.key()
                if not key_bytes.startswith(prefix):
                    break
                commit_data = json.loads(iterator.value().decode('utf-8'))
                tx_data = self._extract_tx_from_commit(commit_data)
                if tx_data and tx_data.get("tx_hash") == target_tx_hash:
                    return key_bytes.decode('utf-8').split(":", 1)[1]
                iterator.next()
        except Exception as e:
            log.debug(f"[AnchorOracle] Commit lookup by tx_hash failed: {e}")
        return None

    async def get_unspent_outputs(self, owner: str, asset_type: str = "fuel") -> List[Dict[str, Any]]:
        self._sync_readonly_view()
        prefix = b"commit:"
        spent_pointers = set()
        candidate_outputs: Dict[str, Dict[str, Any]] = {}
        
        try:
            iterator = self.db.iter()
            iterator.seek(prefix)
            while iterator.valid():
                key_bytes = iterator.key()
                if not key_bytes.startswith(prefix):
                    break
                    
                commit_hash = key_bytes.decode('utf-8').split(":", 1)[1]
                commit_data = json.loads(iterator.value().decode('utf-8'))
                tx_data = self._extract_tx_from_commit(commit_data)
                
                if tx_data:
                    tx_hash = tx_data.get("tx_hash", "")
                    tx_meta = tx_data.get("metadata", {})
                    
                    # 1. 트랜잭션 Input에 기록된 소비(Spent) 포인터 수집
                    for inp in tx_data.get("inputs", []):
                        ptr = inp.get("pointer")
                        if isinstance(ptr, str):
                            spent_pointers.add(ptr)
                        elif isinstance(ptr, dict):
                            spent_pointers.add(f"{ptr.get('tx_hash')}:{ptr.get('output_index')}")
                            
                    # 2. 요청된 owner 및 asset_type에 해당하는 Output 후보 수집
                    for idx, out in enumerate(tx_data.get("outputs", [])):
                        if out.get("owner") == owner and out.get("asset_type") == asset_type:
                            ptr_str = f"{tx_hash}:{idx}"
                            candidate_outputs[ptr_str] = {
                                "pointer": {"tx_hash": tx_hash, "output_index": idx},
                                "output": out,
                                "metadata": tx_meta,
                                "commit_hash": commit_hash
                            }
                
                iterator.next()
                
        except Exception as e:
            log.error(f"[AnchorOracle] Failed to scan commits for owner '{owner}': {e}", exc_info=True)
            return []

        # 3. 전체 생성된 Output 중 어떤 트랜잭션의 Input으로도 소비되지 않은 항목만 반환
        return [
            item for ptr_str, item in candidate_outputs.items()
            if ptr_str not in spent_pointers
        ]

    async def get_balance(self, owner: str, asset_type: str = "fuel") -> int:
        outputs = await self.get_unspent_outputs(owner, asset_type)
        confirmed_balance = sum(item["output"].get("amount", 0) for item in outputs)
        
        log.debug(f"[AnchorOracle] Confirmed balance for {owner[:12]}... -> {confirmed_balance} {asset_type}")
        return confirmed_balance

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