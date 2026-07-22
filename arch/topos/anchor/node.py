# arch.topos.anchor.node
import json
from pathlib import Path
from typing import Dict, List, Optional, Callable

# 기존 RepoCommit, AnchorCommit 임포트는 WASM으로 스키마 검증이 이관되었으므로 사용하지 않습니다.
from watcher.kernel.ledger import KernelStore, ToposBlob

class ActorNode:
    """@role: execution unit + lineage inscription node"""
    def __init__(self, name: str, path: str, runner: Callable):
        self.name = name
        self.path = Path(path).expanduser().resolve()
        self.runner = runner
        self.store = KernelStore()

    def inscribe(
        self, 
        nexus_id: int, 
        parent_nexus_id: int, 
        parent_commit_id: str, 
        message: str, 
        apply: bool = False
    ) -> str:
        """
        Models and inscribes the lineage of the current generation.
        @note: String anchor_id is replaced by integer nexus_id for Parity-based deterministic consensus.
        """
        # WASM의 RepoCommit 스키마와 동일한 형태의 딕셔너리 생성
        model_dict = {
            "nexus_id": nexus_id,
            "parent_nexus_id": parent_nexus_id,
            "parent_commit_id": parent_commit_id
        }

        # Git commit 메시지에 물리적으로 각인 (결정론적 직렬화)
        json_payload = json.dumps(model_dict, separators=(',', ':'), sort_keys=True)
        full_message = f"{message}\n\n{json_payload}"
        
        # Git Commit 실행
        new_commit_id = self.runner(self.path, full_message, apply)

        if apply:
            blob = ToposBlob(
                action=f"align.commit::{self.name}",
                from_state=parent_commit_id,
                to_state=new_commit_id,
                tension=1.0, 
                details=json_payload
            )
            self.store.save_transition(blob)
            self.store.update_head(self.name, new_commit_id)

        print(f"  └─ [{self.name}] Inscribed. Nexus: {nexus_id} | State: {new_commit_id}")
        return new_commit_id


class EpochManager(ActorNode):
    """@role: boundary (synchronization frame) + era manager"""
    ERA_DEPTH = 3

    def __init__(self, name: str, path: str, runner: Callable):
        super().__init__(name, path, runner)
        self.registry_key = f"legacy_registry:{self.name}".encode('utf-8')

    def load_history(self) -> List[Dict]:
        """
        @desc: Perfect backward compatibility method for external protocol.commit invocations.
               Extracts and returns the legacy .registry.json array format directly from RocksDB.
        """
        if self.registry_key in self.store.db:
            try:
                raw_data = self.store.db[self.registry_key].decode('utf-8')
                return json.loads(raw_data).get("history", [])
            except Exception:
                pass
        return []

    def resolve(self, repo_name: str) -> str:
        """Resolves the state based on historical consistency rather than the absolute HEAD from KernelStore."""
        history = self.load_history()
        for snapshot in reversed(history[-self.ERA_DEPTH:]):
            if repo_name in snapshot.get("repos", {}):
                return snapshot["repos"][repo_name]
            if repo_name in snapshot.get("cached_states", {}):
                return snapshot["cached_states"][repo_name]
        return "0000000"

    def project(self, states: Dict[str, str]) -> Dict[str, str]:
        return states

    # [REMOVED] anchoring() 메서드 제거됨.
    # 이유: Epoch Seal 로직(상태 델타, Tension, KernelCommit 생성)은 모두 
    # WASM(theoria.src.anchor) 내부로 이관되었으며, 
    # Python 측 프로토콜(anchor_git_commit)에서 WASM 결과를 받아 직접 KernelStore를 업데이트합니다.