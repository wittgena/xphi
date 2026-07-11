# phase.gov.node.anchor
"""
@align.commit: lineage inscription over execution results
@node: execution-capable self (not tied to repo)
@anchor: synchronization boundary across nodes (era frame)
@commit: parent linkage under anchor-constrained selection
@state.partition: aligned vs lag (no non-existence)
@integration: ConsensusKernelStore (Merkle Ledger) for O(1) state resolution
"""
import json
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Callable

from arch.proto.event.next import next_id
from phase.gov.repo.schema import RepoCommit, AnchorCommit
from phase.bind.resolver import resolve_path

from watcher.kernel.store import KernelStore, ToposBlob, KernelCommit

DEFAULT_ID = "0000000"

class ActorNode:
    """@role: execution unit + lineage inscription node"""
    def __init__(self, name: str, path: str, runner: Callable):
        self.name = name
        self.path = Path(path).expanduser().resolve()
        self.runner = runner
        self.store = KernelStore()

    def inscribe(self, anchor_id: str, parent_anchor_id: Optional[str], parent_commit_id: str, message: str, apply: bool = False) -> str:
        """현재 세대의 계보를 모델링하여 각인함"""
        model = RepoCommit(
            anchor_id=anchor_id,
            parent_anchor_id=parent_anchor_id or DEFAULT_ID,
            parent_commit_id=parent_commit_id
        )

        full_message = f"{message}\n\n{model.to_json()}"
        new_commit_id = self.runner(self.path, full_message, apply)

        if apply:
            blob = ToposBlob(
                action=f"align.commit::{self.name}",
                from_state=parent_commit_id,
                to_state=new_commit_id,
                tension=1.0, 
                details=model.to_json()
            )
            self.store.save_transition(blob)
            self.store.update_head(self.name, new_commit_id)

        print(f"  └─ [{self.name}] Inscribed. State: {new_commit_id}")
        return new_commit_id


class EpochManager(ActorNode):
    """@role: boundary (synchronization frame) + era manager"""
    ERA_DEPTH = 3

    def __init__(self, name: str, path: str, runner: Callable):
        super().__init__(name, path, runner)
        # 💡 [FIX] protocol.commit 호환성을 위해 파일 대신 RocksDB의 전용 Key를 사용합니다.
        self.registry_key = f"legacy_registry:{self.name}".encode('utf-8')

    def load_history(self) -> List[Dict]:
        """
        @desc: protocol.commit(외부 모듈) 호출을 위한 완벽한 하위 호환성 메서드.
               RocksDB에서 기존 .registry.json과 동일한 배열 포맷을 추출하여 반환합니다.
        """
        if self.registry_key in self.store.db:
            try:
                raw_data = self.store.db[self.registry_key].decode('utf-8')
                return json.loads(raw_data).get("history", [])
            except Exception:
                pass
        return []

    def resolve(self, repo_name: str) -> str:
        """KernelStore의 HEAD(최신 상태) 대신, 역사적 정합성을 위해 History 기반으로 Resolve"""
        history = self.load_history()
        for snapshot in reversed(history[-3:]):
            if repo_name in snapshot.get("repos", {}):
                return snapshot["repos"][repo_name]
            if repo_name in snapshot.get("cached_states", {}):
                return snapshot["cached_states"][repo_name]
        return DEFAULT_ID

    def project(self, states: Dict[str, str]) -> Dict[str, str]:
        return states

    def anchoring(self, anchor_id: str, parent_anchor_id: Optional[str], 
                      repos: Dict[str, str], cached_states: Dict[str, str], 
                      message: str, apply: bool = False) -> str:
        
        history = self.load_history()
        self_parent_state = self.resolve(self.name)
        
        model = AnchorCommit(
            anchor_id=anchor_id,
            parent_anchor_id=parent_anchor_id or DEFAULT_ID,
            parent_commit_id=self_parent_state,
            repos=repos,
            cached_states=cached_states
        )

        # 1. 자기 자신 Inscribe
        new_anchor_commit_id = super().inscribe(anchor_id, parent_anchor_id, self_parent_state, message, apply)

        # 2. KernelStore 밀봉 및 History 호환성 유지
        if apply:
            commit = KernelCommit(
                stream_id="global_era_anchor",
                executable_payload=model.to_json(),
                tension_at_seal=len(repos),
                blob_hashes=list(repos.values()),
                parent_hash=parent_anchor_id or DEFAULT_ID
            )
            
            # KernelStore 위상 장부 기록
            signature = self.store.save_kernel(commit)
            self.store.update_head("global_era_anchor", signature)
            for repo_name, repo_commit_hash in repos.items():
                self.store.update_head(repo_name, repo_commit_hash)

            full_history = history + [json.loads(model.to_json())]
            try:
                # CLI 환경은 메인 루프(Leader) 위에서 작동하므로 안전하게 Write 가능
                self.store.db[self.registry_key] = json.dumps({"history": full_history}).encode('utf-8')
            except Exception as e:
                # Follower 노드에서 강제 실행될 경우의 방어 코드
                print(f"[EpochManager] Warning: Could not update legacy registry: {e}")

        return new_anchor_commit_id