# node.repo.protocol
"""
@align.commit: lineage inscription over execution results
@node: execution-capable self (not tied to repo)
@anchor: synchronization boundary across nodes (era frame)
@commit: parent linkage under anchor-constrained selection
@state.partition: aligned vs lag (no non-existence)
"""
import json
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Callable
from anchor.resolver import resolve_path
from model.event import next_id
from model.commit import RepoCommitModel, AnchorCommitModel

DEFAULT_ID = "0000000"
META_ROOT = resolve_path('io') / 'meta'

class RepoNode:
    """@role: execution unit + lineage inscription node"""
    def __init__(self, name: str, path: str, runner: Callable):
        self.name = name
        self.path = Path(path).expanduser().resolve()
        self.meta_dir = META_ROOT
        self.runner = runner ## CommitRunner
        self.lineage_file = self.meta_dir / f"{self.name}.lineage.json"

    def local_commit(self, anchor_id: str, parent_anchor_id: Optional[str], parent_commit_id: str, message: str, apply: bool = False) -> str:
        """현재 세대의 계보를 모델링하여 각인함"""
        model = RepoCommitModel(
            anchor_id=anchor_id,
            parent_anchor_id=parent_anchor_id or DEFAULT_ID,
            parent_commit_id=parent_commit_id
        )

        self.meta_dir.mkdir(parents=True, exist_ok=True)
        with open(self.lineage_file, "w") as f:
            f.write(model.to_json())

        full_message = f"{message}\n\n{model.to_json()}"
        new_commit_id = self.runner(self.path, full_message, apply)
        print(f"  └─ [{self.name}] Inscribed. State: {new_commit_id}")
        return new_commit_id

class AnchorNode(RepoNode):
    """@role: boundary (synchronization frame) + era manager"""
    ERA_DEPTH = 3

    def __init__(self, name: str, path: str, runner: Callable):
        super().__init__(name, path, runner)
        self.registry_file = self.meta_dir / f"{self.name}.registry.json"

    def load_history(self) -> List[Dict]:
        """registry에서 히스토리 추출"""
        if not self.registry_file.exists(): return []
        with open(self.registry_file, "r") as f:
            try: return json.load(f).get("history", [])
            except: return []

    def resolve(self, repo_name: str) -> str:
        history = self.load_history()
        # 최근 3대 시대(Era) 내 탐색
        for snapshot in reversed(history[-3:]):
            # repos와 lag_repos 모두에서 마지막 상태를 찾음
            if repo_name in snapshot.get("repos", {}):
                return snapshot["repos"][repo_name]
            if repo_name in snapshot.get("lag_repos", {}):
                return snapshot["lag_repos"][repo_name]
        return DEFAULT_ID

    def project(self, states: Dict[str, str]) -> Dict[str, str]:
        return states

    def anchor_commit(self, anchor_id: str, parent_anchor_id: Optional[str], 
                      repos: Dict[str, str], lag_repos: Dict[str, str], 
                      message: str, apply: bool = False) -> str:
        """AnchorCommitModel을 사용하여 시대를 고정"""
        history = self.load_history()
        self_parent_state = self.resolve(self.name)
        model = AnchorCommitModel(
            anchor_id=anchor_id,
            parent_anchor_id=parent_anchor_id or DEFAULT_ID,
            parent_commit_id=self_parent_state,
            repos=repos,
            lag_repos=lag_repos
        )

        ## Apply 시점에만 전역 레지스트리 업데이트
        if apply:
            full_history = history + [json.loads(model.to_json())]
            with open(self.registry_file, "w") as f:
                json.dump({"history": full_history}, f, indent=2)
        return super().local_commit(anchor_id, parent_anchor_id, self_parent_state, message, apply)

def anchor_commit_protocol(repos: List[RepoNode], anchor: AnchorNode, message: str, apply: bool = False):
    """@protocol: era-based alignment cycle over execution results"""
    print(f"--- Era-based Alignment Cycle Initiated ({'APPLY' if apply else 'DRY-RUN'}) ---")
    
    ## 히스토리에서 부모 앵커 식별
    history = anchor.load_history()
    last_snapshot = history[-1] if history else None
    parent_anchor_id = last_snapshot["anchor_id"] if last_snapshot else DEFAULT_ID

    new_anchor_id = next_id()

    ## 이번 정렬에 참여한 노드들 실행
    current_aligned_states = {}
    for r in repos:
        parent_state = anchor.resolve(r.name)
        ## Runner를 통해 실제 해시 획득
        current_aligned_states[r.name] = r.local_commit(new_anchor_id, parent_anchor_id, parent_state, message, apply)

    ## lag_repos 계산
    lag_repos = {}
    if last_snapshot:
        prev_total = {**last_snapshot.get("repos", {}), **last_snapshot.get("lag_repos", {})}
        for name, last_hash in prev_total.items():
            if name not in current_aligned_states and name != anchor.name:
                lag_repos[name] = last_hash

    ## 앵커 고정 (Self 포함)
    final_anchor_hash = anchor.anchor_commit(
        new_anchor_id, parent_anchor_id, current_aligned_states, lag_repos, message, apply
    )
    print(f"## Era Fixed: {new_anchor_id} (Aligned: {len(current_aligned_states)}, Lagged: {len(lag_repos)})")
