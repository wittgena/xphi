# topos.arch.repo.protocol
## @lineage: arch.model.repo.protocol
"""
@anchor: synchronization boundary across nodes (era frame)
@commit: parent linkage under anchor-constrained selection
@state.partition: aligned vs lag (no non-existence)
"""
import json
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Callable
from phase.runtime.contract.event.next import next_id
from topos.arch.repo.node import RepoNode, AnchorNode
from topos.bound.resolver import resolve_path

DEFAULT_ID = "0000000"

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
