# arch.topos.node.anchor
## @lineage: phase.gov.node.anchor
"""
@node: execution-capable self (not tied to repo)
@anchor: synchronization boundary across nodes (era frame)
@commit: parent linkage under anchor-constrained selection
"""
import json
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Callable

from arch.contract.event.next import next_id
from arch.contract.schema.repo import RepoCommit, AnchorCommit
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
        """Models and inscribes the lineage of the current generation."""
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

        ## Inscribe self (EpochManager's own repo)
        new_anchor_commit_id = super().inscribe(anchor_id, parent_anchor_id, self_parent_state, message, apply)

        ## Seal KernelStore and maintain history compatibility
        if apply:
            commit = KernelCommit(
                stream_id="global_era_anchor",
                executable_payload=model.to_json(),
                tension_at_seal=len(repos),
                blob_hashes=list(repos.values()),
                parent_hash=parent_anchor_id or DEFAULT_ID
            )
            
            ## Record to the topological ledger (KernelStore)
            signature = self.store.save_kernel(commit)
            self.store.update_head("global_era_anchor", signature)
            for repo_name, repo_commit_hash in repos.items():
                self.store.update_head(repo_name, repo_commit_hash)

            full_history = history + [json.loads(model.to_json())]
            try:
                ## CLI environments operate on the main loop (Leader), making direct writes safe
                self.store.db[self.registry_key] = json.dumps({"history": full_history}).encode('utf-8')
            except Exception as e:
                ## Defensive fallback in case it's forcibly executed on a Follower node
                print(f"[EpochManager] Warning: Could not update legacy registry: {e}")

        return new_anchor_commit_id