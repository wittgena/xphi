# xphi.state.phase.commit
import json
import time
import hashlib
import asyncio
from typing import Dict, List, Optional, Callable, Any
from pathlib import Path
from dataclasses import asdict

from xphi.node.runtime.wasm.broker import WasmBroker
from xphi.node.space.bind.resolver import resolve_path
from xphi.arch.bound.adapter.pta import SignStateAdapter
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter
from xphi.state.anchor.store import AnchorStore, ToposBlob, PhaseCommit, AnchorRole

log = get_emitter("phase.commit", phase="COMMIT")

WASM_LOCK_FILENAME = "wasm.lock.json"

def _build_wasm_chain_state(prev_chain_hash: str, nexus_id: int, anchor_parent: str) -> tuple[str, str, Dict[str, str]]:
    """@role: Side-effect-free WASM hash scanner & chain calculator"""
    manifest: Dict[str, str] = {}
    try:
        time_dir = resolve_path("time")
        if time_dir.exists():
            for wf in sorted(time_dir.rglob("*.wasm")):
                if wf.is_file():
                    rel_key = wf.relative_to(time_dir).as_posix()
                    manifest[rel_key] = hashlib.sha256(wf.read_bytes()).hexdigest()
    except Exception as e:
        log.warning(f"[Chain] Failed to scan WASM directory (fallback to empty): {e}")

    canonical_manifest = json.dumps(manifest, separators=(',', ':'), sort_keys=True).encode('utf-8')
    wasm_root = hashlib.sha256(canonical_manifest).hexdigest()
    
    chain_input = f"{prev_chain_hash}:{nexus_id}:{anchor_parent}:{wasm_root}".encode('utf-8')
    chain_hash = hashlib.sha256(chain_input).hexdigest()
    return wasm_root, chain_hash, manifest


class Attractor:
    """@role: execution unit + lineage inscription node"""
    def __init__(self, name: str, path: str, runner: Callable):
        self.name = name
        self.path = Path(path).expanduser().resolve()
        self.runner = runner
        self.store = AnchorStore()

    def inscribe(
        self, 
        nexus_id: int, 
        parent_nexus_id: int, 
        parent_commit_id: str, 
        message: str, 
        apply: bool = False,
        tag: Optional[str] = None,
        chain_hash: Optional[str] = None,
        wasm_root: Optional[str] = None
    ) -> str:
        model_dict = {
            "nexus_id": nexus_id,
            "parent_nexus_id": parent_nexus_id,
            "parent_commit_id": parent_commit_id
        }
        
        if tag:
            model_dict["version_tag"] = tag
        if chain_hash:
            model_dict["chain_hash"] = chain_hash
        if wasm_root:
            model_dict["wasm_root"] = wasm_root

        json_payload = json.dumps(model_dict, separators=(',', ':'), sort_keys=True)
        full_message = f"{message}\n\n{json_payload}"
        new_commit_id = self.runner(self.path, full_message, apply, tag=tag)

        if apply:
            blob = ToposBlob(
                action=f"align.commit::{self.name}",
                from_state=parent_commit_id,
                to_state=new_commit_id,
                tension=1.0, 
                details=json_payload
            )
            self.store.save_transition(blob)

        tag_info = f" | Tag: {tag}" if tag else ""
        print(f"  └─ [{self.name}] Inscribed. Nexus: {nexus_id} | State: {new_commit_id[:7]}{tag_info}")
        return new_commit_id

class EpochManager(Attractor):
    """@role: boundary (synchronization frame) + era manager"""
    ERA_DEPTH = 3

    def __init__(self, name: str, path: str, runner: Callable):
        super().__init__(name, path, runner)
        self.registry_key = f"legacy_registry:{self.name}".encode('utf-8')

    def load_history(self) -> List[Dict]:
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
        
        return self.store.get_head_hash(repo_name) or "genesis"

    def project(self, states: Dict[str, str]) -> Dict[str, str]:
        return states

async def anchor_commit(
    repos: List[Attractor], 
    anchor: EpochManager, 
    broker: WasmBroker, 
    message: str, 
    apply: bool = False,
    tag: Optional[str] = None
) -> None:
    mode = "APPLY" if apply else "DRY-RUN"
    log.info(f"## Era-based Alignment Cycle Initiated ({mode})")
    current_ts = int(time.time() * 1000)
    
    init_req: Dict[str, Any] = {
        "ts": current_ts,
        "topo": 1,
        "press": len(repos),
        "rupture": False,
        "injected_tick": None
    }
    
    if tag:
        init_req["version_tag"] = tag
        log.info(f"[Protocol] Global Version Tag '{tag}' will be cryptographically sealed.")
    
    log.info("[Protocol] Requesting Parity Triplet from WASM Engine...")
    init_payload_str = StateAdapter.to_canonical_bytes(init_req).decode('utf-8')
    init_res = await broker.invoke("init_epoch", init_payload_str)
    
    if not init_res.success:
        log.error(f"Failed to initialize Epoch via WASM: {str(init_res.error)}")
        return
        
    parity = json.loads(init_res.output)
    nexus_id = parity.get("nexus_id")
    log.info(f"[Protocol] Nexus ID Generated: {nexus_id}")

    history = anchor.load_history()
    last_snapshot = history[-1] if history else None
    parent_nexus_id = last_snapshot.get("parity", {}).get("nexus_id", 0) if last_snapshot else 0
    anchor_parent_state = anchor.resolve(anchor.name)

    prev_chain_hash = last_snapshot.get("chain_hash", "genesis") if last_snapshot else "genesis"
    wasm_root, chain_hash, wasm_manifest = _build_wasm_chain_state(
        prev_chain_hash=prev_chain_hash,
        nexus_id=nexus_id,
        anchor_parent=anchor_parent_state
    )

    if apply and wasm_manifest:
        try:
            lock_payload = {
                "nexus_id": nexus_id,
                "parent_nexus_id": parent_nexus_id,
                "prev_chain_hash": prev_chain_hash,
                "chain_hash": chain_hash,
                "wasm_root": wasm_root,
                "artifacts": wasm_manifest,
                "timestamp": current_ts
            }
            lock_path = resolve_path("time") / WASM_LOCK_FILENAME
            lock_path.write_text(json.dumps(lock_payload, indent=2, sort_keys=True), encoding="utf-8")
        except Exception as e:
            log.warning(f"[Chain] Failed to write {WASM_LOCK_FILENAME}: {e}")

    current_aligned_states: Dict[str, str] = {}
    
    for r in repos:
        parent_state = anchor.resolve(r.name)
        commit_hash = await asyncio.to_thread(
            r.inscribe,
            nexus_id=nexus_id,
            parent_nexus_id=parent_nexus_id,
            parent_commit_id=parent_state,
            message=message,
            apply=apply,
            tag=tag,
            chain_hash=chain_hash,
            wasm_root=wasm_root
        )
        current_aligned_states[r.name] = commit_hash

    cached_states: Dict[str, str] = {}
    if last_snapshot:
        prev_total = {**last_snapshot.get("repos", {}), **last_snapshot.get("cached_states", {})}
        for name, last_hash in prev_total.items():
            if name not in current_aligned_states and name != anchor.name:
                cached_states[name] = last_hash

    anchor_commit_dict = StateAdapter.build_anchor_commit(
        parity=parity,
        parent_nexus_id=parent_nexus_id,
        parent_commit_id=anchor_parent_state,
        repos=current_aligned_states,
        cached_states=cached_states
    )
    
    signature_hex = SignStateAdapter.sign_state_payload(anchor_commit_dict)
    current_pubkey = SignStateAdapter.get_signer_pubkey()
    seal_payload = StateAdapter.build_seal_epoch_payload(
        parity=parity,
        parent_nexus_id=parent_nexus_id,
        self_parent_state=anchor_parent_state,
        repos=current_aligned_states,
        cached_states=cached_states,
        timestamp=float(current_ts),
        signers=[current_pubkey],
        signatures=[signature_hex],
        threshold=1
    )

    log.info("[Protocol] Sealing Epoch cryptographically...")
    seal_payload_str = StateAdapter.to_canonical_bytes(seal_payload).decode('utf-8')
    seal_res = await broker.invoke("seal_epoch", seal_payload_str)
    
    if not seal_res.success:
        log.critical(f"## Epoch Seal Rejected by WASM: {str(seal_res.error)}")
        if apply:
            log.warning("Physical commits were made, but WASM seal failed. Requires subsequent sync to recover.")
        return

    if apply:
        sealed_data = json.loads(seal_res.output)
        kernel_commit_data = sealed_data.get("kernel_commit")
        
        if "anchor_result" in sealed_data:
            seal_payload["repos"][anchor.name] = sealed_data["anchor_result"]["commit_hash"]

        seal_payload["chain_hash"] = chain_hash
        seal_payload["wasm_root"] = wasm_root
        full_history = history + [seal_payload]
        try:
            if hasattr(anchor.store, 'db') and anchor.store.db is not None:
                anchor.store.db[anchor.registry_key] = json.dumps({"history": full_history}).encode('utf-8')
        except Exception as e:
            log.warning(f"Failed to update legacy registry: {e}")
        
        commit_obj = PhaseCommit(**kernel_commit_data)
        if not commit_obj.parent_hash:
            commit_obj.parent_hash = anchor.store.get_head_hash("global_era_anchor") or "genesis"
        
        if hasattr(anchor.store, 'role') and anchor.store.role == AnchorRole.PROPOSER:
            log.warning("[Protocol] Node is FOLLOWER. Proposing Epoch Seal to Mempool instead of direct disk write.")
            anchor.store._put_object("commit_proposal", asdict(commit_obj))
        else:
            try:
                commit_hash = anchor.store.seal_system_epoch(
                    commit=commit_obj, 
                    signatures=[signature_hex], 
                    threshold=1
                )
                
                anchor.store.update_head("global_era_anchor", commit_hash)
                for repo_name, repo_hash in current_aligned_states.items():
                    anchor.store.update_head(repo_name, repo_hash)
                    
            except PermissionError as pe:
                log.critical(f"Physical state finalization aborted due to Kernel Store privilege denial: {pe}")
                return

    log.info(f"## Era Fixed. Nexus: {nexus_id} | Chain: {chain_hash[:8]} (Aligned: {len(current_aligned_states)}, Lagged: {len(cached_states)})")