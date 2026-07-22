# arch.topos.anchor.protocol
"""
@module: arch.topos.anchor.protocol
@desc: 
- Protocol for orchestrating Era-based Alignment cycles.
- Synchronizes local repository states with the WASM engine.
- [EVOLUTION] Enforces Zero Trust architecture. Final state commits to the Ledger
  must pass through the KernelStore's Ring 0 Multi-Sig Privileged Wrapper.
"""
import json
import time
import asyncio
from typing import List, Dict, Any

from arch.crypto.signer import NodeSigner
from arch.topos.anchor.node import ActorNode, EpochManager
from phase.wasm.broker import WasmBroker
from phase.wasm.resolver.adapter import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("anchor.protocol", phase="SYSTEM")

async def anchor_git_commit_async(
    repos: List[ActorNode], 
    anchor: EpochManager, 
    broker: WasmBroker, 
    message: str, 
    apply: bool = False
) -> None:
    """
    @protocol: Era-based Alignment (Asynchronous)
    @desc: Executes the alignment cycle across multiple topological nodes. Coordinates 
           physical state commits and validates them through the WASM FFI layer.
           The physical finalization is guarded by a Multi-Sig privilege wrapper.

    Args:
        repos: List of ActorNodes representing the target repositories to align.
        anchor: The EpochManager responsible for global state tracking.
        broker: The WasmBroker instance for FFI communication.
        message: The commit message for the alignment cycle.
        apply: If True, physically persists the state. If False, performs a dry-run.
    """
    mode = "APPLY" if apply else "DRY-RUN"
    log.info(f"## Era-based Alignment Cycle Initiated ({mode})")
    current_ts = int(time.time() * 1000)

    # =========================================================================
    # PHASE 1: Parity Triplet Generation (WASM FFI Communication)
    # =========================================================================
    init_req: Dict[str, Any] = {
        "ts": current_ts,
        "topo": 1,
        "press": len(repos),
        "rupture": False,
        "injected_tick": None
    }
    
    log.info("[Protocol] Requesting Parity Triplet from WASM Engine...")
    init_payload_str = StateAdapter.to_canonical_bytes(init_req).decode('utf-8')
    init_res = await broker.invoke("init_epoch", init_payload_str)
    
    if not init_res.success:
        log.error(f"Failed to initialize Epoch via WASM: {str(init_res.error)}")
        return
        
    parity = json.loads(init_res.output)
    nexus_id = parity.get("nexus_id")
    log.info(f"[Protocol] Nexus ID Generated: {nexus_id}")

    # =========================================================================
    # PHASE 2: Physical Commits (Thread Offloading)
    # =========================================================================
    history = anchor.load_history()
    last_snapshot = history[-1] if history else None
    parent_nexus_id = last_snapshot.get("parity", {}).get("nexus_id", 0) if last_snapshot else 0

    current_aligned_states: Dict[str, str] = {}
    
    # Offload physical Git operations to background threads to prevent blocking the async event loop
    for r in repos:
        parent_state = anchor.resolve(r.name)
        commit_hash = await asyncio.to_thread(
            r.inscribe,
            nexus_id,
            parent_nexus_id,
            parent_state,
            message,
            apply
        )
        current_aligned_states[r.name] = commit_hash

    # =========================================================================
    # PHASE 3: Epoch Sealing (Cryptographic Signatures & WASM)
    # =========================================================================
    cached_states: Dict[str, str] = {}
    if last_snapshot:
        prev_total = {**last_snapshot.get("repos", {}), **last_snapshot.get("cached_states", {})}
        for name, last_hash in prev_total.items():
            if name not in current_aligned_states and name != anchor.name:
                cached_states[name] = last_hash

    # Retrieve Singleton Identity Manager (auto-loads ENV or SSH keys)
    signer = NodeSigner.get_instance()

    # Construct the raw AnchorCommit data for signature verification
    anchor_commit_dict = StateAdapter.build_anchor_commit(
        parity=parity,
        parent_nexus_id=parent_nexus_id,
        parent_commit_id=anchor.resolve(anchor.name),
        repos=current_aligned_states,
        cached_states=cached_states
    )
    
    # Convert to Canonical JSON (JCS) bytes to guarantee deterministic hashing
    canonical_bytes = StateAdapter.to_canonical_bytes(anchor_commit_dict)
    
    # Execute Python-side SHA256 hashing followed by Ed25519 signature
    signature_hex = signer.sign_anchor_commit(canonical_bytes)

    # Build the final payload for the WASM engine (Multi-Sig schema applied)
    seal_payload = StateAdapter.build_seal_epoch_payload(
        parity=parity,
        parent_nexus_id=parent_nexus_id,
        self_parent_state=anchor.resolve(anchor.name),
        repos=current_aligned_states,
        cached_states=cached_states,
        timestamp=float(current_ts),
        signers=[signer.pubkey_hex],     # Multi-Sig array schema applied
        signatures=[signature_hex],      # Multi-Sig array schema applied
        threshold=1                      # Single active signer threshold
    )

    log.info("[Protocol] Sealing Epoch cryptographically...")
    seal_payload_str = StateAdapter.to_canonical_bytes(seal_payload).decode('utf-8')
    seal_res = await broker.invoke("seal_epoch", seal_payload_str)
    
    if not seal_res.success:
        log.critical(f"## Epoch Seal Rejected by WASM: {str(seal_res.error)}")
        if apply:
            log.warning("Physical commits were made, but WASM seal failed. Requires subsequent sync to recover.")
        return

    # =========================================================================
    # PHASE 4: State Finalization & Store Registration (Ring 0 Privileged Flow)
    # =========================================================================
    if apply:
        sealed_data = json.loads(seal_res.output)
        kernel_commit_data = sealed_data.get("kernel_commit")
        
        # Inline import to prevent circular dependencies at module initialization
        from watcher.kernel.ledger import KernelCommit, LedgerRole
        from dataclasses import asdict
        
        commit_obj = KernelCommit(**kernel_commit_data)
        
        # [EVOLUTION] Enforce Zero Trust: Delegate persistence to the Store based on Consensus Role
        if hasattr(anchor.store, 'role') and anchor.store.role == LedgerRole.FOLLOWER:
            log.warning("[Protocol] Node is FOLLOWER. Proposing Epoch Seal to Mempool instead of direct disk write.")
            # FOLLOWER delegates raw commit proposal to mempool
            anchor.store._put_object("commit_proposal", asdict(commit_obj))
        else:
            try:
                # [EVOLUTION] LEADER must pass the Ring 0 Multi-Sig Privilege Wrapper
                # Submit the identical signature validated by WASM to the physical Ledger for execution
                commit_hash = anchor.store.seal_system_epoch(
                    commit=commit_obj, 
                    signatures=[signature_hex], 
                    threshold=1
                )
                
                # If privilege wrapper passes, finalize the Merkle Head updates
                anchor.store.update_head("global_era_anchor", sealed_data["anchor_result"]["commit_hash"])
                
                for repo_name, repo_hash in current_aligned_states.items():
                    anchor.store.update_head(repo_name, repo_hash)
                    
            except PermissionError as pe:
                log.critical(f"Physical state finalization aborted due to Kernel Store privilege denial: {pe}")
                return
            
        # Append to Legacy Registry for backwards compatibility
        full_history = history + [seal_payload]
        try:
            if hasattr(anchor.store, 'db') and anchor.store.db is not None:
                anchor.store.db[anchor.registry_key] = json.dumps({"history": full_history}).encode('utf-8')
        except Exception as e:
            log.warning(f"Failed to update legacy registry: {e}")

    log.info(f"## Era Fixed. Nexus: {nexus_id} (Aligned: {len(current_aligned_states)}, Lagged: {len(cached_states)})")