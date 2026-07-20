# arch.topos.anchor.protocol
import json
import time
import asyncio
from typing import List

from arch.topos.anchor.node import ActorNode, EpochManager
from phase.wasm.broker import WasmBroker
from watcher.plane.emitter import get_emitter

log = get_emitter("protocol.commit")

async def anchor_git_commit_async(
    repos: List[ActorNode], 
    anchor: EpochManager, 
    broker: WasmBroker, 
    message: str, 
    apply: bool = False
):
    """@protocol: WasmBroker를 경유하여 비동기로 Era-based Alignment 수행"""
    log.info(f"## Era-based Alignment Cycle Initiated ({'APPLY' if apply else 'DRY-RUN'})")
    current_ts = int(time.time() * 1000)

    # ---------------------------------------------------------
    # [Phase 1] Parity Triplet 일괄 발급 (WASM 통신)
    # ---------------------------------------------------------
    init_req = {
        "ts": current_ts,
        "topo": 1,
        "press": len(repos),
        "rupture": False,
        "injected_tick": None
    }
    
    log.info("[Protocol] Requesting Parity Triplet from WASM Engine...")
    init_res = await broker.invoke("InitEpoch", json.dumps(init_req))
    if not init_res.success:
        log.error(f"Failed to initialize Epoch via WASM: {init_res.error.message}")
        return
        
    parity = json.loads(init_res.output)
    nexus_id = parity.get("nexus_id")
    log.info(f"[Protocol] Nexus ID Generated: {nexus_id}")

    # ---------------------------------------------------------
    # [Phase 2] 물리적 커밋 (Thread Offloading)
    # ---------------------------------------------------------
    history = anchor.load_history()
    # 레거시 데이터 호환성 방어: 문자열 포맷일 경우 0으로 롤백
    last_snapshot = history[-1] if history else None
    parent_nexus_id = last_snapshot.get("parity", {}).get("nexus_id", 0) if last_snapshot else 0

    current_aligned_states = {}
    for r in repos:
        parent_state = anchor.resolve(r.name)
        # Git Commit(동기)은 블로킹을 방지하기 위해 백그라운드 스레드에서 수행
        commit_hash = await asyncio.to_thread(
            r.inscribe,
            nexus_id,
            parent_nexus_id,
            parent_state,
            message,
            apply
        )
        current_aligned_states[r.name] = commit_hash

    # ---------------------------------------------------------
    # [Phase 3] Epoch Seal (WASM 통신)
    # ---------------------------------------------------------
    cached_states = {}
    if last_snapshot:
        prev_total = {**last_snapshot.get("repos", {}), **last_snapshot.get("cached_states", {})}
        for name, last_hash in prev_total.items():
            if name not in current_aligned_states and name != anchor.name:
                cached_states[name] = last_hash

    seal_payload = {
        "parity": parity,
        "parent_nexus_id": parent_nexus_id,
        "self_parent_state": anchor.resolve(anchor.name),
        "repos": current_aligned_states,
        "cached_states": cached_states,
        "timestamp": current_ts,
        "pubkey": "mock_pubkey",
        "signature": "mock_signature" # 실제 환경에서는 Ed25519 서명 주입
    }

    log.info("[Protocol] Sealing Epoch cryptographically...")
    seal_res = await broker.invoke("SealEpoch", json.dumps(seal_payload, sort_keys=True))
    
    if not seal_res.success:
        log.critical(f"## Epoch Seal Rejected by WASM: {seal_res.error.message}")
        return

    # ---------------------------------------------------------
    # [Phase 4] 상태 확정 및 레거시 레지스트리 갱신
    # ---------------------------------------------------------
    if apply:
        sealed_data = json.loads(seal_res.output)
        kernel_commit_data = sealed_data.get("kernel_commit")
        
        from watcher.kernel.store import KernelCommit
        commit_obj = KernelCommit(**kernel_commit_data)
        
        # KernelStore에 상태 등록
        anchor.store.save_kernel(commit_obj)
        anchor.store.update_head("global_era_anchor", sealed_data["anchor_result"]["commit_hash"])
        for repo_name, repo_hash in current_aligned_states.items():
            anchor.store.update_head(repo_name, repo_hash)
            
        # Legacy Registry 호환성 (히스토리 어펜드)
        full_history = history + [seal_payload]
        try:
            anchor.store.db[anchor.registry_key] = json.dumps({"history": full_history}).encode('utf-8')
        except Exception as e:
            log.warning(f"Failed to update legacy registry: {e}")

    log.info(f"## Era Fixed. Nexus: {nexus_id} (Aligned: {len(current_aligned_states)}, Lagged: {len(cached_states)})")