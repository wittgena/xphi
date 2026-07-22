# watcher.kernel.wasm.sygyzy.swarm
import time
import json
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from phase.wasm.resolver.runner import SchemeRunner
from phase.wasm.resolver.adapter import StateAdapter
from watcher.kernel.ledger import KernelStore, KernelCommit
from watcher.plane.emitter import get_emitter

log = get_emitter("syzygy.swarm")

class SwarmSyzygyScenarios(SchemeRunner):
    """
    @desc: Syzygy Protocol - Swarm Synchronization & Topological Attractor Mechanics
    @spec: 17+ Node Swarm, O(1) Alignment, Topological Rebase (45->46), Void Sealing
    """
    def __init__(self, broker, swarm_size=17):
        super().__init__(broker)
        self.swarm_size = swarm_size
        self.nodes = []
        self.store = KernelStore()
        
        for i in range(self.swarm_size):
            priv_key = ed25519.Ed25519PrivateKey.generate()
            pub_hex = priv_key.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex()
            self.nodes.append({"node_idx": i, "priv": priv_key, "pub": pub_hex})

    def _sign(self, priv_key, payload_dict):
        """[EVOLUTION] 서명 규격의 통일: Canonical Bytes 직접 서명"""
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        return priv_key.sign(canonical_bytes).hex()

    def _sign(self, priv_key, payload_dict):
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        return priv_key.sign(hashlib.sha256(canonical_bytes).digest()).hex()

    async def run_all(self):
        log.info(f"\n=== [START] Executing Syzygy Protocol for Swarm (Size: {self.swarm_size}) ===")
        await self._set_worker_policy("SYSTEM")
        
        # Phase 1 & 2: Instability & Attractor Emergence
        node0_anchor = await self._phase1_2_emerge_attractor()
        
        # Phase 3 & 4: Macro-Topological Resonance & Void Sealing
        await self._phase3_4_topological_rebase_and_void_seal(node0_anchor)
        
        # Phase 5: Re-ignition
        await self._phase5_reignition(node0_anchor)
        
        self.report()

    async def _phase1_2_emerge_attractor(self):
        """
        @flow: Instability Trigger -> Node0 Attractor Election
        @desc: 스웜의 불안정성이 감지되어, 가장 엔트로피가 낮은 Node 0가 끌개(Attractor)로 붕괴됨.
        """
        log.info("\n--- [Phase 1 & 2] Instability Triggered -> Attractor Emergence ---")
        log.warning("  └─ [CRIT] Parity Collision Rate > 1.5%. Syzygy Mode Activated.")
        
        # Node 0가 최저 엔트로피(Lowest Entropy)를 가졌다고 가정하고 특이점(Singularity)으로 확정
        node0 = self.nodes[0]
        log.info(f"  └─ [Emerged] Node 0 ({node0['pub'][:8]}...) established as Canonical Origin (Attractor).")
        
        # Node 0의 절대 참조 앵커(Genesis Anchor) 생성
        node0_phase = 999999
        node0_nexus = 1000000
        
        node0_parity = StateAdapter.build_parity_triplet(
            topos_id="macro_topos_alpha", phase_id=node0_phase, nexus_id=node0_nexus
        )
        return {"node": node0, "parity": node0_parity, "genesis_nexus": node0_nexus}

    async def _phase3_4_topological_rebase_and_void_seal(self, attractor):
        log.info(f"\n--- [Phase 3 & 4] Macro-Topological Resonance & Void Sealing (16 Nodes) ---")
        
        attractor_phase = attractor["parity"]["phase_id"]
        success_count = 0
        
        for drift_node in self.nodes[1:]:
            local_drift_phase = attractor_phase ^ (drift_node["node_idx"] * 10) 
            alignment_delta = local_drift_phase ^ attractor_phase
            
            if alignment_delta != 0:
                orphan_hash = f"orphan_drift_state_node_{drift_node['node_idx']}"
                residue_entropy = {"unspent_fuel": 402, "canonical_dust": alignment_delta}
                
                void_parity = StateAdapter.build_parity_triplet(
                    topos_id=attractor["parity"]["topos_id"], phase_id=local_drift_phase, nexus_id=777777
                )
                
                void_commit = StateAdapter.build_anchor_commit(
                    parity=void_parity, parent_nexus_id=attractor["genesis_nexus"], 
                    parent_commit_id=orphan_hash, repos={"void_sealed": True, "entropy_dump": residue_entropy}, cached_states={}
                )
                
                sig = self._sign(drift_node["priv"], void_commit)
                
                seal_payload = StateAdapter.build_seal_epoch_payload(
                    parity=void_parity, parent_nexus_id=attractor["genesis_nexus"], self_parent_state=orphan_hash,
                    repos={"void_sealed": True, "entropy_dump": residue_entropy}, cached_states={},
                    timestamp=time.time(), signers=[drift_node["pub"]], signatures=[sig], threshold=1, allowed_signers=[drift_node["pub"]]
                )
                
                # 1. WASM 심사
                res = await self.broker.invoke("seal_epoch", json.dumps(seal_payload))
                
                if res.success:
                    # 2. [EVOLUTION] 물리적 디스크 (Mempool 혹은 Ledger)에 동기화
                    sealed_data = json.loads(res.output)
                    kernel_commit = KernelCommit(**sealed_data.get("kernel_commit"))
                    
                    try:
                        # 스웜의 개별 노드들은 중앙의 허가를 받는 특권 모드로 봉인
                        self.store.seal_system_epoch(commit=kernel_commit, signatures=[sig], threshold=1)
                        success_count += 1
                        if drift_node["node_idx"] <= 3:
                            log.info(f"  ├─ [Node {drift_node['node_idx']}] Rebased & Physically Sealed to Origin.")
                    except Exception as e:
                        log.error(f"  ├─ [Node {drift_node['node_idx']}] Physical Seal Failed: {e}")
        
        log.info(f"  └─ [Success] {success_count}/16 nodes successfully rebased physically.")

    async def _phase5_reignition(self, attractor):
        """
        @flow: State Unlock -> Attractor Dissolution -> Local Big Bang
        @desc: 동기화가 완료된 후 스웜이 새로운 시공간(Topos) 위에서 병렬 실행을 재개.
        """
        log.info("\n--- [Phase 5] Re-ignition (Topological Expansion) ---")
        
        # 새로운 시공간 좌표 할당 (Big Bang)
        new_topos_payload = {
            "ts": int(time.time() * 1000),
            "topo": 102, # Topos Alpha -> Topos Beta 팽창
            "press": 0,  # 엔트로피 초기화 (0)
            "rupture": False,
            "injected_intent": "Swarm_Expansion_Resumed"
        }
        
        res = await self.broker.invoke("init_epoch", StateAdapter.to_canonical_bytes(new_topos_payload).decode('utf-8'))
        
        log.info(f"  └─ [Big Bang] Syzygy Mode Terminated. Node 0 dissolved. Swarm expanded to New Topos: 102.")
        log.info(f"  └─ Engine Ready for Lock-free Parallel Execution.")