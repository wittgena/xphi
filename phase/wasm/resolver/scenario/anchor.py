# phase.wasm.resolver.scenario.anchor
import time
import json
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from phase.wasm.resolver.runner import SchemeRunner
from watcher.plane.emitter import get_emitter
from phase.wasm.resolver.adapter import StateAdapter

log = get_emitter("scenario.anchor")

class TrustlessEpochBase(SchemeRunner):
    def __init__(self, broker, scenario_name: str):
        super().__init__(broker)
        self.scenario_name = scenario_name
        self.master_key = ed25519.Ed25519PrivateKey.generate()
        self.master_pubhex = self.master_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

    def _sign_payload(self, private_key, commit_dict: dict) -> str:
        """
        [개선됨] 일반 json.dumps를 제거하고 StateAdapter의 RFC 8785 JCS 변환을 사용하여
        Rust의 serde_jcs와 100% 동일한 결정론적 바이트 배열 및 해시를 보장합니다.
        """
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest()
        return private_key.sign(commit_hash.encode('utf-8')).hex()

    async def execute_anchor_lifecycle(self, topo: int, press: int, rupture: bool):
        log.info(f"\n=== [Lifecycle START] {self.scenario_name} ===")
        
        log.info(f"--- [Flow 1] Initialization: Requesting Parity Triplet ---")
        current_ts = int(time.time() * 1000)
        # 단순 FFI 요청이므로 오버엔지니어링 방지를 위해 dict 유지
        init_req = {"ts": current_ts, "topo": topo, "press": press, "rupture": rupture, "injected_tick": None}
        
        res = await self.broker.invoke("init_epoch", json.dumps(init_req))
        if not res.success:
            log.error(f"  [FAIL] init_epoch Failed: {res.error}")
            self.fail_count += 1
            return
            
        parity_triplet = json.loads(res.output)
        log.info(f"  └─ Generated Nexus ID: {parity_triplet.get('nexus_id')}")
        
        log.info(f"--- [Flow 2] Inscription: Gathering Local Node States ---")
        repos = await self.hook_inscribe_nodes(parity_triplet)
        
        log.info(f"--- [Flow 3] Sealing: Cryptographic Epoch Alignment ---")
        seal_payload = await self.hook_seal_epoch(parity_triplet, repos, current_ts)
        
        seal_res = await self.broker.invoke("seal_epoch", json.dumps(seal_payload))
        if not seal_res.success:
            log.error(f"  [FAIL] seal_epoch Failed: {seal_res.error}")
            self.fail_count += 1
            return
            
        sealed_data = json.loads(seal_res.output)
        log.info("  └─ Epoch Sealed Successfully.")

        log.info(f"--- [Flow 4] Transition: Validating & Applying State Evolution ---")
        anchor_result = sealed_data.get("anchor_result", sealed_data)
        commit_hash = anchor_result.get("commit_hash", "mock_fallback_hash_0x99")
        
        state_node_struct = await self.hook_build_phase_root(commit_hash, repos)
        
        # [개선됨] 스키마에 정의되지 않은 임의 필드를 제거하고 어댑터를 통해 안전한 컨텍스트 생성
        evo_ctx = StateAdapter.build_evolution_context(
            phase_root=state_node_struct,
            external_rules=[]
        )
        transition_payload = StateAdapter.build_transition_payload(
            intent_action="commit_era",
            intent_payload=anchor_result,
            evolution_ctx=evo_ctx
        )
        
        await self._run_case(f"{self.scenario_name} (Flow 4): Execute Transition", "execute_transition", transition_payload, expected_success=True)

        log.info(f"--- [Flow 5] Finality: Zero-Trust Parity & Recovery Verification ---")
        t_id_low32 = int(parity_triplet["topos_id"].split('_')[-1]) if '_' in parity_triplet["topos_id"] else 0
        # 단순 검증 요청이므로 dict 유지
        parity_req = {
            "topos_id_low32": t_id_low32,
            "phase_id": parity_triplet["phase_id"],
            "nexus_id": parity_triplet["nexus_id"]
        }
        await self._run_case(f"{self.scenario_name} (Flow 5): Verify Parity Completeness", "verify_parity", parity_req, expected_success=True)

    async def hook_inscribe_nodes(self, parity_triplet: dict) -> dict: raise NotImplementedError
    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, timestamp: int) -> dict: raise NotImplementedError
    async def hook_build_phase_root(self, commit_hash: str, repos: dict) -> dict: raise NotImplementedError

class SwarmConsensusScenario(TrustlessEpochBase):
    def __init__(self, broker):
        super().__init__(broker, "AI Agent Swarm Consensus")
        
    async def hook_inscribe_nodes(self, parity_triplet: dict) -> dict:
        nexus_id = parity_triplet["nexus_id"]
        agents = {"CodeAgent": "hash-code-v1", "SecurityAgent": "hash-sec-v1"}
        for agent_name, state_hash in agents.items():
            agent_key = ed25519.Ed25519PrivateKey.generate()
            pubhex = agent_key.public_key().public_bytes(encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw).hex()
            
            # [개선됨] 하드코딩 방식을 버리고 StateAdapter 빌더 적용
            repo_commit = StateAdapter.build_repo_commit(
                nexus_id=nexus_id, 
                parent_nexus_id=0, 
                parent_commit_id=state_hash
            )
            
            payload = StateAdapter.build_inscribe_payload(
                nexus_id=nexus_id,
                parent_nexus_id=None,
                parent_commit_id=state_hash,
                pubkey=pubhex,
                signature=self._sign_payload(agent_key, repo_commit)
            )
            await self._run_case(f"Swarm: Inscribe {agent_name}", "inscribe_actor", payload, expected_success=True)
        return agents

    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, timestamp: int) -> dict:
        # [개선됨] 구버전의 sort_dict_recursive를 완전히 제거하고 StateAdapter로 생성
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, 
            parent_nexus_id=0, 
            parent_commit_id="swarm-base",
            repos=repos, 
            cached_states={}
        )
        sig_hex = self._sign_payload(self.master_key, anchor_commit)
        
        return StateAdapter.build_seal_epoch_payload(
            parity=parity_triplet,
            parent_nexus_id=None, # Option<u32> for FFI
            self_parent_state="swarm-base",
            repos=repos,
            cached_states={},
            timestamp=timestamp,
            pubkey=self.master_pubhex,
            signature=sig_hex
        )

    async def hook_build_phase_root(self, commit_hash: str, repos: dict) -> dict:
        return StateAdapter.adapt_swarm_to_phase_root(commit_hash, agents_dict=repos)

class ProvenanceAlignmentScenario(TrustlessEpochBase):
    def __init__(self, broker):
        super().__init__(broker, "Cross-Repo Provenance Alignment")
        
    async def hook_inscribe_nodes(self, parity_triplet: dict) -> dict:
        return {
            "ml_training_code_repo": "git-hash-code-77",
            "model_weights_repo": "git-hash-weights-99",
            "curated_dataset_repo": "git-hash-data-88"
        }

    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, timestamp: int) -> dict:
        # [개선됨] 구버전의 sort_dict_recursive를 완전히 제거하고 StateAdapter로 생성
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, 
            parent_nexus_id=907040, 
            parent_commit_id="infra-state-v1",
            repos=repos, 
            cached_states={"hyperparameters_repo": "git-hash-hyper-old"}
        )
        sig_hex = self._sign_payload(self.master_key, anchor_commit)
        
        return StateAdapter.build_seal_epoch_payload(
            parity=parity_triplet,
            parent_nexus_id=907040,
            self_parent_state="infra-state-v1",
            repos=repos,
            cached_states={"hyperparameters_repo": "git-hash-hyper-old"},
            timestamp=timestamp,
            pubkey=self.master_pubhex,
            signature=sig_hex
        )

    async def hook_build_phase_root(self, commit_hash: str, repos: dict) -> dict:
        return StateAdapter.adapt_provenance_to_phase_root(commit_hash, repos_dict=repos)

class AnchorScenarios(SchemeRunner):
    async def run_all(self):
        log.info("\n=== [START] Executing 5-Flow Complete Epoch Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        
        swarm = SwarmConsensusScenario(self.broker)
        swarm.success_count, swarm.fail_count = self.success_count, self.fail_count
        await swarm.execute_anchor_lifecycle(topo=1, press=3, rupture=False)
        
        prov = ProvenanceAlignmentScenario(self.broker)
        prov.success_count, prov.fail_count = swarm.success_count, swarm.fail_count
        await prov.execute_anchor_lifecycle(topo=1, press=3, rupture=True)
        
        self.success_count, self.fail_count = prov.success_count, prov.fail_count
        self.report()