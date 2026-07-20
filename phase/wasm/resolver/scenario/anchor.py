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
    """@desc: Base scenario executor for the 5-Flow Epoch Lifecycle with Multi-sig support."""
    def __init__(self, broker, scenario_name: str):
        super().__init__(broker)
        self.scenario_name = scenario_name
        
        # [NEW] Generate a 3-member committee for dynamic ACL & multi-signature
        self.committee_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.committee_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.committee_keys
        ]

    def _sign_multisig(self, signers: list, commit_dict: dict) -> list:
        """Generates an array of Ed25519 signatures from JCS deterministic hash."""
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest().encode('utf-8')
        return [k.sign(commit_hash).hex() for k in signers]

    async def execute_anchor_lifecycle(self, topo: int, press: int, rupture: bool):
        log.info(f"\n=== [Lifecycle START] {self.scenario_name} ===")
        
        # ---------------------------------------------------------
        # [Flow 1] Initialization: Requesting Parity Triplet
        # ---------------------------------------------------------
        log.info(f"--- [Flow 1] Initialization: Requesting Parity Triplet ---")
        current_ts = int(time.time() * 1000)
        init_req = {"ts": current_ts, "topo": topo, "press": press, "rupture": rupture, "injected_tick": None}
        
        res = await self.broker.invoke("init_epoch", json.dumps(init_req))
        if not res.success:
            log.error(f"  [FAIL] init_epoch Failed: {res.error}")
            self.fail_count += 1
            return
            
        parity_triplet = json.loads(res.output)
        log.info(f"  └─ Generated Nexus ID: {parity_triplet.get('nexus_id')}")
        
        # ---------------------------------------------------------
        # [Flow 2] Inscription: Gathering Local Node States
        # ---------------------------------------------------------
        log.info(f"--- [Flow 2] Inscription: Gathering Local Node States ---")
        repos = await self.hook_inscribe_nodes(parity_triplet)
        
        # ---------------------------------------------------------
        # [Flow 3] Sealing: Cryptographic Epoch Alignment (Multi-sig)
        # ---------------------------------------------------------
        log.info(f"--- [Flow 3] Sealing: Cryptographic Epoch Alignment ---")
        seal_payload = await self.hook_seal_epoch(parity_triplet, repos, current_ts)
        
        seal_res = await self.broker.invoke("seal_epoch", json.dumps(seal_payload))
        if not seal_res.success:
            log.error(f"  [FAIL] seal_epoch Failed: {seal_res.error}")
            self.fail_count += 1
            return
            
        sealed_data = json.loads(seal_res.output)
        log.info("  └─ Epoch Sealed Successfully via Multi-sig Consensus.")

        # ---------------------------------------------------------
        # [Flow 4] Transition: Validating & Applying State Evolution
        # ---------------------------------------------------------
        log.info(f"--- [Flow 4] Transition: Validating & Applying State Evolution ---")
        anchor_result = sealed_data.get("anchor_result", sealed_data)
        commit_hash = anchor_result.get("commit_hash", "mock_fallback_hash_0x99")
        
        state_node_struct = await self.hook_build_phase_root(commit_hash, repos)
        evo_ctx = StateAdapter.build_evolution_context(phase_root=state_node_struct, external_rules=[])
        transition_payload = StateAdapter.build_transition_payload(
            intent_action="commit_era",
            intent_payload=anchor_result,
            evolution_ctx=evo_ctx
        )
        
        await self._run_case(f"{self.scenario_name} (Flow 4): Execute Transition", "execute_transition", transition_payload, expected_success=True)

        # ---------------------------------------------------------
        # [Flow 5] Finality: Zero-Trust Parity & Recovery Verification
        # ---------------------------------------------------------
        log.info(f"--- [Flow 5] Finality: Zero-Trust Parity & Recovery Verification ---")
        t_id_low32 = int(parity_triplet["topos_id"].split('_')[-1]) if '_' in parity_triplet["topos_id"] else 0
        parity_req = {
            "topos_id_low32": t_id_low32,
            "phase_id": parity_triplet["phase_id"],
            "nexus_id": parity_triplet["nexus_id"]
        }
        await self._run_case(f"{self.scenario_name} (Flow 5): Verify Parity Completeness", "verify_parity", parity_req, expected_success=True)

    # Subclass hooks
    async def hook_inscribe_nodes(self, parity_triplet: dict) -> dict: raise NotImplementedError
    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, timestamp: int) -> dict: raise NotImplementedError
    async def hook_build_phase_root(self, commit_hash: str, repos: dict) -> dict: raise NotImplementedError


class SwarmConsensusScenario(TrustlessEpochBase):
    def __init__(self, broker):
        super().__init__(broker, "AI Agent Swarm Consensus (M-of-N)")
        
    async def hook_inscribe_nodes(self, parity_triplet: dict) -> dict:
        nexus_id = parity_triplet["nexus_id"]
        agents = {"CodeAgent": "hash-code-v1", "SecurityAgent": "hash-sec-v1"}
        
        for agent_name, state_hash in agents.items():
            # Individual node inscriptions require 1-of-1 self-signature
            agent_key = ed25519.Ed25519PrivateKey.generate()
            pubhex = agent_key.public_key().public_bytes(encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw).hex()
            
            repo_commit = StateAdapter.build_repo_commit(nexus_id=nexus_id, parent_nexus_id=0, parent_commit_id=state_hash)
            
            payload = StateAdapter.build_inscribe_payload(
                nexus_id=nexus_id, parent_nexus_id=0, parent_commit_id=state_hash,
                signers=[pubhex], 
                signatures=self._sign_multisig([agent_key], repo_commit),
                threshold=1,
                allowed_signers=[pubhex]
            )
            await self._run_case(f"Swarm: Inscribe {agent_name}", "inscribe_actor", payload, expected_success=True)
            
        return agents

    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, timestamp: int) -> dict:
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, parent_nexus_id=0, parent_commit_id="swarm-base",
            repos=repos, cached_states={}
        )
        
        # 3-of-3 Full Committee Consensus
        signatures = self._sign_multisig(self.committee_keys, anchor_commit)
        
        return StateAdapter.build_seal_epoch_payload(
            parity=parity_triplet, parent_nexus_id=0, self_parent_state="swarm-base",
            repos=repos, cached_states={}, timestamp=timestamp,
            signers=self.committee_pubs, signatures=signatures, threshold=3,
            allowed_signers=self.committee_pubs
        )

    async def hook_build_phase_root(self, commit_hash: str, repos: dict) -> dict:
        return StateAdapter.adapt_swarm_to_phase_root(commit_hash, agents_dict=repos)


class ProvenanceAlignmentScenario(TrustlessEpochBase):
    def __init__(self, broker):
        super().__init__(broker, "Cross-Repo Provenance Alignment (M-of-N)")
        
    async def hook_inscribe_nodes(self, parity_triplet: dict) -> dict:
        nexus_id = parity_triplet["nexus_id"]
        repos = {
            "ml_training_code": "git-hash-code-77",
            "model_weights": "git-hash-weights-99"
        }
        
        for repo_name, state_hash in repos.items():
            agent_key = ed25519.Ed25519PrivateKey.generate()
            pubhex = agent_key.public_key().public_bytes(encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw).hex()
            repo_commit = StateAdapter.build_repo_commit(nexus_id=nexus_id, parent_nexus_id=907040, parent_commit_id=state_hash)
            
            payload = StateAdapter.build_inscribe_payload(
                nexus_id=nexus_id, parent_nexus_id=907040, parent_commit_id=state_hash,
                signers=[pubhex], 
                signatures=self._sign_multisig([agent_key], repo_commit),
                threshold=1,
                allowed_signers=[pubhex]
            )
            await self._run_case(f"Provenance: Inscribe {repo_name}", "inscribe_actor", payload, expected_success=True)
            
        return repos

    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, timestamp: int) -> dict:
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet, parent_nexus_id=907040, parent_commit_id="infra-state-v1",
            repos=repos, cached_states={"hyperparameters": "git-hash-hyper-old"}
        )
        
        # 2-of-3 Partial Committee Consensus (Dynamic Thresholding)
        active_keys = self.committee_keys[:2]
        active_pubs = self.committee_pubs[:2]
        signatures = self._sign_multisig(active_keys, anchor_commit)
        
        return StateAdapter.build_seal_epoch_payload(
            parity=parity_triplet, parent_nexus_id=907040, self_parent_state="infra-state-v1",
            repos=repos, cached_states={"hyperparameters": "git-hash-hyper-old"}, timestamp=timestamp,
            signers=active_pubs, signatures=signatures, threshold=2,
            allowed_signers=self.committee_pubs
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