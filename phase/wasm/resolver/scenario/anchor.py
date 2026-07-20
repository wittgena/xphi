# phase.wasm.resolver.scenario.anchor
import time
import json
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from phase.wasm.resolver.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("scenario.anchor")

# =====================================================================
# 1. Base Class: 5-Flow Lifecycle (Strict Rule Enforcement)
# =====================================================================
class TrustlessEpochBase(SchemeRunner):
    """@desc: Base class enforcing the 5-Flow Completeness of an Anchor Commit."""
    def __init__(self, broker, scenario_name: str):
        super().__init__(broker)
        self.scenario_name = scenario_name
        self.master_key = ed25519.Ed25519PrivateKey.generate()
        self.master_pubhex = self.master_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

    def _sign_payload(self, private_key, commit_dict: dict) -> str:
        """
        [FIXED] Removed sort_keys=True. Rust's serde_json serializes fields in their 
        declared order. Python dict must precisely match the Rust struct layout.
        """
        commit_json = json.dumps(commit_dict, separators=(',', ':'))
        commit_hash = hashlib.sha256(commit_json.encode('utf-8')).hexdigest()
        return private_key.sign(commit_hash.encode('utf-8')).hex()

    async def execute_anchor_lifecycle(self, topo: int, press: int, rupture: bool):
        """@core: The 5-Flow pipeline ensuring the completeness of an anchor commit"""
        log.info(f"\n=== [Lifecycle START] {self.scenario_name} ===")
        
        # [Flow 1] Epoch Initialization (Parity Triplet Issuance)
        log.info(f"--- [Flow 1] Initialization: Requesting Parity Triplet ---")
        current_ts = int(time.time() * 1000)
        init_req = {"ts": current_ts, "topo": topo, "press": press, "rupture": rupture, "injected_tick": None}
        
        res = await self.broker.invoke("InitEpoch", json.dumps(init_req))
        if not res.success:
            log.error(f"  [FAIL] InitEpoch Failed: {res.error}")
            self.fail_count += 1
            return
            
        parity_triplet = json.loads(res.output)
        log.info(f"  └─ Generated Nexus ID: {parity_triplet.get('nexus_id')}")
        
        # [Flow 2] Distributed Inscription (Abstract hook - implemented in subclasses)
        log.info(f"--- [Flow 2] Inscription: Gathering Local Node States ---")
        repos = await self.hook_inscribe_nodes(parity_triplet)
        
        # [Flow 3] Topological Sealing (Abstract hook - implemented in subclasses)
        log.info(f"--- [Flow 3] Sealing: Cryptographic Epoch Alignment ---")
        seal_payload = await self.hook_seal_epoch(parity_triplet, repos, current_ts)
        
        seal_res = await self.broker.invoke("SealEpoch", json.dumps(seal_payload))
        if not seal_res.success:
            log.error(f"  [FAIL] SealEpoch Failed: {seal_res.error}")
            self.fail_count += 1
            return
            
        sealed_data = json.loads(seal_res.output)
        log.info("  └─ Epoch Sealed Successfully.")

        # [Flow 4] State Transition (Intent Validation & Spatial Collapse)
        log.info(f"--- [Flow 4] Transition: Validating & Applying State Evolution ---")
        transition_payload = {
            "intent_action": "commit_era",
            "intent_payload": sealed_data.get("kernel_commit", {}),
            # Mock Evolution Context for sandbox acceptance
            "evolution_ctx": {"phase": "collapse", "target_tier": "CORE"}
        }
        await self._run_case(f"{self.scenario_name} (Flow 4): Execute Transition", "execute_transition", transition_payload, expected_success=True)

        # [Flow 5] Deterministic Parity Validation (Ultimate Completeness Proof)
        log.info(f"--- [Flow 5] Finality: Zero-Trust Parity & Recovery Verification ---")
        t_id_low32 = int(parity_triplet["topos_id"].split('_')[-1]) if '_' in parity_triplet["topos_id"] else 0
        parity_req = {
            "topos_id_low32": t_id_low32,
            "phase_id": parity_triplet["phase_id"],
            "nexus_id": parity_triplet["nexus_id"]
        }
        await self._run_case(f"{self.scenario_name} (Flow 5): Verify Parity Completeness", "verify_parity", parity_req, expected_success=True)

    # --- Abstract Hooks ---
    async def hook_inscribe_nodes(self, parity_triplet: dict) -> dict:
        raise NotImplementedError
        
    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, timestamp: int) -> dict:
        raise NotImplementedError


# =====================================================================
# 2. Subclass A: Swarm Consensus (Multi-Agent, Multi-Signature)
# =====================================================================
class SwarmConsensusScenario(TrustlessEpochBase):
    def __init__(self, broker):
        super().__init__(broker, "AI Agent Swarm Consensus")
        
    async def hook_inscribe_nodes(self, parity_triplet: dict) -> dict:
        nexus_id = parity_triplet["nexus_id"]
        agents = {"CodeAgent": "hash-code-v1", "SecurityAgent": "hash-sec-v1"}
        
        for agent_name, state_hash in agents.items():
            agent_key = ed25519.Ed25519PrivateKey.generate()
            pubhex = agent_key.public_key().public_bytes(encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw).hex()
            
            repo_commit = {"nexus_id": nexus_id, "parent_nexus_id": 0, "parent_commit_id": state_hash}
            payload = {
                **repo_commit, "parent_nexus_id": None, "pubkey": pubhex,
                "signature": self._sign_payload(agent_key, repo_commit)
            }
            await self._run_case(f"Swarm: Inscribe {agent_name}", "inscribe_actor", payload, expected_success=True)
            
        return agents

    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, timestamp: int) -> dict:
        anchor_commit = {
            "parity": parity_triplet, 
            "parent_nexus_id": 0, 
            "parent_commit_id": "swarm-base",
            "repos": repos, 
            "cached_states": {}
        }
        return {
            **anchor_commit, 
            "parent_nexus_id": None, 
            "self_parent_state": "swarm-base",
            "timestamp": timestamp, 
            "pubkey": self.master_pubhex,
            "signature": self._sign_payload(self.master_key, anchor_commit)
        }


# =====================================================================
# 3. Subclass B: Provenance Alignment (Large-scale Data Alignment)
# =====================================================================
class ProvenanceAlignmentScenario(TrustlessEpochBase):
    def __init__(self, broker):
        super().__init__(broker, "Cross-Repo Provenance Alignment")
        
    async def hook_inscribe_nodes(self, parity_triplet: dict) -> dict:
        # The infrastructure entity directly binds large data repositories.
        return {
            "ml_training_code_repo": "git-hash-code-77",
            "curated_dataset_repo": "git-hash-data-88",
            "model_weights_repo": "git-hash-weights-99"
        }

    async def hook_seal_epoch(self, parity_triplet: dict, repos: dict, timestamp: int) -> dict:
        anchor_commit = {
            "parity": parity_triplet, 
            "parent_nexus_id": 907040, 
            "parent_commit_id": "infra-state-v1",
            "repos": repos, 
            "cached_states": {"hyperparameters_repo": "git-hash-hyper-old"}
        }
        return {
            **anchor_commit, 
            "self_parent_state": "infra-state-v1",
            "timestamp": timestamp, 
            "pubkey": self.master_pubhex,
            "signature": self._sign_payload(self.master_key, anchor_commit)
        }


# =====================================================================
# 4. Main Runner
# =====================================================================
class AnchorScenarios(SchemeRunner):
    """@desc: Main entry point orchestrating 5-Flow Lifecycle Scenarios"""
    async def run_all(self):
        log.info("\n=== [START] Executing 5-Flow Complete Epoch Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        
        # 1. Execute Swarm Lifecycle
        swarm = SwarmConsensusScenario(self.broker)
        swarm.success_count, swarm.fail_count = self.success_count, self.fail_count
        await swarm.execute_anchor_lifecycle(topo=1, press=3, rupture=False)
        
        # 2. Execute Provenance Lifecycle
        prov = ProvenanceAlignmentScenario(self.broker)
        prov.success_count, prov.fail_count = swarm.success_count, swarm.fail_count
        await prov.execute_anchor_lifecycle(topo=1, press=3, rupture=True)
        
        # Synchronize statistics and report
        self.success_count, self.fail_count = prov.success_count, prov.fail_count
        self.report()