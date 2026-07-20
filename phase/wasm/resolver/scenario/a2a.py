# phase.wasm.resolver.scenario.a2a
import time
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from phase.wasm.resolver.runner import SchemeRunner
from watcher.plane.emitter import get_emitter
from phase.wasm.resolver.adapter import StateAdapter

log = get_emitter("scenario.a2a")

class A2AScenarios(SchemeRunner):
    """@desc: Agent-to-Agent (A2A) Proof-of-Compute and Trustless Execution scenarios"""
    def __init__(self, broker):
        super().__init__(broker)
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.public_key = self.private_key.public_key()
        self.pubkey_hex = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        ).hex()

    async def run_all(self):
        log.info("\n=== [START] Executing A2A (Agent-to-Agent) Structural Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        
        await self._test_a2a_intent_validation()
        await self._test_a2a_trustless_execution()
        await self._test_a2a_proof_generation()
        await self._test_a2a_ledger_inscription()
        
        self.report()

    def _generate_signature(self, commit_dict: dict) -> str:
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest()
        signature = self.private_key.sign(commit_hash.encode('utf-8'))
        return signature.hex()

    async def _test_a2a_intent_validation(self):
        log.info("\n--- Running Suite: Phase 1 - Intent Validation ---")
        payload = {
            "requester_id": "agent-a-gpt4",
            "responder_id": "agent-b-data-oracle",
            "action": "compute_financial_risk",
            "max_fuel_budget": 5_000_000,
            "timestamp": int(time.time() * 1000)
        }
        await self._run_case("A2A: Validate Execution Intent", "validate_intent", payload, expected_success=True)

    async def _test_a2a_trustless_execution(self):
        log.info("\n--- Running Suite: Phase 2 - Trustless Execution (Sandboxed) ---")
        await self._set_worker_policy("STANDARD")
        code_payload = """
def analyze_risk():
    risk_score = 42.5
    return f'Validated Risk Score: {risk_score}'
print(analyze_risk())
"""
        await self._run_case("A2A: Execute Constrained Task (Fuel Tracked)", "execute_code", code_payload, expected_success=True)

    async def _test_a2a_proof_generation(self):
        log.info("\n--- Running Suite: Phase 3 - Cryptographic Proof Generation ---")
        proof_payload = {
            "execution_hash": "dummy_output_hash_abc123",
            "fuel_consumed": 15420,
            "verification_seed": "random_seed_999"
        }
        await self._run_case("A2A: Generate Proof-of-Compute", "generate_proof", proof_payload, expected_success=True)

    async def _test_a2a_ledger_inscription(self):
        log.info("\n--- Running Suite: Phase 4 - Cryptographic Ledger Inscription ---")
        await self._set_worker_policy("SYSTEM")
        
        repo_commit = StateAdapter.build_repo_commit(
            nexus_id=907049,
            parent_nexus_id=0,
            parent_commit_id="proof-hash-xyz"
        )
        
        sig_hex = self._generate_signature(repo_commit)
        
        # [FIXED] Multi-sig & Dynamic ACL 구조에 맞추어 인자 변경 (1-of-1 서명)
        payload = StateAdapter.build_inscribe_payload(
            nexus_id=907049,
            parent_nexus_id=None,
            parent_commit_id="proof-hash-xyz",
            signers=[self.pubkey_hex],
            signatures=[sig_hex],
            threshold=1,
            allowed_signers=[self.pubkey_hex]
        )
        
        await self._run_case("A2A: Inscribe Transaction for State Finality (Nexus ID)", "inscribe_actor", payload, expected_success=True)