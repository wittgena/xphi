# phase.wasm.resolver.scenario.a2a
import time
import json
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from phase.wasm.resolver.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("scenario.a2a")

class A2AScenarios(SchemeRunner):
    """@desc: Agent-to-Agent (A2A) Proof-of-Compute and Trustless Execution scenarios"""
    def __init__(self, broker):
        super().__init__(broker)
        ## Ed25519 key pair generation for agent/node cryptographic signatures
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.public_key = self.private_key.public_key()
        self.pubkey_hex = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        ).hex()

    async def run_all(self):
        log.info("\n=== [START] Executing A2A (Agent-to-Agent) Structural Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        
        """A2A Trustless Compute Pipeline: 4 Core Phases"""
        await self._test_a2a_intent_validation()
        await self._test_a2a_trustless_execution()
        await self._test_a2a_proof_generation()
        await self._test_a2a_ledger_inscription()
        
        self.report()

    def _generate_signature(self, commit_dict: dict) -> str:
        """
        [개선됨] Rust WASM BTreeMap의 결정론적 해시 구조(알파벳순 정렬)와 일치시키기 위해 
        sort_keys=True 옵션을 반드시 사용해야 합니다.
        """
        commit_json = json.dumps(commit_dict, separators=(',', ':'), sort_keys=True)
        commit_hash = hashlib.sha256(commit_json.encode('utf-8')).hexdigest()
        signature = self.private_key.sign(commit_hash.encode('utf-8'))
        return signature.hex()

    async def _test_a2a_intent_validation(self):
        """@flow: Agent A Execution Intent -> dphi.wasm Membrane -> Structural Integrity Validation -> Malicious Command Filter -> Approval"""
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
        """@flow: Approved Code Payload -> Cgroup 'STANDARD' Tier Enforcement -> Isolated Sandbox Execution -> Resource (Fuel) Constraint Check"""
        log.info("\n--- Running Suite: Phase 2 - Trustless Execution (Sandboxed) ---")
        await self._set_worker_policy("STANDARD")
        
        """Sample computation logic exchanged between agents"""
        code_payload = """
def analyze_risk():
    risk_score = 42.5
    return f'Validated Risk Score: {risk_score}'
print(analyze_risk())
"""
        await self._run_case("A2A: Execute Constrained Task (Fuel Tracked)", "execute_code", code_payload, expected_success=True)

    async def _test_a2a_proof_generation(self):
        """@flow: Deterministic Execution Output -> Fuel Consumption Mapping -> dphi.wasm Cryptographic Proof Generation -> Trustless Verification"""
        log.info("\n--- Running Suite: Phase 3 - Cryptographic Proof Generation ---")
        proof_payload = {
            "execution_hash": "dummy_output_hash_abc123",
            "fuel_consumed": 15420,
            "verification_seed": "random_seed_999"
        }
        await self._run_case("A2A: Generate Proof-of-Compute", "generate_proof", proof_payload, expected_success=True)

    async def _test_a2a_ledger_inscription(self):
        """
        @flow: Cryptographic Proof -> Ed25519 Signature Linkage -> dphi.wasm Ledger Validation -> Immutable State Finality
        [개선됨] 문자열 anchor_id에서 정수형 Parity(nexus_id) 스키마로 전환
        """
        log.info("\n--- Running Suite: Phase 4 - Cryptographic Ledger Inscription ---")
        await self._set_worker_policy("SYSTEM")
        
        # Rust의 RepoCommit 구조체와 동일한 형태 (서명용 객체)
        repo_commit = {
            "nexus_id": 907049,
            "parent_nexus_id": 0, # Genesis fallback
            "parent_commit_id": "proof-hash-xyz"
        }
        sig_hex = self._generate_signature(repo_commit)
        
        # WASM에 전달할 FFI Payload (InscribePayload 규격)
        payload = {
            "nexus_id": 907049,
            "parent_nexus_id": None, # Rust 내부에서 None은 0으로 처리됨
            "parent_commit_id": "proof-hash-xyz",
            "pubkey": self.pubkey_hex,
            "signature": sig_hex
        }
        await self._run_case("A2A: Inscribe Transaction for State Finality (Nexus ID)", "inscribe_actor", payload, expected_success=True)