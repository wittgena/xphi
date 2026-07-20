# phase.wasm.resolver.scenario.ledger
import time
import json
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from phase.wasm.resolver.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("scenario.ledger")

class LedgerScenarios(SchemeRunner):
    """@desc: Blockchain Consensus, Ed25519 Signatures, and Lineage Validation scenarios"""
    def __init__(self, broker):
        super().__init__(broker)
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.public_key = self.private_key.public_key()
        self.pubkey_hex = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        ).hex()

    async def run_all(self):
        log.info("\n=== [START] Executing Ledger & Blockchain Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        await self._test_inscribe_actor_authorized()
        await self._test_inscribe_actor_tampered()
        await self._test_seal_epoch_consensus()
        
        self.report()

    def _generate_signature(self, commit_dict: dict) -> str:
        """
        [수정됨] Rust WASM의 serde_json은 구조체 필드가 선언된 순서대로 직렬화합니다.
        알파벳순 정렬(sort_keys=True)을 제거하고, 파이썬 딕셔너리의 키 삽입 순서를 유지합니다.
        """
        commit_json = json.dumps(commit_dict, separators=(',', ':'))
        commit_hash = hashlib.sha256(commit_json.encode('utf-8')).hexdigest()
        signature = self.private_key.sign(commit_hash.encode('utf-8'))
        return signature.hex()

    async def _test_inscribe_actor_authorized(self):
        log.info("\n--- Running Suite: Authorized Actor Inscription (Nexus ID) ---")
        
        # Rust의 RepoCommit 구조체 필드 선언 순서에 완벽히 일치해야 합니다.
        # pub nexus_id: u32
        # pub parent_nexus_id: u32
        # pub parent_commit_id: String
        repo_commit = {
            "nexus_id": 907049,
            "parent_nexus_id": 0,
            "parent_commit_id": "commit-0000"
        }
        
        sig_hex = self._generate_signature(repo_commit)
        
        # WASM에 전달할 FFI Payload (InscribePayload 구조체)
        payload = {
            "nexus_id": 907049,
            "parent_nexus_id": None, # Option<u32>이므로 None 전달 -> Rust 내부에서 0으로 처리됨
            "parent_commit_id": "commit-0000",
            "pubkey": self.pubkey_hex,
            "signature": sig_hex
        }
        await self._run_case("Ledger: Inscribe Actor with Valid Ed25519 Signature", "inscribe_actor", payload, expected_success=True)

    async def _test_inscribe_actor_tampered(self):
        log.info("\n--- Running Suite: Fraudulent Inscription (Tamper-Proof Guard) ---")
        
        # 원본 서명 객체 생성 (순서 보장)
        repo_commit = {
            "nexus_id": 907049,
            "parent_nexus_id": 0,
            "parent_commit_id": "commit-0000"
        }
        sig_hex = self._generate_signature(repo_commit)
        
        # 악의적으로 nexus_id를 위조한 Payload (서명은 원본 유지)
        tampered_payload = {
            "nexus_id": 999999, # HACKED
            "parent_nexus_id": None,
            "parent_commit_id": "commit-0000",
            "pubkey": self.pubkey_hex,
            "signature": sig_hex
        }
        await self._run_case("Ledger: Reject Tampered Payload (UNAUTHORIZED_ACTOR)", "inscribe_actor", tampered_payload, expected_success=False)

    async def _test_seal_epoch_consensus(self):
        log.info("\n--- Running Suite: Epoch Sealing Consensus (Parity Triplet) ---")
        
        # Rust의 ParityTriplet 구조체 선언 순서: topos_id -> phase_id -> nexus_id
        parity_triplet = {
            "topos_id": "1767225600000_w1_d1_0",
            "phase_id": 999999,
            "nexus_id": 907049
        }
        
        # Rust의 AnchorCommit 구조체 선언 순서와 완벽히 일치시켜야 합니다.
        anchor_commit = {
            "parity": parity_triplet,
            "parent_nexus_id": 123456,
            "parent_commit_id": "state-xyz",
            "repos": {"repoA": "hashA", "repoB": "hashB"},
            "cached_states": {"key1": "val1"}
        }
        sig_hex = self._generate_signature(anchor_commit)
        
        # WASM에 전달할 FFI Payload (SealEpochPayload 구조체)
        payload = {
            "parity": parity_triplet,
            "parent_nexus_id": 123456,
            "self_parent_state": "state-xyz",
            "repos": {"repoA": "hashA", "repoB": "hashB"},
            "cached_states": {"key1": "val1"},
            "timestamp": time.time(),
            "pubkey": self.pubkey_hex,
            "signature": sig_hex
        }
        await self._run_case("Ledger: Seal Epoch with Proposer Signature", "seal_epoch", payload, expected_success=True)