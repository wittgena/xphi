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
        Rust WASM 내부의 compute_deterministic_hash 함수와 동일하게 동작하도록 
        파이썬 딕셔너리를 공백 없는 JSON으로 직렬화한 후 SHA-256 해싱 후 서명합니다.
        """
        commit_json = json.dumps(commit_dict, separators=(',', ':'))
        commit_hash = hashlib.sha256(commit_json.encode('utf-8')).hexdigest()
        signature = self.private_key.sign(commit_hash.encode('utf-8'))
        return signature.hex()

    async def _test_inscribe_actor_authorized(self):
        log.info("\n--- Running Suite: Authorized Actor Inscription ---")
        repo_commit = {
            "anchor_id": "anc-1001",
            "parent_anchor_id": "0000000",
            "parent_commit_id": "commit-0000"
        }
        
        sig_hex = self._generate_signature(repo_commit)
        payload = {
            **repo_commit,
            "parent_anchor_id": None,
            "pubkey": self.pubkey_hex,
            "signature": sig_hex
        }
        await self._run_case("Ledger: Inscribe Actor with Valid Ed25519 Signature", "inscribe_actor", payload, expected_success=True)

    async def _test_inscribe_actor_tampered(self):
        log.info("\n--- Running Suite: Fraudulent Inscription (Tamper-Proof Guard) ---")
        repo_commit = {
            "anchor_id": "anc-9999",
            "parent_anchor_id": "0000000",
            "parent_commit_id": "commit-0000"
        }
        
        sig_hex = self._generate_signature(repo_commit)
        tampered_payload = {
            "anchor_id": "anc-hacked", 
            "parent_anchor_id": None,
            "parent_commit_id": "commit-0000",
            "pubkey": self.pubkey_hex,
            "signature": sig_hex
        }
        await self._run_case("Ledger: Reject Tampered Payload (UNAUTHORIZED_ACTOR)", "inscribe_actor", tampered_payload, expected_success=False)

    async def _test_seal_epoch_consensus(self):
        log.info("\n--- Running Suite: Epoch Sealing Consensus ---")
        anchor_commit = {
            "anchor_id": "epoch-200",
            "parent_anchor_id": "epoch-199",
            "parent_commit_id": "state-xyz",
            "repos": {"repoA": "hashA", "repoB": "hashB"},
            "cached_states": {"key1": "val1"}
        }
        
        sig_hex = self._generate_signature(anchor_commit)
        payload = {
            "anchor_id": "epoch-200",
            "parent_anchor_id": "epoch-199",
            "self_parent_state": "state-xyz",
            "repos": {"repoA": "hashA", "repoB": "hashB"},
            "cached_states": {"key1": "val1"},
            "timestamp": time.time(),
            "pubkey": self.pubkey_hex,
            "signature": sig_hex
        }
        await self._run_case("Ledger: Seal Epoch with Proposer Signature", "seal_epoch", payload, expected_success=True)