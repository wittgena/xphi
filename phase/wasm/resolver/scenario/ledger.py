# phase.wasm.resolver.scenario.ledger
import time
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from phase.wasm.resolver.runner import SchemeRunner
from watcher.plane.emitter import get_emitter
from phase.wasm.resolver.adapter import StateAdapter

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
        [개선됨] 구조체 선언 순서에 의존하던 기존 로직을 버리고, 
        StateAdapter를 통해 Rust의 serde_jcs와 100% 동일한 RFC 8785 정렬 규격을 적용합니다.
        """
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest()
        signature = self.private_key.sign(commit_hash.encode('utf-8'))
        return signature.hex()

    async def _test_inscribe_actor_authorized(self):
        log.info("\n--- Running Suite: Authorized Actor Inscription (Nexus ID) ---")
        
        # [개선됨] 하드코딩 딕셔너리 대신 Adapter 빌더 사용
        repo_commit = StateAdapter.build_repo_commit(
            nexus_id=907049,
            parent_nexus_id=0,
            parent_commit_id="commit-0000"
        )
        
        sig_hex = self._generate_signature(repo_commit)
        
        # [개선됨] WASM에 전달할 FFI Payload 빌드
        payload = StateAdapter.build_inscribe_payload(
            nexus_id=907049,
            parent_nexus_id=None, # Option<u32>이므로 None 전달 -> Rust 내부에서 0으로 처리됨
            parent_commit_id="commit-0000",
            pubkey=self.pubkey_hex,
            signature=sig_hex
        )
        await self._run_case("Ledger: Inscribe Actor with Valid Ed25519 Signature", "inscribe_actor", payload, expected_success=True)

    async def _test_inscribe_actor_tampered(self):
        log.info("\n--- Running Suite: Fraudulent Inscription (Tamper-Proof Guard) ---")
        
        # 원본 서명 객체 생성
        repo_commit = StateAdapter.build_repo_commit(
            nexus_id=907049,
            parent_nexus_id=0,
            parent_commit_id="commit-0000"
        )
        sig_hex = self._generate_signature(repo_commit)
        
        # 악의적으로 nexus_id를 위조한 Payload (서명은 원본 유지)
        tampered_payload = StateAdapter.build_inscribe_payload(
            nexus_id=999999, # HACKED
            parent_nexus_id=None,
            parent_commit_id="commit-0000",
            pubkey=self.pubkey_hex,
            signature=sig_hex
        )
        await self._run_case("Ledger: Reject Tampered Payload (UNAUTHORIZED_ACTOR)", "inscribe_actor", tampered_payload, expected_success=False)

    async def _test_seal_epoch_consensus(self):
        log.info("\n--- Running Suite: Epoch Sealing Consensus (Parity Triplet) ---")
        
        # [개선됨] 하드코딩 딕셔너리 대신 Adapter 빌더 사용
        parity_triplet = StateAdapter.build_parity_triplet(
            topos_id="1767225600000_w1_d1_0",
            phase_id=999999,
            nexus_id=907049
        )
        
        # 서명을 위한 AnchorCommit 빌드
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity_triplet,
            parent_nexus_id=123456,
            parent_commit_id="state-xyz",
            repos={"repoA": "hashA", "repoB": "hashB"},
            cached_states={"key1": "val1"}
        )
        sig_hex = self._generate_signature(anchor_commit)
        
        # WASM에 전달할 최종 FFI Payload 빌드
        payload = StateAdapter.build_seal_epoch_payload(
            parity=parity_triplet,
            parent_nexus_id=123456,
            self_parent_state="state-xyz",
            repos={"repoA": "hashA", "repoB": "hashB"},
            cached_states={"key1": "val1"},
            timestamp=time.time(),
            pubkey=self.pubkey_hex,
            signature=sig_hex
        )
        await self._run_case("Ledger: Seal Epoch with Proposer Signature", "seal_epoch", payload, expected_success=True)