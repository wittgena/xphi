# phase.wasm.resolver.scenario.ecosystem
import time
import json
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from phase.wasm.resolver.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("scenario.ecosystem")

class EcosystemScenarios(SchemeRunner):
    """@desc: Zero-Trust Data Pipeline & Autonomous State Engine scenarios."""
    def __init__(self, broker):
        super().__init__(broker)
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.public_key = self.private_key.public_key()
        self.pubkey_hex = self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        ).hex()

    async def run_all(self):
        log.info("\n=== [START] Executing Ecosystem Structural Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        
        """@pipeline.1: Zero-Trust Data Integrity"""
        await self._test_oracle_packet_integrity()
        await self._test_oracle_data_provenance()
        await self._test_oracle_epoch_initialization() # [변경] InitEpoch 통합 테스트로 명칭 변경
        await self._test_oracle_self_healing()
        
        """@pipeline.2: Autonomous Protocol State Engine"""
        await self._test_dao_tension_evaluation()
        await self._test_dao_state_evolution()
        await self._test_dao_epoch_sealing()
        
        self.report()

    def _generate_signature(self, commit_dict: dict) -> str:
        """
        [개선] Rust BTreeMap의 결정론적 해시 구조(알파벳순 정렬)와 일치시키기 위해 
        sort_keys=True 옵션을 추가하여 Ed25519 서명을 생성합니다.
        """
        commit_json = json.dumps(commit_dict, separators=(',', ':'), sort_keys=True)
        commit_hash = hashlib.sha256(commit_json.encode('utf-8')).hexdigest()
        signature = self.private_key.sign(commit_hash.encode('utf-8'))
        return signature.hex()

    """@pipeline.1: Zero-Trust Data Integrity (High-Availability Verification)"""
    async def _test_oracle_packet_integrity(self):
        """@flow: Inbound Off-chain Stream -> dphi.wasm Membrane -> Structure Validation -> Integrity Check"""
        log.info("\n--- [Data Pipeline] Phase 1: Packet Integrity Check ---")
        payload = {
            "packet_id": "ext-data-2026",
            "files": {"transaction_log.csv": "hash_xyz"}
        }
        await self._run_case("Pipeline: Verify Incoming Data Stream", "verify_packet", payload, expected_success=True)

    async def _test_oracle_data_provenance(self):
        """@flow: Validated Payload -> Deterministic SHA-256 Hashing -> Tamper-Proof Root Fingerprint Generation"""
        log.info("\n--- [Data Pipeline] Phase 2: Provenance Fingerprinting ---")
        payload = {"dummy_data": "Node_State_Report_Data..."}
        await self._run_case("Pipeline: Compute Tamper-Proof Root Fingerprint", "compute_root_fingerprint", payload, expected_success=True)

    async def _test_oracle_epoch_initialization(self):
        """
        @flow: Fingerprint -> Spatiotemporal Context Injection -> Parity Triplet (InitEpoch)
        [개선] 단순 Topos 생성이 아닌, 단일 트랜잭션 기반 InitEpoch 래퍼 호출을 테스트합니다.
        """
        log.info("\n--- [Data Pipeline] Phase 3: Epoch Initialization & Parity Triplet ---")
        current_ts = int(time.time() * 1000)
        payload = {
            "ts": current_ts,
            "topo": 1,
            "press": 5,
            "rupture": False,
            "injected_tick": None
        }
        await self._run_case("Pipeline: Generate Parity Triplet (InitEpoch)", "InitEpoch", payload, expected_success=True)

    async def _test_oracle_self_healing(self):
        """@flow: Fragmented Topology (Missing Phase ID) -> dphi.wasm Tripartite XOR Parity -> 100% Deterministic State Reconstruction"""
        log.info("\n--- [Data Pipeline] Phase 4: Self-Healing Recovery ---")
        t_id, n_id = 101010, 907049
        payload = {"topos_id_low32": t_id, "nexus_id": n_id}
        await self._run_case("Pipeline: Recover Lost Data via XOR Parity", "verify_parity", payload, expected_success=True)


    """@pipeline.2: Autonomous Protocol State Engine (Off-chain Rollup)"""
    async def _test_dao_tension_evaluation(self):
        """
        @flow: Network Metrics -> Tension Evaluation Algorithm -> Dynamic Protocol Parameter Adjustment
        [개선] Rust SymbolTopology::from_raw가 파싱하는 "current|previous" 문자열 포맷으로 수정
        """
        log.info("\n--- [State Engine] Phase 1: Ecosystem Tension Evaluation ---")
        # 교집합: node_b, node_c / 합집합: node_a, node_b, node_c, node_d
        payload = "node_a,node_b,node_c|node_b,node_c,node_d" 
        await self._run_case("Engine: Evaluate Network Tension & Load", "evaluate_tension", payload, expected_success=True)

    async def _test_dao_state_evolution(self):
        """@flow: Evaluated Tension -> Off-chain Sandbox Execution -> Deterministic State Evolution"""
        log.info("\n--- [State Engine] Phase 2: Protocol State Evolution ---")
        payload = {
            "phase_root": {
                "name": "ecosystem_root",
                "kind": "CORE",
                "content": "epoch_399_state",
                "ref_target": None,
                "children": {
                    "pending_proposal": {
                        "name": "pending_proposal",
                        "kind": "SYMLINK",
                        "content": None,
                        "ref_target": "ipfs_hash_xyz",
                        "children": {}
                    }
                }
            },
            "external_rules": [
                {
                    "src": "legacy_data",
                    "dest": "archived_data",
                    "kind": "CORE"
                }
            ]
        }
        await self._run_case("Engine: Process High-Speed Off-chain Evolution", "process_evolution", payload, expected_success=True)

    async def _test_dao_epoch_sealing(self):
        """
        @flow: Evolved State -> Parity Triplet Linkage -> Cryptographic Signature -> Immutable Epoch Sealing
        [개선] ParityTriplet 스키마 및 parent_nexus_id 반영
        """
        log.info("\n--- [State Engine] Phase 3: Epoch Sealing & Consensus ---")
        
        parity_triplet = {
            "topos_id": "1767225600000_w1_d1_0",
            "phase_id": 999999,
            "nexus_id": 907049
        }
        
        # Rust의 AnchorCommit 구조체 스키마
        anchor_commit = {
            "parity": parity_triplet,
            "parent_nexus_id": 123456,
            "parent_commit_id": "state-v2-hash",
            "repos": {"ledger": "hash_a", "registry": "hash_b"},
            "cached_states": {"tension_rate": "5.5%"}
        }
        sig_hex = self._generate_signature(anchor_commit)
        
        # WASM에 전달할 FFI Payload
        payload = {
            "parity": parity_triplet,
            "parent_nexus_id": 123456,
            "self_parent_state": "state-v2-hash",
            "repos": {"ledger": "hash_a", "registry": "hash_b"},
            "cached_states": {"tension_rate": "5.5%"},
            "timestamp": time.time(),
            "pubkey": self.pubkey_hex,
            "signature": sig_hex
        }
        await self._run_case("Engine: Seal Epoch & Finalize State Transition", "SealEpoch", payload, expected_success=True)