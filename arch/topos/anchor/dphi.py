# arch.topos.anchor.dphi
"""
@role: 
  1. Validates structural integrity (Tripartite XOR Parity).
  2. Enforces Multi-Signature Consensus (BFT).
  3. Permanently seals the Epoch and emits Deterministic Receipts.
"""
import json
import time
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional

from arch.topos.exchange.adapter import TransactionReceipt
from phase.wasm.broker import WasmBroker
from watcher.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("wasm.anchor")


@dataclass
class AnchorProposal:
    """
    @desc: 수용체(Receptor)나 에이전트 스웜이 합의를 위해 제출하는 상태 각인 제안서
    """
    receptor_id: str
    proposed_parity: Dict[str, Any]
    parent_nexus_id: int
    repos: Dict[str, str]
    signers: List[str]          # 합의에 참여한 노드/에이전트들의 공개키
    signatures: List[str]       # 각 노드의 Ed25519 서명
    timestamp: float = field(default_factory=time.time)


@dataclass
class AnchorResult:
    """
    @desc: 앵커가 위상을 확정하고 외부 우주로 방출(Emit)하는 절대적 진실(Receipt)
    """
    is_sealed: bool
    nexus_id: Optional[int] = None
    commit_hash: Optional[str] = None
    receipt: Optional[TransactionReceipt] = None
    rupture_reason: Optional[str] = None


class NexusAnchor:
    """
    @desc: dphi.wasm의 시간과 기억을 통제하는 최종 게이트키퍼.
           위상 붕괴를 막기 위한 패리티 검증과 에포크 봉인(Seal)을 전담합니다.
    """
    def __init__(self, broker: WasmBroker, consensus_threshold: int = 1, allowed_committee: List[str] = None):
        self.broker = broker
        self.consensus_threshold = consensus_threshold
        self.allowed_committee = allowed_committee or []

    async def _verify_tripartite_parity(self, parity_dict: dict) -> bool:
        """
        [Phase 1] 구조적 무결성 검증 (Tripartite XOR Check)
        상태 전이 결과가 topos_id, phase_id, nexus_id의 위상 수학적 균형을 유지하는지 검사합니다.
        """
        payload_json = StateAdapter.to_canonical_bytes(parity_dict).decode('utf-8')
        res = await self.broker.invoke("verify_parity", payload_json)
        
        if not res.success:
            log.error(f"[Nexus] 🚨 Parity Verification Crashed: {res.error}")
            return False
            
        output = json.loads(res.output)
        is_valid = output.get("is_valid", False)
        
        # 자가 치유(Self-Healing) 트리거
        if not is_valid and "recovered_missing" in output:
            log.warning(f"[Nexus] ⚠️ Parity fractured but recovered via XOR! "
                        f"Restored {output.get('recovered_type')}: {output.get('recovered_missing')}")
            return True
            
        return is_valid

    async def anchor_state(self, proposal: AnchorProposal) -> AnchorResult:
        """
        @flow:
          1. Parity Check (위상 균형 검증)
          2. Consensus Sealing (다중 서명 및 BFT 합의)
          3. Receipt Emission (결정론적 영수증 방출)
        """
        log.info(f"[Nexus] ⚓ Anchoring topological state from [{proposal.receptor_id}]...")

        # -------------------------------------------------------------
        # Step 1: Tripartite Parity Check
        # -------------------------------------------------------------
        if not await self._verify_tripartite_parity(proposal.proposed_parity):
            log.critical("[Nexus] 💥 Topological Rupture Detected! Parity Check Failed.")
            return AnchorResult(
                is_sealed=False, 
                rupture_reason="Tripartite Parity Check Failed"
            )

        # -------------------------------------------------------------
        # Step 2: Epoch Sealing (다중 서명 검증 및 시간의 봉인)
        # -------------------------------------------------------------
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=proposal.proposed_parity,
            parent_nexus_id=proposal.parent_nexus_id,
            self_parent_state="genesis",
            repos=proposal.repos,
            cached_states={},
            timestamp=proposal.timestamp,
            signers=proposal.signers,
            signatures=proposal.signatures,
            threshold=self.consensus_threshold,
            allowed_signers=self.allowed_committee
        )

        canonical_payload = StateAdapter.to_canonical_bytes(seal_payload).decode('utf-8')
        seal_res = await self.broker.invoke("seal_epoch", canonical_payload)

        if not seal_res.success:
            log.error(f"[Nexus] 🚫 Consensus Failed (UNAUTHORIZED_PROPOSER): {seal_res.error}")
            return AnchorResult(
                is_sealed=False,
                rupture_reason=f"Consensus Failed: {seal_res.error}"
            )

        seal_data = json.loads(seal_res.output)
        commit_hash = seal_data.get("anchor_result", {}).get("commit_hash", "UNKNOWN_HASH")
        new_nexus_id = proposal.proposed_parity.get("nexus_id", 0)

        log.info(f"[Nexus] ⏳ Epoch successfully sealed. Commit: {commit_hash[:8]}...")

        ## Step 3: Receipt Emission (결정론적 물리 영수증 방출)
        receipt = TransactionReceipt(
            job_id=f"nexus_{new_nexus_id}_{int(time.time())}",
            topos_id=proposal.proposed_parity.get("topos_id", "0"),
            unified_parity_hash=commit_hash,
            clearing_signatures=proposal.signatures,
            fuel_consumed=getattr(seal_res, 'fuel_consumed', 0),
            settlement_status="COMMITTED_TO_NEXUS"
        )
        log.info(f"[Nexus] 🧾 Deterministic Truth Emitted. (Receipt: {receipt.job_id})")
        return AnchorResult(
            is_sealed=True,
            nexus_id=new_nexus_id,
            commit_hash=commit_hash,
            receipt=receipt
        )