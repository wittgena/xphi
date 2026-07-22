# phase.wasm.receptor
"""
@desc: Cellular Membrane Receptor for dphi.wasm Kernel.
@role: Ingests non-deterministic external signals (Agent/Deno proofs), 
       validates cryptographic invariants, enforces WasmCG policies, 
       and transduces them into deterministic Topological Transitions (Entries).
"""
import json
import time
import hashlib
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional

from arch.crypto.signer import NodeSigner
from phase.wasm.broker import WasmBroker
from phase.wasm.resolver.adapter import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("wasm.receptor")


@dataclass
class ReceptorSignal:
    """
    @desc: 외부 감각/운동 기관(Deno/Agent)이 커널 자극을 위해 제출하는 원시 신호 패킷
    """
    requester_id: str
    intent_action: str
    proposed_payload: Dict[str, Any]
    proof_of_compute: Optional[Dict[str, Any]] = None
    max_fuel_budget: int = 10_000_000
    timestamp: float = field(default_factory=time.time)


@dataclass
class ReceptorBindingResult:
    """
    @desc: WASM 수용체가 신호를 결합(Binding)하고 위상 전이를 유도한 최종 결과
    """
    is_bound: bool
    receptor_id: str
    commit_hash: Optional[str] = None
    mutated_parity: Optional[Dict[str, Any]] = None
    fuel_consumed: int = 0
    rupture_reason: Optional[str] = None


class WasmReceptor:
    """
    @desc: dphi.wasm 멤브레인에 부착된 동적 진입점 수용체(Receptor).
           외부의 카오스(비결정론)를 커널 내부의 질서(위상 상태)로 번역 및 신호 전달.
    """
    def __init__(self, broker: WasmBroker, receptor_id: str = "rec_core_v1"):
        self.broker = broker
        self.receptor_id = receptor_id
        self.signer = NodeSigner.get_instance()

    def _hash_canonical_payload(self, payload_dict: dict) -> str:
        """JCS (RFC 8785) 인코딩 후 SHA-256 해시 도출"""
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        return hashlib.sha256(canonical_bytes).hexdigest()

    async def transduce_signal(self, signal: ReceptorSignal) -> ReceptorBindingResult:
        """
        @flow:
          1. Intent Validation (입국 심사)
          2. Off-chain Compute Proof Audit (연산 증명 검증)
          3. Cryptographic Inscription (노드 신원 서명 각인)
          4. Topological State Transduction (WASM 전이)
        """
        log.info(f"[{self.receptor_id}] ⚡ Ingesting external signal: '{signal.intent_action}' from [{signal.requester_id}]")

        # -------------------------------------------------------------
        # Phase 1: Intent Validation (의도 및 가스비 한도 검증)
        # -------------------------------------------------------------
        intent_payload = {
            "requester_id": signal.requester_id,
            "action": signal.intent_action,
            "max_fuel_budget": signal.max_fuel_budget,
            "timestamp": int(signal.timestamp * 1000)
        }
        
        # dphi.wasm 내 validate_intent Entry 호출
        val_res = await self.broker.invoke("validate_intent", json.dumps(intent_payload))
        if not val_res.success:
            log.warn(f"[{self.receptor_id}] 🚫 Intent Validation Rejected: {val_res.error}")
            return ReceptorBindingResult(
                is_bound=False, 
                receptor_id=self.receptor_id, 
                rupture_reason=f"Intent Rejected: {val_res.error}"
            )

        # -------------------------------------------------------------
        # Phase 2: Proof-of-Compute Audit (연산 증명 무결성 검사)
        # -------------------------------------------------------------
        if signal.proof_of_compute:
            proof_res = await self.broker.invoke("generate_proof", json.dumps(signal.proof_of_compute))
            if not proof_res.success:
                log.error(f"[{self.receptor_id}] 🚨 Invalid Proof-of-Compute Payload")
                return ReceptorBindingResult(
                    is_bound=False, 
                    receptor_id=self.receptor_id, 
                    rupture_reason=f"Proof Audit Failed: {proof_res.error}"
                )

        # -------------------------------------------------------------
        # Phase 3: Cryptographic Inscription & Alignment (암호학적 서명 각인)
        # -------------------------------------------------------------
        # 수용체가 서명할 Anchor Commit 객체 조립
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=StateAdapter.build_parity_triplet(f"topos_{self.receptor_id}", 1, 0),
            parent_nexus_id=0,
            parent_commit_id="genesis",
            repos={signal.requester_id: self._hash_canonical_payload(signal.proposed_payload)},
            cached_states={}
        )

        canonical_bytes = StateAdapter.to_canonical_bytes(anchor_commit)
        signature_hex = self.signer.sign_anchor_commit(canonical_bytes)

        # Inscribe Payload 생성 (1-of-1 Receptor Authority)
        inscribe_payload = StateAdapter.build_inscribe_payload(
            nexus_id=int(time.time()) % 4294967295,  # u32 호환
            parent_nexus_id=0,
            parent_commit_id="genesis",
            signers=[self.signer.pubkey_hex],
            signatures=[signature_hex],
            threshold=1,
            allowed_signers=[self.signer.pubkey_hex]
        )

        inscribe_json = StateAdapter.to_canonical_bytes(inscribe_payload).decode('utf-8')
        inscribe_res = await self.broker.invoke("inscribe_actor", inscribe_json)

        if not inscribe_res.success:
            log.error(f"[{self.receptor_id}] ❌ Inscription to Ledger failed: {inscribe_res.error}")
            return ReceptorBindingResult(
                is_bound=False, 
                receptor_id=self.receptor_id, 
                rupture_reason=f"Inscription Failed: {inscribe_res.error}"
            )

        inscribe_output = json.loads(inscribe_res.output)
        commit_hash = inscribe_output.get("commit_hash")

        # -------------------------------------------------------------
        # Phase 4: Topological State Evolution (위상 상태 전이 확정)
        # -------------------------------------------------------------
        trans_rule = StateAdapter.build_trans_rule(
            src=signal.requester_id,
            dest=f"receptor:{self.receptor_id}",
            kind="CORE"
        )
        
        evolution_ctx = StateAdapter.build_evolution_context(
            phase_root=StateAdapter.adapt_swarm_to_phase_root(
                commit_hash=commit_hash,
                agents_dict={signal.requester_id: commit_hash}
            ),
            external_rules=[trans_rule]
        )

        transition_payload = StateAdapter.build_transition_payload(
            intent_action=signal.intent_action,
            intent_payload=signal.proposed_payload,
            evolution_ctx=evolution_ctx
        )

        transition_json = StateAdapter.to_canonical_bytes(transition_payload).decode('utf-8')
        trans_res = await self.broker.invoke("execute_transition", transition_json)

        if trans_res.success:
            trans_output = json.loads(trans_res.output)
            log.info(f"[{self.receptor_id}] ✨ Signal successfully bound to dphi.wasm! Commit: {commit_hash[:8]}...")
            
            return ReceptorBindingResult(
                is_bound=True,
                receptor_id=self.receptor_id,
                commit_hash=commit_hash,
                mutated_parity=trans_output.get("final_root"),
                fuel_consumed=getattr(trans_res, 'fuel_consumed', 0)
            )
        else:
            log.error(f"[{self.receptor_id}] 💥 Transition Collapsed: {trans_res.error}")
            return ReceptorBindingResult(
                is_bound=False,
                receptor_id=self.receptor_id,
                rupture_reason=f"Transition Collapsed: {trans_res.error}"
            )