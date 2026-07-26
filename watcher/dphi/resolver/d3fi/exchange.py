# watcher.dphi.resolver.d3fi.exchange
import time
import json
import hashlib
from typing import List
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from watcher.dphi.adapter.exchange import D3fiExchangeAdapter
from watcher.dphi.adapter.state import StateAdapter
from watcher.dphi.resolver.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("scenario.exchange")

class ExchangeScenario(SchemeRunner):
    """
    @desc: Order Ingress & Deterministic Settlement (Intent-based P2P Matching)
    @flow: Raw Order -> Gateway Validation -> Order Binding (Matched State) -> On-Chain Settlement Commit -> Exchange Receipt
    @spec.insight:
      - [EVM/Smart Contract]: Lock-free parallel processing. Replaces AMM liquidity pools with Intent-based order matching.
      - [TradFi/Quant]: Zero counterparty risk. Cryptographically-bound OTC clearing via M-of-N deterministic multi-sig.
      - [ZK/Rollup]: Lightweight O(1) state verification. Replaces heavy ZK circuits via XOR-based Parity Hash check.
      - [Architecture]: WASM Resource Sandbox. Strict compute (Fuel) bounds mitigate resource-exhaustive HFT/ML algorithms.
      - [Exchange]: Quantizes computational fuel and issues verifiable receipts for monetization.
    """
    def __init__(self, broker):
        super().__init__(broker)
        # 1. Generate keys for the two Trading Agents (Buyer / Seller)
        self.agent_a_key = ed25519.Ed25519PrivateKey.generate()
        self.agent_b_key = ed25519.Ed25519PrivateKey.generate()
        
        self.agent_a_pub = self._get_pub_hex(self.agent_a_key)
        self.agent_b_pub = self._get_pub_hex(self.agent_b_key)
        
        # 2. Generate key for the Clearing House / Matching Engine (State Coordinator)
        self.field_key = ed25519.Ed25519PrivateKey.generate()
        self.field_pub = self._get_pub_hex(self.field_key)
        
        # [NEW] 3. 결제 어댑터(환전소) 인스턴스화
        self.exchange_adapter = D3fiExchangeAdapter(clearing_house_pub_key=self.field_pub)
        self.last_receipt = None

    def _get_pub_hex(self, priv_key):
        return priv_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

    def _sign(self, priv_key, payload_dict):
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        return priv_key.sign(hashlib.sha256(canonical_bytes).digest()).hex()

    async def run_all(self):
        log.info("\n=== [START] Executing P2P Order Ingress & Deterministic Settlement ===")
        await self._set_worker_policy("SYSTEM")
        
        # [Step 1] Gateway: Ingress of external trade intent & Sequence (Topos) assignment
        phase_a = await self._step1_gateway_ingress(self.agent_a_pub, "offer_tokenX_for_tokenY")
        phase_b = await self._step1_gateway_ingress(self.agent_b_pub, "offer_tokenY_for_tokenX")
        
        # [Step 2] Matching Engine: Binding the two Session IDs (Phases) and validating zero imbalance
        entangled_state = await self._step2_exchange_entanglement(phase_a, phase_b)
        
        # [Step 3] Settlement: Deterministic commit of the matched state into the immutable Ledger (Nexus)
        signatures = await self._step3_nexus_collapse(entangled_state)
        
        # [NEW] [Step 4] Finalize & Exchange: Convert resolved state into a monetizable, external receipt
        # 실제 시스템에서는 broker를 통해 실시간 연료(Fuel) 소모량을 가져와야 하지만, 시나리오상 목업 데이터 주입
        estimated_fuel = 35000 
        
        self.last_receipt = self.exchange_adapter.finalize_settlement(
            entangled_state=entangled_state,
            signatures=signatures,
            cost_metrics={"fuel_consumed": estimated_fuel},
            tier="SYSTEM"
        )
        
        # 영수증을 외부(EVM 스마트 컨트랙트 등)로 쏠 수 있는 페이로드 형태로 변환
        external_payload = self.exchange_adapter.generate_settlement_payload(self.last_receipt)
        log.info(f"\n[Exchange Ready] Payload for External Network (Rollup Sequencer):")
        log.info(json.dumps(external_payload, indent=2))
        
        self.report()

    async def _step1_gateway_ingress(self, agent_pub, intent_action):
        """
        @flow: Raw Request -> API Gateway Validation -> Session (Phase) Initialization
        @spec: 
          - [Resource Sandbox]: Imposes a 'Fuel/Press' compute limit on raw transaction intents.
          - [Lock-free Concurrency]: Generates independent Session IDs (Phase IDs) without locking the global state tree.
        """
        log.info(f"\n--- [Gateway Ingress] Validating trade intent from {agent_pub[:8]}... ---")
        
        raw_intent = {
            "agent_id": agent_pub,
            "action": intent_action,
            "timestamp": int(time.time() * 1000)
        }
        
        # Gateway applies boundaries: Assigns global sequence (Topos) and allocates compute limits (Press/Fuel)
        ingress_payload = {
            "ts": raw_intent["timestamp"],
            "topo": 101,          # Gateway Sequence Constraint (Anti-replay/Ordering)
            "press": 5,           # Computational Complexity (Mapped to WASM Fuel Limit)
            "rupture": False,
            "injected_intent": raw_intent
        }
        
        # Serializing payload into Canonical JSON (JCS) and initializing execution state
        res = await self.broker.invoke("init_epoch", StateAdapter.to_canonical_bytes(ingress_payload).decode('utf-8'))
        parity_triplet = json.loads(res.output)
        
        log.info(f"  └─ [Ingress Validated] Assigned Sequence (Topos): {parity_triplet['topos_id']}, Generated Session (Phase): {parity_triplet['phase_id']}")
        return parity_triplet

    async def _step2_exchange_entanglement(self, phase_a, phase_b):
        """
        @flow: Matching Engine -> Order Binding & State Merging
        @spec:
          - [Intent Matching]: Matches complementary orders directly. Eliminates need for AMM liquidity pools.
          - [O(1) Parity Validation]: Binds execution states via XOR for constant-time state verification by validators.
        """
        log.info("\n--- [Matching Engine] Binding Execution State A (Bid) and State B (Ask) ---")
        
        # Clearing logic validates if the paired Sessions leave zero remaining balance (imbalance/tension = 0)
        # (Business logic is abstracted; focusing on cryptographic state binding)
        entangled_repos = {
            "participant_a": phase_a["phase_id"],
            "participant_b": phase_b["phase_id"],
            "field_status": "matched_fully_filled"  # Orders completely offset each other
        }
        
        log.info("  └─ [Matched] Opposite intents paired successfully. Remaining imbalance = 0.")
        
        # Create a unified Parity Triplet representing the merged state of the Matching Engine
        unified_topos = f"clearing_batch_{int(time.time())}"
        unified_phase = phase_a["phase_id"] ^ phase_b["phase_id"]  # Cryptographic XOR State Binding
        
        unified_parity = StateAdapter.build_parity_triplet(
            topos_id=unified_topos,
            phase_id=unified_phase,
            nexus_id=777777  # Pending Settlement State (Pre-commit Nexus)
        )
        
        return {"parity": unified_parity, "repos": entangled_repos}

    async def _step3_nexus_collapse(self, entangled_state) -> List[str]:
        """
        @flow: Settlement -> Seal Epoch
        @spec:
          - [Deterministic Clearing]: Requires tripartite M-of-N consensus (Buyer + Seller + Clearing Engine).
          - [Finality Commit]: Commits the pending matched state into a single, permanent Ledger State (Nexus ID).
        @return: The list of cryptographic signatures representing the M-of-N consensus.
        """
        log.info("\n--- [Trade Settlement] Finalizing clearing via 3-of-3 Multi-sig Consensus ---")
        
        parity = entangled_state["parity"]
        
        # Construct Anchor state for deterministic settlement commit
        anchor_commit = StateAdapter.build_anchor_commit(
            parity=parity,
            parent_nexus_id=0, # Independent clearing batch genesis
            parent_commit_id="genesis",
            repos=entangled_state["repos"],
            cached_states={}
        )
        
        # Multi-sig Committee: Tripartite Consensus (Agent A, Agent B, and the Clearing House)
        signers = [self.agent_a_pub, self.agent_b_pub, self.field_pub]
        signatures = [
            self._sign(self.agent_a_key, anchor_commit),
            self._sign(self.agent_b_key, anchor_commit),
            self._sign(self.field_key, anchor_commit)
        ]
        
        seal_payload = StateAdapter.build_seal_epoch_payload(
            parity=parity,
            parent_nexus_id=0,
            self_parent_state="genesis",
            repos=entangled_state["repos"],
            cached_states={},
            timestamp=time.time(),
            signers=signers,
            signatures=signatures,
            threshold=3,
            allowed_signers=signers
        )
        
        await self._run_case(
            "Trade Settlement: 3-of-3 Multi-sig State Committed to Nexus", 
            "seal_epoch", 
            seal_payload, 
            expected_success=True
        )
        return signatures