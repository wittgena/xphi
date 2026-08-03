# kernel.dphi.adapter.eco
## @lineage: watcher.dphi.adapter.eco
import os
import time
import json
import hashlib
from typing import Any, Dict, List

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from arch.xor.surge.model import DynamicSurgeModel
from arch.xor.secret.manager import get_secret_str
from watcher.plane.emitter import get_emitter
from kernel.dphi.exchange.config import billing_config
from kernel.dphi.cgroup import Tier

log = get_emitter("dphi.adapter")

class Ap2MandateConstraints(DynamicSurgeModel):
    max_spend_usdc: str
    expiration_ts: int

class Ap2MandatePayload(DynamicSurgeModel):
    protocol: str
    requester_id: str
    target_action: str
    constraints: Ap2MandateConstraints
    issued_at: int

class Ap2Authorization(DynamicSurgeModel):
    signer_pub: str
    signature: str

class Ap2MandateResult(DynamicSurgeModel):
    mandate: Ap2MandatePayload
    authorization: Ap2Authorization

class X402Invoice(DynamicSurgeModel):
    status: str
    x_payment_protocol: str
    pay_to: str
    amount_usdc: str
    resource_id: str
    timestamp: int

class X402SettlementReceipt(DynamicSurgeModel):
    receipt_id: str
    tx_hash: str
    network: str
    paid_amount_usdc: str
    payer_wallet: str
    settled_at: int

class TransactionReceipt(DynamicSurgeModel):
    job_id: str
    topos_id: str
    unified_parity_hash: int  
    clearing_signatures: List[str] 
    fuel_consumed: int
    settlement_status: str

class SettlementPayload(DynamicSurgeModel):
    batch_id: str
    state_root: int
    validators: List[str]
    gas_used: int
    timestamp: int

class EcoAdapter:
    @classmethod
    def build_ap2_mandate(
        cls, 
        requester_id: str, 
        target_action: str, 
        max_spend_usdc: str, 
        signer_key: ed25519.Ed25519PrivateKey,
        validity_ms: int = 3600000
    ) -> Ap2MandateResult:
        """@desc: Creates a Verifiable Credential-like Mandate for agent authorization."""
        expiration_ts = int(time.time() * 1000) + validity_ms
        
        constraints = Ap2MandateConstraints.suture(
            max_spend_usdc=max_spend_usdc,
            expiration_ts=expiration_ts
        )
        
        payload = Ap2MandatePayload.suture(
            protocol="AP2-v1.0",
            requester_id=requester_id,
            target_action=target_action,
            constraints=constraints,
            issued_at=int(time.time() * 1000)
        )
        
        # Pydantic V2 model_dump() 사용
        mandate_dict = payload.model_dump()
        canonical_bytes = json.dumps(mandate_dict, sort_keys=True, separators=(',', ':')).encode('utf-8')
        signature = signer_key.sign(hashlib.sha256(canonical_bytes).digest()).hex()
        pubhex = signer_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

        auth = Ap2Authorization.suture(signer_pub=pubhex, signature=signature)
        return Ap2MandateResult.suture(mandate=payload, authorization=auth)

    @classmethod
    def build_x402_invoice(cls, payee_address: str, amount_usdc: str, resource_id: str) -> X402Invoice:
        """@desc: Generates an HTTP 402 Payment Required invoice for M2M interactions."""
        return X402Invoice.suture(
            status="HTTP_402_PAYMENT_REQUIRED",
            x_payment_protocol="x402/base-sepolia",
            pay_to=payee_address,
            amount_usdc=amount_usdc,
            resource_id=resource_id,
            timestamp=int(time.time() * 1000)
        )

    @classmethod
    def process_x402_settlement(
        cls, 
        invoice: X402Invoice, 
        agent_wallet_address: str,
        wallet_adapter: Any = None
    ) -> X402SettlementReceipt:
        """@desc: Settles the strictly typed X402Invoice via injected WalletAdapter"""
        tx_hash = ""
        if wallet_adapter and not wallet_adapter.simulate:
            tx_hash = wallet_adapter.transfer(
                to_address=invoice.pay_to,
                amount=invoice.amount_usdc,
                asset="usdc"
            )
        else:
            # 시뮬레이션
            mock_tx_seed = f"{invoice.pay_to}_{invoice.amount_usdc}_{time.time()}".encode('utf-8')
            tx_hash = f"0x{hashlib.sha256(mock_tx_seed).hexdigest()}"

        return X402SettlementReceipt.suture(
            receipt_id=f"rcpt_{tx_hash[2:14]}",
            tx_hash=tx_hash,
            network=invoice.x_payment_protocol,
            paid_amount_usdc=invoice.amount_usdc,
            payer_wallet=agent_wallet_address,
            settled_at=int(time.time() * 1000)
        )

    @classmethod
    def embed_economy_state(
        cls, 
        base_cached_states: Dict[str, Any], 
        mandate: Ap2MandateResult | None = None, 
        receipt: X402SettlementReceipt | None = None
    ) -> Dict[str, Any]:
        """
        @desc: Embeds strictly typed models back to raw dicts into the generic cached state.
        Adapter maintains rigid type contracts. Any bypass must explicitly pass `None`.
        """
        updated_state = dict(base_cached_states) if base_cached_states else {}
        
        # 유연한 예외 처리(dict, hasattr 등)를 배제하고 명확한 모델 타입이 있을 때만 덤프 허용
        if mandate is not None:
            updated_state["ap2_mandate"] = mandate.model_dump()
            
        if receipt is not None:
            updated_state["x402_settlement_receipt"] = receipt.model_dump()
            
        return updated_state


# ==============================================================================
# 2. Exchange Adapter (System & Clearing Layer)
# ==============================================================================
class ExchangeAdapter:
    def __init__(self, clearing_house_pub_key: str):
        self.clearing_house_pub = clearing_house_pub_key

    def _quantize_fuel_cost(self, fuel_consumed: int, tier: Tier) -> float:
        base_cost = (fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
        multiplier = 1.5 if tier == Tier.SYSTEM else 1.0
        return base_cost * multiplier

    def finalize_settlement(
        self, 
        entangled_state: dict, 
        signatures: List[str], 
        cost_metrics: dict, 
        tier: Tier = Tier.SYSTEM
    ) -> TransactionReceipt:
        parity = entangled_state.get("parity", {})
        unified_topos = parity.get("topos_id", f"unknown_batch_{int(time.time())}")
        unified_phase = parity.get("phase_id", 0) 
        
        fuel_consumed = cost_metrics.get("fuel_consumed", 0)
        estimated_usd = self._quantize_fuel_cost(fuel_consumed, tier)
        
        log.info(f"[Exchange Adapter] Settlement Finalized. Topos: {unified_topos}, Parity: {unified_phase}, Cost: ${estimated_usd:.6f}")
        return TransactionReceipt.suture(
            job_id=unified_topos,
            topos_id=unified_topos,
            unified_parity_hash=unified_phase,
            clearing_signatures=signatures,
            fuel_consumed=fuel_consumed,
            settlement_status="COMMITTED_TO_NEXUS"
        )
        
    def generate_settlement_payload(self, receipt: TransactionReceipt) -> SettlementPayload:
        """@desc: Translates a TransactionReceipt into a strict external payload."""
        return SettlementPayload.suture(
            batch_id=receipt.job_id,
            state_root=receipt.unified_parity_hash,
            validators=receipt.clearing_signatures,
            gas_used=receipt.fuel_consumed,
            timestamp=int(time.time())
        )


# ==============================================================================
# 3. Wallet Adapter (Infrastructure Layer)
# ==============================================================================
def inject_and_clear_secrets(secrets: dict[str, str], action_fn: callable):
    os.environ.update(secrets)
    try:
        return action_fn()
    finally:
        for k in secrets.keys():
            os.environ.pop(k, None)

class WalletAdapter:
    def __init__(self, network_id: str = "base-sepolia", simulate: bool = False):
        self.network_id = network_id
        self.simulate = simulate
        self.wallet = None
        
        if not self.simulate:
            self._initialize_secure_wallet()

    def _initialize_secure_wallet(self):
        try:
            from coinbase_agentkit import CdpWalletProvider
        except ImportError as e:
            log.error("[Wallet] coinbase_agentkit not installed.")
            raise RuntimeError("Missing required SDK for secure wallet") from e

        api_name = get_secret_str("CDP_API_KEY_NAME")
        api_pkey = get_secret_str("CDP_API_KEY_PRIVATE_KEY")
        
        if not api_name or not api_pkey:
            log.error("[Wallet] CDP API Keys missing in SecretManager.")
            raise ValueError("Incomplete credentials for CDP Wallet initialization")

        injected_secrets = {
            "CDP_API_KEY_NAME": api_name,
            "CDP_API_KEY_PRIVATE_KEY": api_pkey
        }
        
        try:
            self.wallet = inject_and_clear_secrets(
                injected_secrets, 
                lambda: CdpWalletProvider.create_wallet(network_id=self.network_id)
            )
            log.info(f"[Wallet] CDP Wallet created successfully on {self.network_id} (Secured via SecretManager)")
        except Exception as e:
            log.error(f"[Wallet] Failed to initialize CDP Wallet: {e}")
            raise

    def fund_wallet(self, asset: str = "usdc", amount: str = "0.1") -> bool:
        if self.simulate:
            log.info(f"[Wallet-Sim] Simulated funding {amount} {asset}.")
            return True
            
        log.info(f"[Wallet] Requesting faucet for {amount} {asset}...")
        try:
            self.wallet.fund(asset=asset, amount=amount)
            return True
        except Exception as e:
            log.error(f"[Wallet] Faucet funding failed: {e}")
            raise

    def transfer(self, to_address: str, amount: str, asset: str = "usdc") -> str:
        if self.simulate:
            mock_hash = f"0xsim_{int(time.time()*1000)}"
            log.info(f"[Wallet-Sim] Transferred {amount} {asset} to {to_address}. Tx: {mock_hash}")
            return mock_hash

        log.info(f"[Wallet] Transferring {amount} {asset} to {to_address}...")
        try:
            receipt = self.wallet.transfer(to_address=to_address, amount=amount, asset=asset)
            tx_hash = getattr(receipt, "transaction_hash", getattr(receipt, "hash", str(receipt)))
            log.info(f"[Wallet] Transfer success. Tx: {tx_hash}")
            return tx_hash
        except Exception as e:
            log.error(f"[Wallet] Transfer failed: {e}")
            raise