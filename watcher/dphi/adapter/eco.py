# watcher.dphi.adapter.eco
import time
import json
import hashlib
from typing import Any
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

class EcoAdapter:
    @classmethod
    def build_ap2_mandate(
        cls, 
        requester_id: str, 
        target_action: str, 
        max_spend_usdc: str, 
        signer_key: ed25519.Ed25519PrivateKey,
        validity_ms: int = 3600000
    ) -> dict[str, Any]:
        """@desc: Creates a Verifiable Credential-like Mandate for agent authorization."""
        expiration_ts = int(time.time() * 1000) + validity_ms
        
        mandate_payload = {
            "protocol": "AP2-v1.0",
            "requester_id": requester_id,
            "target_action": target_action,
            "constraints": {
                "max_spend_usdc": max_spend_usdc,
                "expiration_ts": expiration_ts
            },
            "issued_at": int(time.time() * 1000)
        }
        
        # JCS (JSON Canonicalization Standard) for deterministic hashing
        canonical_bytes = json.dumps(mandate_payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        signature = signer_key.sign(hashlib.sha256(canonical_bytes).digest()).hex()
        pubhex = signer_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        ).hex()

        return {
            "mandate": mandate_payload,
            "authorization": {
                "signer_pub": pubhex,
                "signature": signature
            }
        }

    @classmethod
    def build_x402_invoice(cls, payee_address: str, amount_usdc: str, resource_id: str) -> dict[str, Any]:
        """@desc: Generates an HTTP 402 Payment Required invoice for M2M interactions."""
        return {
            "status": "HTTP_402_PAYMENT_REQUIRED",
            "x_payment_protocol": "x402/base-sepolia",
            "pay_to": payee_address,
            "amount_usdc": amount_usdc,
            "resource_id": resource_id,
            "timestamp": int(time.time() * 1000)
        }

    @classmethod
    def process_x402_settlement(
        cls, 
        invoice: dict[str, Any], 
        agent_wallet_address: str,
        wallet_adapter: Any = None
    ) -> dict[str, Any]:
        """@desc: Settles the invoice via injected WalletAdapter"""
        tx_hash = ""
        if wallet_adapter and not wallet_adapter.simulate:
            tx_hash = wallet_adapter.transfer(
                to_address=invoice["pay_to"],
                amount=invoice["amount_usdc"],
                asset="usdc"
            )
        else:
            # 시뮬레이션
            mock_tx_seed = f"{invoice['pay_to']}_{invoice['amount_usdc']}_{time.time()}".encode('utf-8')
            tx_hash = f"0x{hashlib.sha256(mock_tx_seed).hexdigest()}"

        return {
            "receipt_id": f"rcpt_{tx_hash[2:14]}",
            "tx_hash": tx_hash,
            "network": invoice.get("x_payment_protocol", "base-sepolia"),
            "paid_amount_usdc": invoice["amount_usdc"],
            "payer_wallet": agent_wallet_address,
            "settled_at": int(time.time() * 1000)
        }

    @classmethod
    def embed_economy_state(cls, base_cached_states: dict[str, Any], mandate: dict, receipt: dict) -> dict[str, Any]:
        """@desc: Embeds AP2 Mandate and x402 Receipt into the cached state for Epoch Sealing."""
        updated_state = dict(base_cached_states) if base_cached_states else {}
        updated_state["ap2_mandate"] = mandate
        updated_state["x402_settlement_receipt"] = receipt
        return updated_state