# xphi.arch.bound.adapter.pta
import os
import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass, field, asdict

import nacl.signing
import nacl.encoding
import nacl.exceptions
import cryptography.exceptions
from cryptography.hazmat.primitives import serialization

from xphi.state.anchor.store import AnchorStore, LogicStream
from xphi.state.anchor.oracle import AnchorOracle
from xphi.node.runtime.wasm.broker import DphiBroker
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.bound.adapter.state import StateAdapter

log = get_emitter("adapter.pta", phase="KERNEL")

class NodeSigner:
    """Singleton identity manager responsible for hierarchical key loading and deterministic cryptographic signature generation for the node"""
    _instance = None

    def __init__(self):
        self.signing_key = self._load_key_chain()
        self.verify_key = self.signing_key.verify_key
        self.pubkey_hex = self.verify_key.encode(encoder=nacl.encoding.HexEncoder).decode('utf-8')
        log.info(f"[Crypto] Node identity loaded. PubKey: {self.pubkey_hex[:8]}...")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _load_key_chain(self) -> nacl.signing.SigningKey:
        env_seed = os.environ.get("XPHI_NODE_SEED")
        if env_seed and len(env_seed) == 64:
            return nacl.signing.SigningKey(env_seed, encoder=nacl.encoding.HexEncoder)

        ssh_key_path = Path.home() / ".ssh" / "fiber_ed25519"
        if ssh_key_path.exists():
            try:
                with open(ssh_key_path, "rb") as key_file:
                    private_key = serialization.load_ssh_private_key(
                        key_file.read(),
                        password=None
                    )
                raw_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PrivateFormat.Raw,
                    encryption_algorithm=serialization.NoEncryption()
                )
                return nacl.signing.SigningKey(raw_bytes)
            except Exception as e:
                log.warning(f"Failed to load SSH key ({e}). Falling back to ephemeral key generation...")

        log.warning("[Crypto] No persistent identity found. Using ephemeral (in-memory) key.")
        return nacl.signing.SigningKey.generate()

    def sign_payload(self, canonical_bytes: bytes) -> str:
        payload_hash_str = hashlib.sha256(canonical_bytes).hexdigest()
        signed = self.signing_key.sign(payload_hash_str.encode('utf-8'))
        return signed.signature.hex()

    def verify_signature(self, canonical_bytes: bytes, signature_hex: str, pubkey_hex: Optional[str] = None) -> bool:
        try:
            payload_hash_str = hashlib.sha256(canonical_bytes).hexdigest()
            message_bytes = payload_hash_str.encode('utf-8')
            signature_bytes = bytes.fromhex(signature_hex)

            if pubkey_hex:
                vk = nacl.signing.VerifyKey(pubkey_hex, encoder=nacl.encoding.HexEncoder)
            else:
                vk = self.verify_key

            vk.verify(message_bytes, signature_bytes)
            return True
        except (nacl.exceptions.BadSignatureError, ValueError) as e:
            log.debug(f"[Crypto] Invalid signature detected: {e}")
            return False
        except Exception as e:
            log.error(f"[Crypto] Verification process failed: {e}")
            return False


class SignStateAdapter:
    @staticmethod
    def sign_state_payload(payload_dict: Dict[str, Any]) -> str:
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        signer = NodeSigner.get_instance()
        return signer.sign_payload(canonical_bytes)
        
    @staticmethod
    def get_signer_pubkey() -> str:
        return NodeSigner.get_instance().pubkey_hex


def compute_merkle_root(tx_hashes: List[str]) -> str:
    if not tx_hashes:
        return ""
    
    current_level = tx_hashes
    while len(current_level) > 1:
        next_level = []
        for i in range(0, len(current_level), 2):
            left = current_level[i]
            right = current_level[i+1] if i+1 < len(current_level) else left
            combined = f"{left}{right}".encode('utf-8')
            next_level.append(hashlib.sha256(combined).hexdigest())
        current_level = next_level
        
    return current_level[0]


@dataclass
class PtaPointer:
    tx_hash: str
    output_index: int

    def to_key(self) -> str:
        return f"{self.tx_hash}:{self.output_index}"


@dataclass
class PtaInput:
    pointer: PtaPointer
    signature: str  
    owner_address: str = "" 


@dataclass
class PtaOutput:
    amount: int
    owner: str
    asset_type: str = "fuel"


@dataclass
class PhaseAnchorOutput(PtaOutput):
    handle_id: str = ""
    phase_status: str = ""    
    executable_payload: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.asset_type = "mcp_state_anchor"


def create_mcp_state_anchor(handle_id: str, status: str, payload: Optional[Dict[str, Any]] = None) -> PhaseAnchorOutput:
    if payload is None:
        payload = {}
        
    return PhaseAnchorOutput(
        amount=0,                                
        owner=f"mcp_bridge_{handle_id}",       
        handle_id=handle_id,
        phase_status=status.upper(),
        executable_payload=payload
    )


@dataclass
class PtaTransaction:
    inputs: List[PtaInput]
    outputs: List[PtaOutput]
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    tx_hash: str = field(init=False)

    def __post_init__(self):
        self.tx_hash = self._compute_hash()

    def _compute_hash(self) -> str:
        payload = {
            "inputs": [{"tx": i.pointer.tx_hash, "idx": i.pointer.output_index} for i in self.inputs],
            "outputs": [asdict(o) for o in self.outputs],
            "meta": self.metadata,
            "ts": self.timestamp_ms
        }
        canonical_str = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "inputs": [{"pointer": i.pointer.to_key(), "signature": i.signature, "owner": i.owner_address} for i in self.inputs],
            "outputs": [asdict(o) for o in self.outputs],
            "metadata": self.metadata,
            "timestamp_ms": self.timestamp_ms
        }


class PtaAdapter:
    _shared_unfold_pool: Dict[str, PtaOutput] = {}
    _shared_spent_mempool: Set[str] = set()
    _shared_tx_commit_map: Dict[str, str] = {}

    def __init__(self, broker: DphiBroker):
        self.broker = broker
        self.ledger = AnchorStore()
        self.oracle = AnchorOracle(broker=broker)
        
        self._unfold_pool = PtaAdapter._shared_unfold_pool
        self._spent_mempool = PtaAdapter._shared_spent_mempool
        self._tx_commit_map = PtaAdapter._shared_tx_commit_map
        self._inst_id = hex(id(self))[-6:].upper()
        log.debug(f"[PtaAdapter:{self._inst_id}] 🟢 Initialized new adapter instance.")

    def _verify_ed25519_signature(self, payload: str, signature_hex: str, owner_address: str) -> bool:
        try:
            payload_dict = {"payload": payload}
            canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
            
            signer = NodeSigner.get_instance()
            is_valid = signer.verify_signature(
                canonical_bytes=canonical_bytes, 
                signature_hex=signature_hex, 
                pubkey_hex=owner_address
            )
            if not is_valid:
                log.warning(f"[PtaAdapter:{self._inst_id}] Cryptographic Auth Failed: Signature mismatch.")
                
            return is_valid
        except cryptography.exceptions.InvalidSignature:
            log.warning(f"[PtaAdapter:{self._inst_id}] Signature structurally invalid or modified.")
            return False
        except Exception as e:
            log.error(f"[PtaAdapter:{self._inst_id}] Signature verification crashed: {e}")
            return False

    def _resolve_commit_hash(self, tx_hash: str, stream_id: Optional[str] = None) -> str:
        """tx_hash로부터 매핑된 실제 DB commit 해시(signature)를 역참조합니다."""
        if tx_hash in self._tx_commit_map:
            return self._tx_commit_map[tx_hash]

        target_stream_id = stream_id if stream_id is not None else f"pta_tx_{tx_hash[:8]}"
        head_hash = self.ledger.get_head_hash(target_stream_id)
        if head_hash:
            self._tx_commit_map[tx_hash] = head_hash
            return head_hash

        scanned_hash = self.oracle.find_commit_hash_by_tx(tx_hash)
        if scanned_hash:
            self._tx_commit_map[tx_hash] = scanned_hash
            return scanned_hash

        return tx_hash

    async def execute_transaction(self, tx: PtaTransaction, stream_id: Optional[str] = None) -> str:
        log.debug(f"[PtaAdapter:{self._inst_id}] ⚡ Executing Tx: {tx.tx_hash[:8]} (Inputs: {len(tx.inputs)}, Outputs: {len(tx.outputs)})")
        
        for idx, current_input in enumerate(tx.inputs):
            if current_input.owner_address:
                payload_to_verify = current_input.pointer.to_key()
                
                if payload_to_verify in self._spent_mempool:
                    raise PermissionError(f"Double Spend Detected for input {payload_to_verify}")
                    
                is_valid = self._verify_ed25519_signature(payload_to_verify, current_input.signature, current_input.owner_address)
                if not is_valid:
                    raise PermissionError(f"Cryptographic Auth Failed for input index {idx}")

        # [선형 소비 보장] 경쟁 시 최초 1개만 통과시키고 나머지는 Double Spend로 즉시 폐기하기 위해
        # await 진입 전에 input 포인터를 _spent_mempool에 선점 등록
        spent_keys = [current_input.pointer.to_key() for current_input in tx.inputs]
        for pointer_key in spent_keys:
            self._spent_mempool.add(pointer_key)

        target_stream_id = stream_id if stream_id is not None else f"pta_tx_{tx.tx_hash[:8]}"
        stream = LogicStream(
            id=target_stream_id,
            action="PTA_STATE_TRANSITION",
            payload=tx.to_dict(),
            metadata=tx.metadata
        )
        
        try:
            sealed_kernel = await self.ledger.propose_and_seal(stream)
        except Exception:
            for pointer_key in spent_keys:
                self._spent_mempool.discard(pointer_key)
            raise

        if sealed_kernel:
            for pointer_key in spent_keys:
                self._unfold_pool.pop(pointer_key, None)
                
            produced_keys = []
            for idx, output in enumerate(tx.outputs):
                new_pointer_key = f"{tx.tx_hash}:{idx}"
                self._unfold_pool[new_pointer_key] = output
                produced_keys.append(f"{new_pointer_key}({output.amount})")
                
            receipt_signature = sealed_kernel.signature
            self._tx_commit_map[tx.tx_hash] = receipt_signature
            log.debug(f"[PtaAdapter:{self._inst_id}] 🔒 Mempool Update -> SPENT: {spent_keys} | PRODUCED: {produced_keys}")
            log.info(f"[PtaAdapter:{self._inst_id}] Tx Sealed. Receipt: {receipt_signature[:8]}... | Hash: {tx.tx_hash[:8]}")
        else:
            for pointer_key in spent_keys:
                self._spent_mempool.discard(pointer_key)
            log.warning(
                f"[PtaAdapter:{self._inst_id}] ⚠️ Tx Unsealed (Relay Timeout or Rejected). "
                f"Skipped local mempool mutation. Stream: {target_stream_id} | Hash: {tx.tx_hash[:8]}"
            )
            
        return tx.tx_hash

    async def get_balance(self, owner_address: str, asset_type: str = "fuel") -> int:
        log.debug(f"[PtaAdapter:{self._inst_id}] 🔍 Balance query requested for '{owner_address}' ({asset_type})")
        
        confirmed_outputs = await self.oracle.get_unspent_outputs(owner_address, asset_type)
        
        oracle_sum = 0
        spent_sum = 0
        produced_sum = 0
        confirmed_pointers = set()
        
        for item in confirmed_outputs:
            amount = item['output'].get('amount', 0)
            oracle_sum += amount
            pointer_key = f"{item['pointer']['tx_hash']}:{item['pointer']['output_index']}"
            confirmed_pointers.add(pointer_key)
            self._unfold_pool.pop(pointer_key, None)
            if pointer_key in self._spent_mempool:
                spent_sum += amount
                
        for pointer_key, output in list(self._unfold_pool.items()):
            if pointer_key not in confirmed_pointers and output.owner == owner_address and output.asset_type == asset_type:
                produced_sum += output.amount
                
        total_balance = oracle_sum - spent_sum + produced_sum
        log.debug(
            f"[PtaAdapter:{self._inst_id}] 📊 Balance Breakdown: "
            f"Oracle({oracle_sum}) - SpentLocal({spent_sum}) + ProducedLocal({produced_sum}) = FINAL({total_balance})"
        )
        return total_balance

    async def get_available_inputs(self, owner_address: str, asset_type: str = "fuel") -> List[Dict]:
        confirmed_outputs = await self.oracle.get_unspent_outputs(owner_address, asset_type)
        available_inputs = []
        confirmed_pointers = set()
        
        for item in confirmed_outputs:
            pointer_key = f"{item['pointer']['tx_hash']}:{item['pointer']['output_index']}"
            confirmed_pointers.add(pointer_key)
            self._unfold_pool.pop(pointer_key, None)
            if pointer_key not in self._spent_mempool:
                available_inputs.append({
                    "pointer": PtaPointer(tx_hash=item['pointer']['tx_hash'], output_index=item['pointer']['output_index']),
                    "amount": item['output'].get('amount', 0),
                    "output": item['output'],
                    "metadata": item.get("metadata", {})
                })
                
        for pointer_key, output in list(self._unfold_pool.items()):
            if pointer_key not in confirmed_pointers and output.owner == owner_address and output.asset_type == asset_type:
                tx_hash, idx = pointer_key.split(":")
                available_inputs.append({
                    "pointer": PtaPointer(tx_hash=tx_hash, output_index=int(idx)),
                    "amount": output.amount,
                    "output": asdict(output),
                    "metadata": {}
                })
                
        return available_inputs

    async def verify_lineage(self, tx_hash: str, depth: int = 5) -> bool:
        try:
            commit_hash = self._resolve_commit_hash(tx_hash)
            res = await self.oracle.verify_kernel_lineage(commit_hash, depth)
            is_valid = res.get("is_valid", False)
            if not is_valid:
                log.warning(f"[PtaAdapter:{self._inst_id}] PTA Lineage verification failed for {tx_hash[:8]}")
            return is_valid
        except Exception as e:
            log.error(f"[PtaAdapter:{self._inst_id}] Lineage verification error: {e}")
            return False

    def shutdown(self):
        self.oracle.close()