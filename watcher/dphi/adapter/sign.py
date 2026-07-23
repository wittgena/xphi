# watcher.dphi.adapter.sign
## @lineage: arch.crypto.signer
import os
from pathlib import Path
import hashlib
import nacl.signing
import nacl.encoding
from cryptography.hazmat.primitives import serialization
from typing import Dict, Any

from watcher.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("crypto.signer")

class NodeSigner:
    """
    Singleton identity manager responsible for hierarchical key loading 
    and deterministic cryptographic signature generation for the node.
    """
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
        """
        Resolves the node's signing key through a prioritized fallback mechanism.
        
        Priority 1: Environment variable (Server/Daemon environment)
        Priority 2: Local SSH Ed25519 key (Developer CLI environment)
        Priority 3: Ephemeral auto-generation (In-memory fallback)
        """
        # Priority 1: Load a 64-character raw hex seed from environment variables
        env_seed = os.environ.get("XPHI_NODE_SEED")
        if env_seed and len(env_seed) == 64:
            return nacl.signing.SigningKey(env_seed, encoder=nacl.encoding.HexEncoder)

        # Priority 2: Fallback to the local SSH key
        ssh_key_path = Path.home() / ".ssh" / "id_ed25519"
        if ssh_key_path.exists():
            try:
                # Parse the OpenSSH private key (assuming no passphrase)
                with open(ssh_key_path, "rb") as key_file:
                    private_key = serialization.load_ssh_private_key(
                        key_file.read(),
                        password=None
                    )
                # Convert the cryptography key object to PyNaCl compatible raw bytes
                raw_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PrivateFormat.Raw,
                    encryption_algorithm=serialization.NoEncryption()
                )
                return nacl.signing.SigningKey(raw_bytes)
            except Exception as e:
                log.warning(f"Failed to load SSH key ({e}). Falling back to ephemeral key generation...")

        # Priority 3: Generate an ephemeral key if no persistent identity is found
        log.warning("[Crypto] No persistent identity found. Using ephemeral (in-memory) key.")
        return nacl.signing.SigningKey.generate()

    def sign_payload(self, canonical_bytes: bytes) -> str:
        """
        Generates an Ed25519 signature for the given payload.
        
        [CRITICAL ARCHITECTURE NOTE]
        This method must perfectly mirror the WASM engine's verification logic. 
        The WASM spatial fence expects the signature to be generated against the 
        SHA-256 hex string representation of the Canonical JSON (JCS) payload.
        Args:
            canonical_bytes (bytes): The deterministically serialized JSON data.
        Returns:
            str: A 128-character hexadecimal string representing the signature.
        """
        # Step 1: Generate the SHA-256 hex string of the canonical bytes
        payload_hash_str = hashlib.sha256(canonical_bytes).hexdigest()
        
        # Step 2: Sign the UTF-8 encoded hash string
        signed = self.signing_key.sign(payload_hash_str.encode('utf-8'))
        
        # Step 3: Return the resulting hexadecimal signature
        return signed.signature.hex()

class LedgerAuthAdapter:
    """
    @module: LedgerAuthAdapter
    @desc: 
    - Ledger 상태 전이를 위한 보안/인증 전용 어댑터.
    - 객체(Dict)를 Canonical JCS로 변환 후 물리적 노드 키로 서명합니다.
    """

    @staticmethod
    def sign_state_payload(payload_dict: Dict[str, Any]) -> str:
        """
        주어진 딕셔너리를 FFI 호환 JCS(Canonical JSON)로 변환하고 서명합니다.
        """
        # 1. StateAdapter를 이용해 결정론적 바이트 변환 (JCS)
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        
        # 2. 물리 노드의 Signer 인스턴스를 통해 서명
        signer = NodeSigner.get_instance()
        return signer.sign_payload(canonical_bytes)
        
    @staticmethod
    def get_signer_pubkey() -> str:
        """현재 노드의 공개키 반환 (Multi-Sig 스키마 빌드용)"""
        return NodeSigner.get_instance().pubkey_hex