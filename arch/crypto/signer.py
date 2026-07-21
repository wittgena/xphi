# arch.crypto.signer
# phase/crypto/signer.py
import os
from pathlib import Path
import hashlib
import nacl.signing
import nacl.encoding
from cryptography.hazmat.primitives import serialization
from watcher.plane.emitter import get_emitter

log = get_emitter("crypto.signer")

class NodeSigner:
    """계층적 키 로딩 및 서명 생성을 담당하는 싱글톤 Identity Manager"""
    _instance = None

    def __init__(self):
        self.signing_key = self._load_key_chain()
        self.verify_key = self.signing_key.verify_key
        # 공개키를 Hex 문자열로 캐싱
        self.pubkey_hex = self.verify_key.encode(encoder=nacl.encoding.HexEncoder).decode('utf-8')
        log.info(f"[Crypto] Node Identity Loaded. PubKey: {self.pubkey_hex[:8]}...")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _load_key_chain(self) -> nacl.signing.SigningKey:
        # 1순위: 환경변수 (.env)에서 64자리 Raw Hex Seed 로드 (서버/데몬용)
        env_seed = os.environ.get("XPHI_NODE_SEED")
        if env_seed and len(env_seed) == 64:
            return nacl.signing.SigningKey(env_seed, encoder=nacl.encoding.HexEncoder)

        # 2순위: 로컬 SSH 키 폴백 (개발자 로컬 CLI용)
        ssh_key_path = Path.home() / ".ssh" / "id_ed25519"
        if ssh_key_path.exists():
            try:
                # OpenSSH 프라이빗 키 파싱
                with open(ssh_key_path, "rb") as key_file:
                    private_key = serialization.load_ssh_private_key(
                        key_file.read(),
                        password=None # Passphrase가 없다고 가정
                    )
                # cryptography 객체에서 pynacl로 호환 변환
                raw_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.Raw,
                    format=serialization.PrivateFormat.Raw,
                    encryption_algorithm=serialization.NoEncryption()
                )
                return nacl.signing.SigningKey(raw_bytes)
            except Exception as e:
                log.warning(f"Failed to load SSH key ({e}). Generating ephemeral key...")

        # 3순위: 모두 없으면 휘발성(일회용) 키 생성
        log.warning("[Crypto] No identity found. Using ephemeral (in-memory) key.")
        return nacl.signing.SigningKey.generate()

    def sign_anchor_commit(self, canonical_bytes: bytes) -> str:
        """
        [매우 중요] WASM 엔진의 로직과 동일하게 동작해야 함.
        WASM은 AnchorCommit을 JCS로 만들고 -> SHA256 해시를 뜬 '문자열'에 대해 서명을 검증함.
        """
        # 1. 파이썬 쪽에서 먼저 SHA256 해시 생성 (WASM과 동일한 조건 생성)
        commit_hash_str = hashlib.sha256(canonical_bytes).hexdigest()
        
        # 2. 그 해시 문자열의 바이트를 대상으로 서명
        signed = self.signing_key.sign(commit_hash_str.encode('utf-8'))
        
        # 3. 128자리 Hex 서명 반환
        return signed.signature.hex()