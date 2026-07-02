# watcher.kernel.store
"""@desc: Merkle Tree backed RocksDB storage dedicated for Execution Kernel Ledger"""
import time
import json
import hashlib
from typing import Any, Dict, Optional, List
from dataclasses import dataclass, field, asdict
from rocksdict import Rdict, Options
from watcher.plane.emitter import get_emitter
from phase.bind.resolver import resolve_path

log = get_emitter("kernel.store")

LEDGER_DB_PATH = resolve_path("xor") / "ledger.rocks.db"

def deterministic_hash(data: Dict[str, Any]) -> str:
    """결정론적 직렬화를 통한 무결성 해시(SHA-256) 생성"""
    serialized = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode('utf-8')).hexdigest()

@dataclass
class ToposBlob:
    """단일 상태 전이(Transition) 기록 (Git의 Blob)"""
    action: str
    from_state: str
    to_state: str
    tension: float
    details: str
    timestamp: float = field(default_factory=time.time)

@dataclass
class KernelCommit:
    """임계치를 넘어 밀봉된 커널의 스냅샷 (Git의 Commit)"""
    stream_id: str
    executable_payload: Any
    tension_at_seal: float
    blob_hashes: List[str]
    parent_hash: Optional[str] = None
    sealed_at: float = field(default_factory=time.time)

class KernelStore:
    """커널 위상 장부를 위한 전용 Content-Addressable Storage"""
    _instance = None

    def __new__(cls, path=LEDGER_DB_PATH):
        if cls._instance is None:
            cls._instance = super(KernelStore, cls).__new__(cls)
            opt = Options()
            opt.create_if_missing(True)
            cls._instance.db = Rdict(str(path), opt)
        return cls._instance

    def _put_object(self, obj_type: str, data: Dict[str, Any]) -> str:
        obj_hash = deterministic_hash(data)
        key = f"{obj_type}:{obj_hash}".encode('utf-8')
        if key not in self.db:
            self.db[key] = json.dumps(data).encode('utf-8')
        return obj_hash

    def save_transition(self, blob: ToposBlob) -> str:
        """상태 전이 기록을 Blob으로 저장"""
        return self._put_object("blob", asdict(blob))

    def save_kernel(self, commit: KernelCommit) -> str:
        """밀봉된 커널을 Commit으로 저장"""
        return self._put_object("commit", asdict(commit))

    def update_head(self, stream_id: str, commit_hash: str) -> None:
        """특정 로직 스트림의 최신 커널 포인터(Ref) 업데이트"""
        key = f"ref:{stream_id}".encode('utf-8')
        self.db[key] = commit_hash.encode('utf-8')

    def get_head_hash(self, stream_id: str) -> Optional[str]:
        """해당 스트림의 최신 커널 해시 반환"""
        key = f"ref:{stream_id}".encode('utf-8')
        if key in self.db:
            return self.db[key].decode('utf-8')
        return None
        
    def close(self):
        self.db.close()