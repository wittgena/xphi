# bridge.executor.residue
"""@flow: ψ → ResidueExecutor(Buffer → Minimal Tension Eval) → ResidueStore(rocks.db)"""
import asyncio
import time
import json
import hashlib
import sys
from rocksdict import Rdict, Options
from typing import List
from interface.pir import PsiType
from plane.emitter import get_logger
from bridge.executor.base import BaseExecutor
from plane.node.runtime import NodeRuntime
from anchor.resolver import find_current_self, resolve_path

log = get_logger("executor.residue")

try:
    SELF_ROOT = find_current_self()
    CACHE_ROOT = resolve_path("cache")
except Exception as e:
    log.error(f"[Critical] 시스템 경로 로드 실패: {e}")
    sys.exit(1)

ROCKS_PATH = CACHE_ROOT / "residue_rocks.db"

class ResidueStore:
    def __init__(self, path=ROCKS_PATH):
        opt = Options()
        opt.create_if_missing(True)
        self.db = Rdict(str(path), opt)

    def deposit(self, snapshot: dict):
        key = hashlib.sha1(json.dumps(snapshot).encode()).hexdigest().encode()
        self.db[key] = json.dumps(snapshot)

    def close(self):
        self.db.close()

class ResidueExecutor(BaseExecutor):
    """추출된 Block들을 버퍼링하고 Tension을 평가하여 db에 결정화"""
    def __init__(self, store: ResidueStore, batch_size: int = 5, threshold: float = 0.65):
        super().__init__()
        self.store = store
        self.batch_size = batch_size
        self.threshold = threshold
        self.buffer: List[dict] = []
        self.pressure: float = 0.0

    async def execute(self, psi: PsiType) -> List[PsiType]:
        block_data = json.loads(psi.symbol())
        self.buffer.append(block_data)

        if len(self.buffer) < self.batch_size:
            return [psi]

        batch = self.buffer
        self.buffer = []

        topology = {f"section::{hash(b.get('section', '')) % 5}" for b in batch}
        symbols = set(sym for b in batch for sym in b.get("symbols", []))

        if not symbols:
            return [psi]

        tension = min(len(symbols) / 10.0, 1.0)
        self.pressure = (self.pressure * 0.7) + tension

        if self.pressure >= self.threshold:
            snapshot = {
                "pressure": self.pressure,
                "tension": tension,
                "topology": list(topology),
                "symbols": list(symbols),
                "blocks": batch,  # 실제 파싱된 블록들을 함께 저장
                "ts": time.time()
            }
            self.store.deposit(snapshot)
            self.log.signal(f"[residue] DEPOSIT P={self.pressure:.3f} (Blocks: {len(batch)})")
            self.pressure = 0.0  
        
        return [psi]
