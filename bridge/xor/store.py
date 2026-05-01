# xor.store
"""@flow: ψ → ResidueExecutor(Buffer → Minimal Tension Eval) → ResidueStore(rocks.db)"""
import asyncio
import time
import json
import hashlib
import sys
from typing import List, Set, Any, Dict
from dataclasses import dataclass, field, asdict
from rocksdict import Rdict, Options
from bridge.event.psi import PsiType
from flow.surface.emitter import get_logger
from bound.resolver import find_current_self, resolve_path
from bridge.node.runtime import NodeRuntime
from contract.executor.base import BaseExecutor

log = get_logger("xe.store")

try:
    SELF_ROOT = find_current_self()
    XOR_ROOT = resolve_path("xor")
except Exception as e:
    log.error(f"[Critical] 시스템 경로 로드 실패: {e}")
    sys.exit(1)

ROCKS_PATH = XOR_ROOT / "residue.rocks.db"

@dataclass
class ResidueBlock:
    """단일 실행(psi)에서 추출된 잔여 블록 데이터 모델"""
    section: str = ""
    symbols: List[str] = field(default_factory=list)
    raw_data: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ResidueSnapshot:
    """RocksDB에 결정화(저장)될 위상 스냅샷 데이터 모델"""
    pressure: float
    tension: float
    topology_nodes: List[str]
    symbols: List[str]
    blocks: List[Dict[str, Any]]
    timestamp: float = field(default_factory=time.time)
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))

class ResidueStore:
    """잔여물(xe)의 물리적 저장소 (RocksDB)"""
    def __init__(self, path=ROCKS_PATH):
        opt = Options()
        opt.create_if_missing(True)
        self.db = Rdict(str(path), opt)

    def deposit(self, snapshot: ResidueSnapshot):
        """Data Model을 직렬화하여 안전하게 저장"""
        json_data = snapshot.to_json()
        key = hashlib.sha1(json_data.encode()).hexdigest().encode()
        self.db[key] = json_data

    def close(self):
        self.db.close()


class ResidueExecutor(BaseExecutor):
    """@flow: Buffer → Tension Eval → Deposit"""
    def __init__(self, store: ResidueStore, batch_size: int = 5, threshold: float = 0.65):
        super().__init__()
        self.store = store
        self.batch_size = batch_size
        self.threshold = threshold
        
        self.buffer: List[ResidueBlock] = []
        self.current_pressure: float = 0.0

    async def execute(self, psi: PsiType) -> List[PsiType]:
        ## Psi Payload 파싱 및 Data Model 매핑
        try:
            raw_dict = json.loads(psi.symbol)
            block = ResidueBlock(
                section=raw_dict.get("section", "unknown"),
                symbols=raw_dict.get("symbols", []),
                raw_data=raw_dict
            )
            self.buffer.append(block)
        except json.JSONDecodeError:
            log.warning("Psi symbol is not valid JSON. Skipped residue extraction.")
            return [psi]

        ## 배치 사이즈 도달 대기
        if len(self.buffer) < self.batch_size:
            return [psi]

        ## 위상 평가 (Tension & Topology Evaluation)
        batch = self.buffer
        self.buffer = []  # 버퍼 초기화

        topology_nodes: Set[str] = {f"section::{hash(b.section) % 5}" for b in batch}
        extracted_symbols: Set[str] = {sym for b in batch for sym in b.symbols}

        if not extracted_symbols:
            return [psi]

        ## 긴장도 산출 및 누적 압력 계산
        tension = min(len(extracted_symbols) / 10.0, 1.0)
        self.current_pressure = (self.current_pressure * 0.7) + tension

        ## 임계치 도달 시 결정화 (Crystallization)
        if self.current_pressure >= self.threshold:
            snapshot = ResidueSnapshot(
                pressure=self.current_pressure,
                tension=tension,
                topology_nodes=list(topology_nodes),
                symbols=list(extracted_symbols),
                blocks=[b.raw_data for b in batch]
            )
            
            self.store.deposit(snapshot)
            log.signal(f"[residue] DEPOSIT P={self.current_pressure:.3f} (Blocks: {len(batch)})")
            
            ## 압력 해소 (응결 완료)
            self.current_pressure = 0.0  
        
        return [psi]