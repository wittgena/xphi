# bridge.interface.event
import asyncio
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional

class SnowflakeGenerator:
    def __init__(self, worker_id: int = 1, datacenter_id: int = 1):
        # 파라미터 제약 조건 (각 5비트이므로 0~31)
        self.worker_id = worker_id & 0x1F
        self.datacenter_id = datacenter_id & 0x1F
        self.sequence = 0
        
        # Epoch 설정 (2026-01-01 기준 예시)
        self.epoch = 1767225600000 
        
        self.last_timestamp = -1
        self._lock = threading.Lock()

    def _timestamp(self) -> int:
        return int(time.time() * 1000)

    def generate(self) -> int:
        with self._lock:
            timestamp = self._timestamp()

            if timestamp < self.last_timestamp:
                raise Exception("Clock moved backwards!")

            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & 0xFFF # 12비트 마스크
                if self.sequence == 0:
                    # 동일 밀리초 내 시퀀스 소진 시 다음 밀리초까지 대기
                    while timestamp <= self.last_timestamp:
                        timestamp = self._timestamp()
            else:
                self.sequence = 0

            self.last_timestamp = timestamp

            # 비트 시프트 결합
            # (Timestamp - Epoch) << 22 | (DC) << 17 | (Worker) << 12 | Sequence
            return ((timestamp - self.epoch) << 22) | \
                   (self.datacenter_id << 17) | \
                   (self.worker_id << 12) | \
                   self.sequence

## 전역 싱글톤 인스턴스 (환경변수 등에서 ID를 받아오도록 확장 가능)
generator = SnowflakeGenerator(worker_id=1, datacenter_id=1)

def next_id() -> str:
    """PsiEvent에 사용하기 좋게 문자열로 반환"""
    return str(generator.generate())

def parse_id(snowflake_id: str):
    """ID를 분석하여 생성 시점과 작업자 정보를 복원"""
    sid = int(snowflake_id)
    timestamp = (sid >> 22) + 1767225600000 # epoch 반영
    datacenter_id = (sid >> 17) & 0x1F
    worker_id = (sid >> 12) & 0x1F
    sequence = sid & 0xFFF
    return {
        "timestamp_ms": timestamp,
        "worker_info": f"{datacenter_id}:{worker_id}",
        "seq": sequence
    }

@dataclass
class LogEvent:
    """
    @event.contract: Telemetry counterpart to PsiEvent
    Identity(2) + Origin(3) + Content(3) + Metrics(3)
    """
    ## 1. Identity (PsiEvent 호환)
    event_id: str = field(default_factory=next_id)
    parent_id: Optional[str] = None  # 어떤 PsiEvent에 의해 발생한 로그인지 추적 가능
    
    ## 2. Origin & Temporal (PsiEvent 호환)
    source_id: str = "unknown"       # 기존 source -> source_id 로 변경
    scope: str = "LOG"               # 기본 스코프
    tick: Optional[int] = None       # 시간축
    
    ## 3. Content (Log 고유)
    level: str = "INFO"
    kind: str = "log"                # "log" or "summary" (carrier.kind 역할)
    message: str = ""
    
    ## 4. Context (PsiEvent 호환 - 분산된 메타데이터 응집)
    # 기존 flow_id, phase, bound, payload가 모두 이 안으로 통합됩니다.
    context: Dict[str, Any] = field(default_factory=dict)
    
    ## 5. Plane Metrics (BoundPlane 전용 제어 상태 - 동적 할당 방지)
    density: float = 0.0
    gain: float = 1.0
    fold_count: int = 1

