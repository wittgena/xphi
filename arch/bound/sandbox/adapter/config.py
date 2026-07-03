# arch.bound.sandbox.adapter.config
## @lineage: arch.bound.sandbox.adapter
"""
@desc: Universal Infrastructure Adapter Base
@flow: 시스템 전역의 MQ/State Store 연결 정보 파싱 및 프로토콜 규격을 정의합니다.
"""
import os
import urllib.parse
from enum import Enum
from dataclasses import dataclass
from typing import Tuple

from watcher.plane.emitter import get_emitter
log = get_emitter("sandbox.adapter")

class BackendProtocol(str, Enum):
    """지원하는 백엔드 프로토콜 규격"""
    REDIS = "redis"
    KAFKA = "kafka"
    MEMORY = "memory"

@dataclass
class MqConfig:
    """인프라 연결 설정을 담는 불변 객체"""
    engine: BackendProtocol
    host: str
    port: int

    @property
    def default_url(self) -> str:
        return f"{self.engine.value}://{self.host}:{self.port}/0"

def resolve_default_config() -> MqConfig:
    """@flow: 환경 변수에서 공통 인프라 설정을 추출 (Fallback 사슬 적용)"""
    engine_str = os.getenv("MQ_ENGINE", "redis")
    host = os.getenv("MQ_HOST", os.getenv("REDIS_HOST", "localhost"))
    port = int(os.getenv("MQ_PORT", os.getenv("REDIS_PORT", "6379")))
    
    try:
        engine = BackendProtocol(engine_str)
    except ValueError:
        log.warning(f"[Adapter] Unknown MQ_ENGINE '{engine_str}'. Falling back to REDIS.")
        engine = BackendProtocol.REDIS

    return MqConfig(engine=engine, host=host, port=port)

def parse_connection_urls(target_url: str) -> Tuple[BackendProtocol, str, str]:
    """
    @flow: 입력된 URL을 분석하여 (프로토콜, 상태 저장소 URL, MQ URL) 세 쌍을 반환합니다.
    Kafka 등 상태 저장이 불가능한 MQ일 경우, 상태 저장소는 Redis로 자동 분리(Routing)됩니다.
    """
    parsed = urllib.parse.urlparse(target_url)
    try:
        scheme = BackendProtocol(parsed.scheme)
    except ValueError:
        log.warning(f"[Adapter] Unknown scheme '{parsed.scheme}'. Falling back to REDIS.")
        scheme = BackendProtocol.REDIS

    if scheme == BackendProtocol.REDIS:
        state_url = target_url
        mq_url = target_url
    else:
        # 이기종 인프라 라우팅 (Kafka는 MQ로, 상태 저장은 기존 Redis로 분리)
        state_url = os.getenv("STATE_STORE_URL", "redis://localhost:6379/0")
        mq_url = target_url

    return scheme, state_url, mq_url