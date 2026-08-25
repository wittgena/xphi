# kernel.space.topos.tunnel.config
import os
import urllib.parse
import logging
from enum import Enum
from dataclasses import dataclass
from typing import Tuple

log = logging.getLogger("tunnel.config")

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
        state_url = os.getenv("STATE_STORE_URL", "redis://localhost:6379/0")
        mq_url = target_url

    return scheme, state_url, mq_url