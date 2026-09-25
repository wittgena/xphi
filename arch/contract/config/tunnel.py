# xphi.arch.contract.config.tunnel
import urllib.parse
import logging
from enum import Enum
from dataclasses import dataclass
from typing import Tuple

from xphi.arch.contract.config import env

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
    """@flow: 환경 변수에서 공통 인프라 설정을 추출 (env 모듈 사용)"""
    # [변경됨] os.getenv 모두 제거하고 env 모듈 사용
    engine_str = env.MQ_ENGINE
    host = env.MQ_HOST
    port = env.MQ_PORT
    
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
        state_url = env.STATE_STORE_URL
        mq_url = target_url

    return scheme, state_url, mq_url