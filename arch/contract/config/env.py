# xphi.arch.contract.config.env
import os
from xphi.arch.contract.config.resolver import config as dynamic_config

def _get_string(key: str, default: str) -> str:
    if key in dynamic_config._local_overrides:
        return dynamic_config.get(key)
    return os.getenv(key, default)

def _get_int(key: str, default: str) -> int:
    if key in dynamic_config._local_overrides:
        return int(dynamic_config.get(key))
    return int(os.getenv(key, default))

XPHI_BASE = _get_string("XPHI_BASE", "http://localhost:8079")
LOG_LEVEL = _get_string("LOG_LEVEL", "INFO")

"""Cryptography & KMS"""
AIRGAP_MODE = _get_string("AIRGAP_MODE", "0")

"""Infrastructure & Tunnel"""
REDIS_HOST = _get_string("XPHI_REDIS_HOST", _get_string("REDIS_HOST", "localhost"))
REDIS_PORT = _get_int("XPHI_REDIS_PORT", _get_int("REDIS_PORT", "6379"))

MQ_ENGINE = _get_string("MQ_ENGINE", "redis")
MQ_HOST = _get_string("MQ_HOST", REDIS_HOST)
MQ_PORT = _get_int("MQ_PORT", str(REDIS_PORT))

# State Store URL의 기본값을 REDIS_HOST/PORT를 참조하여 동적으로 생성
_default_state_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
STATE_STORE_URL = _get_string("STATE_STORE_URL", _default_state_url)