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

XPHI_BASE = _get_string("XPHI_BASE", "http://localhost:8000")
LOG_LEVEL = _get_string("LOG_LEVEL", "INFO")
USER_AGENT = _get_string("XPHI_USER_AGENT", "xphi-agent/1.0.1")

RPC_QUEUE_TOPIC = os.getenv("RPC_QUEUE_TOPIC", "internal.rpc.queue")

"""Cryptography & KMS"""
DPHI_ENV = _get_string("DPHI_ENV", "prod")
AIRGAP_MODE = _get_string("AIRGAP_MODE", "0")

"""Infrastructure & Tunnel"""
REDIS_HOST = _get_string("XPHI_REDIS_HOST", _get_string("REDIS_HOST", "localhost"))
REDIS_PORT = _get_int("XPHI_REDIS_PORT", _get_int("REDIS_PORT", "6379"))

MQ_ENGINE = _get_string("MQ_ENGINE", "redis")
MQ_HOST = _get_string("MQ_HOST", REDIS_HOST)
MQ_PORT = _get_int("MQ_PORT", str(REDIS_PORT))

_default_state_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
STATE_STORE_URL = _get_string("STATE_STORE_URL", _default_state_url)

## COMPAT & VCR
FIBER_COMPAT_RULES_PATH = _get_string("FIBER_COMPAT_RULES_PATH", "")
VCR_INCLUDE_RAW = _get_string("VCR_INCLUDE_RAW", "false")