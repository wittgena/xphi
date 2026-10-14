# xphi.arch.contract.protocol.rpc
import json
import time
import uuid
from typing import Dict, Any

from xphi.kernel.wasm.broker import DphiBroker

class WorkerContext:
    def __init__(
        self, broker: DphiBroker, store: Any, nexus: Any, exchange_adapter: Any,
        pta_adapter: Any, policy_engine: Any, profile_service: Any, phase_store: Any = None 
    ):
        self.broker = broker
        self.store = store
        self.nexus = nexus
        self.exchange_adapter = exchange_adapter
        self.pta_adapter = pta_adapter
        self.policy_engine = policy_engine
        self.profile_service = profile_service
        self.phase_store = phase_store

def _build_error(code: int, message: str) -> dict:
    """RPC 표준 에러 응답 빌더"""
    return {"error": True, "code": code, "message": message}