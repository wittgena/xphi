# xphi.arch.contract.protocol.rpc
import json
import time
import uuid
from typing import Dict, Any

from xphi.node.runtime.wasm.broker import WasmBroker

class WorkerContext:
    def __init__(
        self, 
        broker: WasmBroker,
        store: Any,
        exchange_adapter: Any,
        pta_adapter: Any, 
        profile_service: Any
    ):
        self.broker = broker
        self.store = store
        self.exchange_adapter = exchange_adapter
        self.pta_adapter = pta_adapter
        self.profile_service = profile_service

def _build_error(code: int, message: str) -> dict:
    """RPC 표준 에러 응답 빌더"""
    return {"error": True, "code": code, "message": message}