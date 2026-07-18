# phase.runtime.wasm.broker
import json
import uuid
import time
from typing import Optional, Any, Mapping, Callable
from arch.xor.proto.code import ExecutionResult, ExecutionError
from arch.topos.bound.tunnel import TunnelFactory
from watcher.plane.emitter import get_emitter

log = get_emitter("wasm.broker")

class WasmBroker:
    """Proxy client delegating execution to WasmTaskerDaemon via Stream & Pub/Sub."""
    def __init__(self, request_stream: str = "wasm:execute:stream", timeout: float = 10.0):
        self.request_stream = request_stream
        self.control_channel = "wasm:control:req"
        self.timeout = timeout
        
    def _dispatch_and_wait(self, job_id: str, payload: dict, response_channel: str, target_route: str = None) -> ExecutionResult:
        route = target_route or self.request_stream
        tunnel = TunnelFactory.get_sync()
        
        try:
            listen_client = TunnelFactory.get_isolated_sync()
            pubsub = listen_client.pubsub()
            pubsub.subscribe(response_channel)
            
            # [핵심 라우팅] 제어 명령은 PubSub(Broadcast), 실행 명령은 Stream(Exclusive)
            if route == self.control_channel:
                log.info(f"[{job_id[:8]}] Broadcasting control task '{payload.get('target_func', 'unknown')}'...")
                tunnel.publish(route, json.dumps(payload))
            else:
                log.info(f"[{job_id[:8]}] Enqueuing execution task '{payload.get('target_func', 'unknown')}' to Stream...")
                # UniversalFacade의 state_store(Redis)를 통한 XADD 수행
                tunnel.state_store.xadd(route, {"data": json.dumps(payload)})
            
            # 응답 대기 로직은 동일
            start_time = time.time()
            while time.time() - start_time < self.timeout:
                msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                if msg and msg["type"] == "message":
                    try:
                        result_data = json.loads(msg["data"])
                        if result_data.get("success"):
                            return ExecutionResult(success=True, output=result_data.get("output", ""))
                        else:
                            return ExecutionResult(success=False, error=ExecutionError(result_data.get("error")))
                    except json.JSONDecodeError:
                        continue
                        
            return ExecutionResult(success=False, error=ExecutionError(f"Remote execution timeout ({self.timeout}s)"))
        finally:
            if 'pubsub' in locals(): pubsub.close()
            if 'listen_client' in locals(): listen_client.close()

    def update_policy(self, tier: str) -> bool:
        """[Broadcast] 모든 워커의 Policy 동시 업데이트"""
        job_id = str(uuid.uuid4())
        response_channel = f"wasm:control:res:{job_id}"
        payload = {
            "job_id": job_id,
            "target_func": "update_policy",
            "tier": tier.upper(),
            "response_channel": response_channel
        }
        res = self._dispatch_and_wait(job_id, payload, response_channel, target_route=self.control_channel)
        return res.success

    def invoke(self, target_func: str, payload: str, wasm_path: Optional[str] = None) -> ExecutionResult:
        """[Exclusive] 특정 함수를 Stream Queue를 통해 실행 위임"""
        job_id = str(uuid.uuid4())
        response_channel = f"wasm:execute:res:{job_id}"
        msg_payload = {
            "job_id": job_id, 
            "target_func": target_func, 
            "data": payload, 
            "response_channel": response_channel
        }
        if wasm_path:
            msg_payload["wasm_path"] = wasm_path
            
        return self._dispatch_and_wait(job_id, msg_payload, response_channel)

    def execute(self, code: str, variables: Mapping[str, Any] | None = None) -> ExecutionResult:
        """[Exclusive] 레거시 파이썬 코드 실행 (Stream Queue)"""
        job_id = str(uuid.uuid4())
        response_channel = f"wasm:execute:res:{job_id}"
        msg_payload = {
            "job_id": job_id,
            "target_func": "execute_code", 
            "data": code,                  
            "variables": variables or {},
            "response_channel": response_channel
        }
        return self._dispatch_and_wait(job_id, msg_payload, response_channel)