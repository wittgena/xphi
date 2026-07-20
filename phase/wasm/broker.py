# phase.wasm.broker
import json
import uuid
import asyncio
from typing import Optional, Any, Mapping
from arch.xor.proto.code import ExecutionResult, ExecutionError
from arch.topos.bound.tunnel import TunnelFactory
from watcher.plane.emitter import get_emitter

log = get_emitter("wasm.broker")

class WasmBroker:
    """
    @role: Non-blocking Proxy client delegating intents to WasmTaskerDaemon.
    @flow: Enqueues tasks to Stream (XADD) and awaits asynchronously via Pub/Sub.
    """
    def __init__(self, request_stream: str = "wasm:execute:stream", timeout: float = 10.0):
        self.request_stream = request_stream
        self.control_channel = "wasm:control:req"
        self.timeout = timeout
        
    async def _dispatch_and_wait_async(self, job_id: str, payload: dict, response_channel: str, target_route: str = None) -> ExecutionResult:
        route = target_route or self.request_stream
        tunnel = await TunnelFactory.get_default()
        listen_client = await TunnelFactory.get_isolated()
        pubsub = listen_client.pubsub()
        
        try:
            await pubsub.subscribe(response_channel)
            
            if route == self.control_channel:
                log.info(f"[{job_id[:8]}] Broadcasting control task '{payload.get('target_func', 'unknown')}'...")
                await tunnel.publish(route, json.dumps(payload))
            else:
                log.info(f"[{job_id[:8]}] Enqueuing execution intent '{payload.get('target_func', 'unknown')}' to WASM Stream...")
                await tunnel.state_store.xadd(route, {"data": json.dumps(payload)})
            
            try:
                async with asyncio.timeout(self.timeout):
                    async for msg in pubsub.listen():
                        if msg and msg["type"] == "message":
                            try:
                                result_data = json.loads(msg["data"])
                                if result_data.get("success"):
                                    return ExecutionResult(success=True, output=result_data.get("output", ""))
                                else:
                                    return ExecutionResult(success=False, error=ExecutionError(result_data.get("error", "Unknown Execution Error")))
                            except json.JSONDecodeError:
                                log.warning(f"[{job_id[:8]}] Unparseable response received, skipping.")
                                continue
            except asyncio.TimeoutError:
                log.error(f"[{job_id[:8]}] Remote execution timeout ({self.timeout}s) on {route}")
                return ExecutionResult(success=False, error=ExecutionError(f"Remote execution timeout ({self.timeout}s)"))
                
        finally:
            await pubsub.unsubscribe(response_channel)
            await pubsub.close()
            if hasattr(listen_client.state_store, 'aclose'):
                await listen_client.state_store.aclose()
            elif hasattr(listen_client.state_store, 'close'):
                await listen_client.state_store.close()

    async def update_policy(self, tier: str) -> bool:
        job_id = str(uuid.uuid4())
        response_channel = f"wasm:control:res:{job_id}"
        payload = {
            "job_id": job_id,
            "target_func": "update_policy",
            "tier": tier.upper(),
            "response_channel": response_channel
        }
        res = await self._dispatch_and_wait_async(job_id, payload, response_channel, target_route=self.control_channel)
        return res.success

    async def invoke(self, target_func: str, payload: str, wasm_path: Optional[str] = None) -> ExecutionResult:
        job_id = str(uuid.uuid4())
        response_channel = f"wasm:execute:res:{job_id}"
        msg_payload = {
            "job_id": job_id, 
            "target_func": target_func, 
            "payload": payload,
            "response_channel": response_channel
        }
        if wasm_path:
            msg_payload["wasm_path"] = wasm_path
            
        return await self._dispatch_and_wait_async(job_id, msg_payload, response_channel)

    async def execute(self, code: str, variables: Mapping[str, Any] | None = None) -> ExecutionResult:
        job_id = str(uuid.uuid4())
        response_channel = f"wasm:execute:res:{job_id}"
        msg_payload = {
            "job_id": job_id,
            "target_func": "execute_code", 
            "payload": {"code": code, "variables": variables or {}},
            "response_channel": response_channel
        }
        return await self._dispatch_and_wait_async(job_id, msg_payload, response_channel)