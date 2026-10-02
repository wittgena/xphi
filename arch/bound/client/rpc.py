# xphi.arch.bound.client.rpc
import asyncio
import orjson
from typing import Dict, Any

from xphi.arch.contract.config import env
from xphi.arch.bound.event.next import uuid4 as topos_uuid4
from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("client.rpc")

class RpcException(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"[{status_code}] {detail}")

class InternalRpcClient:
    def __init__(self, queue_name: str = env.RPC_QUEUE_TOPIC):
        self.queue_name = queue_name

    async def call(self, method: str, params: Dict[str, Any], timeout: float = 15.0) -> Dict[str, Any]:
        tunnel = await TunnelFactory.get_default()
        job_id = f"rpc_{topos_uuid4().hex[:16]}"
        reply_channel = f"reply.{job_id}"
        
        pubsub = tunnel.pubsub()
        await pubsub.subscribe(reply_channel)
        
        try:
            rpc_payload = orjson.dumps({
                "id": job_id,
                "method": method,
                "params": params,
                "reply_to": reply_channel
            }, option=orjson.OPT_NON_STR_KEYS).decode('utf-8')
            
            log.debug(f"[RPC Request] {method} ({job_id})")
            await tunnel.stream_produce(self.queue_name, {"payload": rpc_payload})
            
            async with asyncio.timeout(timeout):
                async for msg in pubsub.listen():
                    if isinstance(msg, dict) and msg.get("type") == "message":
                        response = orjson.loads(msg["data"])
                        err_data = response.get("error")
                        if err_data:
                            code = err_data.get("code", 500)
                            message = err_data.get("message", "Internal RPC Error")
                            log.warning(f"[RPC Error] {method} failed: {message}")
                            raise RpcException(status_code=code, detail=message)
                            
                        return response.get("result", {})
                        
        except asyncio.TimeoutError:
            log.error(f"[RPC Timeout] Method {method} exceeded {timeout}s.")
            raise RpcException(
                status_code=504, 
                detail=f"Gateway Timeout: Upstream edge worker failed to respond ({method})"
            )
        except RpcException:
            raise
        except Exception as e:
            log.error(f"[RPC Exception] Method {method} crashed: {e}", exc_info=True)
            raise RpcException(status_code=500, detail="Internal Edge Communication Error")
        finally:
            await pubsub.unsubscribe(reply_channel)
            await pubsub.close()

    async def publish_intent(self, channel: str, payload: Dict[str, Any]):
        tunnel = await TunnelFactory.get_default()
        try:
            encoded_payload = orjson.dumps(payload, option=orjson.OPT_NON_STR_KEYS).decode('utf-8')
            await tunnel.publish(channel, encoded_payload)
            log.debug(f"[RPC Broadcast] Sent intent to {channel}")
        except Exception as e:
            log.error(f"[RPC Broadcast Error] Failed to publish to {channel}: {e}", exc_info=True)
            raise