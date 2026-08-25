# kernel.space.topos.tunnel.flare
import logging
import asyncio
import httpx
from typing import Optional, Any, List, Tuple

from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.topos.tunnel.config import BackendProtocol, resolve_default_config, parse_connection_urls

log = get_emitter("tunnel.flare")

class FlareFacade:
    """
    @role: Cloudflare Edge 환경(HTTP/Fetch)에 최적화된 독립적 라우팅 파사드
    @desc: 기존 UniversalFacade(Redis/TCP)를 상속하지 않고 시그니처만 100% 복제(Duck Typing)하여 종속성 오염 원천 차단
    """
    def __init__(self, state_url: str, mq_url: str, mq_protocol: BackendProtocol, **kwargs):
        self.mq_protocol = mq_protocol
        self.mq_url = mq_url
        self.wasm_broker = None
        
        # Redis TCP Socket 대신 비동기 HTTP Client로 엣지 네트워크 통신
        pool_kwargs = {
            "timeout": httpx.Timeout(10.0, connect=5.0),
            "verify": False
        }
        self.http_client = httpx.AsyncClient(base_url=self.mq_url, **pool_kwargs)
        log.info(f"[FlareTunnel] Initialized FlareFacade (Stateless HTTP) at {self.mq_url}")

    def bind_wasm_broker(self, broker):
        self.wasm_broker = broker
        log.info("[FlareTunnel] WasmBroker bound to FlareFacade for background telemetry auditing.")

    async def _audit_provenance(self, raw_payload: dict, message_id: str):
        try:
            if isinstance(raw_payload, dict) and "_wasm_envelope" in raw_payload:
                data_str = raw_payload.get("data")
                if data_str and self.wasm_broker:
                    res = await self.wasm_broker.invoke("compute_root_fingerprint", data_str)
                    if res.success:
                        log.info(f"🧾 [Audit] Message {message_id} proven at Edge. Fingerprint: {res.output}")
                    else:
                        log.warning(f"⚠️ [Audit] Failed to prove message {message_id}: {res.error}")
        except Exception as e:
            log.error(f"[Audit] Background provenance task failed for message {message_id}: {e}")

    async def stream_produce(self, topic: str, payload: dict, maxlen: int = 100000) -> str:
        """기존 Redis xadd를 CF Queues/Worker Endpoint POST 요청으로 치환"""
        response = await self.http_client.post(f"/api/stream/{topic}/produce", json={
            "payload": payload,
            "maxlen": maxlen
        })
        response.raise_for_status()
        msg_id = response.json().get("message_id", "")
        
        if self.wasm_broker:
            asyncio.create_task(self._audit_provenance(payload, msg_id))
            
        return msg_id

    async def stream_consume(self, topic: str, group: str, consumer: str, count: int = 1, block: int = 0) -> List[Tuple]:
        """기존 Redis xreadgroup을 CF Queues Polling/Worker Endpoint로 치환"""
        response = await self.http_client.post(f"/api/stream/{topic}/consume", json={
            "group": group,
            "consumer": consumer,
            "count": count,
            "block": block
        })
        if response.status_code == 404:  # 스트림이 없거나 비었을 때 예외 처리
            return []
        response.raise_for_status()
        return response.json().get("messages", [])

    async def stream_ack(self, topic: str, group: str, message_id: str):
        """기존 Redis xack를 CF Endpoint로 치환"""
        response = await self.http_client.post(f"/api/stream/{topic}/ack", json={
            "group": group,
            "message_id": message_id
        })
        response.raise_for_status()
        return response.json().get("acknowledged", 0)

    async def publish(self, channel: str, message: Any):
        """기존 Redis publish를 CF Endpoint로 치환"""
        response = await self.http_client.post(f"/api/pubsub/{channel}/publish", json={
            "message": message
        })
        response.raise_for_status()
        return response.json().get("receivers", 0)

    async def close(self):
        """TCP 커넥션 풀 종료 대신 HTTP 클라이언트 세션 종료"""
        await self.http_client.aclose()


class FlareTunnelFactory:
    """
    기존 TunnelFactory와 동일한 메서드(get_default, get_isolated 등)를 제공.
    호출하는 곳(Router, Agent 등)의 코드를 수정하지 않고 Factory 교체만으로 주입(Injection) 가능.
    """
    _async_instance: Optional[FlareFacade] = None

    @classmethod
    async def get_default(cls, **kwargs) -> FlareFacade:
        if cls._async_instance is None:
            config = resolve_default_config()
            scheme, state_url, mq_url = parse_connection_urls(config.default_url)
            
            # [FIX] kwargs에서 오버라이드용 값을 안전하게 추출(pop)하여 충돌 원천 차단
            mq_url = kwargs.pop("mq_url", mq_url)
            state_url = kwargs.pop("state_url", state_url)
            scheme = kwargs.pop("mq_protocol", scheme)
            
            cls._async_instance = FlareFacade(state_url, mq_url, scheme, **kwargs)
            log.info(f"[FlareTunnelFactory] Provisioned Async Flare Tunnel: {mq_url}")
        return cls._async_instance

    @classmethod
    async def get_isolated(cls, **kwargs) -> FlareFacade:
        config = resolve_default_config()
        scheme, state_url, mq_url = parse_connection_urls(config.default_url)
        
        mq_url = kwargs.pop("mq_url", mq_url)
        state_url = kwargs.pop("state_url", state_url)
        scheme = kwargs.pop("mq_protocol", scheme)
        
        return FlareFacade(state_url, mq_url, scheme, **kwargs)
        
    @classmethod
    async def get_provenant(cls, wasm_broker, **kwargs) -> FlareFacade:
        tunnel = await cls.get_default(**kwargs)
        tunnel.bind_wasm_broker(wasm_broker)
        return tunnel

    @classmethod
    async def close_all(cls):
        if cls._async_instance:
            await cls._async_instance.close()
            cls._async_instance = None