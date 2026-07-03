# arch.bound.sandbox.tunnel
"""
@desc: Universal Message/State Tunnel (Async & Sync Implementation)
@flow: 
  비동기(Async) 처리를 기본으로 하되, 비동기 루프를 사용할 수 없는 
  동기(Sync) 환경을 위한 최소한의 파사드를 함께 제공합니다.
"""
import redis
import redis.asyncio as actual_redis
import redis.exceptions
from typing import Optional, Any, List, Tuple
from arch.bound.sandbox.adapter.config import BackendProtocol, resolve_default_config, parse_connection_urls
from watcher.plane.emitter import get_emitter

log = get_emitter("sandbox.tunnel")

class UniversalPubSub:
    def __init__(self, protocol: BackendProtocol, actual_pubsub=None, mq_client=None):
        self.protocol = protocol
        self.actual_pubsub = actual_pubsub
        self.mq_client = mq_client

    async def subscribe(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: return await self.actual_pubsub.subscribe(*args, **kwargs)

    async def psubscribe(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: return await self.actual_pubsub.psubscribe(*args, **kwargs)

    async def get_message(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: return await self.actual_pubsub.get_message(*args, **kwargs)

    async def listen(self):
        if self.protocol == BackendProtocol.REDIS:
            async for msg in self.actual_pubsub.listen():
                yield msg

    async def close(self):
        if self.protocol == BackendProtocol.REDIS and hasattr(self.actual_pubsub, 'close'):
            await self.actual_pubsub.close()


class UniversalFacade:
    """@role: 상태(State), 큐(Queue), 스트림(Stream), 신호(PubSub)를 통합 라우팅하는 비동기 파사드"""
    def __init__(self, state_url: str, mq_url: str, mq_protocol: BackendProtocol):
        self.state_store = actual_redis.from_url(state_url, decode_responses=True)
        self.mq_protocol = mq_protocol
        self.mq_url = mq_url
        self.mq_client = None

        if self.mq_protocol == BackendProtocol.KAFKA:
            log.info(f"[Tunnel] Initializing Kafka Producer/Consumer at {self.mq_url}")
            pass

    async def stream_produce(self, topic: str, payload: dict, maxlen: int = 100000) -> str:
        if self.mq_protocol == BackendProtocol.KAFKA: pass
        return await self.state_store.xadd(topic, payload, maxlen=maxlen)

    async def stream_consume(self, topic: str, group: str, consumer: str, count: int = 1, block: int = 0) -> List[Tuple]:
        if self.mq_protocol == BackendProtocol.KAFKA: pass
        try:
            await self.state_store.xgroup_create(topic, group, id='0', mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e): raise
        return await self.state_store.xreadgroup(group, consumer, {topic: '>'}, count=count, block=block)

    async def stream_ack(self, topic: str, group: str, message_id: str):
        if self.mq_protocol == BackendProtocol.KAFKA: pass
        return await self.state_store.xack(topic, group, message_id)

    async def publish(self, channel: str, message: Any):
        if self.mq_protocol == BackendProtocol.KAFKA: pass
        return await self.state_store.publish(channel, message)

    def pubsub(self) -> UniversalPubSub:
        if self.mq_protocol == BackendProtocol.KAFKA: return UniversalPubSub(self.mq_protocol, mq_client=self.mq_client)
        return UniversalPubSub(self.mq_protocol, actual_pubsub=self.state_store.pubsub())
    
    def __getattr__(self, name: str):
        """명시되지 않은 모든 비동기 메서드(llen, keys, lpush 등)를 실제 Redis Async 클라이언트로 자동 위임"""
        return getattr(self.state_store, name)

class UniversalFacadeSync:
    """
    @role: 동기(Blocking) 방식 라우팅을 위한 파사드
    @flow: 메서드 이름이 Redis 원본과 다르거나 예외 처리(BUSYGROUP)가 필요한 스트림(Stream) 메서드만 명시하고,
           나머지 단순 패스스루(lpush, get, pubsub 등)는 __getattr__로 자동 위임합니다.
    """
    def __init__(self, state_url: str, mq_url: str, mq_protocol: BackendProtocol):
        self.state_store = redis.from_url(state_url, decode_responses=True)
        self.mq_protocol = mq_protocol
        self.mq_url = mq_url
        self.mq_client = None

        if self.mq_protocol == BackendProtocol.KAFKA:
            log.info(f"[SyncTunnel] Initializing Sync Kafka Producer/Consumer at {self.mq_url}")
            pass

    def stream_produce(self, topic: str, payload: dict, maxlen: int = 100000) -> str:
        if self.mq_protocol == BackendProtocol.KAFKA: pass
        return self.state_store.xadd(topic, payload, maxlen=maxlen)

    def stream_consume(self, topic: str, group: str, consumer: str, count: int = 1, block: int = 0) -> List[Tuple]:
        if self.mq_protocol == BackendProtocol.KAFKA: pass
        try:
            self.state_store.xgroup_create(topic, group, id='0', mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e): raise
        return self.state_store.xreadgroup(group, consumer, {topic: '>'}, count=count, block=block)

    def stream_ack(self, topic: str, group: str, message_id: str):
        if self.mq_protocol == BackendProtocol.KAFKA: pass
        return self.state_store.xack(topic, group, message_id)

    def publish(self, channel: str, message: Any):
        if self.mq_protocol == BackendProtocol.KAFKA: pass
        return self.state_store.publish(channel, message)

    def __getattr__(self, name: str):
        return getattr(self.state_store, name)


class TunnelFactory:
    _async_instance: Optional[UniversalFacade] = None
    _sync_instance: Optional[UniversalFacadeSync] = None

    @classmethod
    async def get_default(cls) -> UniversalFacade:
        """비동기 환경을 위한 기본 터널 (기존 await 호출 유지)"""
        if cls._async_instance is None:
            config = resolve_default_config()
            scheme, state_url, mq_url = parse_connection_urls(config.default_url)
            cls._async_instance = UniversalFacade(state_url, mq_url, scheme)
            log.info(f"[TunnelFactory] Provisioned Async Tunnel: {config.default_url}")
        return cls._async_instance

    @classmethod
    async def get_isolated(cls) -> UniversalFacade:
        """격리된 비동기 커넥션"""
        config = resolve_default_config()
        scheme, state_url, mq_url = parse_connection_urls(config.default_url)
        return UniversalFacade(state_url, mq_url, scheme)

    @classmethod
    def get_sync(cls) -> UniversalFacadeSync:
        """동기 환경(SurfaceMQ 등)을 위한 터널 (await 없이 호출)"""
        if cls._sync_instance is None:
            config = resolve_default_config()
            scheme, state_url, mq_url = parse_connection_urls(config.default_url)
            cls._sync_instance = UniversalFacadeSync(state_url, mq_url, scheme)
            log.info(f"[TunnelFactory] Provisioned Sync Tunnel: {config.default_url}")
        return cls._sync_instance

    @classmethod
    def get_isolated_sync(cls) -> UniversalFacadeSync:
        """격리된 동기 커넥션"""
        config = resolve_default_config()
        scheme, state_url, mq_url = parse_connection_urls(config.default_url)
        return UniversalFacadeSync(state_url, mq_url, scheme)

    @classmethod
    async def close_all(cls):
        """전역 커넥션 풀 종료"""
        if cls._async_instance:
            await cls._async_instance.aclose()
            cls._async_instance = None
        if cls._sync_instance:
            cls._sync_instance.close()
            cls._sync_instance = None

async def from_url(url: str, **kwargs) -> UniversalFacade:
    """@legacy: 명시적인 URL 주입이 필요한 특수 목적용 (Async)"""
    scheme, state_url, mq_url = parse_connection_urls(url)
    return UniversalFacade(state_url=state_url, mq_url=mq_url, mq_protocol=scheme)

def sync_from_url(url: str, **kwargs) -> UniversalFacadeSync:
    """@legacy: 명시적인 URL 주입이 필요한 특수 목적용 (Sync)"""
    scheme, state_url, mq_url = parse_connection_urls(url)
    return UniversalFacadeSync(state_url=state_url, mq_url=mq_url, mq_protocol=scheme)