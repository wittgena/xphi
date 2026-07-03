# arch.bound.sandbox.tunnel
"""
@desc: Universal Message/State Tunnel (Async I/O Implementation)
@flow: 
  [Adapter Parse] ↦ Async State & MQ Routing ↦ UniversalFacade(Async)
"""
import redis.asyncio as actual_redis
import redis.exceptions
from typing import Optional, Any, List, Tuple

# [Architecture Align] 공통 어댑터 규격 수입
from arch.bound.sandbox.adapter import BackendProtocol, resolve_default_config, parse_connection_urls
from watcher.plane.emitter import get_emitter

log = get_emitter("sandbox.tunnel")

class UniversalPubSub:
    """위상 관측(Scanner)이나 교란(Perturbator) 등 유실되어도 무방한 찰나의 파동을 처리합니다."""
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

    # [Pass-through Area]
    async def lpush(self, name, *values): return await self.state_store.lpush(name, *values)
    async def brpop(self, keys, timeout=0): return await self.state_store.brpop(keys, timeout=timeout)
    async def sadd(self, name, *values): return await self.state_store.sadd(name, *values)
    async def srem(self, name, *values): return await self.state_store.srem(name, *values)
    async def smembers(self, name): return await self.state_store.smembers(name)
    async def set(self, name, value, **kwargs): return await self.state_store.set(name, value, **kwargs)
    async def get(self, name): return await self.state_store.get(name)
    async def delete(self, *names): return await self.state_store.delete(*names)
    async def hset(self, name, key=None, value=None, mapping=None): return await self.state_store.hset(name, key, value, mapping)
    async def hgetall(self, name): return await self.state_store.hgetall(name)
    async def keys(self, pattern='*'): return await self.state_store.keys(pattern)
    async def aclose(self): await self.state_store.aclose()


class TunnelFactory:
    """@role: 시스템 전역의 기본 비동기 터널 연결을 관리하는 팩토리"""
    _shared_instance: Optional[UniversalFacade] = None

    @classmethod
    async def get_default(cls) -> UniversalFacade:
        if cls._shared_instance is None:
            config = resolve_default_config()
            log.info(f"[TunnelFactory] Provisioning default shared tunnel: {config.default_url}")
            cls._shared_instance = await from_url(config.default_url)
        return cls._shared_instance

    @classmethod
    async def get_isolated(cls) -> UniversalFacade:
        config = resolve_default_config()
        return await from_url(config.default_url)

    @classmethod
    async def close_all(cls):
        if cls._shared_instance:
            await cls._shared_instance.aclose()
            cls._shared_instance = None

async def from_url(url: str, **kwargs) -> UniversalFacade:
    """@legacy: 명시적인 URL 주입이 필요한 특수 목적용"""
    scheme, state_url, mq_url = parse_connection_urls(url)
    return UniversalFacade(state_url=state_url, mq_url=mq_url, mq_protocol=scheme)