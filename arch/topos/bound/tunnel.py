# arch.topos.bound.tunnel
"""
@desc: Universal Message/State Tunnel (Async & Sync Implementation)
@flow: 
- Defaults to asynchronous processing, while providing a minimal 
- facade for synchronous environments where an async event loop is unavailable
- [Added] Built-in Connection Pool limits to prevent "Too many connections" infrastructure collapse.
"""
import redis
import redis.asyncio as actual_redis
import redis.exceptions
import logging
from typing import Optional, Any, List, Tuple
from arch.topos.bound.adapter.config import BackendProtocol, resolve_default_config, parse_connection_urls

log = logging.getLogger("bound.tunnel")

class UniversalPubSub:
    def __init__(self, protocol: BackendProtocol, actual_pubsub=None, mq_client=None):
        self.protocol = protocol
        self.actual_pubsub = actual_pubsub
        self.mq_client = mq_client

    async def subscribe(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: return await self.actual_pubsub.subscribe(*args, **kwargs)

    async def psubscribe(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: return await self.actual_pubsub.psubscribe(*args, **kwargs)

    async def unsubscribe(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: 
            return await self.actual_pubsub.unsubscribe(*args, **kwargs)

    async def punsubscribe(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: 
            return await self.actual_pubsub.punsubscribe(*args, **kwargs)

    async def get_message(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: return await self.actual_pubsub.get_message(*args, **kwargs)

    async def listen(self):
        if self.protocol == BackendProtocol.REDIS:
            async for msg in self.actual_pubsub.listen():
                yield msg

    async def close(self):
        if self.protocol == BackendProtocol.REDIS and hasattr(self.actual_pubsub, 'close'):
            await self.actual_pubsub.close()

    def __getattr__(self, name: str):
        if self.protocol == BackendProtocol.REDIS and self.actual_pubsub:
            return getattr(self.actual_pubsub, name)
        raise AttributeError(f"'UniversalPubSub' object has no attribute '{name}'")

class UniversalFacade:
    """
    @role: Asynchronous facade unifying routing for State, Queue, Stream, and PubSub signals
    @defense: Configures connection pooling defaults to prevent Redis overload.
    """
    def __init__(self, state_url: str, mq_url: str, mq_protocol: BackendProtocol, **kwargs):
        self.mq_protocol = mq_protocol
        self.mq_url = mq_url
        self.mq_client = None

        # [방어장치 1] 커넥션 풀 제한 설정 (기본값: 100, 대기 시간: 5초)
        pool_kwargs = {
            "max_connections": 100,
            "socket_timeout": 5.0,
            "socket_connect_timeout": 5.0,
            "retry_on_timeout": True
        }
        pool_kwargs.update(kwargs)

        self.state_store = actual_redis.from_url(state_url, decode_responses=True, **pool_kwargs)

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
        """Automatically delegates unmapped async methods (e.g., llen, keys, lpush) to the underlying Redis Async client."""
        return getattr(self.state_store, name)

class UniversalFacadeSync:
    """
    @role: Synchronous (Blocking) routing facade
    """
    def __init__(self, state_url: str, mq_url: str, mq_protocol: BackendProtocol, **kwargs):
        self.mq_protocol = mq_protocol
        self.mq_url = mq_url
        self.mq_client = None

        # [방어장치 1] 동기 클라이언트에도 풀 제한 적용 (동기 환경은 더 적은 커넥션 유지)
        pool_kwargs = {
            "max_connections": 50,
            "socket_timeout": 5.0,
            "socket_connect_timeout": 5.0,
            "retry_on_timeout": True
        }
        pool_kwargs.update(kwargs)

        self.state_store = redis.from_url(state_url, decode_responses=True, **pool_kwargs)

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
    async def get_default(cls, **kwargs) -> UniversalFacade:
        """Default tunnel for asynchronous environments (maintains standard await calls)"""
        if cls._async_instance is None:
            config = resolve_default_config()
            scheme, state_url, mq_url = parse_connection_urls(config.default_url)
            cls._async_instance = UniversalFacade(state_url, mq_url, scheme, **kwargs)
            log.info(f"[TunnelFactory] Provisioned Async Tunnel: {config.default_url}")
        return cls._async_instance

    @classmethod
    async def get_isolated(cls, **kwargs) -> UniversalFacade:
        """Isolated asynchronous connection. 
        Note: Use sparingly to prevent connection pool exhaustion."""
        config = resolve_default_config()
        scheme, state_url, mq_url = parse_connection_urls(config.default_url)
        return UniversalFacade(state_url, mq_url, scheme, **kwargs)

    @classmethod
    def get_sync(cls, **kwargs) -> UniversalFacadeSync:
        """Tunnel for synchronous environments like SurfaceMQ (called without await)"""
        if cls._sync_instance is None:
            config = resolve_default_config()
            scheme, state_url, mq_url = parse_connection_urls(config.default_url)
            cls._sync_instance = UniversalFacadeSync(state_url, mq_url, scheme, **kwargs)
            log.info(f"[TunnelFactory] Provisioned Sync Tunnel: {config.default_url}")
        return cls._sync_instance

    @classmethod
    def get_isolated_sync(cls, **kwargs) -> UniversalFacadeSync:
        """Isolated synchronous connection"""
        config = resolve_default_config()
        scheme, state_url, mq_url = parse_connection_urls(config.default_url)
        return UniversalFacadeSync(state_url, mq_url, scheme, **kwargs)

    @classmethod
    async def close_all(cls):
        """Terminates the global connection pool"""
        if cls._async_instance:
            if hasattr(cls._async_instance.state_store, 'aclose'):
                await cls._async_instance.state_store.aclose()
            elif hasattr(cls._async_instance.state_store, 'close'):
                await cls._async_instance.state_store.close()
            cls._async_instance = None
            
        if cls._sync_instance:
            cls._sync_instance.state_store.close()
            cls._sync_instance = None

async def from_url(url: str, **kwargs) -> UniversalFacade:
    """@legacy: Special-purpose Async builder requiring explicit URL injection"""
    scheme, state_url, mq_url = parse_connection_urls(url)
    return UniversalFacade(state_url=state_url, mq_url=mq_url, mq_protocol=scheme, **kwargs)

def sync_from_url(url: str, **kwargs) -> UniversalFacadeSync:
    """@legacy: Special-purpose Sync builder requiring explicit URL injection"""
    scheme, state_url, mq_url = parse_connection_urls(url)
    return UniversalFacadeSync(state_url=state_url, mq_url=mq_url, mq_protocol=scheme, **kwargs)