# arch.bound.sandbox.sync
"""
@desc: Universal Sync Adapter (Mirror of arch.bound.sandbox.tunnel)
@flow: 
  비동기 이벤트 루프(asyncio)를 사용할 수 없는 동기(Sync) 블로킹 환경
  (예: 서브프로세스 제어, 제너레이터 yield, 레거시 WSGI)을 위한 통신 파사드.
"""
import redis  # [핵심] redis.asyncio가 아닌 순수 동기 클라이언트
import redis.exceptions
from typing import Optional, Any, List, Tuple

# [Architecture Align] 공통 어댑터 규격 수입 (환경변수 파싱 및 라우팅 위임)
from arch.bound.sandbox.adapter import BackendProtocol, resolve_default_config, parse_connection_urls
from watcher.plane.emitter import get_emitter

log = get_emitter("sandbox.sync")

# ==========================================
# 1. Universal PubSub (Sync)
# ==========================================
class UniversalPubSubSync:
    """동기식 블로킹 PubSub 래퍼"""
    def __init__(self, protocol: BackendProtocol, actual_pubsub=None, mq_client=None):
        self.protocol = protocol
        self.actual_pubsub = actual_pubsub
        self.mq_client = mq_client

    def subscribe(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: return self.actual_pubsub.subscribe(*args, **kwargs)
        
    def psubscribe(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: return self.actual_pubsub.psubscribe(*args, **kwargs)

    def get_message(self, *args, **kwargs):
        if self.protocol == BackendProtocol.REDIS: return self.actual_pubsub.get_message(*args, **kwargs)

    def listen(self):
        """@flow: 동기식 제너레이터 (yield) - SurfaceMQ 등에서 완벽하게 동작함"""
        if self.protocol == BackendProtocol.REDIS:
            for msg in self.actual_pubsub.listen():
                yield msg

    def close(self):
        if self.protocol == BackendProtocol.REDIS and hasattr(self.actual_pubsub, 'close'):
            self.actual_pubsub.close()

# ==========================================
# 2. Universal Facade (Sync)
# ==========================================
class UniversalFacadeSync:
    """
    @role: 상태, 큐, 스트림을 동기(Blocking) 방식으로 라우팅하는 파사드
    """
    def __init__(self, state_url: str, mq_url: str, mq_protocol: BackendProtocol):
        # [핵심] decode_responses=True로 설정된 동기식 Redis 커넥션 풀
        self.state_store = redis.from_url(state_url, decode_responses=True)
        self.mq_protocol = mq_protocol
        self.mq_url = mq_url
        self.mq_client = None
        
        if self.mq_protocol == BackendProtocol.KAFKA:
            log.info(f"[SyncTunnel] Initializing Sync Kafka Producer/Consumer at {self.mq_url}")
            pass

    # ---------------------------------------------------------
    # [Stream Area] (Kafka-like Append-only Log)
    # ---------------------------------------------------------
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

    # ---------------------------------------------------------
    # [MQ / PubSub Area]
    # ---------------------------------------------------------
    def publish(self, channel: str, message: Any):
        if self.mq_protocol == BackendProtocol.KAFKA: pass
        return self.state_store.publish(channel, message)

    def pubsub(self) -> UniversalPubSubSync:
        if self.mq_protocol == BackendProtocol.KAFKA: return UniversalPubSubSync(self.mq_protocol, mq_client=self.mq_client)
        return UniversalPubSubSync(self.mq_protocol, actual_pubsub=self.state_store.pubsub())

    # ---------------------------------------------------------
    # [State Store / Legacy Area]
    # ---------------------------------------------------------
    def lpush(self, name, *values): return self.state_store.lpush(name, *values)
    def brpop(self, keys, timeout=0): return self.state_store.brpop(keys, timeout=timeout)
    def sadd(self, name, *values): return self.state_store.sadd(name, *values)
    def srem(self, name, *values): return self.state_store.srem(name, *values)
    def smembers(self, name): return self.state_store.smembers(name)
    def get(self, name): return self.state_store.get(name)
    def set(self, name, value, **kwargs): return self.state_store.set(name, value, **kwargs)
    def delete(self, *names): return self.state_store.delete(*names)
    def hset(self, name, key=None, value=None, mapping=None): return self.state_store.hset(name, key, value, mapping)
    def hgetall(self, name): return self.state_store.hgetall(name)
    def keys(self, pattern='*'): return self.state_store.keys(pattern)
    
    def close(self):
        """동기식 커넥션 풀 닫기"""
        self.state_store.close()

# ==========================================
# 3. Factory & Instance Manager
# ==========================================
class SyncTunnelFactory:
    """
    @role: 동기 세계(Sync World)를 위한 팩토리
    """
    _shared_instance: Optional[UniversalFacadeSync] = None

    @classmethod
    def get_default(cls) -> UniversalFacadeSync:
        """@flow: 어댑터의 resolve_default_config를 호출하여 기본 동기 터널을 프로비저닝합니다."""
        if cls._shared_instance is None:
            config = resolve_default_config()
            log.info(f"[SyncTunnelFactory] Provisioning default shared sync tunnel: {config.default_url}")
            cls._shared_instance = from_url(config.default_url)
        return cls._shared_instance

    @classmethod
    def get_isolated(cls) -> UniversalFacadeSync:
        """@flow: 공유되지 않는 격리된 동기 커넥션을 생성합니다."""
        config = resolve_default_config()
        return from_url(config.default_url)

    @classmethod
    def close_all(cls):
        if cls._shared_instance:
            cls._shared_instance.close()
            cls._shared_instance = None

def from_url(url: str, **kwargs) -> UniversalFacadeSync:
    """@legacy: 명시적인 URL 주입이 필요한 특수 목적용"""
    scheme, state_url, mq_url = parse_connection_urls(url)
    return UniversalFacadeSync(state_url=state_url, mq_url=mq_url, mq_protocol=scheme)