# arch.topos.tunnel.subs
import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TypeVar, Optional, Any, List

from pydantic import ConfigDict, BaseModel
from pydantic.dataclasses import dataclass as pydantic_dataclass

from arch.contract.event.next import next_id
from arch.topos.tunnel.factory import UniversalFacade 
from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

T = TypeVar("T")
ToposId = str

class Subscriber[T](ABC):
    @abstractmethod
    async def __call__(self, event: T):
        """Invoke this subscriber"""

    async def close(self):
        """Clean up this subscriber"""

def _safe_serialize(obj: Any) -> str:
    """객체를 안전하게 JSON 문자열로 직렬화합니다."""
    if isinstance(obj, BaseModel):
        return obj.model_dump_json()
    elif isinstance(obj, dict):
        return json.dumps(obj, default=str)
    elif hasattr(obj, "__dict__"):
        return json.dumps(obj.__dict__, default=str)
    return json.dumps(obj)

@pydantic_dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class DistributedPubSub[T]:
    channel: str
    tunnel: UniversalFacade
    _subscribers: dict[ToposId, Subscriber[T]] = field(default_factory=dict)
    _listener_task: Optional[asyncio.Task] = None

    async def start_listening(self):
        """Tunnel의 스트림을 청취하여 로컬 Subscriber들에게 분배하는 백그라운드 태스크"""
        if self._listener_task:
            return

        async def _listen_loop():
            pubsub = self.tunnel.pubsub()
            await pubsub.subscribe(self.channel)
            logger.info(f"DistributedPubSub started listening on tunnel channel: {self.channel}")
            
            try:
                async for msg in pubsub.listen():
                    if isinstance(msg, dict) and msg.get("type") == "message":
                        event_data = json.loads(msg["data"]) 
                        for sub_id, subscriber in list(self._subscribers.items()):
                            try:
                                await subscriber(event_data)
                            except Exception as e:
                                logger.error(f"Error in subscriber {sub_id}: {e}", exc_info=True)
            except asyncio.CancelledError:
                pass
            finally:
                if hasattr(pubsub, 'unsubscribe'):
                    await pubsub.unsubscribe(self.channel)
                await pubsub.close()

        self._listener_task = asyncio.create_task(_listen_loop())

    def subscribe(self, subscriber: Subscriber[T]) -> ToposId:
        subscriber_id = next_id()
        self._subscribers[subscriber_id] = subscriber
        logger.debug(f"Subscribed local subscriber with ID: {subscriber_id}")
        return subscriber_id

    def unsubscribe(self, subscriber_id: ToposId) -> bool:
        if subscriber_id in self._subscribers:
            del self._subscribers[subscriber_id]
            return True
        return False

    async def __call__(self, event: T) -> None:
        """단건 이벤트 발행"""
        event_payload = _safe_serialize(event)
        await self.tunnel.publish(self.channel, event_payload)

    # [핵심 수정] 누락되어 있던 publish_batch 메서드 구현
    async def publish_batch(self, topic: str, events: List[Any]) -> None:
        """다건(Batch) 이벤트를 특정 토픽 메타데이터와 함께 발행"""
        batch_payload = _safe_serialize({
            "topic": topic,
            "events": events
        })
        await self.tunnel.publish(self.channel, batch_payload)
        logger.debug(f"Published batch of {len(events)} events to {topic}")

    async def close(self):
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        
        await asyncio.gather(
            *[subscriber.close() for subscriber in self._subscribers.values()],
            return_exceptions=True
        )
        self._subscribers.clear()