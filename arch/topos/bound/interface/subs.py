# arch.topos.bound.interface.subs
import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TypeVar, Optional

from pydantic import ConfigDict
from pydantic.dataclasses import dataclass as pydantic_dataclass

from arch.contract.event.next import next_id
from arch.topos.bound.tunnel import UniversalFacade 
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


@dataclass
class PubSub[T]:
    _subscribers: dict[ToposId, Subscriber[T]] = field(default_factory=dict)

    def subscribe(self, subscriber: Subscriber[T]) -> ToposId:
        subscriber_id = next_id()
        self._subscribers[subscriber_id] = subscriber
        logger.debug(f"Subscribed subscriber with ID: {subscriber_id}")
        return subscriber_id

    def unsubscribe(self, subscriber_id: ToposId) -> bool:
        if subscriber_id in self._subscribers:
            del self._subscribers[subscriber_id]
            logger.debug(f"Unsubscribed subscriber with ID: {subscriber_id}")
            return True
        else:
            logger.warning(
                f"Attempted to unsubscribe unknown subscriber ID: {subscriber_id}"
            )
            return False

    async def __call__(self, event: T) -> None:
        for subscriber_id, subscriber in list(self._subscribers.items()):
            try:
                await subscriber(event)
            except Exception as e:
                logger.error(f"Error in subscriber {subscriber_id}: {e}", exc_info=True)

    async def close(self):
        await asyncio.gather(
            *[subscriber.close() for subscriber in self._subscribers.values()]
        )
        self._subscribers.clear()

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
        event_payload = json.dumps(event if isinstance(event, dict) else event.__dict__)
        await self.tunnel.publish(self.channel, event_payload)

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