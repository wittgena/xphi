# rhythm.coupler
import time
import json
from typing import Dict, Any, Optional
import redis.asyncio as redis_async
from bridge.event.psi import PsiType
from bound.plane import BoundPlane
from contract.proto.interface import IEventBus

class RhythmCoupler:
    def __init__(self, loop, redis, bus: Optional[IEventBus] = None):
        self.loop = loop
        self.redis = redis
        self.bus = bus

    async def start(self):
        pubsub = self.redis.pubsub()

        await pubsub.subscribe("rhythm.heart")
        await pubsub.subscribe("phase:decision")

        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue

            data = json.loads(msg["data"])

            if "kind" in data:
                if data["kind"].startswith("heart:"):
                    await self.loop.emit(data["kind"], {"strength": 0.5})

            if "tension" in data:
                tension = data["tension"]

                if tension > 1.2:
                    await self.redis.publish(
                        "runtime:signal",
                        json.dumps({"type": "heart:shock"})
                    )

                elif tension < 0.15:
                    await self.redis.publish(
                        "runtime:signal",
                        json.dumps({"type": "heart:pace"})
                    )