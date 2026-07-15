# phase.reflect.dynamics.carrier
## @lineage: phase.dynamics.carrier
from __future__ import annotations
import asyncio
import json
import redis.asyncio as redis_async
from typing import List, Dict, Optional, Any

from arch.topos.bound.tunnel import TunnelFactory
from arch.contract.event.next import next_id
from arch.contract.event.psi import PsiEvent, PsiCarrier
from arch.contract.executor import BaseExecutor
from watcher.xe.cont import XeCont

class LoopCarrier(BaseExecutor):
    """
    @role: self-driven Ψ loop generator (시뮬레이션 및 백테스팅 용도)
    @flow: 각 Tick마다 Snowflake ID를 새로 생성하여 정밀한 순서 보장 (능동적 루프)
    """
    def __init__(self, xe: XeCont, max_ticks: int = 100, interval: float = 0.1):
        super().__init__()
        self.xe = xe
        self.tick = 0
        self.max_ticks = max_ticks
        self.interval = interval

    async def execute(self, psi: Any) -> List[Any]:
        out = []
        xe_out = await self.xe.execute(psi)
        out.extend(xe_out) 

        if self.tick < self.max_ticks:
            await asyncio.sleep(self.interval)
            next_psi = psi.__class__(
                event_id=next_id(), 
                parent_id=getattr(psi, "event_id", None),
                source_id="loop.carrier",
                scope=getattr(psi, "scope", "GLOBAL"),
                carrier=getattr(psi, "carrier", None),
                phase_id=getattr(self.xe, 'phase_id', 0),
                tick=self.tick + 1,
                context=getattr(psi, "context", {}).copy()
            )
            
            if hasattr(self, "node") and self.node:
                await self.node.bus.publish(next_psi)
            else:
                out.append(next_psi) 
            self.tick += 1
        return out

async def rhythm_loop_tick(event_bus, channel: str = "rhythm.heart"):
    """
    @desc: Universal Tunnel에서 심장 박동(Heartbeat) 이벤트를 수신하여 EventBus로 중계
    """
    tunnel = await TunnelFactory.get_default()
    pubsub = tunnel.pubsub()
    await pubsub.subscribe(channel)
    print(f"[System] Syncing with Heartbeat Field via Tunnel on channel: {channel}")

    try:
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue

            raw_data = json.loads(message["data"])
            carrier_data = raw_data.get("carrier", {})
            
            event = PsiEvent(
                event_id=raw_data.get("event_id"),
                parent_id=raw_data.get("parent_id"),
                source_id=raw_data.get("source_id", "tunnel.bridge"),
                scope=raw_data.get("scope", "GLOBAL"),
                tick=raw_data.get("tick", 0),
                carrier=PsiCarrier(
                    kind=carrier_data.get("kind"),
                    tag=carrier_data.get("tag"),
                    payload=carrier_data.get("payload")
                ),
                context=raw_data.get("context", {})
            )
            await event_bus.publish(event)
    except asyncio.CancelledError:
        await pubsub.close()
        print("[System] Heartbeat sync stopped.")
    except Exception as e:
        print(f"[System Error] Heartbeat loop failure: {e}")