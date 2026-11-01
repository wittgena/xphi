# xphi.arch.contract.event.bus
from __future__ import annotations

import asyncio
import json
from typing import Dict, Any, List, Optional, Callable, Tuple

from xphi.arch.contract.event.psi import PsiEvent
from xphi.arch.contract.interface import IPhaseAtor, IPhaseField, IEventBus
from xphi.node.space.tunnel.factory import UniversalFacade
from xphi.watcher.plane.emitter import get_emitter

"""Local Asynchronous Event Bus (In-Memory)"""
class AsyncEventBus(IEventBus):
    def __init__(self):
        self.actors: List[IPhaseAtor] = []
        self.field: Optional[IPhaseField] = None
        self.log = get_emitter("system.bus.async", phase="NETWORK")

    def bind_field(self, field: IPhaseField):
        self.field = field

    def subscribe(self, actor: IPhaseAtor):
        self.actors.append(actor)
        actor_id = getattr(actor, 'actor_id', 'unknown')
        self.log.debug(f"[AsyncBus] Ator '{actor_id}' subscribed.")

    async def publish(self, event: PsiEvent):
        """
        @desc: Bounded fan-out to all local subscribers concurrently.
        """
        tasks = [
            self._safe_react(actor, event)
            for actor in self.actors
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_react(self, ator: IPhaseAtor, event: PsiEvent):
        try:
            await ator.react(event, self.field, self)
        except Exception as e:
            actor_id = getattr(ator, 'actor_id', 'unknown')
            event_type = getattr(event, 'event_type', type(event).__name__)
            self.log.error(f"[FAIL] {actor_id} crashed on {event_type}: {e}")

"""Distributed Tunnel Event Bus (Redis / Message Queue)"""
class TunnelEventBus(IEventBus):
    def __init__(self, tunnel: UniversalFacade, topic: str = "runtime:bus:stream"):
        self.tunnel = tunnel
        self.topic = topic
        self.field: Optional[IPhaseField] = None
        
        self.subscribers: List[Tuple[IPhaseAtor, Callable[[PsiEvent], bool]]] = []
        self.log = get_emitter("system.bus.tunnel", phase="NETWORK")

    def bind_field(self, field: IPhaseField) -> None:
        self.field = field

    def subscribe(self, ator: IPhaseAtor, predicate: Callable[[PsiEvent], bool] = lambda e: True) -> None:
        self.subscribers.append((ator, predicate))
        ator_id = getattr(ator, 'actor_id', 'unknown')
        self.log.info(f"[TunnelBus] Ator '{ator_id}' subscribed to local routing.")

    async def publish(self, event: PsiEvent) -> None:
        try:
            payload = {"data": event.to_json()}
            await self.tunnel.stream_produce(topic=self.topic, payload=payload)
            symbol = getattr(event, 'symbol', type(event).__name__)
            self.log.debug(f"[TunnelBus] Published event {symbol} to stream {self.topic}")
        except Exception as e:
            symbol = getattr(event, 'symbol', type(event).__name__)
            self.log.error(f"[TunnelBus] Failed to publish event {symbol}: {e}")

    async def dispatch_local(self, event: PsiEvent) -> None:
        tasks = []
        for ator, predicate in self.subscribers:
            if predicate(event):
                tasks.append(self._safe_react(ator, event))
                
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_react(self, ator: IPhaseAtor, event: PsiEvent) -> None:
        try:
            await ator.react(event, self.field, self)
        except Exception as e:
            ator_id = getattr(ator, 'actor_id', 'unknown')
            symbol = getattr(event, 'symbol', type(event).__name__)
            self.log.error(f"[FAIL] Ator {ator_id} crashed on {symbol}: {e}")