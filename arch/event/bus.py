# xphi.arch.event.bus
from __future__ import annotations

import asyncio
import json
from typing import Dict, Any, List, Optional, Callable, Tuple

from xphi.arch.event.psi import PsiEvent
from xphi.arch.contract.interface import IPhaseAtor, IPhaseField, IEventBus
from xphi.arch.event.mesh.transport import MeshP2PTransport
from xphi.kernel.space.topos.tunnel.factory import UniversalFacade

from xphi.watcher.plane.emitter import get_emitter

"""Local Asynchronous Event Bus (In-Memory)"""
class AsyncEventBus(IEventBus):
    """
    @desc: Actor isolation + bounded fan-out.
    In-memory local event routing. Suitable for single-process architectures.
    """
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
    """
    @desc: Distributed event routing via UniversalFacade (Redis Streams).
    Suitable for multi-agent, distributed clusters with decoupled daemons.
    """
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

"""Hybrid Event Bus (Tunnel + P2P Mesh Integration)"""
class HybridMeshBus(IEventBus):
    """
    @xe.desc: Unified Event Bus binding both centralized (Tunnel) and decentralized (Mesh) backends.
    """
    def __init__(self, tunnel: UniversalFacade, mesh: MeshP2PTransport):
        self.tunnel = tunnel
        self.mesh = mesh
        self.field: Optional[IPhaseField] = None
        self.subscribers: List[Tuple[IPhaseAtor, Callable[[PsiEvent], bool]]] = []
        self.local_phase_pointer: int = 0
        self.log = get_emitter("system.bus.hybrid", phase="ROUTING")

    def bind_field(self, field: IPhaseField) -> None:
        self.field = field

    async def initialize(self, mesh_topics: List[str]):
        """
        @desc: Binds dual ingress listeners for both libp2p mesh and Redis tunnel.
        """
        # 1. Mesh(libp2p) Transport 인그레스 콜백 주입 및 토픽 바인딩
        await self.mesh.bind_and_start(ingress_callback=self._mesh_ingress_handler)
        for topic in mesh_topics:
            await self.mesh.join_topic(topic)
            
        # 2. Tunnel(Redis) Ingress 루프 백그라운드 실행
        asyncio.create_task(self._tunnel_ingress_loop())
        self.log.info("[HybridBus] Dual Ingress bindings active (Tunnel + Mesh).")

    def subscribe(self, ator: IPhaseAtor, predicate: Callable[[PsiEvent], bool] = lambda e: True) -> None:
        self.subscribers.append((ator, predicate))
        ator_id = getattr(ator, 'actor_id', 'unknown')
        self.log.info(f"[HybridBus] Ator '{ator_id}' subscribed.")

    async def publish(self, event: PsiEvent) -> None:
        try:
            # 이벤트에 명시된 scope를 확인하여 적절한 전파 경로를 선택 (기본: LOCAL)
            scope = getattr(event, 'scope', 'LOCAL')
            topic = event.context.get("topic", "default_topic") if hasattr(event, "context") else "default_topic"
            
            if not hasattr(event, "context") or event.context is None:
                event.context = {}
                
            event.context["topos_phase"] = self.local_phase_pointer
            payload_bytes = json.dumps(event.to_json()).encode('utf-8')

            # Scope-based Dynamic Routing
            if scope in ('GLOBAL', 'MACRO'):
                await self.tunnel.stream_produce(topic=f"tunnel:{topic}", payload={"data": payload_bytes.decode('utf-8')})
                self.log.debug(f"[HybridBus] Event {event.symbol} routed to GLOBAL Tunnel.")
            elif scope in ('MESH', 'GOSSIP'):
                await self.mesh.broadcast(topic, payload_bytes)
                self.log.debug(f"[HybridBus] Event {event.symbol} broadcasted to MESH P2P.")
            else:
                await self.dispatch_local(event)
                
        except Exception as e:
            self.log.error(f"[HybridBus] Publish error on event {getattr(event, 'symbol', 'Unknown')}: {e}")

    async def _mesh_ingress_handler(self, sender_id: str, raw_bytes: bytes):
        try:
            data = json.loads(raw_bytes.decode('utf-8'))
            
            # 💡 [보완] from_json이 딕셔너리가 아닌 문자열을 기대할 수 있으므로 분기 처리
            if isinstance(data, str):
                event = PsiEvent.from_json(data)
            else:
                # from_json이 객체 생성을 지원하도록 조정이 필요할 수 있음
                # 여기서는 json.dumps로 다시 직렬화 후 from_json 호출
                event = PsiEvent.from_json(json.dumps(data))
            
            incoming_phase = event.context.get("topos_phase", 0) if hasattr(event, "context") else 0
            
            # XOR Validation for State Drift Detection
            if (self.local_phase_pointer ^ incoming_phase) != 0:
                self.log.warning(f"[HybridBus] Mesh Drift from {sender_id}. Routing to Syzygy Buffer.")
                if not hasattr(event, "context"): event.context = {}
                event.context["requires_syzygy_resolution"] = True
                
            await self.dispatch_local(event)
        except Exception as e:
            self.log.error(f"[HybridBus] Mesh Ingress Parse Error from {sender_id}: {e}")

    async def _tunnel_ingress_loop(self):
        """
        @desc: Background task to pull from Redis streams.
        (Placeholder: Real consume logic depends on the specific daemon orchestrator)
        """
        while True:
            await asyncio.sleep(1)

    async def dispatch_local(self, event: PsiEvent) -> None:
        tasks = [self._safe_react(ator, event) for ator, pred in self.subscribers if pred(event)]
        if tasks: 
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_react(self, ator: IPhaseAtor, event: PsiEvent) -> None:
        try: 
            await ator.react(event, self.field, self)
        except Exception as e: 
            ator_id = getattr(ator, 'actor_id', 'unknown')
            self.log.error(f"[FAIL] Ator {ator_id} crashed on {getattr(event, 'symbol', 'Unknown')}: {e}")