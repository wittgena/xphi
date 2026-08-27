# xphi.arch.contract.event.mesh.hybrid
## @lineage: arch.contract.event.mesh.hybrid
## arch.contract.event.swarm.adapter
import asyncio
import json
from typing import Callable, List, Tuple, Optional

from xphi.arch.contract.event.psi import PsiEvent
from xphi.arch.contract.event.mesh.transport import MeshP2PTransport
from xphi.arch.contract.interface import IPhaseAtor, IPhaseField, IEventBus
from xphi.kernel.space.topos.tunnel.factory import UniversalFacade

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("mesh.hybrid", phase="ROUTING")

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

    async def initialize(self, mesh_topics: List[str]):
        # 1. Mesh(libp2p) Transport에 HybridBus의 인그레스 메서드를 콜백으로 주입
        await self.mesh.bind_and_start(ingress_callback=self._mesh_ingress_handler)
        for topic in mesh_topics:
            await self.mesh.join_topic(topic)
            
        # 2. Tunnel(Redis) Ingress 루프 백그라운드 실행
        asyncio.create_task(self._tunnel_ingress_loop())
        log.info("[HybridBus] Dual Ingress bindings active (Tunnel + Mesh).")

    def subscribe(self, ator: IPhaseAtor, predicate: Callable[[PsiEvent], bool] = lambda e: True) -> None:
        self.subscribers.append((ator, predicate))

    async def publish(self, event: PsiEvent) -> None:
        try:
            scope = getattr(event, 'scope', 'LOCAL')
            topic = event.context.get("topic", "default_topic")
            
            if not event.context: event.context = {}
            event.context["topos_phase"] = self.local_phase_pointer

            payload_bytes = json.dumps(event.to_json()).encode('utf-8')

            if scope in ('GLOBAL', 'MACRO'):
                await self.tunnel.stream_produce(topic=f"tunnel:{topic}", payload={"data": payload_bytes.decode('utf-8')})
            elif scope in ('MESH', 'GOSSIP'):
                await self.mesh.broadcast(topic, payload_bytes)
            else:
                await self.dispatch_local(event)
        except Exception as e:
            log.error(f"[HybridBus] Publish error: {e}")

    async def _mesh_ingress_handler(self, sender_id: str, raw_bytes: bytes):
        try:
            data = json.loads(raw_bytes.decode('utf-8'))
            event = PsiEvent.from_json(data)
            
            incoming_phase = event.context.get("topos_phase", 0)
            if (self.local_phase_pointer ^ incoming_phase) != 0:
                log.warning(f"[HybridBus] Mesh Drift from {sender_id}. Routing to Syzygy Buffer.")
                event.context["requires_syzygy_resolution"] = True
                
            await self.dispatch_local(event)
        except Exception as e:
            log.error(f"[HybridBus] Mesh Ingress Parse Error: {e}")

    async def _tunnel_ingress_loop(self):
        while True:
            # Redis Stream Consume Logic
            await asyncio.sleep(1)

    async def dispatch_local(self, event: PsiEvent) -> None:
        tasks = [self._safe_react(ator, event) for ator, pred in self.subscribers if pred(event)]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_react(self, ator: IPhaseAtor, event: PsiEvent) -> None:
        try: await ator.react(event, self.field, self)
        except Exception as e: log.error(f"[HybridBus] Ator failed: {e}")