# xphi.arch.contract.event.mesh.swarm
import asyncio
import json
from typing import List, Optional, Callable, Tuple

from xphi.arch.contract.event.psi import PsiEvent
from xphi.arch.contract.interface import IPhaseAtor, IPhaseField, IEventBus
from xphi.arch.contract.promise import future, Promise
from xphi.arch.contract.event.mesh.transport import MeshP2PTransport
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("mesh.swarm", phase="ROUTING")

gossip_causality_promise = Promise(
    contract="The SwarmBus must preserve causality and topological alignment across isolated P2P partitions.",
    invariant="Inbound PsiEvents must be mathematically evaluated for topological drift before local Ator dispatch.",
    consequence="Dispatching divergent events causes irrecoverable semantic corruption (Split-Brain) in local Ators."
)

class SwarmBus(IEventBus):
    def __init__(self, transport: MeshP2PTransport, topic: str = "swarm:gossip:global"):
        self.transport = transport
        self.topic = topic
        self.field: Optional[IPhaseField] = None
        self.subscribers: List[Tuple[IPhaseAtor, Callable[[PsiEvent], bool]]] = []
        self.local_phase_pointer: int = 0  

    async def initialize(self):
        # 도메인 버스가 자신의 Ingress 메서드를 하위 Transport의 콜백으로 꽂아 넣음
        await self.transport.bind_and_start(ingress_callback=self.process_ingress)
        await self.transport.join_topic(self.topic)

    def bind_field(self, field: IPhaseField) -> None:
        self.field = field

    def subscribe(self, ator: IPhaseAtor, predicate: Callable[[PsiEvent], bool] = lambda e: True) -> None:
        self.subscribers.append((ator, predicate))

    @future(promise=gossip_causality_promise)
    async def publish(self, event: PsiEvent) -> None:
        try:
            if not event.context: event.context = {}
            event.context["topos_phase"] = self.local_phase_pointer

            payload_bytes = json.dumps(event.to_json()).encode('utf-8')
            await self.transport.broadcast(self.topic, payload_bytes)
        except Exception as e:
            log.error(f"[SwarmBus] Failed to broadcast event {event.symbol}: {e}")

    @future(promise=gossip_causality_promise)
    async def process_ingress(self, sender_id: str, raw_bytes: bytes) -> None:
        try:
            data = json.loads(raw_bytes.decode('utf-8'))
            event = PsiEvent.from_json(data) 
            
            incoming_phase = event.context.get("topos_phase", 0)
            if (self.local_phase_pointer ^ incoming_phase) != 0:
                log.warning(f"[SwarmBus] Topological drift detected from {sender_id}. Routing to Syzygy Buffer.")
                event.context["requires_syzygy_resolution"] = True

            tasks = [self._safe_react(ator, event) for ator, pred in self.subscribers if pred(event)]
            if tasks: await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            log.error(f"[SwarmBus] Ingress parsing failed: {e}")

    async def _safe_react(self, ator: IPhaseAtor, event: PsiEvent) -> None:
        try: await ator.react(event, self.field, self)
        except Exception as e: log.error(f"[FAIL] Ator crashed: {e}")