# arch.contract.event.tunnelbus
"""
@desc: Distributed Event Bus leveraging UniversalFacade (Redis Streams).
       Bridges the local Ator manifold with the global system Tunnel.
"""
import asyncio
from typing import Dict, Any, List, Optional, Callable, Tuple

from arch.contract.event.psi import PsiEvent
from arch.contract.interface import IPhaseAtor, IPhaseField, IEventBus
from arch.topos.tunnel.factory import UniversalFacade
from watcher.plane.emitter import get_emitter

class TunnelEventBus(IEventBus):
    """
    @role: Distributed ψ-router.
    @flow: 
      - Outbound (publish): Local Ator -> TunnelEventBus -> Redis Stream
      - Inbound (dispatch): EventBusDaemon -> TunnelEventBus -> Local Ator (Fan-out)
    """
    def __init__(self, tunnel: UniversalFacade, topic: str = "runtime:bus:stream"):
        self.tunnel = tunnel
        self.topic = topic
        self.field: Optional[IPhaseField] = None
        
        # 로컬 구독자 레지스트리 (Ator, 조건식)
        self.subscribers: List[Tuple[IPhaseAtor, Callable[[PsiEvent], bool]]] = []
        self.log = get_emitter("system.tunnelbus", phase="NETWORK")

    def bind_field(self, field: IPhaseField) -> None:
        """@desc: Binds a shared phase field (Φ) to the bus context."""
        self.field = field

    def subscribe(self, ator: IPhaseAtor, predicate: Callable[[PsiEvent], bool] = lambda e: True) -> None:
        """
        @desc: Registers a local actor to receive events matching the predicate.
               기본적으로 조건식(predicate)이 없으면 모든 이벤트를 수신합니다.
        """
        self.subscribers.append((ator, predicate))
        ator_id = getattr(ator, 'ator_id', 'unknown')
        self.log.info(f"[TunnelBus] Ator '{ator_id}' subscribed to local routing.")

    async def publish(self, event: PsiEvent) -> None:
        """
        @desc: 이벤트를 분산 터널(Redis Stream)로 발행하여 전체 클러스터(Manifold)에 전파합니다.
               Ator는 자신이 던진 이벤트가 로컬 큐에 담기는지 네트워크를 타는지 알 필요가 없습니다.
        """
        try:
            # UniversalFacade의 stream_produce 활용 (O(1) 속도)
            payload = {"data": event.to_json()}
            await self.tunnel.stream_produce(topic=self.topic, payload=payload)
            self.log.debug(f"[TunnelBus] Published event {event.symbol} to stream {self.topic}")
        except Exception as e:
            self.log.error(f"[TunnelBus] Failed to publish event {event.symbol}: {e}")

    async def dispatch_local(self, event: PsiEvent) -> None:
        """
        @desc: EventBusDaemon이 터널에서 이벤트를 낚아채면 이 메서드를 호출합니다.
               가져온 이벤트를 Predicate 조건에 맞는 로컬 Ator들에게만 Bounded Fan-out 합니다.
        """
        tasks = []
        for ator, predicate in self.subscribers:
            # 필터링 통과 시에만 React 태스크 생성
            if predicate(event):
                tasks.append(self._safe_react(ator, event))
                
        # asyncio.gather를 통한 병렬 안전 실행 (폭주 방지)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_react(self, ator: IPhaseAtor, event: PsiEvent) -> None:
        """@desc: 개별 Ator의 오류가 전체 버스 및 다른 Ator에게 전파되지 않도록 격리(Isolate)합니다."""
        try:
            await ator.react(event, self.field, self)
        except Exception as e:
            ator_id = getattr(ator, 'ator_id', 'unknown')
            self.log.error(f"[FAIL] Ator {ator_id} crashed on {event.symbol}: {e}")