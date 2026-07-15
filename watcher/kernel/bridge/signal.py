# watcher.kernel.bridge.signal
import asyncio
from typing import Optional
from arch.contract.event.next import LogEvent

from arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType 
from arch.contract.event.bus import AsyncEventBus
from watcher.plane.surface import EventObserver
from watcher.plane.emitter import get_emitter

class SignalBridgeWatcher(EventObserver):
    """
    @desc: 
    관제망(SurfacePlane)과 제어망(AsyncEventBus)을 연결하는 브릿지.
    SIGNAL 레벨의 관제 로그를 시스템의 상태 전이 제어 신호(PsiEvent)로 변환합니다.
    """
    def __init__(self, event_bus: AsyncEventBus):
        self.bus = event_bus
        self.log = get_emitter("watcher.bridge", phase="KERNEL")

    def update(self, event: LogEvent) -> None:
        if event.level != "SIGNAL":
            return
            
        event_message = str(event.message).strip()
        payload = event.context.copy() if event.context else {}
        
        # 메타데이터 주입
        payload["bridged_from"] = "LogEvent"
        payload["original_source"] = event.source_id

        # [수정] 1. PsiCarrier 먼저 생성
        # kind는 큰 범주(SIGNAL), tag는 세부 이벤트명("SYSTEM_MEMBRANE_ESTABLISHED")으로 사용
        carrier = PsiCarrier(
            kind="SIGNAL",          # PsiEvent.event_type 으로 매핑됨
            tag=event_message,      # PsiEvent.tag 로 매핑됨
            payload=payload,
            carrier_type=CarrierType.FIXED
        )

        # [수정] 2. PsiEvent의 필수 파라미터 모두 전달하여 생성
        psi_event = PsiEvent(
            event_id=event.event_id,
            parent_id=event.parent_id,
            source_id="watcher.bridge",         # 브릿지가 발행했음을 명시
            scope=getattr(event, 'scope', 'LOG'),
            tick=getattr(event, 'tick', 0) or 0,
            carrier=carrier,                    # 생성한 Carrier 객체 주입
            context=payload
        )

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._async_dispatch(psi_event))
        except RuntimeError:
            self.log.error(f"[Bridge] Event loop is not running. Cannot dispatch {event_message}")

    async def _async_dispatch(self, psi_event: PsiEvent):
        try:
            # psi_event.symbol을 호출하면 "SIGNAL:SYSTEM_MEMBRANE_ESTABLISHED" 형태로 출력됨
            self.log.trace(f"[Bridge] Elevating SIGNAL -> PsiEvent: {psi_event.symbol}")
            await self.bus.publish(psi_event)
        except Exception as e:
            self.log.error(f"[Bridge] Failed to dispatch PsiEvent: {e}", exc_info=True)