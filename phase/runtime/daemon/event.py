# phase.runtime.daemon.event
import asyncio
import json
from typing import Optional

from arch.topos.bound.tunnel import UniversalFacade
from arch.contract.event.psi import PsiEvent
from arch.contract.event.tunnelbus import TunnelEventBus
from phase.runtime.daemon.base import AbstractDaemon
from watcher.plane.emitter import get_emitter

class EventBusDaemon(AbstractDaemon):
    """
    @desc: Redis Streams 기반 분산 이벤트 컨슈머 (Sticky & Exclusive)
    @flow: Tunnel(Stream) → XREADGROUP → TunnelEventBus(Local Dispatch) → XACK
    """
    def __init__(self, tunnel: UniversalFacade, event_bus: TunnelEventBus, node_id: str, group_name: str = "node_manifold_group"):
        super().__init__("EventBus")
        self.tunnel = tunnel
        self.event_bus = event_bus
        self.node_id = node_id
        
        # Redis Stream 핵심 설정
        self.topic = getattr(self.event_bus, 'topic', "runtime:bus:stream")
        self.group_name = group_name
        self.consumer_name = f"consumer-{self.node_id}"
        
        self.poll_timeout_ms = 1000  # 1초
        self.log = get_emitter(f"daemon.eventbus.{self.node_id}", phase="SYSTEM")

    async def _init_consumer_group(self):
        """@desc: Consumer Group 초기화. Race condition 방어를 위해 BUSYGROUP 예외를 삼킵니다."""
        try:
            # Tunnel의 stream_consume 내부에서 처리해주지만, 명시적으로 미리 생성해둡니다.
            await self.tunnel.state_store.xgroup_create(
                name=self.topic, groupname=self.group_name, id='0', mkstream=True
            )
            self.log.info(f"Consumer Group '{self.group_name}' initialized.")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                self.log.error(f"Failed to initialize Consumer Group: {e}")

    async def run(self):
        self.log.info(f"EventBusDaemon 가동. Sticky Consumer 수신 대기 (Topic: {self.topic})")
        
        await self._init_consumer_group()

        while self.running:
            try:
                # 1. 터널에서 새로운 이벤트 1개를 할당받음 (XREADGROUP)
                streams = await self.tunnel.stream_consume(
                    topic=self.topic,
                    group=self.group_name,
                    consumer=self.consumer_name,
                    count=1,
                    block=self.poll_timeout_ms
                )

                if not streams:
                    continue  # Timeout 발생 (새 이벤트 없음)

                for stream_name, messages in streams:
                    for message_id, msg_data in messages:
                        # Redis Stream은 field-value 구조이므로 "data" 필드를 추출
                        json_payload = msg_data.get("data", msg_data.get(b"data", b"{}"))
                        if isinstance(json_payload, bytes):
                            json_payload = json_payload.decode('utf-8')
                            
                        event = None
                        try:
                            # 2. PsiEvent 객체로 복원
                            event = PsiEvent.from_json(json_payload)
                        except json.JSONDecodeError:
                            self.log.error(f"Invalid JSON payload in stream: {json_payload}")
                        
                        if event:
                            # 3. 로컬 Ator들에게 팬아웃(Dispatch)
                            # (내부에서 Task로 던지므로 daemon의 수신 루프가 막히지 않음)
                            await self.event_bus.dispatch_local(event)

                        # 4. 완료 통보 (XACK) - 이 노드가 독점 처리했음을 확정
                        await self.tunnel.stream_ack(self.topic, self.group_name, message_id)
                        self.log.debug(f"ACKed message {message_id} on {self.topic}")

            except asyncio.CancelledError:
                self.log.info("EventBusDaemon received cancellation signal.")
                break
            except Exception as e:
                # Redis 연결 끊김 등의 일시적 물리 에러 방어
                self.log.error(f"Stream Consume Error: {e}")
                await asyncio.sleep(2)
                
        self.log.info("EventBusDaemon successfully evaporated.")