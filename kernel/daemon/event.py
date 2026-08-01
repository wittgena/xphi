# phase.runtime.daemon.event
import asyncio
import json
from typing import Optional

from arch.topos.tunnel.factory import UniversalFacade
from arch.contract.event.psi import PsiEvent
from arch.contract.event.tunnelbus import TunnelEventBus
from kernel.daemon.base import AbstractDaemon
from watcher.plane.emitter import get_emitter

class EventBusDaemon(AbstractDaemon):
    """
    @desc: Distributed event consumer based on Redis Streams (Sticky & Exclusive)
    @flow: Tunnel(Stream) → XREADGROUP → TunnelEventBus(Local Dispatch) → XACK
    """
    def __init__(self, tunnel: UniversalFacade, event_bus: TunnelEventBus, node_id: str, group_name: str = "node_manifold_group"):
        super().__init__("EventBus")
        self.tunnel = tunnel
        self.event_bus = event_bus
        self.node_id = node_id
        
        self.topic = getattr(self.event_bus, 'topic', "runtime:bus:stream")
        self.group_name = group_name
        self.consumer_name = f"consumer-{self.node_id}"
        self.poll_timeout_ms = 1000
        self.log = get_emitter(f"daemon.eventbus.{self.node_id}", phase="SYSTEM")

    async def _init_consumer_group(self):
        """@desc: Initialize Consumer Group. Swallows BUSYGROUP exceptions to prevent race conditions."""
        try:
            ## Explicitly create the group in advance, although Tunnel's stream_consume handles it internally.
            await self.tunnel.state_store.xgroup_create(name=self.topic, groupname=self.group_name, id='0', mkstream=True)
            self.log.info(f"Consumer Group '{self.group_name}' initialized.")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                self.log.error(f"Failed to initialize Consumer Group: {e}")

    async def run(self):
        self.log.info(f"EventBusDaemon started. Waiting for Sticky Consumer events (Topic: {self.topic})")
        await self._init_consumer_group()
        while self.running:
            try:
                ## Fetch 1 new event from the tunnel (XREADGROUP)
                streams = await self.tunnel.stream_consume(
                    topic=self.topic,
                    group=self.group_name,
                    consumer=self.consumer_name,
                    count=1,
                    block=self.poll_timeout_ms
                )

                if not streams:
                    continue

                for stream_name, messages in streams:
                    for message_id, msg_data in messages:
                        ## Redis Streams use a field-value structure, so extract the "data" field
                        json_payload = msg_data.get("data", msg_data.get(b"data", b"{}"))
                        if isinstance(json_payload, bytes):
                            json_payload = json_payload.decode('utf-8')
                            
                        event = None
                        try:
                            ## Restore as a PsiEvent object
                            event = PsiEvent.from_json(json_payload)
                        except json.JSONDecodeError:
                            self.log.error(f"Invalid JSON payload in stream: {json_payload}")
                        
                        if event:
                            ## Fan-out (Dispatch) to local Actors
                            await self.event_bus.dispatch_local(event)

                        ## Acknowledge (XACK) - Confirm exclusive processing by this node
                        await self.tunnel.stream_ack(self.topic, self.group_name, message_id)
                        self.log.debug(f"ACKed message {message_id} on {self.topic}")

            except asyncio.CancelledError:
                self.log.info("EventBusDaemon received cancellation signal.")
                break
            except Exception as e:
                self.log.error(f"Stream Consume Error: {e}")
                await asyncio.sleep(2)
                
        self.log.info("EventBusDaemon successfully evaporated.")