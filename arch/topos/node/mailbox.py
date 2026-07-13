# arch.topos.node.mailbox
"""
@desc: Distributed Network Sidecar for GanNode.
@flow: Redis Subscription -> Payload Decoding -> Local Queue Injection
"""
import asyncio
import json
from arch.topos.node.gan import GanNode, Message
from watcher.plane.emitter import get_emitter

log = get_emitter("node.mailbox")

MAILBOX_KEYS = {
    "QUEUE_PREFIX": "runtime:queue:",
    "HEARTBEAT_PREFIX": "runtime:heartbeat:"
}

class NodeMailbox:
    """
    @role: Network Proxy Sidecar.
    @desc: Adapts a native in-memory GanNode for distributed environments. 
           Intercepts external Redis messages and injects them into the local 
           GanNode queue without altering the native actor logic.
    """
    def __init__(self, target_node: GanNode, runtime):
        self.node = target_node
        self.runtime = runtime
        
        self.queue_key = f"{MAILBOX_KEYS['QUEUE_PREFIX']}{self.node.name}"
        self.heartbeat_key = f"{MAILBOX_KEYS['HEARTBEAT_PREFIX']}{self.node.name}"
        
        self._running = False
        self._listener_task = None
        self._heartbeat_task = None

    def attach(self):
        """@step: Ignite sidecar and bind to the target node."""
        self._running = True
        self._listener_task = asyncio.create_task(self._listen_redis())
        self._heartbeat_task = asyncio.create_task(self._beat_heart())
        log.info(f"[Mailbox] Attached to {self.node.name}. Listening on {self.queue_key}")

    async def detach(self):
        """@step: Sever network bindings and halt reception."""
        self._running = False
        if self._listener_task:
            self._listener_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            
        await self.runtime.bus.delete(self.heartbeat_key)
        log.info(f"[Mailbox] Detached from {self.node.name}.")

    async def _listen_redis(self):
        """
        @flow: Redis BLPOP -> JSON Decode -> GanNode Message -> Local Queue Injection.
        """
        try:
            while self._running:
                result = await self.runtime.bus.blpop(self.queue_key, timeout=1.0)
                
                if result:
                    _, payload_bytes = result
                    payload = json.loads(payload_bytes.decode('utf-8'))
                    
                    intent = payload.get("flow_aspect", "default")
                    msg = Message(name=intent, bubble=False, **payload.get("flow_payload", {}))
                    self.node.post_message(msg)
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"[Mailbox] Listener error on {self.node.name}: {e}")

    async def _beat_heart(self):
        """@desc: Emits a periodic survival pulse to notify the NodePool of its vitality"""
        try:
            while self._running:
                await self.runtime.bus.set(self.heartbeat_key, "alive", ex=10)
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass