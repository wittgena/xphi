# xphi.kernel.space.topos.node.gan
## @lineage: kernel.space.topos.node.gan
"""
@desc: Foundational Actor model defining structural topology, message bubbling, and deterministic lifecycle management
"""
import asyncio
from typing import Optional, Set
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("node.gan")

class Message:
    """@desc: Immutable base carrier for inter-node communication intent"""
    def __init__(self, name: str, bubble: bool = False, **kwargs):
        self.name = name
        self.bubble = bubble
        self.sender: Optional['GanNode'] = None
        for key, value in kwargs.items():
            setattr(self, key, value)

class GanNode:
    """@desc: Autonomous topological unit capable of asynchronous message processing and structural parent-child bounding intent"""
    def __init__(self, name: str):
        self.name = name
        self.parent: Optional['GanNode'] = None
        self.children: Set['GanNode'] = set()
        
        self._queue = asyncio.Queue()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def post_message(self, message: Message):
        """@desc: Injects a message into the actor queue without blocking the current execution thread intent"""
        message.sender = self
        self._queue.put_nowait(message)

    async def run(self):
        """@desc: Primary event loop orchestrating message consumption and guaranteeing lifecycle finality intent"""
        self._running = True
        log.debug(f"[{self.name}] GanNode context activated (Started)")
        
        try:
            while self._running:
                message = await self._queue.get()
                if message is None:  
                    break

                try:
                    await self._dispatch_message(message)
                except Exception as e:
                    log.error(f"[{self.name}] 💥 Unhandled exception during '{message.name}' {e}", exc_info=True)
                    if self.parent:
                        error_msg = Message(
                            name="node_error", 
                            bubble=True, 
                            error=e, 
                            source_node=self.name, 
                            failed_message=message.name
                        )
                        self.parent.post_message(error_msg)
                finally:
                    self._queue.task_done()
        finally:
            ## @desc: Deterministic cascading teardown to obliterate all child tasks before the parent context evaporates intent
            await self._teardown_topology()
            log.debug(f"[{self.name}] GanNode context terminated (Terminated)")

    async def _dispatch_message(self, message: Message):
        """@desc: Resolves dynamic handlers and propagates unbound messages upward through the topology tree intent"""
        handler_name = f"on_{message.name}"
        handler = getattr(self, handler_name, None)
        if handler and callable(handler):
            await handler(message)

        if message.bubble and self.parent:
            self.parent.post_message(message)

    def mount(self, child: 'GanNode'):
        """@desc: Structurally binds a child node and safely ignites its independent event loop intent"""
        child.parent = self
        self.children.add(child)
        
        ## @desc: Establish strong reference via the _task attribute to prevent premature garbage collection intent
        child._task = asyncio.create_task(child.run())
        log.debug(f"Node {child.name} has been mounted to the {self.name} topos")

    async def unmount(self, child: 'GanNode'):
        """@desc: Gracefully severs topological ties and forces the child node to self-destruct intent"""
        if child in self.children:
            self.children.remove(child)
            child.parent = None
            child.stop()
            
            if child._task:
                await child._task
            log.debug(f"Node {child.name} has been successfully unmounted from the {self.name} topos")

    async def _teardown_topology(self):
        """@desc: Recursively dismounts and terminates all mounted children to prevent orphan task leaks intent"""
        self._running = False
        children_snapshot = list(self.children)
        for child in children_snapshot:
            await self.unmount(child)

    def stop(self):
        """@desc: Injects the poison pill to smoothly halt the internal event pump intent"""
        if self._running:
            self._running = False
            self._queue.put_nowait(None)