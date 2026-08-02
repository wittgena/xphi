# arch.topos.node.gan
import asyncio
from typing import Optional, Set
from watcher.plane.emitter import get_emitter

log = get_emitter("node.gan")

class Message:
    def __init__(self, name: str, bubble: bool = False, **kwargs):
        self.name = name
        self.bubble = bubble
        self.sender: Optional['GanNode'] = None
        for key, value in kwargs.items():
            setattr(self, key, value)

class GanNode:
    def __init__(self, name: str):
        self.name = name
        self.parent: Optional['GanNode'] = None
        self.children: Set['GanNode'] = set()
        
        ## Internal State & Lifecycle Management
        self._queue = asyncio.Queue()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def post_message(self, message: Message):
        message.sender = self
        self._queue.put_nowait(message)

    async def run(self):
        self._running = True
        log.debug(f"[{self.name}] GanNode context activated (Started)")
        
        while self._running:
            message = await self._queue.get()
            if message is None:  
                break

            try:
                await self._dispatch_message(message)
            except Exception as e:
                log.error(f"[{self.name}] 💥 Unhandled exception during '{message.name}': {e}", exc_info=True)
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
            
        log.debug(f"[{self.name}] GanNode context terminated (Terminated)")

    async def _dispatch_message(self, message: Message):
        handler_name = f"on_{message.name}"
        handler = getattr(self, handler_name, None)
        if handler and callable(handler):
            await handler(message)

        if message.bubble and self.parent:
            self.parent.post_message(message)

    def mount(self, child: 'GanNode'):
        child.parent = self
        self.children.add(child)
        
        # @step: Register the child node to the event loop as an independent Actor
        child._task = asyncio.create_task(child.run())
        log.debug(f"Node {child.name} has been mounted to the {self.name} topos.")

    async def unmount(self, child: 'GanNode'):
        if child in self.children:
            self.children.remove(child)
            child.parent = None
            child._running = False
            child._queue.put_nowait(None)
            
            if child._task:
                await child._task
            log.debug(f"Node {child.name} has been successfully unmounted from the {self.name} topos.")
    
    def stop(self):
        self._running = False
        self._queue.put_nowait(None)