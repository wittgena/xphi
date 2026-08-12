# kernel.daemon.base
## @lineage: kernel.phase.daemon.base
import asyncio
import json
import time
import random
from abc import ABC, abstractmethod
from typing import Optional, Callable, Awaitable, List, Tuple
from contextlib import suppress

from watcher.plane.emitter import get_emitter

log = get_emitter("daemon.base")

class AbstractDaemon(ABC):
    """@loop.contract: 스스로의 생명주기를 가지는 독립적 주기 컴포넌트"""
    def __init__(self, name: str):
        self.name = name
        self.running = False
        self.task: Optional[asyncio.Task] = None
        self.log = get_emitter(f"daemon.{name.lower()}", phase="SYSTEM")

    async def start(self) -> asyncio.Task:
        self.running = True
        self.task = asyncio.create_task(self.run(), name=f"Daemon-{self.name}")
        return self.task

    async def stop(self):
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task

    @abstractmethod
    async def run(self):
        pass
