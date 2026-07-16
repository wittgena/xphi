# watcher.plane.observer.event
from typing import Protocol, Optional
from arch.contract.event.next import LogEvent

class EventObserver(Protocol):
    def update(self, event: LogEvent) -> None:
        ...