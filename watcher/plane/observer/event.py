# xphi.watcher.plane.observer.event
## @lineage: watcher.plane.observer.event
from typing import Protocol, Optional
from xphi.arch.contract.event.next import LogEvent

class EventObserver(Protocol):
    def update(self, event: LogEvent) -> None:
        ...