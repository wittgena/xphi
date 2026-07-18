# phase.runtime.context
from typing import Any
from dataclasses import dataclass

from arch.topos.bound.tunnel import UniversalFacade
from arch.contract.event.tunnelbus import TunnelEventBus
from phase.runtime.task.supervisor import Dispatcher

@dataclass
class RuntimeContext:
    node_id: str
    tunnel: UniversalFacade
    bus: TunnelEventBus
    dispatcher: Dispatcher
    sensor: Any
    actuator: Any
    idle_timeout: float
    watch_dir: str
    shutdown_hook: callable