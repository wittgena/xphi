# xphi.node.runtime.context
from typing import Any
from dataclasses import dataclass

from xphi.node.space.tunnel.factory import UniversalFacade
from xphi.arch.contract.event.bus import TunnelEventBus
from xphi.node.ops.task.supervisor import Dispatcher

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