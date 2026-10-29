# xphi.node.ops.boot
import os
import json
import asyncio
from typing import Optional

from xphi.arch.contract.event.next import EventObserver
from xphi.arch.contract.event.next import LogEvent
from xphi.arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType
from xphi.arch.contract.event.bus import AsyncEventBus
from xphi.arch.contract.registry.unified import registry

from xphi.state.phase.executor.base import BaseExecutor
from xphi.state.phase.executor.cont import SwarmExecutor
from xphi.state.phase.executor.flow import FlowExecutor
from xphi.state.phase.reactor import PhaseReactor

from xphi.state.anchor.store import AnchorStore, AnchorRole

from xphi.node.runtime.anchor import RuntimeAnchor
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.node.runtime.wasm.broker import DphiBroker

from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.plane.regulator import default_plane
from xphi.watcher.receptor.bootstrap import receptor_bootstrap

log = get_emitter("node.ops.boot")

_node_instance: Optional[RuntimeAnchor] = None
_background_tasks = set()

class PhaseSignal(EventObserver):
    def __init__(self, event_bus: AsyncEventBus):
        self.bus = event_bus
        self.log = get_emitter("anchor.bridge", phase="BOOT")

    def update(self, event: LogEvent) -> None:
        if event.level != "SIGNAL":
            return
            
        event_message = str(event.message).strip()
        payload = event.context.copy() if event.context else {}
        payload.update({
            "bridged_from": "Surface.LogEvent",
            "original_source": getattr(event, 'source_id', 'unknown'),
            "boundary": "surface_to_kernel"
        })

        carrier = PsiCarrier(
            kind="SIGNAL",          
            tag=event_message,      
            payload=payload,
            carrier_type=CarrierType.FIXED
        )

        psi_event = PsiEvent(
            event_id=event.event_id,
            parent_id=getattr(event, 'parent_id', None),
            source_id="phase.bridge", 
            scope=getattr(event, 'scope', 'GLOBAL'),
            tick=getattr(event, 'tick', 0) or 0,
            phase_id=getattr(event, 'phase_id', 0),
            carrier=carrier,
            context={"domain": "boundary.crossing"}
        )

        try:
            loop = asyncio.get_running_loop()
            if loop.is_closed():
                raise RuntimeError("Event loop is closed")
            loop.create_task(self._async_dispatch(psi_event, event_message))
        except RuntimeError as e:
            self.log.error(f"[PhaseBridge] Cannot dispatch SIGNAL. Event loop unavailable: {e}")

    async def _async_dispatch(self, psi_event: PsiEvent, original_msg: str):
        try:
            symbol = getattr(psi_event, 'symbol', original_msg)
            self.log.trace(f"[PhaseBridge] Elevating SIGNAL -> Psi: {symbol}")
            await self.bus.publish(psi_event)
        except Exception as e:
            self.log.error(f"[PhaseBridge] Dispatch failed for {original_msg}: {e}", exc_info=True)

class RoutingExecutor(BaseExecutor):
    def __init__(self, completion_signal: asyncio.Event):
        super().__init__()
        self.completion_signal = completion_signal
        self.swarm_executor = SwarmExecutor(completion_signal)
        self.flow_executor = FlowExecutor(completion_signal)
        self._node = None

    @property
    def node(self):
        return self._node

    @node.setter
    def node(self, value):
        self._node = value
        self.swarm_executor.node = value
        self.flow_executor.node = value

    async def execute(self, psi) -> list:
        if not hasattr(psi, 'carrier') or psi.carrier.kind != "COMMAND":
            return []

        context = psi.carrier.payload.get("_context", {})
        command = context.get("command") or psi.carrier.tag
        task_info_list = registry.registered_cli_tasks.get(command)
        if not task_info_list:
            return await self.swarm_executor.execute(psi)

        task_type = task_info_list[0].get("type", "cli")
        if task_type == "flow":
            try:
                return await self.flow_executor.execute(psi)
            except Exception as e:
                log.error(f"[Router] FlowExecutor failed with {e}. Falling back to SwarmExecutor.")
                return await self.swarm_executor.execute(psi)
        else:
            return await self.swarm_executor.execute(psi)


async def main_async():
    global _node_instance
    
    log.info("[Boot] Initiating Control Plane (Master Watcher)...")
    system_bus = AsyncEventBus()
    
    bridge_watcher = PhaseSignal(event_bus=system_bus)
    default_plane.attach(bridge_watcher)

    tunnel = await TunnelFactory.get_default()
    receptor_task = asyncio.create_task(receptor_bootstrap(tunnel))
    _background_tasks.add(receptor_task)
    receptor_task.add_done_callback(_background_tasks.discard)

    log.info("[Boot] Waiting for module discovery and registry sync...")
    active_daemons_str = os.getenv("NODE_DAEMONS", "")
    active_daemons = [d.strip() for d in active_daemons_str.split(",") if d.strip()]
    
    wait_timeout = 30.0
    elapsed = 0.0
    poll_interval = 0.2
    if active_daemons:
        while elapsed < wait_timeout:
            discovered = getattr(registry, "_daemons", {})
            if all(d in discovered for d in active_daemons):
                log.info(f"[Boot] Registry sync verified in {elapsed:.1f}s. Proceeding to ignite node...")
                break
                
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        else:
            missing_daemons = [d for d in active_daemons if d not in getattr(registry, "_daemons", {})]
            log.warning(f"[Boot] Registry sync timed out after {wait_timeout}s! Missing daemons: {missing_daemons}")
    else:
        log.info("[Boot] No specific NODE_DAEMONS requested. Proceeding directly to ignite node...")

    wasm_timeout = float(os.getenv("XPHI_WASM_TIMEOUT", "30.0"))
    log.info(f"[Boot] Provisioning Shared WASM Broker (Timeout: {wasm_timeout}s)...")
    shared_broker = DphiBroker.get_shared(timeout=wasm_timeout, tunnel_factory=TunnelFactory)

    log.info("[Boot] Initializing AnchorStore and injecting Shared Broker...")
    store = AnchorStore()
    
    log.info("[Boot] Igniting Embedded Phase Runtime Node...")
    completion_signal = asyncio.Event()
    executor = RoutingExecutor(completion_signal)
    
    _node_instance = RuntimeAnchor(executor=executor)
    _node_instance.tunnel = tunnel
    _node_instance.broker = shared_broker
    
    await _node_instance.start()

    log.info("[Boot] Control Plane and Embedded Node fully operational.")
    log.info("[Boot] Entering observation mode (External layers delegated to Daemons).")
    await _node_instance.wait_until_stopped()

async def teardown():
    global _node_instance
    log.info("[Boot] Releasing system resources...")
    if _node_instance and getattr(_node_instance, 'running', False):
        await _node_instance.shutdown()

    for task in list(_background_tasks):
        if not task.done():
            task.cancel()

    try:
        AnchorStore().close()
        log.info("[Boot] AnchorStore lock safely released.")
    except Exception as e:
        log.warning(f"[Boot] Error while releasing AnchorStore lock: {e}")

    if DphiBroker._shared_instance is not None:
        try:
            await DphiBroker._shared_instance.close()
            log.info("[Boot] Shared WASM Broker closed cleanly.")
        except Exception as e:
            log.warning(f"[Boot] Error while closing Shared WASM Broker: {e}")
        
    await TunnelFactory.close_all()
    log.info("[Boot] Resource cleanup complete.")

if __name__ == "__main__":
    PhaseReactor.ignite(main_coro_func=main_async, teardown_hook=teardown)