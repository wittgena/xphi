# watcher.kernel.boot
import asyncio
import uvloop
import os
import asyncio

from arch.topos.bound.tunnel import TunnelFactory
from arch.contract.event.bus import AsyncEventBus
from arch.contract.executor import BaseExecutor
from arch.contract.registry.unified import registry
from arch.topos.bound.tunnel import TunnelFactory
from arch.contract.event.bus import AsyncEventBus

from phase.bind.redirector import PhaseAirlock
from phase.executor.swarm import SwarmExecutor
from phase.runtime.node import NodeRuntime, install_os_signal

from watcher.plane.regulator import default_plane
from watcher.plane.flow.executor import FlowExecutor
from watcher.plane.emitter import get_emitter

from phase.runtime.mesh.memory import BridgeMemory
from watcher.kernel.ledger import KernelLedger
from watcher.kernel.mesh import RoutingPolicyEngine, ClusterStateMesh, RoutingDecision
from watcher.kernel.phase.signal import PhaseSignal
from watcher.kernel.receptor import PolymorphicReceptor

log = get_emitter("kernel.boot")

TARGET_REPO = "brane"

class KernelGateway:
    @classmethod
    async def assemble(cls, node) -> None:
        topology = os.getenv("GATEWAY_TOPOLOGY", "EMBEDDED_BYPASS")
        log.info(f"[Orchestrator] Assembling Unified Membrane in {topology} mode.")

        broker_facade = await TunnelFactory.get_default()
        policy_engine = RoutingPolicyEngine(broker_facade)
        state_mesh = ClusterStateMesh(broker_facade)
        await policy_engine.synchronize_initial_state()

        bridge = BridgeMemory.resolve_bridge(topology, policy_engine, state_mesh)
        event_bus = getattr(node, 'bus', AsyncEventBus())
        receptor = PolymorphicReceptor(bus=event_bus, bridge=bridge)

        asyncio.create_task(policy_engine.watch_policy_updates())
        asyncio.create_task(state_mesh.start_mesh_sync())

        if topology == "EXT_PROC":
            stream_handler = ExtProcStreamHandler(policy_engine, state_mesh)
            asyncio.create_task(stream_handler.serve())
        else:
            asyncio.create_task(receptor.listen())

class SearcherAdapter:
    def __init__(self, searcher):
        self.searcher = searcher
        
    def fetch(self, query: str, **kwargs):
        result = self.searcher.search()
        return [{"text": f"Searched Class: {v['class']} in {v['module']}", "score": 1.0} for k, v in result.items()]

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
        
        ## @fallback.1: Safely forward dynamic/legacy tasks missing from the registry to the existing Swarm
        if not task_info_list:
            log.info(f"[Router] Unknown/Legacy command '{command}', falling back to SwarmExecutor.")
            return await self.swarm_executor.execute(psi)

        task_type = task_info_list[0].get("type", "cli")

        ## @routing: Execute as a new isolated subprocess if the type is 'flow'
        if task_type == "flow":
            log.info(f"[Router] Routing '{command}' -> FlowExecutor (Isolated Subprocess)")
            try:
                return await self.flow_executor.execute(psi)
            except Exception as e:
                ## @fallback.2: Optional retry via Swarm in case of a fatal error during isolated process execution
                log.error(f"[Router] FlowExecutor failed with {e}. Falling back to SwarmExecutor.")
                return await self.swarm_executor.execute(psi)
        else:
            log.info(f"[Router] Routing '{command}' -> SwarmExecutor (In-memory)")
            return await self.swarm_executor.execute(psi)

async def main_async():
    log.info("[Boot] Initiating System Bootstrap Sequence...")
    system_bus = AsyncEventBus()
    
    bridge_watcher = PhaseSignal(event_bus=system_bus)
    default_plane.attach(bridge_watcher)

    ## 핵심 런타임 및 Executor 구성
    completion_signal = asyncio.Event()
    executor = RoutingExecutor(completion_signal)
    node = NodeRuntime(executor=executor)
    install_os_signal(node)

    ## Gateway 토폴로지 결합 (핵심: boot가 gateway를 호출)
    log.info("[Boot] Attaching Gateway Topology...")
    await KernelGateway.assemble(node)

    ## 전체 시스템 라이프사이클 시작 및 대기
    try:
        log.info("[Boot] Starting Node Runtime...")
        await node.start()
        await node.wait_until_stopped() 
    except asyncio.CancelledError:
        log.info("[Boot] Main loop cancelled by system.")
    finally:
        if getattr(node, 'running', False):
            await node.shutdown()
            
        log.info("[Boot] Releasing system resources...")
        await TunnelFactory.close_all()
        try:
            KernelLedger().close()
        except Exception as e:
            log.warning(f"[Boot] Error while releasing KernelStore lock: {e}")
            
        log.info("[Boot] Resource cleanup complete.")

if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        log.info("\n[System] Shutdown sequence completed by user interrupt. Fade to black.")