# kernel.phase.boot
import os
import asyncio

from arch.topos.tunnel.factory import TunnelFactory
from arch.contract.event.bus import AsyncEventBus
from arch.contract.executor import BaseExecutor
from arch.contract.registry.unified import registry

from kernel.bind.redirector import PhaseAirlock
from kernel.phase.runtime.executor.swarm import SwarmExecutor
from kernel.phase.runtime.flow.executor import FlowExecutor
from kernel.dphi.ledger.consensus import KernelLedger
from watcher.receptor.mesh.router import RoutingPolicyEngine, ClusterStateMesh, RoutingDecision
from kernel.phase.runtime.node import NodeRuntime
from kernel.phase.signal import PhaseSignal
from kernel.phase.reactor import KernelReactor

from watcher.plane.regulator import default_plane
from watcher.plane.emitter import get_emitter

log = get_emitter("phase.boot")

class KernelGateway:
    @classmethod
    async def assemble(cls, node) -> None:
        topology = os.getenv("GATEWAY_TOPOLOGY", "EMBEDDED_BYPASS")
        log.info(f"[Orchestrator] Assembling Unified Membrane in {topology} mode.")

        broker_facade = await TunnelFactory.get_default()
        policy_engine = RoutingPolicyEngine(broker_facade)
        state_mesh = ClusterStateMesh(broker_facade)
        await policy_engine.synchronize_initial_state()

        asyncio.create_task(policy_engine.watch_policy_updates())
        asyncio.create_task(state_mesh.start_mesh_sync())
        if topology == "EXT_PROC":
            # Note: ExtProcStreamHandler assumes it is available in the environment
            stream_handler = ExtProcStreamHandler(policy_engine, state_mesh)
            asyncio.create_task(stream_handler.serve())
        else:
            log.info(f"[Orchestrator] Running in {topology} mode. (Ingress Receptor removed)")

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
            log.info(f"[Router] Unknown/Legacy command '{command}', falling back to SwarmExecutor.")
            return await self.swarm_executor.execute(psi)

        task_type = task_info_list[0].get("type", "cli")
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

_node_instance = None

async def main_async():
    """비즈니스 로직(컴포넌트 조립 및 구동)만 수행하는 메인 코루틴"""
    global _node_instance
    
    log.info("[Boot] Initiating System Bootstrap Sequence...")
    system_bus = AsyncEventBus()
    
    bridge_watcher = PhaseSignal(event_bus=system_bus)
    default_plane.attach(bridge_watcher)

    completion_signal = asyncio.Event()
    executor = RoutingExecutor(completion_signal)
    
    _node_instance = NodeRuntime(executor=executor)
    log.info("[Boot] Attaching Gateway Topology...")
    await KernelGateway.assemble(_node_instance)
    
    log.info("[Boot] Starting Node Runtime...")
    await _node_instance.start()
    await _node_instance.wait_until_stopped() 

async def teardown():
    """Reactor가 종료 시점에 100% 실행을 보장하는 자원 정리 훅"""
    log.info("[Boot] Releasing system resources...")
    if _node_instance and getattr(_node_instance, 'running', False):
        await _node_instance.shutdown()
        
    await TunnelFactory.close_all()
    try:
        KernelLedger().close()
    except Exception as e:
        log.warning(f"[Boot] Error while releasing KernelStore lock: {e}")
        
    log.info("[Boot] Resource cleanup complete.")

if __name__ == "__main__":
    KernelReactor.ignite(main_coro_func=main_async, teardown_hook=teardown)