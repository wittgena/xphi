# kernel.phase.boot
import os
import sys
import asyncio
import subprocess

from arch.topos.tunnel.factory import TunnelFactory
from arch.contract.event.bus import AsyncEventBus
from arch.contract.executor import BaseExecutor
from arch.contract.registry.unified import registry

from kernel.bind.redirector import PhaseAirlock
from kernel.phase.runtime.executor.swarm import SwarmExecutor
from kernel.phase.runtime.flow.executor import FlowExecutor
from kernel.dphi.ledger.consensus import KernelLedger
from watcher.receptor.policy.router import RoutingPolicyEngine, ClusterStateMesh, RoutingDecision
from kernel.phase.signal import PhaseSignal
from kernel.phase.reactor import PhaseReactor

from watcher.plane.regulator import default_plane
from watcher.plane.emitter import get_emitter

# [추가] Watcher(Receptor) 구동을 위한 모듈 임포트
from watcher.receptor.bootstrap import receptor_bootstrap

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
                log.error(f"[Router] FlowExecutor failed with {e}. Falling back to SwarmExecutor.")
                return await self.swarm_executor.execute(psi)
        else:
            log.info(f"[Router] Routing '{command}' -> SwarmExecutor (In-memory)")
            return await self.swarm_executor.execute(psi)


def spawn_worker_node():
    """
    [추가] 독립된 서브프로세스로 워커 노드(Data Plane)를 실행합니다.
    OS 레벨에서 할당된 유휴 코어를 점유하며 완벽한 Lock-free 상태로 동작합니다.
    """
    log.info("[Boot] Spawning initial Phase Runtime (Worker Node)...")
    subprocess.Popen([sys.executable, "-m", "kernel.phase.runtime.node"])


async def main_async():
    """
    [수정] 비즈니스 로직 연산을 제거하고 Control Plane (마스터 관측자) 역할로 격상
    """
    log.info("[Boot] Initiating Control Plane (Master Watcher)...")
    system_bus = AsyncEventBus()
    
    bridge_watcher = PhaseSignal(event_bus=system_bus)
    default_plane.attach(bridge_watcher)

    # 1. 터널 및 라우팅 상태망 동기화 (NodeRuntime 주입 불필요)
    log.info("[Boot] Attaching Gateway Topology...")
    await KernelGateway.assemble(None)
    
    # 2. Watcher(Receptor) 영구 가동 (Daemon에서 Boot로 이관)
    log.info("[Boot] Igniting Physical Membrane Receptor...")
    tunnel = await TunnelFactory.get_default()
    asyncio.create_task(receptor_bootstrap(tunnel))

    # 3. 최소 1개의 런타임 워커 노드(Data Plane) 최초 스폰
    spawn_worker_node()

    log.info("[Boot] Control Plane fully operational. Entering observation mode.")
    # 마스터 루프는 연산에 참여하지 않고 무한 대기하며 궤적(Trajectory)을 관측함
    await asyncio.Event().wait()


async def teardown():
    """Reactor 종료 시점에 리소스 릴리즈"""
    log.info("[Boot] Releasing system resources...")
    await TunnelFactory.close_all()
    
    try:
        KernelLedger().close()
    except Exception as e:
        log.warning(f"[Boot] Error while releasing KernelStore lock: {e}")
        
    log.info("[Boot] Resource cleanup complete.")

if __name__ == "__main__":
    PhaseReactor.ignite(main_coro_func=main_async, teardown_hook=teardown)