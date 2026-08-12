# kernel.phase.runtime.worker
import asyncio
import sys
import os
import importlib

from arch.topos.tunnel.factory import TunnelFactory
from arch.contract.event.tunnelbus import TunnelEventBus
from kernel.daemon.task.supervisor import TaskSupervisor, Dispatcher
from kernel.phase.runtime.context import RuntimeContext
from kernel.daemon.bootstrap import mount_worker_layer
from arch.contract.event.psi import PsiEvent
from kernel.bind.resolver import find_current_self
from watcher.plane.emitter import get_emitter
from watcher.plane.sink import TunnelSink
from kernel.phase.runtime.sensor import SurfaceActuator

def worker_process_entry(master_id: str, worker_idx: int):
    """
    [순수 Data Plane 진입점] 
    Master의 무거운 의존성 없이 백엔드 WASM 연산 환경만 초경량으로 스폰합니다.
    """
    asyncio.run(_run_worker_loop(master_id, worker_idx))

async def _run_worker_loop(master_id: str, worker_idx: int):
    worker_id = f"{master_id}-w{worker_idx}"
    log = get_emitter(f"node.worker.{worker_idx}", phase="SYSTEM")
    log.info(f"🚀 Worker Process Booting: [{worker_id}] (PID: {os.getpid()})")

    supervisor = TaskSupervisor(source=f"Worker-{worker_id}")
    tunnel = await TunnelFactory.get_default()
    
    # 워커 전용 Dispatcher 생성
    dispatcher = Dispatcher(supervisor=supervisor)
    
    # -------------------------------------------------------------
    # [라우트 1] 분산 핫-리로드 수신 (Master/Receptor의 명령)
    # -------------------------------------------------------------
    def _worker_reload_handler(psi: PsiEvent):
        module_fqn = psi.carrier.payload.get("module_fqn")
        if module_fqn and module_fqn in sys.modules:
            try:
                importlib.reload(sys.modules[module_fqn])
                log.info(f"[Plasticity] Topology synchronized in Worker: {module_fqn}")
            except Exception as e:
                log.error(f"[Cleavage] Failed to sync topology in Worker: {e}")
                
    dispatcher.add_route(
        predicate=lambda p: p.carrier.kind == "system:topology" and p.carrier.tag == "reload",
        handler=_worker_reload_handler
    )

    # -------------------------------------------------------------
    # [라우트 2] CLI 커맨드 무시 (Master에게 위임)
    # -------------------------------------------------------------
    # CLI 파이썬 태스크(COMMAND)는 Control Plane인 Master Node가 
    # SwarmExecutor/FlowExecutor를 통해 처리해야 하므로, 
    # Worker 프로세스는 이를 조용히 무시하여 'No handler resolved' 로그 오염을 방지합니다.
    def _ignore_cli_commands(psi: PsiEvent):
        pass 

    dispatcher.add_route(
        predicate=lambda p: hasattr(p, 'carrier') and p.carrier.kind == "COMMAND",
        handler=_ignore_cli_commands
    )

    ctx = RuntimeContext(
        node_id=worker_id,
        tunnel=tunnel,
        bus=TunnelEventBus(tunnel),
        dispatcher=dispatcher,
        sensor=None, # Worker는 센서를 운영하지 않음
        actuator=SurfaceActuator(TunnelSink(tunnel=tunnel)),
        idle_timeout=0.0,
        watch_dir=find_current_self(),
        shutdown_hook=supervisor.shutdown
    )

    mount_worker_layer(supervisor, ctx)
    
    try:
        while not supervisor._closed:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        log.info(f"Worker {worker_id} received shutdown signal.")
    finally:
        await tunnel.close()
        log.info(f"Worker Process Terminated: [{worker_id}]")