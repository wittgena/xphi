# kernel.phase.daemon.bootstrap
## @lineage: kernel.daemon.bootstrap
## @lineage: phase.runtime.daemon.bootstrap
from kernel.phase.runtime.context import RuntimeContext
from kernel.phase.daemon.task.supervisor import TaskSupervisor
from kernel.phase.daemon.base import SensorDaemon, CaptureDaemon, HeartbeatDaemon, SignalDaemon, ReceptorDaemon
from kernel.phase.daemon.dynamics import DynamicsDaemon
from kernel.phase.daemon.event import EventBusDaemon
from watcher.plane.emitter import get_emitter

log = get_emitter("daemon.bootstrap")

def mount_core_layer(supervisor: TaskSupervisor, ctx: RuntimeContext):
    """L1 Core Infra Layer: 노드의 생존과 통신을 담당하는 필수 데몬 장착"""
    core_daemons = [
        SensorDaemon(ctx.sensor, ctx.bus),
        CaptureDaemon(ctx.tunnel, ctx.dispatcher, ctx.idle_timeout, ctx.shutdown_hook),
        HeartbeatDaemon(ctx.tunnel, ctx.node_id),
        SignalDaemon(ctx.tunnel, ctx.shutdown_hook),
        ReceptorDaemon(ctx.tunnel, ctx.node_id, ctx.watch_dir),
        DynamicsDaemon(ctx.bus),
        EventBusDaemon(ctx.tunnel, ctx.bus, ctx.node_id)
    ]
    
    for daemon in core_daemons:
        supervisor.mount_daemon(daemon)

def mount_app_layer(supervisor: TaskSupervisor, ctx: RuntimeContext):
    """L2 Application Layer: 비즈니스 레벨 데몬 장착 (WasmTasker 등)"""
    try:
        from kernel.phase.daemon.task.wasm import WasmTaskerDaemon
        wasm_daemon = WasmTaskerDaemon(tunnel=ctx.tunnel, supervisor=supervisor)
        supervisor.mount_daemon(wasm_daemon)
        log.info("L2 Plugin Mounted: WasmTaskerDaemon")
    except ImportError as e:
        log.warn(f"WasmTaskerDaemon bypassed (Not installed or import error): {e}")
    except Exception as e:
        log.error(f"Failed to mount WasmTaskerDaemon: {e}")

    # 향후 LLMTaskerDaemon 등 다른 플러그인 데몬들도 이곳에 순차적으로 추가하면 됩니다.