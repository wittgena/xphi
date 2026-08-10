# kernel.phase.daemon.bootstrap
import asyncio
import json
import time
from abc import ABC, abstractmethod
from typing import Optional, Callable, Awaitable, List, Tuple
from contextlib import suppress
from types import SimpleNamespace

from arch.topos.tunnel.factory import UniversalFacade
from arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType
from arch.contract.event.bus import AsyncEventBus
from arch.contract.event.next import next_id
from arch.contract.registry.unified import registry

from kernel.phase.daemon.base import AbstractDaemon
from kernel.phase.daemon.task.supervisor import TaskSupervisor, Dispatcher
from kernel.phase.runtime.context import RuntimeContext
from kernel.phase.runtime.sensor import SurfaceSensor
from watcher.plane.emitter import get_emitter
from kernel.phase.runtime.flow.cont import LoopCarrier, DynamicsXe

log = get_emitter("daemon.bootstrap")
SENSOR_INTERVAL = 1.0

TOPIC_BUS_STREAM        = "runtime:bus:stream"
GROUP_NODE_MANIFOLD     = "node_manifold_group"

KEY_HEARTBEAT_PREFIX    = "runtime:heartbeat:"
KEY_HEARTBEAT_PATTERN   = "runtime:heartbeat:*"
KEY_ACTIVE              = "runtime:active"

class EventBusDaemon(AbstractDaemon):
    def __init__(self, tunnel: UniversalFacade, dispatcher: Dispatcher, node_id: str, 
                 idle_timeout: float, shutdown_hook: Callable[[], Awaitable[None]], 
                 group_name: str = GROUP_NODE_MANIFOLD):
        super().__init__("EventBus")
        self.tunnel = tunnel
        self.dispatcher = dispatcher
        self.node_id = node_id
        self.shutdown_hook = shutdown_hook
        
        self.topic = TOPIC_BUS_STREAM
        self.group_name = group_name
        self.consumer_name = f"consumer-{self.node_id}"
        self.poll_timeout_ms = 1000

    async def _init_consumer_group(self):
        try:
            await self.tunnel.state_store.xgroup_create(name=self.topic, groupname=self.group_name, id='0', mkstream=True)
            self.log.info(f"Consumer Group '{self.group_name}' initialized.")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                self.log.error(f"Failed to initialize Consumer Group: {e}")

    async def run(self):
        self.log.info(f"EventBusDaemon started. Waiting for Sticky Events (Topic: {self.topic}, Consumer: {self.consumer_name})")
        await self._init_consumer_group()
        
        while self.running:
            try:
                streams = await self.tunnel.stream_consume(
                    topic=self.topic, group=self.group_name, consumer=self.consumer_name,
                    count=1, block=self.poll_timeout_ms
                )

                if not streams:
                    continue

                for stream_name, messages in streams:
                    for message_id, msg_data in messages:
                        json_payload = msg_data.get("data", msg_data.get(b"data", b"{}"))
                        if isinstance(json_payload, bytes):
                            json_payload = json_payload.decode('utf-8')
                        
                        try:
                            event_dict = json.loads(json_payload)
                            if 'carrier' in event_dict and isinstance(event_dict['carrier'], dict):
                                event_dict['carrier'] = PsiCarrier(**event_dict['carrier'])
                                
                            event = PsiEvent(**event_dict)
                            if event.carrier.kind == "system:shutdown":
                                self.log.warn("Shutdown signal received via Event Stream. Evaporating...")
                                asyncio.create_task(self.shutdown_hook())
                                break
                                
                            self.dispatcher.dispatch(event)
                        except Exception as e:
                            self.log.error(f"Event parsing or dispatch error: {e}")
                        
                        await self.tunnel.stream_ack(self.topic, self.group_name, message_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Stream Consume Error: {e}")
                await asyncio.sleep(1)


class HeartbeatDaemon(AbstractDaemon):
    def __init__(self, tunnel: UniversalFacade, node_id: str, supervisor: TaskSupervisor):
        super().__init__("Heartbeat")
        self.tunnel = tunnel
        self.node_id = node_id
        self.supervisor = supervisor

    async def run(self):
        self.log.info(f"HeartbeatDaemon initiated. Node [{self.node_id}] emitting vital signs & load metrics...")
        try:
            while self.running:
                await asyncio.gather(
                    self.tunnel.set(f"{KEY_HEARTBEAT_PREFIX}{self.node_id}", int(time.time()), ex=10),
                    self.tunnel.set(KEY_ACTIVE, int(time.time()), ex=10)
                )
                
                try:
                    active_tasks = len(self.supervisor.get_active_tasks())
                    payload = {
                        "signal_id": f"node_load_{self.node_id}",
                        "value": float(active_tasks)
                    }
                    await self.tunnel.publish("meta.self:signals:phase_mutation", json.dumps(payload))
                except Exception as e:
                    self.log.warning(f"Failed to emit load metric: {e}")

                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            self.log.warn("HeartbeatDaemon received cancellation signal.")
        except Exception as e:
            self.log.error(f"HeartbeatDaemon Error: {e}")


class DynamicsDaemon(AbstractDaemon):
    def __init__(self, bus: AsyncEventBus, sensor: Optional[SurfaceSensor] = None):
        super().__init__("Dynamics")
        self.bus = bus
        self.sensor = sensor
        self.carriers: List[Tuple[str, LoopCarrier]] = []

    async def start(self) -> asyncio.Task:
        self.log.info("DynamicsDaemon scanning registry for sensor kernels...")
        kernels_map = getattr(registry, "_kernels", {})
        
        for name, kernel_class in kernels_map.items():
            if name.startswith("sensor."):
                try:
                    kernel_instance = kernel_class() 
                    xe_core = DynamicsXe(bound=kernel_instance, ex=f"genesis.{name}")
                    carrier = LoopCarrier(xe=xe_core, max_ticks=999999999, interval=0.5)
                    carrier.node = SimpleNamespace(bus=self.bus)
                    self.carriers.append((name, carrier))
                except Exception as e:
                    self.log.error(f"Failed to bootstrap kernel '{name}': {e}", exc_info=True)
        
        self.log.info(f"Discovered and bound {len(self.carriers)} kernels.")
        return await super().start()

    async def _legacy_sensor_loop(self):
        """기존 SensorDaemon의 역할을 서브 태스크로 흡수"""
        if not self.sensor: return
        self.log.info("Surface Sensor loop started.")
        while self.running:
            try:
                signals = await self.sensor.sense()
                for psi in signals:
                    await self.bus.publish(psi) 
                await asyncio.sleep(SENSOR_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Surface Sensor Error: {e}")
                await asyncio.sleep(2)

    async def run(self):
        tasks = []
        if self.sensor:
            tasks.append(asyncio.create_task(self._legacy_sensor_loop()))
            
        for name, carrier in self.carriers:
            initial_psi = self._create_genesis_psi(name)
            tasks.append(asyncio.create_task(carrier.execute(initial_psi)))
            
        if not tasks:
            self.log.warn("No sensors or dynamic kernels found. Idling...")
            while self.running:
                await asyncio.sleep(1)
            return

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            self.log.warn("DynamicsDaemon execution cancelled.")
            
    def _create_genesis_psi(self, name: str) -> PsiEvent:
        carrier = PsiCarrier(kind="genesis", tag=name, payload={}, carrier_type=CarrierType.FIXED)
        return PsiEvent(
            event_id=next_id(), parent_id=None, source_id=f"daemon.dynamics.{name}",
            scope="GLOBAL", tick=0, carrier=carrier, phase_id=0, context={"domain": "kernel.bootstrap"}
        )


# =====================================================================
# 1. Master Layer Mounting (Control Plane 용)
# =====================================================================
def mount_master_layer(supervisor: TaskSupervisor, ctx: RuntimeContext):
    """
    Master 노드 전용 데몬입니다. 
    상태 동기화(Heartbeat)와 센서/동적 커널(Dynamics) 구동만 전담합니다.
    (연산 부하가 있는 EventBus 스트림 소비는 하지 않습니다.)
    """
    master_daemons = [
        HeartbeatDaemon(
            tunnel=ctx.tunnel, 
            node_id=ctx.node_id,
            supervisor=supervisor
        ),
        DynamicsDaemon(
            bus=ctx.bus,
            sensor=ctx.sensor
        )
    ]
    for daemon in master_daemons:
        supervisor.mount_daemon(daemon)
    
    log.info("Master Infra Layer (Heartbeat, Dynamics) mounted successfully.")


# =====================================================================
# 2. Worker Layer Mounting (Data Plane 용)
# =====================================================================
def mount_worker_layer(supervisor: TaskSupervisor, ctx: RuntimeContext):
    """
    각 Worker 프로세스 전용 데몬입니다. 
    Redis Consumer Group을 통해 이벤트를 가져오고 WASM 플러그인을 실행합니다.
    """
    worker_daemons = [
        EventBusDaemon(
            tunnel=ctx.tunnel,
            dispatcher=ctx.dispatcher, 
            node_id=ctx.node_id,
            idle_timeout=ctx.idle_timeout,
            shutdown_hook=ctx.shutdown_hook,
            group_name=GROUP_NODE_MANIFOLD
        )
    ]
    for daemon in worker_daemons:
        supervisor.mount_daemon(daemon)

    try:
        from kernel.phase.daemon.task.wasm import WasmTaskerDaemon
        wasm_daemon = WasmTaskerDaemon(tunnel=ctx.tunnel, supervisor=supervisor)
        supervisor.mount_daemon(wasm_daemon)
        log.info("Worker Plugin Mounted: WasmTaskerDaemon")
    except ImportError as e:
        log.warn(f"WasmTaskerDaemon bypassed (Not installed or import error): {e}")
    except Exception as e:
        log.error(f"Failed to mount WasmTaskerDaemon: {e}")

    log.info("Worker Data Layer (EventBus, WasmTasker) mounted successfully.")