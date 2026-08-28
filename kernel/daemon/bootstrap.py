# xphi.kernel.daemon.bootstrap
import asyncio
import json
import time
from abc import ABC, abstractmethod
from typing import Optional, Callable, Awaitable, List, Tuple
from contextlib import suppress
from types import SimpleNamespace

from xphi.kernel.space.topos.tunnel.factory import UniversalFacade
from xphi.arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType
from xphi.arch.contract.event.bus import AsyncEventBus
from xphi.arch.contract.event.next import next_id
from xphi.arch.contract.registry.unified import registry

from xphi.kernel.daemon.base import AbstractDaemon
from xphi.kernel.daemon.task.supervisor import TaskSupervisor, Dispatcher
from xphi.kernel.phase.runtime.context import RuntimeContext
from xphi.kernel.phase.runtime.sensor import SurfaceSensor
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.phase.runtime.flow.cont import LoopCarrier, DynamicsXe

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
            await self.tunnel.state_store.xgroup_create(name=self.topic, groupname=self.group_name, id='$', mkstream=True)
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
    def __init__(self, tunnel: UniversalFacade, node_id: str, supervisor: TaskSupervisor, role: str = "master", capacity: int = 0):
        super().__init__("Heartbeat")
        self.tunnel = tunnel
        self.node_id = node_id
        self.supervisor = supervisor
        self.role = role
        self.capacity = capacity

    async def run(self):
        self.log.info(f"HeartbeatDaemon initiated. Node [{self.node_id}] emitting vital signs (Role: {self.role}, Capacity: {self.capacity})...")
        try:
            while self.running:
                # 1. 상태 메타데이터 구성
                meta_payload = {
                    "ts": int(time.time()),
                    "role": self.role,
                    "capacity": self.capacity
                }
                meta_json = json.dumps(meta_payload)
                
                # 2. Redis에 TTL 10초로 기록
                await asyncio.gather(
                    self.tunnel.set(f"{KEY_HEARTBEAT_PREFIX}{self.node_id}", meta_json, ex=10),
                    self.tunnel.set(KEY_ACTIVE, int(time.time()), ex=10) # 전역 활성화 플래그는 기존 호환성 유지
                )
                
                # 3. 로드 파동(Tension) 전파
                try:
                    active_tasks = len(self.supervisor.get_active_tasks())
                    payload = {
                        "signal_id": f"node_load_{self.node_id}",
                        "value": float(active_tasks)
                    }
                    await self.tunnel.publish("meta.self:signals:phase_mutation", json.dumps(payload))
                except Exception as e:
                    self.log.warning(f"Failed to emit load metric: {e}")

                await asyncio.sleep(1.0) # 부하를 줄이기 위해 0.5초에서 1.0초로 완화
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

def mount_master_layer(supervisor: TaskSupervisor, ctx: RuntimeContext):
    master_daemons = [
        HeartbeatDaemon(
            tunnel=ctx.tunnel, 
            node_id=ctx.node_id,
            supervisor=supervisor,
            role="master",     # 명시적 마스터 롤
            capacity=0         # 마스터는 WASM 처리를 안 하므로 0
        ),
        DynamicsDaemon(
            bus=ctx.bus,
            sensor=ctx.sensor
        )
    ]
    for daemon in master_daemons:
        supervisor.mount_daemon(daemon)
    
    log.info("Master Infra Layer (Heartbeat, Dynamics) mounted successfully.")

def mount_worker_layer(supervisor: TaskSupervisor, ctx: RuntimeContext):
    worker_capacity = 4
    worker_daemons = [
        HeartbeatDaemon(
            tunnel=ctx.tunnel, 
            node_id=ctx.node_id,
            supervisor=supervisor,
            role="worker",        # 명시적 워커 롤
            capacity=worker_capacity
        ),
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
        from xphi.kernel.daemon.task.wasm import TaskWasm
        wasm_daemon = TaskWasm(tunnel=ctx.tunnel, supervisor=supervisor)
        wasm_daemon.concurrency_limit = worker_capacity
        supervisor.mount_daemon(wasm_daemon)
        log.info("Worker Plugin Mounted: WasmTaskerDaemon")
    except ImportError as e:
        log.warn(f"WasmTaskerDaemon bypassed (Not installed or import error): {e}")
    except Exception as e:
        log.error(f"Failed to mount WasmTaskerDaemon: {e}")

    log.info("Worker Data Layer (EventBus, WasmTasker, Heartbeat) mounted successfully.")

    discovered_daemons = getattr(registry, "_daemons", {})
    for daemon_name, DaemonClass in discovered_daemons.items():
        try:
            daemon_instance = DaemonClass(ctx=ctx)
            supervisor.mount_daemon(daemon_instance)
            log.info(f"App Layer Daemon Mounted: {daemon_name} -> {DaemonClass.__name__}")
        except Exception as e:
            log.error(f"Failed to mount dynamic daemon '{daemon_name}': {e}", exc_info=True)
    log.info("Worker Data Layer & App Layer mounted successfully.")
