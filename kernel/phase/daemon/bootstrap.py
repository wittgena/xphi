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
        self.log.info(f"EventBusDaemon started. Waiting for Sticky Events (Topic: {self.topic})")
        await self._init_consumer_group()
        
        while self.running:
            try:
                streams = await self.tunnel.stream_consume(
                    topic=self.topic, group=self.group_name, consumer=self.consumer_name,
                    count=1, block=self.poll_timeout_ms
                )

                if not streams:
                    # [개선] 더 이상 노드 스스로 idle_timeout을 재서 자가 소멸하지 않음.
                    # 생명 주기의 통제권은 전적으로 외부의 Watcher(Control Plane)에게 위임됨.
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
                            
                            # [유지] 오직 Watcher가 발송한 명시적인 shutdown 명령(수렴)에만 반응
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
    """
    [개선] 기존 LivenessDaemon에서 리더 선출 및 Watcher 기생 로직을 완전히 제거.
    오직 자신의 생존(부하 상태)을 터널에 기록하는 순수 Data Plane 워커로 강등됨.
    """
    def __init__(self, tunnel: UniversalFacade, node_id: str):
        super().__init__("Heartbeat")
        self.tunnel = tunnel
        self.node_id = node_id

    async def run(self):
        self.log.info(f"HeartbeatDaemon initiated. Node [{self.node_id}] emitting vital signs...")
        try:
            while self.running:
                await asyncio.gather(
                    self.tunnel.set(f"{KEY_HEARTBEAT_PREFIX}{self.node_id}", int(time.time()), ex=10),
                    self.tunnel.set(KEY_ACTIVE, int(time.time()), ex=10)
                )
                await asyncio.sleep(2.5)
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


def mount_core_layer(supervisor: TaskSupervisor, ctx: RuntimeContext):
    core_daemons = [
        EventBusDaemon(
            tunnel=ctx.tunnel,
            dispatcher=ctx.dispatcher, 
            node_id=ctx.node_id,
            idle_timeout=ctx.idle_timeout,
            shutdown_hook=ctx.shutdown_hook,
            group_name=GROUP_NODE_MANIFOLD
        ),
        # [개선] LivenessDaemon을 HeartbeatDaemon으로 교체 (순수 워커 상태 방출 역할)
        HeartbeatDaemon(
            tunnel=ctx.tunnel, 
            node_id=ctx.node_id
        ),
        DynamicsDaemon(
            bus=ctx.bus,
            sensor=ctx.sensor
        )
    ]
    for daemon in core_daemons:
        supervisor.mount_daemon(daemon)

def mount_app_layer(supervisor: TaskSupervisor, ctx: RuntimeContext):
    try:
        from kernel.phase.daemon.task.wasm import WasmTaskerDaemon
        wasm_daemon = WasmTaskerDaemon(tunnel=ctx.tunnel, supervisor=supervisor)
        supervisor.mount_daemon(wasm_daemon)
        log.info("L2 Plugin Mounted: WasmTaskerDaemon")
    except ImportError as e:
        log.warn(f"WasmTaskerDaemon bypassed (Not installed or import error): {e}")
    except Exception as e:
        log.error(f"Failed to mount WasmTaskerDaemon: {e}")