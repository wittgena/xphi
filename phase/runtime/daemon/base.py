# phase.runtime.daemon.base
import asyncio
import json
import time
import random
from abc import ABC, abstractmethod
from typing import Optional, Callable, Awaitable
from contextlib import suppress

from arch.topos.bound.tunnel import UniversalFacade
from arch.contract.event.psi import PsiEvent, PsiCarrier
from arch.contract.event.bus import AsyncEventBus
from phase.runtime.daemon.task.supervisor import Dispatcher
from phase.runtime.daemon.receptor.bootstrap import receptor_bootstrap
from phase.runtime.sensor import SurfaceSensor
from watcher.plane.emitter import get_emitter

SENSOR_INTERVAL = 1.0

class AbstractDaemon(ABC):
    """@loop.contract: 스스로의 생명주기를 가지는 독립적 주기 컴포넌트"""
    def __init__(self, name: str):
        self.name = name
        self.running = False
        self.task: Optional[asyncio.Task] = None
        self.log = get_emitter(f"daemon.{name.lower()}", phase="SYSTEM")

    async def start(self) -> asyncio.Task:
        self.running = True
        self.task = asyncio.create_task(self.run(), name=f"Daemon-{self.name}")
        return self.task

    async def stop(self):
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task

    @abstractmethod
    async def run(self):
        pass

class SensorDaemon(AbstractDaemon):
    """@psi.observe: surface → bus"""
    def __init__(self, sensor: SurfaceSensor, bus: AsyncEventBus):
        super().__init__("Sensor")
        self.sensor = sensor
        self.bus = bus

    async def run(self):
        self.log.info("Sensor loop started. Observing state space.")
        while self.running:
            try:
                signals = await self.sensor.sense()
                for psi in signals:
                    await self.bus.publish(psi) 
                await asyncio.sleep(SENSOR_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Sensor Error: {e}")
                await asyncio.sleep(2)

class CaptureDaemon(AbstractDaemon):
    """@psi.capture: global queue → dispatcher"""
    def __init__(self, tunnel: UniversalFacade, dispatcher: Dispatcher, idle_timeout: float, shutdown_hook: Callable[[], Awaitable[None]]):
        super().__init__("Capture")
        self.tunnel = tunnel
        self.dispatcher = dispatcher
        self.shutdown_hook = shutdown_hook
        self.base_timeout = float(idle_timeout)
        self.idle_timeout = float(idle_timeout)
        self.last_active_time = time.time()

    async def run(self):
        self.log.info(f"Capture loop started (Idle Timeout: {self.idle_timeout}s)")
        while self.running:
            try:
                res = await self.tunnel.brpop("runtime:queue", timeout=1.0)
                if res:
                    _, data = res
                    event_dict = json.loads(data)
                    if 'carrier' in event_dict and isinstance(event_dict['carrier'], dict):
                        event_dict['carrier'] = PsiCarrier(**event_dict['carrier'])
                    
                    psi = PsiEvent(**event_dict)
                    self.last_active_time = time.time()
                    self.idle_timeout = self.base_timeout 
                    self.dispatcher.dispatch(psi)
                else:
                    if time.time() - self.last_active_time > self.idle_timeout:
                        active_nodes = await self.tunnel.keys("runtime:heartbeat:*")
                        if len(active_nodes) <= 1:
                            decayed = self.idle_timeout * 0.9
                            jitter = random.uniform(-5.0, 5.0)
                            self.idle_timeout = max(10.0, decayed + jitter)
                            self.last_active_time = time.time()
                            self.log.warn(
                                f"Last node standing. Evaporation aborted. "
                                f"Idle timeout mutated to {self.idle_timeout:.1f}s"
                            )
                        else:
                            self.log.warn(f"Idle for {self.idle_timeout:.1f}s. Self-evaporating...")
                            asyncio.create_task(self.shutdown_hook())
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Capture Error: {e}")
                await asyncio.sleep(1)

class HeartbeatDaemon(AbstractDaemon):
    """@phase.liveness: temporal presence 유지"""
    def __init__(self, tunnel: UniversalFacade, node_id: str):
        super().__init__("Heartbeat")
        self.tunnel = tunnel
        self.node_id = node_id

    async def run(self):
        try:
            while self.running:
                await self.tunnel.set(f"runtime:heartbeat:{self.node_id}", int(time.time()), ex=10)
                await self.tunnel.set("runtime:active", int(time.time()), ex=10)
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            pass

class SignalDaemon(AbstractDaemon):
    """@control.inbound: external signal → runtime control"""
    def __init__(self, tunnel: UniversalFacade, shutdown_hook: Callable[[], Awaitable[None]]):
        super().__init__("Signal")
        self.tunnel = tunnel
        self.shutdown_hook = shutdown_hook

    async def run(self):
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe("runtime:signal")
        try:
            async for msg in pubsub.listen():
                if not self.running:
                    break
                if isinstance(msg, dict) and msg.get("type") == "message":
                    parsed = json.loads(msg["data"])
                    if parsed.get("type") == "shutdown":
                        self.log.warn("Shutdown signal received via pubsub.")
                        # [개선] 주입받은 콜백 실행
                        asyncio.create_task(self.shutdown_hook())
                        break
        except asyncio.CancelledError:
            pass
        finally:
            if hasattr(pubsub, 'unsubscribe'):
                await pubsub.unsubscribe("runtime:signal")
            await pubsub.close()

class ReceptorDaemon(AbstractDaemon):
    """@membrane.leader: 스웜(Swarm) 중 단 하나의 노드만 물리적 멤브레인(Watchdog)을 담당하도록 하는 리더 선출 데몬"""
    def __init__(self, tunnel: UniversalFacade, node_id: str, watch_dir: str):
        super().__init__("Receptor")
        self.tunnel = tunnel
        self.node_id = node_id
        self.watch_dir = watch_dir
        self.lock_key = "runtime:receptor:leader"
        self.receptor_task: Optional[asyncio.Task] = None

    async def run(self):
        self.log.info("Receptor daemon initiated. Engaging in leader election...")
        try:
            while self.running:
                acquired = await self.tunnel.set(self.lock_key, self.node_id, nx=True, ex=6)
                
                if not acquired:
                    current_leader = await self.tunnel.get(self.lock_key)
                    if current_leader == self.node_id:
                        await self.tunnel.expire(self.lock_key, 6)
                        acquired = True

                if acquired:
                    if self.receptor_task is None or self.receptor_task.done():
                        self.log.warn(f"[{self.node_id}] Acquired Membrane Leadership. Bootstrapping Receptor...")
                        self.receptor_task = asyncio.create_task(receptor_bootstrap(self.tunnel, self.watch_dir))
                else:
                    if self.receptor_task and not self.receptor_task.done():
                        self.log.warn(f"[{self.node_id}] Lost Membrane Leadership. Shutting down local Receptor...")
                        self.receptor_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await self.receptor_task
                        self.receptor_task = None

                await asyncio.sleep(2)
                
        except asyncio.CancelledError:
            self.log.warn("ReceptorDaemon received cancellation signal.")
        except Exception as e:
            self.log.error(f"ReceptorDaemon Error: {e}")
        finally:
            if self.receptor_task and not self.receptor_task.done():
                self.log.warn("Tearing down Membrane (Watchdog OS Threads)...")
                self.receptor_task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await self.receptor_task 
            
            try:
                if await self.tunnel.get(self.lock_key) == self.node_id:
                    await self.tunnel.delete(self.lock_key)
            except Exception:
                pass
            
            self.log.info("ReceptorDaemon successfully evaporated.")