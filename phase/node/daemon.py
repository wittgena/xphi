# phase.node.daemon
import asyncio
import json
import time
import random
from abc import ABC, abstractmethod
import redis.asyncio as redis_async
from topos.bound.plane.emitter import get_emitter
from arch.contract.event.psi import PsiEvent, PsiCarrier
from phase.node.sensor import sense_once
from arch.contract.event.bus import AsyncEventBus
from phase.node.dispatcher import Dispatcher

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
        self.task = asyncio.create_task(self.run(), name=self.name)
        return self.task

    async def stop(self):
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

    @abstractmethod
    async def run(self):
        pass

class SensorDaemon(AbstractDaemon):
    """@psi.observe: surface → bus"""
    def __init__(self, redis: redis_async.Redis, bus: AsyncEventBus):
        super().__init__("Sensor")
        self.redis = redis
        self.bus = bus

    async def run(self):
        self.log.info("Sensor loop started. Observing state space.")
        while self.running:
            try:
                signals = await sense_once(self.redis)
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
    def __init__(self, redis: redis_async.Redis, dispatcher: Dispatcher, node: 'NodeRuntime', idle_timeout: int):
        super().__init__("Capture")
        self.redis = redis
        self.dispatcher = dispatcher
        self.node = node
        self.base_timeout = idle_timeout  
        self.idle_timeout = idle_timeout
        self.last_active_time = time.time()

    async def run(self):
        self.log.info(f"Capture loop started (Idle Timeout: {self.idle_timeout}s)")
        while self.running:
            try:
                res = await self.redis.brpop("runtime:queue", timeout=1.0)
                if res:
                    _, data = res
                    event_dict = json.loads(data)
                    if 'carrier' in event_dict and isinstance(event_dict['carrier'], dict):
                        event_dict['carrier'] = PsiCarrier(**event_dict['carrier'])
                    
                    psi = PsiEvent(**event_dict)
                    self.last_active_time = time.time()
                    self.idle_timeout = self.base_timeout 
                    await self.dispatcher.send(psi)
                else:
                    if time.time() - self.last_active_time > self.idle_timeout:
                        ## [항상성 확인]: 현재 생존 중인 노드 수 파악
                        active_nodes = await self.redis.keys("runtime:heartbeat:*")
                        if len(active_nodes) <= 1:
                            ## [변이 적용]: 최후의 1인일 경우 90% + 랜덤 지터(±5초)
                            decayed = self.idle_timeout * 0.9
                            jitter = random.uniform(-5.0, 5.0)
                            
                            ## 하한선 10초를 보장하여 음수나 과도한 폴링 방지
                            self.idle_timeout = max(10.0, decayed + jitter)
                            self.last_active_time = time.time()
                            self.log.warn(
                                f"Last node standing. Evaporation aborted. "
                                f"Idle timeout mutated to {self.idle_timeout:.1f}s"
                            )
                        else:
                            ## 다른 노드가 존재하면 정상적으로 증발(Evaporation)
                            self.log.warn(f"Idle for {self.idle_timeout:.1f}s. Self-evaporating...")
                            asyncio.create_task(self.node.shutdown())
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Capture Error: {e}")
                await asyncio.sleep(1)

class HeartbeatDaemon(AbstractDaemon):
    """@phase.liveness: temporal presence 유지"""
    def __init__(self, redis: redis_async.Redis, node_id: str):
        super().__init__("Heartbeat")
        self.redis = redis
        self.node_id = node_id

    async def run(self):
        try:
            while self.running:
                await self.redis.set(f"runtime:heartbeat:{self.node_id}", int(time.time()), ex=10)
                await self.redis.set("runtime:active", int(time.time()), ex=10)
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            pass

class SignalDaemon(AbstractDaemon):
    """@control.inbound: external signal → runtime control"""
    def __init__(self, redis: redis_async.Redis, node: 'NodeRuntime'):
        super().__init__("Signal")
        self.redis = redis
        self.node = node

    async def run(self):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("runtime:signal")
        try:
            async for msg in pubsub.listen():
                if not self.running:
                    break
                if msg["type"] == "message":
                    parsed = json.loads(msg["data"])
                    if parsed.get("type") == "shutdown":
                        asyncio.create_task(self.node.shutdown())
                        break
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe("runtime:signal")
            await pubsub.close()