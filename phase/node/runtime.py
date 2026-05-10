# phase.node.runtime
import asyncio
import signal
import time
import json
import uvloop
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import redis.asyncio as redis_async
from arch.contract.event.psi import PsiEvent, PsiCarrier
from arch.contract.event.bus import AsyncEventBus
from arch.contract.event.next import next_id
from arch.contract.interface import IPhaseAtor, IPhaseField
from topos.plane.emitter import get_emitter
from phase.node.sensor import sense_once, REDIS_URL
from phase.node.dispatcher import Dispatcher
from phase.node.interpreter import NodeInterpreter, AnchorFlow
from phase.node.surface.actuator import SurfaceActuator
from arch.proto.surface.sink import RedisSink
from arch.bound.resolver import resolve_path, find_current_self
from arch.contract.registry import registry
from arch.contract.discover import discover_modules
from phase.node.executor.swarm import SwarmCliExecutor
from phase.node.daemon import SensorDaemon, CaptureDaemon, HeartbeatDaemon, SignalDaemon
from phase.cognitive.coupler import CognitiveCoupler
from phase.cognitive.worker import CognitiveWorker
from phase.node.state.aggregator import KernelStateAggregator
from phase.reflect.client.local.engine import LLMEngine
from arch.model.context.assembler import ContextAssembler

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

class NodeRuntime(IPhaseAtor):
    """
    @runtime.node: closed-loop control manifold
    @flow: ψ → global queue → dispatch → Φ(Interpreter) → Coupler → Worker
    """
    def __init__(self, redis_url=REDIS_URL, executor=None, idle_timeout=353):
        self._id = f"node-{next_id()}" 
        self.node_id = self._id
        self.redis_url = redis_url
        self.redis = None
        self.executor = executor
        self.idle_timeout = idle_timeout
        
        self.running = True
        # [수정 3] NameError 방지를 위해 추상화된 타입으로 변경
        self.daemons: List[Any] = [] 
        
        self.bus = AsyncEventBus()
        self.log = get_emitter("node.runtime", phase="SYSTEM")

        self.interpreter = None
        self.dispatcher = None
        self.actuator = None
        self.coupler = None  # 교량(Coupler) 전역 상태 추가
        
        self.bus.subscribe(self)
        self.local_manifold = {}

    def _handle_exception(self, loop, context):
        msg = context.get("exception", context["message"])
        self.log.crit(f"Unhandled exception in event loop: {msg}")

    def _on_task_done(self, task: asyncio.Task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass  
        except Exception as e:
            self.log.crit(f"Loop Task [{task.get_name()}] died with error: {e}")
            asyncio.create_task(self.shutdown())

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> str:
        return "RUNNING" if self.running else "STOPPED"

    def set_state(self, new_state: str) -> None:
        if new_state == "STOPPED" and self.running:
            self.running = False
        elif new_state == "RUNNING" and not self.running:
            self.running = True

    async def react(self, event: PsiEvent, field: IPhaseField, bus: AsyncEventBus):
        if self.running and self.redis:
            try:
                await self.redis.lpush("runtime:queue", event.to_json())
                self.log.trace(f"Event {event.symbol} escalated to global redis queue.")
            except Exception as e:
                self.log.error(f"Failed to push event to Redis queue: {e}")

    def _create_phase_handler(self, coupler: CognitiveCoupler):
        def handler(psi: PsiEvent):
            ## 반사계의 판단 (Sync)
            judgment = self.interpreter.process(psi.carrier)
            
            ## 교량으로 이관 (Fire and Forget)
            coupler.ingest(psi.carrier, judgment)
            return {
                "psi": judgment.psi_symbol,
                "action": judgment.action.value,
                "phase": judgment.phase,
                "version": judgment.version,
                "resonance": judgment.is_resonance, 
                "node_id": self.node_id
            }
        return handler

    async def start(self):
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(self._handle_exception)
        self.log.info(f"Starting RuntimeNode [{self.node_id}]")

        self.redis = redis_async.from_url(self.redis_url, decode_responses=True)
        if self.executor:
            self.executor.node = self

        discover_modules(find_current_self())
        self.local_manifold = registry.registered_nodes
        self.log.info(f"discovered {len(self.local_manifold)} local phasenodes.")

        await self.register_node()

        all_recepts = set()
        for meta in self.local_manifold.values():
            all_recepts.update(getattr(meta.contract, "recept", []))

        if not all_recepts:
            all_recepts = {"system:signal", "system:ping"}

        ## 척수(Interpreter) 구성
        anchor = AnchorFlow.bootstrap(frozenset(all_recepts))
        self.interpreter = NodeInterpreter(anchor)
        self.log.info(f"Boot phase: {self.interpreter.phase}, boundaries: {len(anchor.recept_boundaries)}")

        ## 인지 코어(Worker) 및 교량(Coupler) 조립
        try:
            engine = LLMEngine()
            assembler = ContextAssembler()
            worker = CognitiveWorker(engine, assembler)
            aggregator = KernelStateAggregator(self.interpreter, self.redis)
            
            self.coupler = CognitiveCoupler(aggregator, worker)
            await self.coupler.start()
            self.log.info("Cognitive Coupler successfully attached.")
        except Exception as e:
            self.log.error(f"Failed to attach Cognitive Coupler: {e}")
            raise

        self.actuator = SurfaceActuator(RedisSink())
        self.dispatcher = Dispatcher(
            handler=self._create_phase_handler(self.coupler),
            executor=self.executor,
            actuator=self.actuator,
        )
        await self.dispatcher.start()
        self.daemons = [
            SensorDaemon(self.redis, self.bus),
            CaptureDaemon(self.redis, self.dispatcher, self, self.idle_timeout),
            HeartbeatDaemon(self.redis, self.node_id),
            SignalDaemon(self.redis, self)
        ]

        tasks = []
        for daemon in self.daemons:
            task = await daemon.start()
            task.add_done_callback(self._on_task_done)
            tasks.append(task)

        await asyncio.gather(*tasks)

    async def shutdown(self):
        if not self.running: return

        self.log.warn("Shutdown sequence initiated...")
        self.running = False
        await self.deregister_node()

        ## 모든 데몬 종료 하달
        stop_tasks = [daemon.stop() for daemon in self.daemons]
        await asyncio.gather(*stop_tasks, return_exceptions=True)

        ## 내부 컴포넌트 해제
        if self.coupler: await self.coupler.stop()
        if self.dispatcher: await self.dispatcher.stop()
        if self.actuator: await self.actuator.close()
        if self.redis: await self.redis.close()
        self.log.info("Teardown complete.")

    async def _recover_from_panic(self, toxic_psi: PsiCarrier, error: Exception):
        self.log.crit(f"Critical anomaly from Ψ({toxic_psi.symbol}). Reason: {error}")
        toxic_psi.kind = f"{toxic_psi.kind}:quarantined" 
        if self.actuator:
            await self.actuator.actuate_psi(toxic_psi)

        current_boundaries = getattr(self.interpreter.anchor, 'recept_boundaries', None)
        stable_anchor = AnchorFlow.bootstrap(current_boundaries)
        
        self.interpreter = NodeInterpreter(stable_anchor)
        if self.dispatcher:
            ## 패닉 복구 시에도 잃어버리지 않도록 교량을 다시 연결
            self.dispatcher.handler = self._create_phase_handler(self.coupler) 
        self.log.signal(f"System restored to Phase: {self.interpreter.phase}")

    async def register_node(self):
        capabilities = {}
        for fqn, meta in self.local_manifold.items():
            contract = meta.contract
            capabilities[fqn] = {
                "requires": list(contract.requires),
                "emits": list(contract.emits)
            }
        data = {
            "node_id": self.node_id,
            "started_at": time.time(),
            "capabilities": json.dumps(capabilities)
        }
        if self.executor:
            data["executor_type"] = type(self.executor).__name__

        await self.redis.hset(f"runtime:node:{self.node_id}", mapping=data)
        for fqn, cap in capabilities.items():
            for req_key in cap["requires"]:
                await self.redis.sadd(f"runtime:index:requires:{req_key}", self.node_id)
            for emit_key in cap["emits"]:
                await self.redis.sadd(f"runtime:index:emits:{emit_key}", self.node_id)
        self.log.signal(f"Node registered with {len(capabilities)} capabilities.")

    async def deregister_node(self):
        if not self.redis: return
        await self.redis.delete(f"runtime:node:{self.node_id}")
        for fqn, meta in self.local_manifold.items():
            contract = meta.contract
            for req_key in contract.requires:
                await self.redis.srem(f"runtime:index:requires:{req_key}", self.node_id)
            for emit_key in contract.emits:
                await self.redis.srem(f"runtime:index:emits:{emit_key}", self.node_id)
        self.log.info("Node and capability indexes deregistered.")

def install_os_signal(node: NodeRuntime):
    """[Phase 3] OS 바인딩 및 부트스트랩"""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(node.shutdown()))
        except NotImplementedError:
            pass

async def main_async():
    completion_signal = asyncio.Event()
    executor = SwarmCliExecutor(completion_signal)
    node = NodeRuntime(executor=executor)
    install_os_signal(node)
    
    try:
        await node.start()
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass