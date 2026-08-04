# kernel.phase.runtime.node
import asyncio
import signal
import time
import json
import uvloop
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

from arch.topos.tunnel.factory import UniversalFacade, TunnelFactory
from arch.contract.event.psi import PsiEvent, PsiCarrier
from arch.contract.event.tunnelbus import TunnelEventBus
from arch.contract.event.next import next_id
from arch.contract.interface import IPhaseAtor, IPhaseField, IEventBus
from arch.contract.registry.unified import registry
from arch.contract.discovery import discover_modules

from kernel.bind.resolver import find_current_self
from kernel.phase.runtime.executor.swarm import SwarmExecutor

from kernel.phase.runtime.sensor import SurfaceSensor, SurfaceActuator
from kernel.phase.daemon.task.supervisor import TaskSupervisor, Dispatcher
from kernel.bind.inter.node import NodeInterpreter, AnchorFlow
from kernel.phase.runtime.context import RuntimeContext
from kernel.phase.daemon.bootstrap import mount_core_layer, mount_app_layer

# [정렬] WASM 샌드박스로 Intent를 전송할 비동기 브로커 추가
from kernel.dphi.broker import WasmBroker

from watcher.plane.sink import TunnelSink
from watcher.plane.emitter import get_emitter

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

RUNTIME_KEY = {
    "queue": "runtime:queue",
    "active": "runtime:active",
    "signal": "runtime:signal",
    "receptor_leader": "runtime:receptor:leader",
    "heartbeat_pattern": "runtime:heartbeat:*",
    "node": "runtime:node:{node_id}",
    "index_requires": "runtime:index:requires:{req_key}",
    "index_emits": "runtime:index:emits:{emit_key}",
    "heartbeat": "runtime:heartbeat:{node_id}"
}

class RuntimeKeyResolver:
    """런타임 시스템에서 사용하는 Redis 키 스키마 해결기"""
    def __init__(self, templates: Optional[Dict[str, str]] = None):
        self.templates = templates or RUNTIME_KEY.copy()

    def get(self, template_key: str, **kwargs) -> str:
        """키 템플릿을 찾아 kwargs를 매핑하여 반환합니다."""
        template = self.templates.get(template_key)
        if not template:
            raise KeyError(f"Unrecognized runtime key template: '{template_key}'")
        
        return template.format(**kwargs)

runtime_keys = RuntimeKeyResolver()

class NodeRuntime(IPhaseAtor):
    """
    @runtime.node: closed-loop control manifold
    @flow: ψ(EventBus) → global queue → CaptureDaemon → Dispatcher → Φ(Interpreter/Plugins)
    """
    def __init__(self, executor=None, idle_timeout=353):
        self._id = f"node-{next_id()}" 
        self.node_id = self._id
        
        self.tunnel: Optional[UniversalFacade] = None
        self.executor = executor
        self.idle_timeout = idle_timeout
        
        self.running = True
        self.supervisor = TaskSupervisor(source=f"NodeRuntime-{self._id}")
        self.supervisor.add_error_handler(self._global_task_error)

        self.bus: Optional[TunnelEventBus] = None
        self.log = get_emitter("node.runtime", phase="SYSTEM")
        
        self.broker: Optional[WasmBroker] = None  # [정렬] IPC 통신을 담당할 Broker 상태 공간 확보
        self.interpreter = None
        
        self.dispatcher = None
        self.sensor = None
        self.actuator = None
        self.ctx: Optional[RuntimeContext] = None
        
        self._stop_event = asyncio.Event()

    def _global_task_error(self, task: asyncio.Task[Any], exc: BaseException) -> None:
        """Global handler invoked when an exception occurs in a task managed by the supervisor"""
        self.log.crit(f"⚠️ Critical fault in Task [{task.get_name()}]: {exc}")
        asyncio.create_task(self.shutdown())

    def _handle_exception(self, loop, context):
        msg = context.get("exception", context["message"])
        self.log.crit(f"Unhandled exception in event loop: {msg}")

    @property
    def local_manifold(self):
        return registry.registered_nodes

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

    async def react(self, event: PsiEvent, field: IPhaseField, bus: IEventBus):
        """@desc: EventBusDaemon이 로컬 전파(Dispatch) 시 호출하는 리액션. 큐로 밀어넣어 처리를 오프로드합니다."""
        if self.running and self.tunnel:
            async def push_task():
                try:
                    await self.tunnel.lpush(runtime_keys.get("queue"), event.to_json())
                except Exception as e:
                    self.log.error(f"Failed to push event to local queue: {e}")
            
            self.supervisor.create(push_task(), name=f"Escalate-{event.symbol}")

    def _create_phase_handler(self):
        """
        [정렬 완료] 
        1. executor 실행을 제거하여 Dispatcher 파이프라인과 중복 방지
        2. WASM Broker를 통한 비동기 상태 판단에 순응하기 위해 async 핸들러로 전환
        """
        async def handler(psi: PsiEvent):
            ## Judgment by the reflex system (I/O-Bound Async Task via WASM Broker)
            judgment = await self.interpreter.process(psi.carrier)

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
        
        # 1. 인프라 바인딩
        self.tunnel = await TunnelFactory.get_default()
        
        self.bus = TunnelEventBus(self.tunnel)
        self.bus.subscribe(self, predicate=lambda e: True) # 로컬 반응계(Ator) 등록
        
        if self.executor:
            self.executor.node = self

        watch_dir = find_current_self()
        discover_modules(watch_dir)
        self.log.info(f"discovered {len(self.local_manifold)} local phasenodes.")
        await self.register_node()

        all_recepts = set()
        for meta in self.local_manifold.values():
            all_recepts.update(getattr(meta.contract, "recept", []))

        if not all_recepts:
            all_recepts = {"system:signal", "system:ping"}

        anchor = AnchorFlow.bootstrap(frozenset(all_recepts))
        
        # [정렬 완료] WasmBroker 인스턴스화 및 NodeInterpreter에 주입 (명시적 키워드 인자 사용)
        self.broker = WasmBroker()
        self.interpreter = NodeInterpreter(broker=self.broker, anchor=anchor)
        
        self.log.info(f"Boot phase: {self.interpreter.phase}, boundaries: {len(anchor.recept_boundaries)}")

        self.sensor = SurfaceSensor(tunnel=self.tunnel)
        self.actuator = SurfaceActuator(TunnelSink(tunnel=self.tunnel))
        
        # 2. 엔진(Dispatcher) 초기화
        self.dispatcher = Dispatcher(
            supervisor=self.supervisor,
            default_handler=self._create_phase_handler(),
            executor=self.executor,
            actuator=self.actuator,
        )
        
        # 3. [핵심 개선] 의존성 주입(DI) 컨텍스트 생성
        self.ctx = RuntimeContext(
            node_id=self.node_id,
            tunnel=self.tunnel,
            bus=self.bus,
            dispatcher=self.dispatcher,
            sensor=self.sensor,
            actuator=self.actuator,
            idle_timeout=self.idle_timeout,
            watch_dir=watch_dir,
            shutdown_hook=self.shutdown # NodeRuntime의 shutdown을 데몬들이 호출할 수 있도록 위임
        )

        # 4. [핵심 개선] 계층화된 데몬 장착 (NodeRuntime은 데몬 목록 및 생명주기를 모름)
        mount_core_layer(self.supervisor, self.ctx)
        self.log.info("Core Infra Layer mounted successfully.")

        mount_app_layer(self.supervisor, self.ctx)
        self.log.info("App Plugin Layer mounted successfully.")
        self.log.info("All components orchestrated under TaskSupervisor.")

    async def shutdown(self):
        if not self.running: return

        self.log.warn("Shutdown sequence initiated...")
        self.running = False

        await self.deregister_node()
        await self.supervisor.shutdown()

        ## @release: internal core components
        if self.actuator: await self.actuator.close()
        if self.tunnel: await self.tunnel.close()

        self.log.info("Teardown complete.")
        self._stop_event.set()

    async def wait_until_stopped(self):
        await self._stop_event.wait()

    async def _recover_from_panic(self, toxic_psi: PsiCarrier, error: Exception):
        self.log.crit(f"Critical anomaly from Ψ({toxic_psi.symbol}). Reason: {error}")
        toxic_psi.kind = f"{toxic_psi.kind}:quarantined" 
        if self.actuator:
            await self.actuator.actuate_psi(toxic_psi)

        current_boundaries = getattr(self.interpreter.anchor, 'recept_boundaries', None)
        stable_anchor = AnchorFlow.bootstrap(current_boundaries)
        
        # [정렬 완료] 패닉 복구 시에도 동일하게 WasmBroker 참조를 주입
        self.interpreter = NodeInterpreter(broker=self.broker, anchor=stable_anchor)
        
        # [개선] Dispatcher의 핸들러 속성 업데이트 (새로운 구조 반영)
        if self.dispatcher:
            self.dispatcher.default_handler = self._create_phase_handler() 
            
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

        node_key = runtime_keys.get("node", node_id=self.node_id)
        await self.tunnel.hset(node_key, mapping=data)
        
        for fqn, cap in capabilities.items():
            for req_key in cap["requires"]:
                req_idx_key = runtime_keys.get("index_requires", req_key=req_key)
                await self.tunnel.sadd(req_idx_key, self.node_id)
            for emit_key in cap["emits"]:
                emit_idx_key = runtime_keys.get("index_emits", emit_key=emit_key)
                await self.tunnel.sadd(emit_idx_key, self.node_id)
                
        self.log.signal(f"Node registered with {len(capabilities)} capabilities.")

    async def deregister_node(self):
        if not self.tunnel: return
        
        node_key = runtime_keys.get("node", node_id=self.node_id)
        await self.tunnel.delete(node_key)
        
        for fqn, meta in self.local_manifold.items():
            contract = meta.contract
            for req_key in contract.requires:
                req_idx_key = runtime_keys.get("index_requires", req_key=req_key)
                await self.tunnel.srem(req_idx_key, self.node_id)
            for emit_key in contract.emits:
                emit_idx_key = runtime_keys.get("index_emits", emit_key=emit_key)
                await self.tunnel.srem(emit_idx_key, self.node_id)
                
        self.log.info("Node and capability indexes deregistered.")

def install_os_signal(node: NodeRuntime):
    """@phase: OS binding and bootstrap"""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(node.shutdown()))
        except NotImplementedError:
            pass

async def main_async():
    completion_signal = asyncio.Event()
    executor = SwarmExecutor(completion_signal)
    node = NodeRuntime(executor=executor)
    install_os_signal(node) 
    
    try:
        await node.start()
        await node.wait_until_stopped()
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass