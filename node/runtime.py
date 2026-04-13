# node.runtime
import asyncio
import signal
import time
import uuid
import uvloop
import json
import redis.asyncio as redis_async
from bridge.pir import PsiEvent, PsiCarrier
from bridge.interpreter import PhaseInterpreter, AnchorFlow
from bridge.bus import AsyncEventBus
from topos.bound import IPhaseAtor, IPhaseField
from bound.sink import RedisSink
from bound.emitter import get_emitter
from node.sensor import sense_once, REDIS_URL
from node.dispatcher import Dispatcher
from surface.actuator import SurfaceActuator
from anchor.resolver import resolve_path, find_current_self
from contract.registry import discover_modules, registry
from contract.executor.swarm import SwarmCliExecutor

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
SENSOR_INTERVAL = 1.0

class NodeRuntime(IPhaseAtor):
    """
    @runtime.node: closed-loop control node
    @flow: ψ → queue → dispatch → Φ → actuator → surface
    """
    def __init__(self, redis_url=REDIS_URL, executor=None, idle_timeout=30):
        self._id = f"node-{uuid.uuid4().hex[:8]}" 
        self.node_id = self._id
        self.redis_url = redis_url
        self.redis = None
        self.executor = executor

        self.idle_timeout = idle_timeout
        self.last_active_time = time.time()
        
        self.running = True
        self.loop_tasks = []
        
        self.bus = AsyncEventBus()
        self._dispatch_queue = asyncio.Queue() 
        self.psi_queue = self._dispatch_queue
        self.log = get_emitter("node.runtime", phase="SYSTEM")

        self.interpreter = None
        self.dispatcher = None
        self.actuator = None
        
        self.bus.subscribe(self)
        self.local_manifold = registry.registered_nodes
        self.log.info(f"discovered {len(self.local_manifold)} local phasenodes.")

    def _handle_exception(self, loop, context):
        """@safety: 루프 전체에서 발생하는 잡히지 않은 예외 포착"""
        msg = context.get("exception", context["message"])
        self.log.crit(f"Unhandled exception in event loop: {msg}")
        ## asyncio.create_task(self.shutdown())

    def _on_task_done(self, task):
        """@monitor: 개별 루프 태스크가 종료되었을 때 원인 분석"""
        try:
            task.result()
        except asyncio.CancelledError:
            pass  ## 정상 종료
        except Exception as e:
            self.log.crit(f"Loop Task [{task.get_name()}] died with error: {e}")
            ## 루프 중 하나가 죽으면 전체 노드의 신뢰성이 깨지므로 shutdown 고려
            asyncio.create_task(self.shutdown())

    @property
    def ator_id(self) -> str:
        return self._id

    @property
    def state(self) -> str:
        return "RUNNING" if self.running else "STOPPED"

    def set_state(self, new_state: str) -> None:
        """IPhaseAtor 인터페이스 구현용 메서드"""
        if new_state == "STOPPED" and self.running:
            # 외부(예: Regime)에서 노드를 강제로 멈추라는 상태 변경이 들어올 경우
            self.running = False
        elif new_state == "RUNNING" and not self.running:
            self.running = True
        else:
            self.log.info(f"Node state requested to change to: {new_state}")

    # IPhaseAtor 구현체 (Event Bus에서 메시지를 수신하는 콜백)
    async def react(self, event: PsiEvent, field: IPhaseField, bus: AsyncEventBus):
        """@bus.subscriber: 버스에서 이벤트를 수신하여 디스패처 큐로 전달"""
        if self.running:
            await self._dispatch_queue.put(event)

    def _create_phase_handler(self):
        """@handler: ψ → interpreter → Φ(state projection)"""
        # 이 내부 함수가 나중에 dispatcher에서 호출(call)될 대상입니다.
        def handler(psi: PsiEvent):
            self.interpreter.process(psi.carrier)
            return {
                "psi": psi.symbol,
                "phase": self.interpreter.phase,
                "version": self.interpreter.anchor.version,
                "node_id": self.node_id
            }
        return handler

    async def start(self):
        """@runtime.bootstrap: discovery -> register -> boot -> assemble -> loop"""
        ## 전역 예외 핸들러 등록
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(self._handle_exception)
        self.log.info(f"Starting RuntimeNode [{self.node_id}]")

        self.redis = redis_async.from_url(self.redis_url, decode_responses=True)
        if self.executor:
            self.executor.node = self
            self.log.info(f"Executor bound to Node [{self.node_id}]")

        ## @discovery: local manifold(structure -> registry)
        discover_modules(find_current_self())
        self.local_manifold = registry.registered_nodes
        self.log.info(f"discovered {len(self.local_manifold)} local phasenodes.")

        ## @binding: node -> global index (capability surface)
        await self.register_node()

        all_recepts = set()
        for meta in self.local_manifold.values():
            all_recepts.update(getattr(meta.contract, "recept", []))

        for task_list in registry.registered_cli_tasks.values():
            for task in task_list:
                recepts = task.get("recept", [])
                if isinstance(recepts, (list, set, frozenset)):
                    all_recepts.update(recepts)

        ## 최소 방어막 설정
        if not all_recepts:
            all_recepts = {"system:signal", "system:ping"}

        ## @boot: anchor -> interpreter (Φ 기준점 생성)
        anchor = AnchorFlow.bootstrap(frozenset(all_recepts))
        self.interpreter = PhaseInterpreter(anchor)
        self.log.info(f"Boot phase: {self.interpreter.phase}, version: {anchor.version}, boundaries: {len(anchor.recept_boundaries)}")
        for bound in anchor.recept_boundaries:
            self.log.info(f"bound; {bound}")

        ## @assembly: dispatcher + actuator 결합 (ψ 처리 경로 형성)
        self.actuator = SurfaceActuator(RedisSink())
        self.dispatcher = Dispatcher(
            handler=self._create_phase_handler(),
            executor=self.executor,
            actuator=self.actuator,
        )
        await self.dispatcher.start()

        ## @loop.spawn: observer / scheduler / control loop 병렬화
        self.loop_tasks = [
            asyncio.create_task(self.sensor_loop(), name="sensor"),
            asyncio.create_task(self.dispatch_loop(), name="dispatch"),
            asyncio.create_task(self.heartbeat_loop(), name="heartbeat"),
            asyncio.create_task(self.signal_loop(), name="signal"), 
        ]
        for task in self.loop_tasks:
            task.add_done_callback(self._on_task_done)

        await asyncio.gather(*self.loop_tasks)

    async def shutdown(self):
        if not self.running:
            return

        self.log.warn("Shutdown sequence initiated...")
        self.running = False

        await self.deregister_node()
        # _dispatch_queue 종료 시그널 주입
        await self._dispatch_queue.put(None) 

        for lt in self.loop_tasks:
            if not lt.done():
                lt.cancel()
        
        await asyncio.gather(*self.loop_tasks, return_exceptions=True)

        if self.dispatcher:
            await self.dispatcher.stop()
        if self.actuator:
            await self.actuator.close()
        if self.redis:
            await self.redis.close()
            
        self.log.info("Teardown complete.")

    async def sensor_loop(self):
        """@psi.observe: surface → ψ (state → bus 발행)"""
        self.log.info("Sensor loop started. Observing state space.")
        while self.running:
            try:
                signals = await sense_once(self.redis)
                for psi in signals:
                    # Queue 대신 Bus로 발행
                    await self.bus.publish(psi) 
                    
                await asyncio.sleep(SENSOR_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"I/O or Sensor Error: {e}")
                await asyncio.sleep(2)

    async def dispatch_loop(self):
        """기존: 로컬 큐 감시 -> 수정: Redis 글로벌 큐에서 BRPOP (Capture)"""
        self.log.info(f"Capture loop started (Idle Timeout: {self.idle_timeout}s)")

        while self.running:
            try:
                # 1. 글로벌 큐에서 사건 낚아채기 (경쟁 지점)
                # 0.5초 단위로 끊어서 running 상태 확인 및 timeout 계산
                res = await self.redis.brpop("runtime:queue", timeout=1.0)
                
                if res:
                    _, data = res
                    event_dict = json.loads(data)
                    if 'carrier' in event_dict and isinstance(event_dict['carrier'], dict):
                        from bridge.pir import PsiCarrier # 필요시 임포트
                        event_dict['carrier'] = PsiCarrier(**event_dict['carrier'])
                    
                    psi = PsiEvent(**event_dict)
                    print(f"DEBUG: psi has carrier? {hasattr(psi, 'carrier')}")
                    print(f"DEBUG: carrier has target_field? {hasattr(psi.carrier, 'target_field')}")
                    self.log.info(f"Captured Event Context: {psi.context}")
                    self.last_active_time = time.time() # 활동 시간 갱신
                    
                    # 2. 위상 정합성 확인 (Boundary Gate)
                    # 여기서는 간단히 dispatcher로 전달
                    await self.dispatcher.send(psi)
                else:
                    # 3. 유휴 시간 체크 (Decay)
                    if time.time() - self.last_active_time > self.idle_timeout:
                        self.log.warn(f"Idle for {self.idle_timeout}s. Self-evaporating...")
                        asyncio.create_task(self.shutdown())
                        break
            except Exception as e:
                self.log.error(f"Capture Error: {e}")
                await asyncio.sleep(1)    

    async def _recover_from_panic(self, toxic_psi: PsiCarrier, error: Exception):
        """
        @recovery
        @flow: ψ(anomaly) → quarantine → reset(anchor) → rebind
        """
        self.log.crit(f"Critical anomaly from Ψ({toxic_psi.symbol}). Reason: {error}")

        ## Quarantine
        self.log.warn(f"Quarantining toxic signal: {toxic_psi.tag}")
        toxic_psi.kind = f"{toxic_psi.kind}:quarantined" 
        if self.actuator:
            await self.actuator.actuate_psi(toxic_psi)

        ## Reset & Rebind
        stable_anchor = AnchorFlow.bootstrap()
        self.interpreter = PhaseInterpreter(stable_anchor)
        if self.dispatcher:
            self.dispatcher.handler = self._create_phase_handler()

        self.log.signal(f"System restored to Phase: {self.interpreter.phase}")

    async def register_node(self):
        """@registry.emit: capability → surface index (discoverability 확보)"""
        
        ## Contract 매니페스트 생성
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
            "capabilities": json.dumps(capabilities)  # 노드의 능력을 JSON으로 직렬화
        }

        if self.executor:
            data["executor_type"] = type(self.executor).__name__

        ## step.1: 노드 메타데이터 등록 (Hash)
        await self.redis.hset(
            f"runtime:node:{self.node_id}", 
            mapping=data
        )

        ## step.2: 글로벌 역색인(Reverse Index) 구축 (Set)
        ## 특정 surface key를 요구(requires)하는 노드가 누구인지 빠르게 찾기 위함
        for fqn, cap in capabilities.items():
            for req_key in cap["requires"]:
                await self.redis.sadd(f"runtime:index:requires:{req_key}", self.node_id)
            
            for emit_key in cap["emits"]:
                await self.redis.sadd(f"runtime:index:emits:{emit_key}", self.node_id)



        self.log.signal(f"Node registered with {len(capabilities)} capabilities.")

    async def deregister_node(self):
        """@registry.cleanup: node → index 제거 (topos clean)"""
        
        ## step.1: 노드 메타데이터 삭제
        await self.redis.delete(f"runtime:node:{self.node_id}")

        ## step.2: 글로벌 역색인에서 현재 node_id 제거
        for fqn, meta in self.local_manifold.items():
            contract = meta.contract
            for req_key in contract.requires:
                await self.redis.srem(f"runtime:index:requires:{req_key}", self.node_id)
            for emit_key in contract.emits:
                await self.redis.srem(f"runtime:index:emits:{emit_key}", self.node_id)
                
        self.log.info("Node and capability indexes deregistered.")

    async def heartbeat_loop(self):
        """@phase.liveness: node → heartbeat (temporal presence 유지)"""

        try:
            while self.running:
                await self.redis.set(f"runtime:heartbeat:{self.node_id}", int(time.time()), ex=10)
                await self.redis.set(f"runtime:active", int(time.time()), ex=10)
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            pass

    async def signal_loop(self):
        """@control.inbound: external signal → runtime control (e.g. shutdown)"""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("runtime:signal")
        try:
            async for msg in pubsub.listen():
                if not self.running:
                    break
                if msg["type"] == "message":
                    data = msg["data"]  # decode_responses=True 덕분에 문자열
                    parsed = json.loads(data)
                    if parsed.get("type") == "shutdown":
                        asyncio.create_task(self.shutdown())
                        break
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe("runtime:signal")
            await pubsub.close()

def install_os_signal(node: NodeRuntime):
    """@binding.os: OS signal → runtime shutdown ψ로 변환"""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(node.shutdown()))
        except NotImplementedError:
            pass

async def main_async():
    """@phase.entry: runtime node 실행 경계"""
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