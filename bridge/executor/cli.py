# bridge.executor.cli
import asyncio
import uuid
import json
from dataclasses import asdict
from typing import Callable, Any
from bridge.executor.base import BaseExecutor
from bridge.pir import PsiEvent, PsiCarrier
from plane.node.runtime import NodeRuntime
from plane.emitter import get_logger
from model.task.result import TaskSummaryEvent, TaskDetailRecord
from model.event import next_id

log = get_logger("executor.cli")

class _GenericCliExecutor(BaseExecutor):
    """bound(transduction) - external CLI → internal Ψ execution"""
    def __init__(self, task_instance, completion_signal: asyncio.Event):
        super().__init__()
        self.task_instance = task_instance
        self.completion_signal = completion_signal
        self.node = None

    async def execute(self, psi) -> list:
        ## @topos.input: Ψ (incoming command signal)
        command = psi.carrier.tag if hasattr(psi, 'carrier') else "CLI_TASK"
        snowflake_id = next_id()
        task_id = f"task-{snowflake_id}"
        detail_key = f"{command.lower()}:cli:{snowflake_id}"
        latest_pointer_key = f"{command.lower()}:cli:latest"
        self.log.info(f"[exec] Executing CLI task: {command} ({task_id})")
        
        try:
            raw_result = self.task_instance.run() or {}

            if self.node and self.node.redis:
                ## Redis에 상세 기록 (기존 로직)
                await self.node.redis.set(detail_key, detail_record.to_json(), ex=3600)
                
                ## 발화자에게 응답 (Response Channel이 있을 경우)
                response_channel = psi.context.get("response_channel")
                if response_channel:
                    await self.node.redis.publish(response_channel, summary_event.to_json())
                    self.log.info(f"[exec] Reflection published to {response_channel}")

            detail_record = TaskDetailRecord(
                task_id=task_id,
                command=command,
                status=raw_result.get("status", "SUCCESS"),
                artifacts=raw_result.get("artifacts", {}),
                metrics=raw_result.get("metrics", {}),
                details=raw_result.get("details", {})
            )

            if self.node and self.node.redis:
                await self.node.redis.set(detail_key, detail_record.to_json(), ex=3600)
                await self.node.redis.set(latest_pointer_key, detail_key, ex=3600)
                self.log.info(f"[exec] Detailed artifacts saved -> Redis[{detail_key}]")

            summary_event = TaskSummaryEvent(
                task_id=task_id,
                command=command,
                status=detail_record.status,
                summary=raw_result.get("summary", f"Task {command} completed."),
                detail_key=detail_key
            )
            _project_to_stdout(summary_event)
            
            result_carrier = PsiCarrier(
                kind="RESULT", 
                tag=command, 
                payload=asdict(summary_event),
            )
            
            result_event = PsiEvent(
                event_id=f"res-{task_id}",
                parent_id=getattr(psi, 'event_id', None),
                source_id="cli.executor",
                scope="GLOBAL",
                tick=1,
                carrier=result_carrier
            )

            if self.node and getattr(self.node, 'bus', None):
                await self.node.bus.publish(result_event)
        except Exception as e:
            self.log.error(f"[exec] Task Failed: {e}")
        finally:
            self.completion_signal.set()
        return []

def _project_to_stdout(summary_event: TaskSummaryEvent):
    """@topos.bound: Ψ → local surface projection (stdout)"""
    try:
        print(f"\n[{summary_event.status}] {summary_event.command}")
        print(f"{summary_event.summary}")
        print(f"detail_key: {summary_event.detail_key}")

        details = getattr(summary_event, "details", None)
        if isinstance(details, dict):
            print("\n## grouped result")
            for k, v in details.items():
                print(f"\n[{k}] ({len(v)})")
                for item in v[:5]:
                    print(f" - {item.get('namespace')}")

    except Exception:
        pass

class CliTaskAdapter:
    """순수 비즈니스 로직을 _GenericCliExecutor가 요구하는 표준 딕셔너리(status, artifacts, metrics 등)로 변환해주는 범용 어댑터"""
    def __init__(self, target_func: Callable, **kwargs):
        self.target_func = target_func
        self.kwargs = kwargs

    def run(self) -> dict:
        try:
            raw_result = self.target_func(**self.kwargs)
            if isinstance(raw_result, dict) and "status" in raw_result:
                return raw_result
                
            return {
                "status": "SUCCESS",
                "summary": "Task completed successfully.",
                "artifacts": [],
                "details": raw_result
            }
        except Exception as e:
            return {
                "status": "FAILED",
                "summary": f"Task failed: {str(e)}",
                "details": {"error": str(e)}
            }

async def _async_run_in_node(task_instance, command_name: str, payload: dict):
    ## 1. Redis 사전 연결 및 스웜 감지
    from plane.node.sensor import REDIS_URL
    import redis.asyncio as redis_async
    r = redis_async.from_url(REDIS_URL, decode_responses=True)
    
    ## 하트비트가 있는 노드가 하나라도 있는지 확인
    # active_node_keys = await r.keys("runtime:heartbeat:*")
    active_node_keys = await r.keys("runtime:active")
    
    if active_node_keys:
        ## [CASE A] Proxy 모드: 사건 발화 후 결과 구독
        task_id = f"task-{uuid.uuid4().hex[:8]}"
        response_channel = f"res:{task_id}"
        
        ## 사건 생성 (글로벌 큐 주입용)
        trigger_event = PsiEvent(
            event_id=task_id,
            source_id=f"{command_name}:proxy",
            scope="GLOBAL",
            carrier=PsiCarrier(kind="COMMAND", tag=command_name.lower(), payload=payload),
            context={"response_channel": response_channel} # 응답 경로 주입
        )
        
        ## 큐에 주입 (누가 잡을지 모름)
        await r.lpush("runtime:queue", trigger_event.to_json())
        
        ## 결과 대기 (PubSub)
        pubsub = r.pubsub()
        await pubsub.subscribe(response_channel)
        log.info(f"[CLI] Event emitted to swarm. Waiting for response on {response_channel}...")
        
        try:
            async for msg in pubsub.listen():
                if msg["type"] == "message":
                    # 결과 출력 후 종료
                    # (간소화를 위해 TaskSummaryEvent 복원 로직 필요)
                    print(f"\n[Swarm Result] Received from captured node.")
                    print(msg["data"]) 
                    break
        finally:
            await pubsub.unsubscribe(response_channel)
            await r.close()
        return

    ## [CASE B] Mutation 모드: 스스로 노드가 되어 상주 시작
    log.info("[CLI] No active nodes. Mutating into Ambient Node...")
    completion_signal = asyncio.Event()
    executor = _GenericCliExecutor(task_instance, completion_signal)
    node = NodeRuntime(executor=executor)
    executor.node = node
    
    node_task = asyncio.create_task(node.start())
    await asyncio.sleep(0.1) 

    ## 최초 작업 트리거 (자신의 큐가 아닌 글로벌 큐로 던져서 스스로 낚아채게 함)
    trigger_event = PsiEvent(
        event_id=f"{command_name}-init",
        source_id=f"{command_name}:initial",
        parent_id=None,
        tick=1,
        scope="GLOBAL",
        carrier=PsiCarrier(kind="COMMAND", tag=command_name.lower(), payload=payload)
    )
    await r.lpush("runtime:queue", trigger_event.to_json())
    await r.close()

    ## 노드가 30초 유휴 후 종료될 때까지 대기
    await node_task

# async def _async_run_in_node(task_instance, command_name: str, payload: dict):
#     completion_signal = asyncio.Event()
    
#     executor = _GenericCliExecutor(task_instance, completion_signal)
#     node = NodeRuntime(executor=executor)
#     executor.node = node
    
#     node_task = asyncio.create_task(node.start())
#     await asyncio.sleep(0.1) # Boot buffer

#     trigger_event = PsiEvent(
#         event_id=f"{command_name}-cli",
#         parent_id=None,
#         source_id=f"{command_name}:cli",
#         scope="LOCAL",
#         tick=1,
#         carrier=PsiCarrier(kind="COMMAND", tag=command_name.lower(), payload=payload),
#         context={"phase": "ex.cli"}
#     )
    
#     await getattr(node, 'bus').publish(trigger_event)
#     await completion_signal.wait()
#     if hasattr(node, 'shutdown'):
#         await node.shutdown()

def execute_cli_task(task_instance, command_name: str = "run", payload: dict = None):
    """@topos.entry: external trigger → Ψ injection"""
    payload = payload or {}
    try:
        asyncio.run(_async_run_in_node(task_instance, command_name, payload))
    except KeyboardInterrupt:
        log.info("[CLI] Task interrupted by user.")