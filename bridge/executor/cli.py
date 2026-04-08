# bridge.executor.cli
import asyncio
import uuid
from dataclasses import asdict
from typing import Callable, Any
from bridge.executor.base import BaseExecutor
from anchor.interface.pir import PsiEvent, PsiCarrier
from plane.node.runtime import NodeRuntime
from plane.emitter import get_logger
from anchor.model.result import TaskSummaryEvent, TaskDetailRecord
from anchor.interface.event import next_id

log = get_logger("executor.cli")

class _GenericCliExecutor(BaseExecutor):
    """bound(transduction) - external CLI → internal Ψ execution"""
    def __init__(self, task_instance, completion_signal: asyncio.Event):
        super().__init__()
        self.task_instance = task_instance
        self.completion_signal = completion_signal
        self.node = None  # NodeRuntime 인스턴스 지연 바인딩용

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
            ## @topos.materialize: Φ → artifact (structured residue)
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

# def _project_to_stdout(summary_event: TaskSummaryEvent):
#     """@topos.boundary: Ψ → local surface projection (stdout)"""
#     try:
#         print(f"[{summary_event.status}] {summary_event.command}")
#         print(f"- {summary_event.summary}")
#         print(f"- detail_key: {summary_event.detail_key}")
#     except Exception:
#         pass

# def _project_to_stdout(summary_event: TaskSummaryEvent):
#     """@topos.boundary: Ψ → local surface projection (stdout)"""
#     try:
#         print(f"\n[{summary_event.status}] {summary_event.command}")
#         print(f"{summary_event.summary}")
#         print(f"detail_key: {summary_event.detail_key}")
#         details = summary_event.summary  # 기본 fallback
#         payload = getattr(summary_event, "summary", None)
#     except Exception:
#         pass

def _project_to_stdout(summary_event: TaskSummaryEvent):
    """@topos.boundary: Ψ → local surface projection (stdout)"""
    try:
        print(f"\n[{summary_event.status}] {summary_event.command}")
        print(f"{summary_event.summary}")
        print(f"detail_key: {summary_event.detail_key}")

        details = getattr(summary_event, "details", None)
        if isinstance(details, dict):
            print("\n--- grouped result ---")
            for k, v in details.items():
                print(f"\n[{k}] ({len(v)})")
                for item in v[:5]:  # 과도 출력 방지
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
    completion_signal = asyncio.Event()
    
    executor = _GenericCliExecutor(task_instance, completion_signal)
    node = NodeRuntime(executor=executor)
    executor.node = node
    
    node_task = asyncio.create_task(node.start())
    await asyncio.sleep(0.1) # Boot buffer

    trigger_event = PsiEvent(
        event_id=f"cli-{command_name}",
        parent_id=None,
        source_id=f"cli.{command_name}",
        scope="LOCAL",
        tick=1,
        carrier=PsiCarrier(kind="COMMAND", tag=command_name.upper(), payload=payload),
        context={"phase": "cli_execution"}
    )
    
    await getattr(node, 'bus').publish(trigger_event)
    await completion_signal.wait()
    if hasattr(node, 'shutdown'):
        await node.shutdown()

def execute_cli_task(task_instance, command_name: str = "run", payload: dict = None):
    """@topos.entry: external trigger → Ψ injection"""
    payload = payload or {}
    try:
        asyncio.run(_async_run_in_node(task_instance, command_name, payload))
    except KeyboardInterrupt:
        log.info("[CLI] Task interrupted by user.")