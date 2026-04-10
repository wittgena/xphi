# bridge.executor.cli
import os
import sys
import uuid
import json
import asyncio
import argparse
import subprocess
from pathlib import Path
import redis.asyncio as redis_async
from typing import Callable, Any
from bridge.pir import PsiEvent, PsiCarrier
from model.event import next_id, LogEvent
from plane.bound import surface
from dataclasses import asdict
from plane.emitter import get_logger, flow_scope
from plane.node.sensor import REDIS_URL
from contract.registry import registry
from model.task.result import TaskSummaryEvent, TaskDetailRecord
from bridge.executor.base import BaseExecutor
from anchor.resolver import get_invoker

log = get_logger("executor.cli")

def parse_local(argv):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--local", action="store_true")
    return parser.parse_known_args(argv)

def dispatch_cli(command_name: str, entry_func: Callable, file_path: str):
    """CLI 엔트리포인트의 공통 배관 로직을 처리하는 유니버설 디스패처"""
    invoker, command = get_invoker(Path(file_path))
    cli_args = sys.argv[1:]
    payload = { "_context": {"invoker": str(invoker), "command": command, "cli_args": cli_args} }
    task = entry_func(cli_args)
    execute_cli_task(task_instance=task, command_name=command_name, payload=payload)

class _GenericCliExecutor(BaseExecutor):
    """bound(transduction) - external CLI → internal Ψ execution"""
    def __init__(self, task_instance, completion_signal: asyncio.Event):
        super().__init__()
        self.task_instance = task_instance
        self.completion_signal = completion_signal
        self.node = None

    async def execute(self, psi) -> list:
        command = psi.carrier.tag if hasattr(psi, 'carrier') else "CLI_TASK"
        task_id = getattr(psi, 'event_id', f"task-{next_id()}")
        ## task_id = f"task-{snowflake_id}"

        detail_key = f"{command.lower()}:cli:{task_id}"
        latest_pointer_key = f"{command.lower()}:cli:latest"
        self.log.info(f"[exec] Executing CLI task: {command} ({task_id})")
        print(f"[exec] Executing CLI task: {command} ({task_id})")
        
        with flow_scope(flow_id=task_id, phase="EXECUTION"):
            self.log.info(f"[exec] Starting flow: {task_id}")
            print(f"[exec] Starting flow: {task_id}")
            try:
                ## 태스크 실행
                raw_result = self.task_instance.run() or {}

                ## 결과 레코드 생성 (저장하기 전에 먼저 생성!)
                detail_record = TaskDetailRecord(
                    task_id=task_id,
                    command=command,
                    status=raw_result.get("status", "SUCCESS"),
                    artifacts=raw_result.get("artifacts", {}),
                    metrics=raw_result.get("metrics", {}),
                    # details=raw_result.get("details", {})
                )

                ## 요약 이벤트 생성
                summary_event = TaskSummaryEvent(
                    task_id=task_id,
                    command=command,
                    status=detail_record.status,
                    summary=raw_result.get("summary", f"Task {command} completed."),
                    detail_key=detail_key,
                    details=raw_result.get("details", {})
                )

                _project_to_stdout(summary_event)

                ## Redis 저장 및 공명(Reflection) 발행
                if self.node and self.node.redis:
                    ## 상세 기록 저장
                    await self.node.redis.set(detail_key, detail_record.to_json(), ex=3600)
                    await self.node.redis.set(latest_pointer_key, detail_key, ex=3600)
                    await asyncio.sleep(0.05)

                    ## 발화자(Proxy)에게 응답 전송
                    response_channel = psi.context.get("response_channel")
                    if response_channel:
                        await self.node.redis.publish(response_channel, summary_event.to_json())
                        self.log.info(f"[exec] Reflection published to {response_channel}")
                    
                    self.log.info(f"[exec] Detailed artifacts saved -> Redis[{detail_key}]")

                ## 내부 버스용 결과 이벤트 발행
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
                import traceback; traceback.print_exc()
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
    r = redis_async.from_url(REDIS_URL, decode_responses=True)
    
    ## 스웜 감지 (하트비트 확인)
    active_node_keys = await r.keys("runtime:heartbeat:*")
    # 2. 실질적 가용성 확인 (큐 적체 여부 확인)
    queue_len = await r.llen("runtime:queue")
    
    # 노드 키는 있는데 큐가 너무 많이 쌓여있다면(예: 5개 이상), 노드가 멈춘 것으로 간주하고 새로 띄움
    should_spawn = not active_node_keys or queue_len > 5
    if should_spawn:
        if queue_len > 5:
            log.warn(f"[CLI] Node seems stalled (Queue: {queue_len}). Forcing fresh spawn...")
        else:
            log.info("[CLI] No active nodes. Spawning background Ambient Node...")
        
        ## -m plane.node.runtime으로 실행하면 노드는 순수 대기 상태로 시작
        subprocess.Popen(
            [sys.executable, "-m", "plane.node.runtime"],
            stdout=subprocess.DEVNULL, # 터미널 출력을 끊음
            stderr=subprocess.DEVNULL,
            start_new_session=True,    # CLI가 종료되어도 노드가 살아남게 함
            env=os.environ.copy()      # 현재 환경변수 전달
        )
        
        # 대기 로직에서 'active' 키가 생성되는지 감시
        for _ in range(10):
            await asyncio.sleep(0.5)
            if await r.keys("runtime:active"): # 노드가 준비 완료되면 'active' 키를 생성하도록 설계 권장
                break

    ## 결과 수신을 위한 채널 준비
    task_id = f"task-{uuid.uuid4().hex[:8]}"
    with flow_scope(flow_id=task_id, phase="CLI"):
        response_channel = f"res:{task_id}"
        log_channel = f"log:{task_id}" ## 로그 채널 정의

        pubsub = r.pubsub()
        await pubsub.subscribe(response_channel, log_channel)
        log.info(f"[CLI] Listening on {response_channel} & {log_channel}...")

        ## 사건(Ψ) 주입 (노드에게 '레시피' 전달)
        trigger_event = PsiEvent(
            event_id=task_id,
            source_id=f"{command_name}:proxy",
            scope="GLOBAL",
            parent_id=None,
            tick=1,
            carrier=PsiCarrier(kind="COMMAND", tag=command_name.lower(), payload=payload),
            context={"response_channel": response_channel}
        )
        
        ## 큐에 사건을 밀어 넣음
        await r.lpush("runtime:queue", json.dumps(asdict(trigger_event)))

        try:
            ## 결과 대기 (노드가 작업을 완료하고 publish 할 때까지)
            async with asyncio.timeout(60):
                async for msg in pubsub.listen():
                    if msg["type"] != "message":
                        continue

                    channel = msg["channel"]
                    if channel == log_channel:
                        try:
                            log_data = json.loads(msg["data"])
                            
                            # [개선] 1. 자기 자신이 쏜 로그(Phase: CLI)는 화면에 다시 투영하지 않음
                            if log_data.get("context", {}).get("phase") == "CLI":
                                continue

                            # [개선] 2. LogEvent 필드에 없는 데이터가 들어와도 에러 나지 않게 필터링
                            from dataclasses import fields
                            valid_fields = {f.name for f in fields(LogEvent)}
                            clean_data = {k: v for k, v in log_data.items() if k in valid_fields}
                            
                            event = LogEvent(**clean_data)
                            surface.update(event)
                        except Exception:
                            continue # 로그 파싱 실패가 전체 로직을 멈추지 않게 함
                    elif channel == response_channel:
                        try:
                            data = json.loads(msg["data"])
                            print_formatted_result(data)
                            # [핵심] 성공적으로 출력했다면 즉시 루프 탈출
                            return 
                        except Exception as e:
                            log.error(f"[CLI] Failed to parse result data: {e}")
                            break # 에러 시에도 무한 대기 방지를 위해 탈출
        except TimeoutError:
            log.error(f"[CLI] Task timed out. Node failed to reflect within 60s.")
        finally:
            await pubsub.unsubscribe(response_channel)
            await r.close()

def execute_cli_task(task_instance, command_name: str = "run", payload: dict = None):
    """@topos.entry: external trigger → Ψ injection"""
    payload = payload or {}
    try:
        asyncio.run(_async_run_in_node(task_instance, command_name, payload))
    except KeyboardInterrupt:
        log.info("[CLI] Task interrupted by user.")

def print_formatted_result(data: dict):
    """전달받은 데이터(dict)를 기반으로 CLI에 리치 결과 출력"""
    status = data.get("status", "UNKNOWN")
    command = data.get("command", "task")
    summary = data.get("summary", "")
    details = data.get("details", {})
    detail_key = data.get("detail_key", "")

    ## 기본 헤더
    color_code = "\033[92m" if status == "SUCCESS" else "\033[91m"
    reset_code = "\033[0m"
    
    print(f"\n{color_code}[{status}]{reset_code} {command}")
    print(f"{summary}")

    ## 상세 정보(details)가 있을 경우 루프를 돌며 출력
    if isinstance(details, dict) and details:
        print("\n## Execution Details")
        for category, items in details.items():
            if isinstance(items, list):
                print(f"\n └─ [{category}] ({len(items)} items)")
                for item in items[:5]:
                    if isinstance(item, dict):
                        # 리포지토리 경로나 태그 등 주요 정보 출력
                        ns = item.get('namespace') or item.get('path') or str(item)
                        print(f"    - {ns}")
                if len(items) > 5:
                    print(f"    ... and {len(items)-5} more.")
            else:
                print(f" └─ {category}: {items}")

    print(f"\n(Full artifacts saved at Redis -> {detail_key})")