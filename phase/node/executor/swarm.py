# phase.node.executor.swarm
import os
import sys
import uuid
import json
import asyncio
import subprocess
import importlib
from typing import Callable, Any
from phase.field.event.psi import PsiEvent, PsiCarrier
from phase.field.event.contract import next_id, LogEvent
from dataclasses import asdict
from bound.surface.plane import surface
from bound.surface.emitter import get_logger, flow_scope
from phase.contract.registry import registry
from phase.node.executor.base import BaseExecutor
from phase.node.executor.cli import _GenericCliExecutor

log = get_logger("executor.cli")

class SwarmCliExecutor(BaseExecutor):
    """특정 인스턴스가 아닌, 레지스트리에서 태스크를 동적으로 찾아 실행하는 스웜용 실행기"""
    def __init__(self, completion_signal: asyncio.Event):
        super().__init__()
        self.completion_signal = completion_signal
        self.node = None

    async def execute(self, psi) -> list:
        context = psi.carrier.payload.get("_context", {})
        command = context.get("command")
        cli_args = context.get("cli_args", [])
        task_id = getattr(psi, 'event_id', None) or f"task-{next_id()}"

        if not command: 
            self.log.error(f"[Swarm] No command: {command}")
            return []

        ## 레지스트리에서 해당 커맨드에 매핑된 모듈/함수 정보 획득
        task_info_list = registry.registered_cli_tasks.get(command)
        if not task_info_list: 
            self.log.error(f"[Swarm] No registered task found for: {command}")
            return []

        with flow_scope(flow_id=task_id, phase="EXECUTION"):
            log.info(f"[SwarmCliExecutor] flow_id: {task_id}")
            print(f"[SwarmCliExecutor] flow_id: {task_id}")

            try:
                task_info = task_info_list[0]
                module_fqn = task_info.get("module_fqn")
                entry_func_name = task_info.get("entry", "entry_task")
                module = importlib.import_module(module_fqn)
                if hasattr(module, entry_func_name):
                    entry_func = getattr(module, entry_func_name)
                    task_adapter = entry_func(cli_args)
                    completion_signal = asyncio.Event()
                    internal_executor = _GenericCliExecutor(task_adapter, completion_signal)

                    if self.node:
                        internal_executor.node = self.node
                    else:
                        self.log.warn("[Swarm] Executor has no node reference! Reflection might fail.")
                    await internal_executor.execute(psi)
                else:
                    self.log.error(f"[Swarm] 'entry_task' not found in {module.__name__}")
            except Exception as e:
                self.log.error(f"[Swarm] Execution Failed: {e}")
            finally:
                self.completion_signal.set()
        return []
