# kernel.phase.daemon.task.wasm
import os
import json
import asyncio
import uuid
import sys
from pathlib import Path
from typing import Optional
from contextlib import suppress

from kernel.phase.daemon.base import AbstractDaemon
from kernel.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter, flow_scope
from kernel.dphi.cgroup import CgroupPolicy
from kernel.dphi.method import DphiMethod

TIME_ROOT = resolve_path("time")

class WasmTaskerDaemon(AbstractDaemon):
    """
    @role: Secure Execution Daemon (Dynamic Late-Binding Router)
    @flow: Stream Event -> Fetch Latest Strategy Module -> Execute logic
    """
    def __init__(self, tunnel, supervisor, node_id: str = None, default_wasm_path: str = "dphi.wasm"):
        super().__init__("WasmTasker")
        self.tunnel = tunnel
        self.supervisor = supervisor
        self.node_id = node_id or uuid.uuid4().hex[:8]
        self.default_wasm_path = default_wasm_path
        
        self.control_channel = "wasm:control:req"
        self.topic = "wasm:execute:stream"
        self.group_name = "wasm_tasker_group"
        self.consumer_name = f"tasker-{self.node_id}"
        self.poll_timeout_ms = 1000
        self.default_tier = "STANDARD"

        self.concurrency_limit = 71 
        self.fetch_batch_size = self.concurrency_limit 

        self._semaphore = asyncio.Semaphore(self.concurrency_limit)
        self._pubsub_task: Optional[asyncio.Task] = None

    async def _init_consumer_group(self):
        try:
            await self.tunnel.state_store.xgroup_create(name=self.topic, groupname=self.group_name, id='0', mkstream=True)
            self.log.info(f"Consumer Group '{self.group_name}' initialized for Secure Execution.")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                self.log.error(f"Failed to init Consumer Group: {e}")

    async def run(self):
        await self._init_consumer_group()
        self._pubsub_task = asyncio.create_task(self._listen_pubsub())
        self.log.info(f"WasmTasker listening: Stream[{self.topic}] | PubSub[{self.control_channel}] (Max Concurrency: {self.concurrency_limit})")
        
        try:
            while self.running:
                streams = await self.tunnel.stream_consume(
                    topic=self.topic, group=self.group_name,
                    consumer=self.consumer_name, count=self.fetch_batch_size, block=self.poll_timeout_ms
                )
                if not streams:
                    continue

                for stream_name, messages in streams:
                    for message_id, msg_data in messages:
                        await self._semaphore.acquire()

                        json_payload = msg_data.get("data", msg_data.get(b"data", b"{}"))
                        if isinstance(json_payload, bytes):
                            json_payload = json_payload.decode('utf-8')
                        
                        try:
                            data = json.loads(json_payload)
                            job_id = data.get("job_id", "unknown")
                            
                            self.supervisor.create(
                                self._process_and_reply(data, message_id), 
                                name=f"ExecGate-{job_id[:8]}"
                            )
                        except json.JSONDecodeError:
                            self.log.error(f"Invalid JSON payload: {json_payload}")
                            await self.tunnel.stream_ack(self.topic, self.group_name, message_id)
                            self._semaphore.release() 
                        except Exception as e:
                            self.log.error(f"Failed to schedule WASM task: {e}")
                            self._semaphore.release()

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log.error(f"WasmTasker Main Loop Error: {e}", exc_info=True)
        finally:
            if self._pubsub_task and not self._pubsub_task.done():
                self._pubsub_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._pubsub_task

    async def _listen_pubsub(self):
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(self.control_channel)
        try:
            async for msg in pubsub.listen():
                if not self.running:
                    break
                if isinstance(msg, dict) and msg.get("type") == "message":
                    ctrl_data = json.loads(msg["data"])
                    await self._handle_control(ctrl_data)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log.error(f"WasmTasker PubSub Error: {e}")
        finally:
            if hasattr(pubsub, 'unsubscribe'):
                await pubsub.unsubscribe(self.control_channel)
            await pubsub.close()

    def _resolve_wasm_path(self, wasm_path: str) -> Path:
        path = Path(wasm_path)
        return path if path.is_absolute() else TIME_ROOT / path

    async def _handle_control(self, payload: dict):
        job_id = payload.get("job_id", "unknown")
        response_channel = payload.get("response_channel")
        tier = payload.get("tier", "STANDARD").upper()
        context = payload.get("context", {})
        
        with flow_scope(**context):
            self.default_tier = tier
            self.log.info(f"[{job_id[:8]}] Global Cgroup default tier updated -> {tier}")
            if response_channel:
                await self.tunnel.publish(response_channel, json.dumps({"success": True, "job_id": job_id}))

    def _get_policy_from_tier(self, tier_str: str) -> CgroupPolicy:
        tier_str = (tier_str or self.default_tier).upper()
        if tier_str == "SYSTEM": return CgroupPolicy.system()
        if tier_str == "UNLIMITED": return CgroupPolicy.custom(mem_mb=1024, fuel=10_000_000_000)
        return CgroupPolicy.standard()

    async def _process_and_reply(self, payload: dict, message_id: str):
        response_channel = payload.get("response_channel")
        job_id = payload.get("job_id", "unknown")

        try:
            # CPU 바운드 연산을 별도 스레드로 분리
            response_data = await asyncio.to_thread(self._execute_isolated, payload)
            
            if isinstance(response_data, dict):
                response_data["job_id"] = job_id
                
                # [핵심 변경]: 순수 연산 결과물(Payload/Output)을 건드리지 않고,
                # 브로커가 파싱하는 "metrics" 메타데이터 영역에 물리적 노드 정보를 밀어넣음
                if "metrics" not in response_data:
                    response_data["metrics"] = {}
                
                response_data["metrics"]["handled_by_node"] = self.node_id
                response_data["metrics"]["handled_by_pid"] = os.getpid()
            
            if response_channel:
                await self.tunnel.publish(response_channel, json.dumps(response_data))
                
        except Exception as e:
            self.log.error(f"Failed to process execution payload: {e}", exc_info=True)
            if response_channel:
                err_payload = {"success": False, "error": f"Tasker Daemon Error: {str(e)}", "job_id": job_id}
                await self.tunnel.publish(response_channel, json.dumps(err_payload))
        finally:
            await self.tunnel.stream_ack(self.topic, self.group_name, message_id)
            self._semaphore.release()

    def _get_strategy_module(self):
        import kernel.phase.daemon.task.strategy as STRATEGY_PATH
        module_name = STRATEGY_PATH.__name__
        if module_name in sys.modules:
            return sys.modules[module_name]
        import importlib
        return importlib.import_module(module_name)

    def _execute_isolated(self, payload: dict) -> dict:
        """라우팅을 수행하고 최신 로직을 주입받아 실행"""
        job_id = payload.get("job_id", "unknown")
        target_func = payload.get("target_func", DphiMethod.EXECUTE_CODE)
        wasm_path = payload.get("wasm_path", self.default_wasm_path)
        tier_str = payload.get("tier", self.default_tier)
        job_policy = self._get_policy_from_tier(tier_str)
        exec_data = payload.get("payload", payload.get("data", ""))
        context = payload.get("context", {})

        strategies = self._get_strategy_module()

        with flow_scope(**context):
            target_path = self._resolve_wasm_path(wasm_path)
            core_wasm_path = self._resolve_wasm_path(self.default_wasm_path)
            
            if not target_path.exists() and target_func != DphiMethod.EXECUTE_CODE:
                error_msg = f"WASM binary not found: {target_path}"
                self.log.warning(f"[{job_id[:8]}] {error_msg}")
                return {"success": False, "output": "", "error": error_msg}

            # ROUTE A: Guarded Execution
            if target_func in (DphiMethod.EXECUTE_CODE, DphiMethod.EXECUTE_DVM):
                safe_payload = strategies.validate_intent_checkpoint(
                    payload, exec_data, context, job_id, core_wasm_path, self.log
                )
                
                if isinstance(safe_payload, dict) and "error" in safe_payload and not safe_payload.get("success", True):
                    return safe_payload

                if target_func == DphiMethod.EXECUTE_DVM:
                    return strategies.run_dvm_sandbox(target_path, job_policy, safe_payload, context, job_id, self.log)
                else:
                    return strategies.run_python_sandbox(job_policy, safe_payload, context, job_id, self.log)

            # ROUTE B: Pure WASM Core Execution
            else:
                return strategies.run_pure_wasm(target_path, target_func, job_policy, exec_data, context, job_id, self.log)