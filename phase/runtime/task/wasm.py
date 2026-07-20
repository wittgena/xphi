# phase.runtime.task.wasm
import json
import asyncio
import uuid
from pathlib import Path
from typing import Optional
from contextlib import suppress

from phase.runtime.daemon.base import AbstractDaemon
from phase.runtime.inter.wasm import WasmInterpreter
from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

SANDBOX_ROOT = resolve_path("sandbox")

class WasmTaskerDaemon(AbstractDaemon):
    """
    @role: WASM 전용 리스너 데몬 (Sticky & Exclusive)
    @flow: 
        1. Execute Plane: Stream(XREADGROUP) -> 스레드 위임 -> Interpreter -> XACK
        2. Control Plane: Pub/Sub -> 정책 동기화 (독립 태스크로 병렬 동작)
    """
    def __init__(self, tunnel, supervisor, node_id: str = None, default_wasm_path: str = "theoria.wasm"):
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

        self._pubsub_task: Optional[asyncio.Task] = None

    async def _init_consumer_group(self):
        try:
            await self.tunnel.state_store.xgroup_create(
                name=self.topic, groupname=self.group_name, id='0', mkstream=True
            )
            self.log.info(f"Consumer Group '{self.group_name}' initialized for WASM.")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                self.log.error(f"Failed to init Consumer Group: {e}")

    async def run(self):
        await self._init_consumer_group()
        self._pubsub_task = asyncio.create_task(self._listen_pubsub())
        self.log.info(f"WasmTasker listening: Stream[{self.topic}] | PubSub[{self.control_channel}]")
        
        try:
            while self.running:
                streams = await self.tunnel.stream_consume(
                    topic=self.topic,
                    group=self.group_name,
                    consumer=self.consumer_name,
                    count=1,
                    block=self.poll_timeout_ms
                )

                if not streams:
                    continue

                for stream_name, messages in streams:
                    for message_id, msg_data in messages:
                        json_payload = msg_data.get("data", msg_data.get(b"data", b"{}"))
                        if isinstance(json_payload, bytes):
                            json_payload = json_payload.decode('utf-8')
                        
                        try:
                            data = json.loads(json_payload)
                            job_id = data.get("job_id", "unknown")
                            
                            self.supervisor.create(
                                self._process_and_reply(data, message_id), 
                                name=f"WasmExec-{job_id[:8]}"
                            )
                        except json.JSONDecodeError:
                            self.log.error(f"Invalid JSON payload: {json_payload}")
                            await self.tunnel.stream_ack(self.topic, self.group_name, message_id)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log.error(f"WasmTasker Main Loop Error: {e}")
        finally:
            if self._pubsub_task and not self._pubsub_task.done():
                self._pubsub_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._pubsub_task

    async def _listen_pubsub(self):
        """별도 태스크로 동작하는 Control Plane 리스너"""
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
        if not path.is_absolute():
            path = SANDBOX_ROOT / path
        return path

    async def _handle_control(self, payload: dict):
        job_id = payload.get("job_id", "unknown")
        response_channel = payload.get("response_channel")
        tier = payload.get("tier", "STANDARD")
        
        self.log.info(f"[{job_id[:8]}] Cgroup policy update requested -> {tier}")
        if response_channel:
            await self.tunnel.publish(response_channel, json.dumps({"success": True}))

    async def _process_and_reply(self, payload: dict, message_id: str):
        response_channel = payload.get("response_channel")
        try:
            response_data = await asyncio.to_thread(self._execute_isolated, payload)
            if response_channel:
                await self.tunnel.publish(response_channel, json.dumps(response_data))
        except Exception as e:
            self.log.error(f"Failed to process WASM payload: {e}")
        finally:
            await self.tunnel.stream_ack(self.topic, self.group_name, message_id)

    def _execute_isolated(self, payload: dict) -> dict:
        job_id = payload.get("job_id", "unknown")
        target_func = payload.get("target_func", "execute_code")
        wasm_path = payload.get("wasm_path", self.default_wasm_path)
        exec_data = payload.get("payload", payload.get("data", ""))

        target_path = self._resolve_wasm_path(wasm_path)
        if not target_path.exists():
            error_msg = f"WASM binary not found: {target_path}"
            self.log.warn(f"[{job_id[:8]}] {error_msg}")
            return {"success": False, "output": "", "error": error_msg}

        try:
            with WasmInterpreter(str(target_path)) as interpreter:
                result = interpreter.invoke(target_func, exec_data)
                return {
                    "success": result.success,
                    "output": result.output if result.success else "",
                    "error": str(result.error) if not result.success else ""
                }
        except Exception as e:
            self.log.error(f"WASM crashed [Job: {job_id[:8]}]: {e}")
            return {"success": False, "output": "", "error": str(e)}