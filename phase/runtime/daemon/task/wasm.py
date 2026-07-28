# phase.runtime.daemon.task.wasm
## @lineage: phase.runtime.task.wasm
import json
import asyncio
import uuid
from pathlib import Path
from typing import Optional
from contextlib import suppress

from phase.runtime.daemon.base import AbstractDaemon
from phase.runtime.inter.wasm import WasmInterpreter
from phase.runtime.inter.python import PythonInterpreter
from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

TIME_ROOT = resolve_path("time")

class WasmTaskerDaemon(AbstractDaemon):
    """
    @role: Secure Execution Daemon (Bifurcated Routing)
    @flow: 
        [ROUTE A] target == 'execute_code' -> Checkpoint(WASM validate) -> Jail(Deno)
        [ROUTE B] target != 'execute_code' -> Direct WASM Kernel Execution
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

        self._pubsub_task: Optional[asyncio.Task] = None

    async def _init_consumer_group(self):
        try:
            await self.tunnel.state_store.xgroup_create(
                name=self.topic, groupname=self.group_name, id='0', mkstream=True
            )
            self.log.info(f"Consumer Group '{self.group_name}' initialized for Secure Execution.")
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
                                name=f"ExecGate-{job_id[:8]}"
                            )
                        except json.JSONDecodeError:
                            self.log.error(f"Invalid JSON payload: {json_payload}")
                            await self.tunnel.stream_ack(self.topic, self.group_name, message_id)

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
        if not path.is_absolute():
            path = TIME_ROOT / path
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
            self.log.error(f"Failed to process execution payload: {e}", exc_info=True)
        finally:
            await self.tunnel.stream_ack(self.topic, self.group_name, message_id)

    def _execute_isolated(self, payload: dict) -> dict:
        job_id = payload.get("job_id", "unknown")
        target_func = payload.get("target_func", "execute_code")
        wasm_path = payload.get("wasm_path", self.default_wasm_path)
        
        # 순수 페이로드 원본 (WASM 커널용)
        exec_data = payload.get("payload", payload.get("data", ""))

        target_path = self._resolve_wasm_path(wasm_path)
        if not target_path.exists():
            error_msg = f"WASM binary not found: {target_path}"
            self.log.warn(f"[{job_id[:8]}] {error_msg}")
            return {"success": False, "output": "", "error": error_msg}

        if target_func == "execute_code":
            try:
                # --- STAGE 1: Checkpoint (validate_intent) ---
                with WasmInterpreter(str(target_path)) as wasm_gate:
                    validation_res = wasm_gate.invoke("validate_intent", json.dumps(payload))
                    
                    if not validation_res.success:
                        # [하위 호환성 방어] dphi.wasm에 아직 validate_intent가 없을 경우 우회 허용
                        if "not registered" in str(validation_res.error) or "not found" in str(validation_res.error):
                            self.log.debug(f"[{job_id[:8]}] 'validate_intent' missing in WASM. Bypassing checkpoint for legacy compatibility.")
                            safe_payload = exec_data
                        else:
                            self.log.error(f"[{job_id[:8]}] WASM Gateway crashed: {validation_res.error}")
                            return {"success": False, "output": "", "error": f"Gateway Fault: {validation_res.error}"}
                    else:
                        val_data = json.loads(validation_res.output)
                        if not val_data.get("is_valid", True):
                            error_code = val_data.get('error_code', 'UNAUTHORIZED_INTENT')
                            self.log.warning(f"[{job_id[:8]}] 🔒 Checkpoint Denied: {error_code}")
                            return {"success": False, "output": "", "error": f"Security Policy Violation: {error_code}"}
                        
                        safe_payload = val_data.get("safe_payload", exec_data)

                # --- STAGE 2: Jail (Deno Sandbox) ---
                with PythonInterpreter(enable_network_access=None) as py_sandbox:
                    
                    # 딕셔너리 구조 차이에 대비한 완벽한 페이로드 파싱 (빈 문자열 실행 방지)
                    if isinstance(safe_payload, str):
                        code_to_run = safe_payload
                        variables = {}
                    elif isinstance(safe_payload, dict):
                        code_to_run = safe_payload.get("code", safe_payload.get("data", ""))
                        variables = safe_payload.get("variables", {})
                    else:
                        code_to_run = ""
                        variables = {}

                    self.log.debug(f"====== [DEBUG X-RAY] ======")
                    self.log.debug(f"1. RAW payload: {payload}")
                    self.log.debug(f"2. safe_payload: {safe_payload}")
                    self.log.debug(f"3. Extracted code_to_run: '{code_to_run}'")
                    self.log.debug(f"===========================")        
                    host_capabilities = {
                        "system_ping": lambda: "pong_from_host"
                    }
                    
                    self.log.info(f"[{job_id[:8]}] 🔓 Entering Deno Jail for Python execution.")
                    result = py_sandbox.execute(
                        code=code_to_run, 
                        variables=variables,
                        callables=host_capabilities
                    )
                    
                    return {
                        "success": result.success,
                        "output": result.output if result.success else "",
                        "error": str(result.error) if not result.success else ""
                    }
                    
            except Exception as e:
                self.log.error(f"[{job_id[:8]}] Execution crashed: {e}", exc_info=True)
                return {"success": False, "output": "", "error": f"Execution Error: {e}"}

        # =====================================================================
        # ROUTE B: Pure WASM Execution (Topology, Resonance, State Collapse)
        # =====================================================================
        else:
            try:
                # 검문소 우회. 원본 exec_data를 들고 dphi.wasm 커널로 직행
                with WasmInterpreter(str(target_path)) as wasm_runner:
                    self.log.debug(f"[{job_id[:8]}] Bypassing Jail. Direct WASM Kernel logic: {target_func}")
                    result = wasm_runner.invoke(target_func, exec_data)
                    return {
                        "success": result.success,
                        "output": result.output if result.success else "",
                        "error": str(result.error) if not result.success else ""
                    }
            except Exception as e:
                self.log.error(f"[{job_id[:8]}] WASM Kernel logic crashed: {e}", exc_info=True)
                return {"success": False, "output": "", "error": str(e)}