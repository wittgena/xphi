# kernel.phase.daemon.task.wasm
import json
import asyncio
import uuid
from pathlib import Path
from typing import Optional
from contextlib import suppress

from kernel.phase.daemon.base import AbstractDaemon
from kernel.bind.inter.wasm import WasmInterpreter
from kernel.bind.inter.python import PythonInterpreter
from kernel.bind.inter.dvm import DvmInterpreter
from kernel.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter, flow_scope
from kernel.dphi.cgroup import CgroupPolicy, Tier

TIME_ROOT = resolve_path("time")

class WasmTaskerDaemon(AbstractDaemon):
    """
    @role: Secure Execution Daemon (Multi-WASM Bifurcated Routing)
    @flow: 
        [ROUTE A] Guarded Execution (Requires 'validate_intent' via Core dphi.wasm)
            ├─ execute_dvm -> dvm.wasm (Rust Multi VM Sandbox)
            └─ execute_code -> Deno Jail (Python Legacy)
        [ROUTE B] Direct Core Execution (Topology, Resonance, State) -> target WASM (dphi.wasm)
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
        tier = payload.get("tier", "STANDARD").upper()
        
        context = payload.get("context", {})
        
        with flow_scope(**context):
            self.default_tier = tier
            self.log.info(f"[{job_id[:8]}] Global Cgroup default tier updated -> {tier}")
            
            if response_channel:
                await self.tunnel.publish(response_channel, json.dumps({"success": True}))

    def _get_policy_from_tier(self, tier_str: str) -> CgroupPolicy:
        tier_str = (tier_str or self.default_tier).upper()
        if tier_str == "SYSTEM":
            return CgroupPolicy.system()
        elif tier_str == "UNLIMITED":
            return CgroupPolicy.custom(mem_mb=1024, fuel=10_000_000_000)
        else:
            return CgroupPolicy.standard()

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
        
        tier_str = payload.get("tier", self.default_tier)
        job_policy = self._get_policy_from_tier(tier_str)
        exec_data = payload.get("payload", payload.get("data", ""))
        
        context = payload.get("context", {})

        with flow_scope(**context):
            
            target_path = self._resolve_wasm_path(wasm_path)
            # execute_code(Python/Deno)의 경우 WASM 바이너리가 불필요하므로 예외 처리
            if not target_path.exists() and target_func != "execute_code":
                error_msg = f"WASM binary not found: {target_path}"
                self.log.warn(f"[{job_id[:8]}] {error_msg}")
                return {"success": False, "output": "", "error": error_msg}

            # ROUTE A: Guarded Execution (Requires 'validate_intent' Checkpoint)
            if target_func in ("execute_code", "execute_dvm"):
                try:
                    ## STAGE 1: Checkpoint (validate_intent via Core Dphi WASM)
                    # 보안 검증은 실행 엔진(dvm)이 아닌 코어 엔진(dphi.wasm)의 프로토콜 룰셋을 따름
                    core_wasm_path = self._resolve_wasm_path(self.default_wasm_path)
                    with WasmInterpreter(str(core_wasm_path), policy=CgroupPolicy.system()) as wasm_gate:
                        validation_res = wasm_gate.invoke("validate_intent", json.dumps(payload), context=context)
                        
                        if not validation_res.success:
                            if "not registered" in str(validation_res.error) or "not found" in str(validation_res.error):
                                self.log.debug(f"[{job_id[:8]}] 'validate_intent' missing in Core WASM. Bypassing checkpoint.")
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

                    ## STAGE 2: Secure Execution Enclave
                    if target_func == "execute_dvm":
                        # Multi-VM Sandbox (dvm.wasm)
                        with DvmInterpreter(wasm_module_name=target_path.name, policy=job_policy) as dvm_sandbox:
                            safe_dict = safe_payload if isinstance(safe_payload, dict) else {}
                            if isinstance(safe_payload, str):
                                with suppress(Exception):
                                    safe_dict = json.loads(safe_payload)
                                    
                            self.log.info(f"[{job_id[:8]}] 🔓 Entering Multi-VM Jail: {target_path.name} (Tier: {job_policy.tier.value})")
                            result = dvm_sandbox.execute(
                                target_address=safe_dict.get("target_address", ""),
                                calldata=safe_dict.get("calldata", ""),
                                state_snapshot=safe_dict.get("state_snapshot", {}),
                                context=context
                            )
                            
                            metrics = dvm_sandbox.get_metrics()
                            if result.success:
                                with suppress(Exception):
                                    out_dict = json.loads(result.output)
                                    metrics["gas_used"] = out_dict.get("gas_used", 0)
                            self.log.info(f"[{job_id[:8]}] 📊 EVM Sandbox Metrics: {metrics}")
                    else:
                        with PythonInterpreter(enable_network_access=None, policy=job_policy) as py_sandbox:
                            if isinstance(safe_payload, str):
                                code_to_run = safe_payload
                                variables = {}
                            elif isinstance(safe_payload, dict):
                                code_to_run = safe_payload.get("code", safe_payload.get("data", ""))
                                variables = safe_payload.get("variables", {})
                            else:
                                code_to_run = ""
                                variables = {}

                            host_capabilities = {
                                "system_ping": lambda: "pong_from_host"
                            }
                            
                            self.log.info(f"[{job_id[:8]}] 🔓 Entering Python Legacy Jail (Tier: {job_policy.tier.value})")
                            result = py_sandbox.execute(
                                code=code_to_run, 
                                variables=variables,
                                callables=host_capabilities,
                                context=context
                            )
                            
                            metrics = py_sandbox.get_metrics()
                            self.log.info(f"[{job_id[:8]}] 📊 Sandbox Metrics: {metrics}")
                            
                    return {
                        "success": result.success,
                        "output": result.output,
                        "error": str(result.error) if not result.success else "",
                        "metrics": metrics
                    }
                        
                except Exception as e:
                    self.log.error(f"[{job_id[:8]}] Execution crashed: {e}", exc_info=True)
                    return {"success": False, "output": "", "error": f"Execution Error: {e}"}

            ## ROUTE B: Pure WASM Core Execution (Topology, Resonance, State Collapse)
            else:
                try:
                    with WasmInterpreter(str(target_path), policy=job_policy) as wasm_runner:
                        self.log.debug(f"[{job_id[:8]}] Bypassing Jail. Direct WASM Kernel logic: {target_func} via {target_path.name} (Tier: {job_policy.tier.value})")
                        
                        if isinstance(exec_data, dict):
                            exec_data_str = json.dumps(exec_data)
                        else:
                            exec_data_str = str(exec_data)
                            
                        result = wasm_runner.invoke(target_func, exec_data_str, context=context)
                        metrics = wasm_runner.get_metrics()
                        
                        self.log.info(f"[{job_id[:8]}] 📊 WASM Metrics: {metrics}")
                        return {
                            "success": result.success,
                            "output": result.output if result.success else "",
                            "error": str(result.error) if not result.success else "",
                            "metrics": metrics
                        }
                except Exception as e:
                    self.log.error(f"[{job_id[:8]}] WASM Kernel logic crashed: {e}", exc_info=True)
                    return {"success": False, "output": "", "error": str(e)}