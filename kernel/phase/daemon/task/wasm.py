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
    @role: Secure Execution Daemon (Dynamic Late-Binding Router with Instance Pooling)
    @flow: Stream Event -> Fetch Pre-warmed Interpreter -> Execute logic -> Return to Pool
    """
    def __init__(self, tunnel, supervisor, node_id: str = None, default_wasm_path: str = "dphi.wasm"):
        super().__init__("WasmTasker")
        self.tunnel = tunnel
        self.supervisor = supervisor
        
        ## Worker 고유 식별을 위해 PID를 명시적으로 추가하여 충돌 방지
        self.node_id = node_id or f"tasker-{uuid.uuid4().hex[:6]}-{os.getpid()}"
        self.default_wasm_path = default_wasm_path
        
        self.control_channel = "wasm:control:req"
        self.topic = "wasm:execute:stream"
        self.group_name = "wasm_tasker_group"
        
        ## Redis Consumer Name은 프로세스 단위로 고유해야 함
        self.consumer_name = f"consumer-{self.node_id}"
        self.poll_timeout_ms = 1000
        self.default_tier = "STANDARD"
        self.concurrency_limit = 4 
        self.fetch_batch_size = self.concurrency_limit 
        
        # [핵심 개편] Semaphore 대신 미리 데워진(Pre-warmed) 인터프리터 큐를 사용합니다.
        self._wasm_pool = asyncio.Queue(maxsize=self.concurrency_limit)
        
        self._pubsub_task: Optional[asyncio.Task] = None

    async def _init_consumer_group(self):
        try:
            await self.tunnel.state_store.xgroup_create(name=self.topic, groupname=self.group_name, id='0', mkstream=True)
            self.log.info(f"Consumer Group '{self.group_name}' initialized for Secure Execution.")
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                self.log.error(f"Failed to init Consumer Group: {e}")

    async def _init_wasm_pool(self):
        """부팅 시 동시성 제한 개수만큼 WASM 샌드박스를 미리 생성(Pre-warm)하여 큐에 적재합니다."""
        self.log.info(f"[{self.node_id}] Pre-warming {self.concurrency_limit} WASM instances...")
        
        from kernel.bind.inter.wasm import WasmInterpreter
        
        for _ in range(self.concurrency_limit):
            # CgroupPolicy는 기본값 사용 (요청별로 런타임에 동적 적용 불가능할 경우 시스템 최고권한 부여 후 
            # 내부 로직에서 검증하거나, Wasmtime Store의 Limit을 런타임에 덮어쓰도록 구현 권장)
            # 여기서는 Pooling 아키텍처 완성을 우선으로 합니다.
            interp = WasmInterpreter(
                wasm_module_path=str(self._resolve_wasm_path(self.default_wasm_path)),
                policy=self._get_policy_from_tier(self.default_tier)
            )
            self._wasm_pool.put_nowait(interp)
            
        self.log.info(f"[{self.node_id}] WASM Instance Pool initialized successfully.")

    async def run(self):
        await self._init_consumer_group()
        await self._init_wasm_pool()
        
        self._pubsub_task = asyncio.create_task(self._listen_pubsub())
        self.log.info(f"WasmTasker listening: Stream[{self.topic}] | PubSub[{self.control_channel}] (Max Concurrency: {self.concurrency_limit}, PID: {os.getpid()})")
        
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
                        interp_instance = await self._wasm_pool.get()

                        json_payload = msg_data.get("data", msg_data.get(b"data", b"{}"))
                        if isinstance(json_payload, bytes):
                            json_payload = json_payload.decode('utf-8')
                        
                        try:
                            data = json.loads(json_payload)
                            job_id = data.get("job_id", "unknown")
                            
                            self.supervisor.create(
                                self._process_and_reply(data, message_id, interp_instance), 
                                name=f"ExecGate-{job_id[:8]}"
                            )
                        except json.JSONDecodeError:
                            self.log.error(f"Invalid JSON payload: {json_payload}")
                            await self.tunnel.stream_ack(self.topic, self.group_name, message_id)
                            # 에러 시 인스턴스를 풀에 반환
                            self._wasm_pool.put_nowait(interp_instance) 
                        except Exception as e:
                            self.log.error(f"Failed to schedule WASM task: {e}")
                            self._wasm_pool.put_nowait(interp_instance)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log.error(f"WasmTasker Main Loop Error: {e}", exc_info=True)
        finally:
            if self._pubsub_task and not self._pubsub_task.done():
                self._pubsub_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._pubsub_task
                    
            # 셧다운 시 큐에 남아있는 모든 인스턴스 종료
            while not self._wasm_pool.empty():
                interp = self._wasm_pool.get_nowait()
                interp.shutdown()

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

    async def _process_and_reply(self, payload: dict, message_id: str, interp_instance):
        response_channel = payload.get("response_channel")
        job_id = payload.get("job_id", "unknown")

        try:
            response_data = await asyncio.to_thread(self._execute_isolated, payload, interp_instance)
            if isinstance(response_data, dict):
                response_data["job_id"] = job_id
                
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
            self._wasm_pool.put_nowait(interp_instance)

    def _get_strategy_module(self):
        import kernel.phase.daemon.task.strategy as STRATEGY_PATH
        module_name = STRATEGY_PATH.__name__
        if module_name in sys.modules:
            return sys.modules[module_name]
        import importlib
        return importlib.import_module(module_name)

    def _execute_isolated(self, payload: dict, interp_instance) -> dict:
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
                    # DvmInterpreter는 내부 구조가 다르므로 현재는 기존 strategy 로직(새로 띄우기)에 의존하거나 
                    # Dvm 풀도 별도로 만들어야 합니다. 여기서는 기존 fallback 유지.
                    return strategies.run_dvm_sandbox(target_path, job_policy, safe_payload, context, job_id, self.log)
                else:
                    # Python Legacy Jail 실행
                    try:
                        self.log.info(f"[{job_id[:8]}] 🔓 Entering Python Legacy Jail (Pooled)")
                        res = interp_instance.execute(
                            code=safe_payload,
                            context=context
                        )
                        metrics = interp_instance.get_metrics()
                        
                        if res.success:
                            return {"success": True, "output": res.output, "metrics": metrics}
                        else:
                            return {"success": False, "output": "", "error": str(res.error), "metrics": metrics}
                    except Exception as e:
                        return {"success": False, "output": "", "error": f"Jail Error: {e}", "metrics": {}}

            # ROUTE B: Pure WASM Core Execution
            else:
                try:
                    exec_payload = exec_data if isinstance(exec_data, str) else json.dumps(exec_data)
                    res = interp_instance.invoke(
                        target_func=target_func,
                        payload=exec_payload,
                        context=context
                    )
                    metrics = interp_instance.get_metrics()
                    
                    if res.success:
                        return {"success": True, "output": res.output, "metrics": metrics}
                    else:
                        return {"success": False, "output": "", "error": str(res.error), "metrics": metrics}
                except Exception as e:
                    return {"success": False, "output": "", "error": f"Invoke Error: {e}", "metrics": {}}