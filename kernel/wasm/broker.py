# xphi.kernel.wasm.broker
import time
import json
import uuid
import asyncio
from enum import Enum
from typing import Optional, Any, Mapping, Union, Dict
from contextlib import suppress

from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.arch.contract.interpreter import ExecutionResult, ExecutionError
from xphi.kernel.wasm.method import DphiMethod
from xphi.watcher.plane.emitter import get_emitter, _flow_context

log = get_emitter("dphi.broker")

class BrokerChannel:
    EXECUTE_STREAM = "wasm:execute:stream"
    CONTROL_REQ = "wasm:control:req"
    
    @staticmethod
    def broker_res(broker_id: str) -> str: 
        return f"wasm:res:broker:{broker_id}"

class PayloadKey:
    JOB_ID = "job_id"
    METHOD_FUNC = "target_func" 
    PAYLOAD = "payload"
    RES_CHANNEL = "response_channel"
    TIER = "tier"
    WASM_PATH = "wasm_path"
    CODE = "code"
    VARS = "variables"
    DATA = "data"
    CONTEXT = "context"

class ResultKey:
    SUCCESS = "success"
    OUTPUT = "output"
    ERROR = "error"
    METRICS = "metrics"

class DphiBroker:
    def __init__(
        self, 
        request_stream: str = BrokerChannel.EXECUTE_STREAM, 
        timeout: float = 10.0, 
        target_auditor=None,
        tunnel_factory=None,
        **kwargs
    ):
        self.request_stream = request_stream
        self.control_channel = BrokerChannel.CONTROL_REQ
        self.timeout = timeout
        self.target_auditor = target_auditor
        self.tunnel_factory = tunnel_factory or TunnelFactory
        
        self.broker_id = uuid.uuid4().hex[:8]
        self.response_channel = BrokerChannel.broker_res(self.broker_id)
        self._pending_jobs: Dict[str, asyncio.Future] = {}
        
        self._listener_task: Optional[asyncio.Task] = None
        self._listener_client = None
        
        # 💡 [핵심 픽스] 구독(Subscribe) 완료 시점을 메인 로직에 알려주기 위한 이벤트 객체
        self._listener_ready = asyncio.Event()

    async def _ensure_listener_started(self):
        # 1. Self-Healing: 기존 태스크가 에러 등으로 종료(Done)되었으면 강제 초기화
        if self._listener_task is not None and self._listener_task.done():
            self._listener_task = None
            self._listener_ready.clear()

        # 2. 백그라운드 리스너 태스크 실행
        if self._listener_task is None:
            self._listener_task = asyncio.create_task(self._listen_responses())

        # 3. 💡 [핵심 픽스] 리스너가 Redis 채널 구독을 완벽히 마칠 때까지 대기
        try:
            # Redis 지연으로 인한 영구 데드락 방지 (최대 5초 대기)
            await asyncio.wait_for(self._listener_ready.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            log.error("[Broker] Listener failed to start within 5s. (Redis Connection Error?)")
            if self._listener_task and not self._listener_task.done():
                self._listener_task.cancel()
            self._listener_task = None
            self._listener_ready.clear()
            raise RuntimeError("Broker Failed to connect to Redis Pub/Sub")

    async def _listen_responses(self):
        try:
            self._listener_client = await self.tunnel_factory.get_isolated()
            pubsub = self._listener_client.pubsub()
            
            # 여기서 실제 네트워크 I/O 발생 -> 구독 완료를 보장
            await pubsub.subscribe(self.response_channel)
            
            # 💡 [핵심 픽스] 구독이 완전히 끝났음을 메인 쓰레드에 통보
            self._listener_ready.set()
            
            async for msg in pubsub.listen():
                if msg and msg["type"] == "message":
                    try:
                        result_data = json.loads(msg["data"])
                        job_id = result_data.get(PayloadKey.JOB_ID)
                        future = self._pending_jobs.get(job_id)
                        if future and not future.done():
                            future.set_result(result_data)
                            
                    except json.JSONDecodeError:
                        log.warning(f"[Broker] Unparseable response received on {self.response_channel}")
                    except Exception as e:
                        log.error(f"[Broker] Error processing response: {e}", exc_info=True)
                        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"[Broker] Listener crashed unexpectedly: {e}")
        finally:
            # 💡 [핵심 픽스] 연결 종료 시 상태 초기화 -> 다음 요청 시 우아하게 재연결 유도
            self._listener_ready.clear()
            
            with suppress(Exception):
                await pubsub.unsubscribe(self.response_channel)
                await pubsub.close()
                
            if self._listener_client:
                with suppress(Exception):
                    if hasattr(self._listener_client.state_store, 'aclose'):
                        await self._listener_client.state_store.aclose()
                    elif hasattr(self._listener_client.state_store, 'close'):
                        await self._listener_client.state_store.close()

    async def _dispatch_and_wait_async(
        self, 
        job_id: str, 
        payload: dict, 
        target_route: str = None, 
        timeout: Optional[float] = None
    ) -> ExecutionResult:
        # 이 시점에서 구독(Subscribe)이 100% 완료되었음을 보장받음
        await self._ensure_listener_started()
        
        route = target_route or self.request_stream
        tunnel = await self.tunnel_factory.get_default()
        method_name = payload.get(PayloadKey.METHOD_FUNC, 'unknown')
        
        payload[PayloadKey.RES_CHANNEL] = self.response_channel
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending_jobs[job_id] = future
        active_timeout = timeout if timeout is not None else self.timeout
        
        try:
            # 워커가 아무리 빨리 응답해도, 이미 수신부가 열려 있으므로 100% 수신됨
            if route == self.control_channel:
                await tunnel.publish(route, json.dumps(payload))
            else:
                await tunnel.state_store.xadd(route, {PayloadKey.DATA: json.dumps(payload)})
            
            async with asyncio.timeout(active_timeout):
                result_data = await future
                
            metrics = result_data.get(ResultKey.METRICS, {})
            edge_hash = result_data.get("edge_canonical_hash") or result_data.get("edge_hash")
            if metrics and self.target_auditor and hasattr(self.target_auditor, "project_state"):
                self.target_auditor.project_state(action=method_name, metrics=metrics)
            
            if result_data.get(ResultKey.SUCCESS):
                return ExecutionResult(success=True, output=result_data.get(ResultKey.OUTPUT, ""), metrics=metrics, edge_hash=edge_hash)
            else:
                return ExecutionResult(
                    success=False, 
                    output=result_data.get(ResultKey.OUTPUT, ""),
                    error=ExecutionError(result_data.get(ResultKey.ERROR, "Unknown Execution Error")),
                    metrics=metrics,
                    edge_hash=edge_hash
                )
        except asyncio.TimeoutError:
            timeout_msg = f"Remote execution timeout ({active_timeout}s)"
            log.warning(f"[{job_id[:8]}] {timeout_msg} on {route}. (Infinite loop or payload blocked)")
            return ExecutionResult(
                success=False,
                output=timeout_msg,
                error=ExecutionError(timeout_msg)
            )
        finally:
            self._pending_jobs.pop(job_id, None)

    def _build_context(self, base_context: Optional[dict], timeout: Optional[float]) -> dict:
        ctx = dict(base_context) if base_context is not None else dict(_flow_context.get() or {})
        if "timestamp" not in ctx:
            ctx["timestamp"] = int(time.time() * 1000)
            
        ctx["timeout"] = timeout or self.timeout
        return ctx

    async def update_policy(self, tier: str, context: Optional[dict] = None) -> bool:
        job_id = str(uuid.uuid4())
        active_context = self._build_context(context, self.timeout)
        payload = {
            PayloadKey.JOB_ID: job_id,
            PayloadKey.METHOD_FUNC: "update_policy",
            PayloadKey.TIER: tier.upper(),
            PayloadKey.CONTEXT: active_context
        }
        res = await self._dispatch_and_wait_async(job_id, payload, target_route=self.control_channel)
        return res.success

    async def invoke(
        self, 
        target_func: Union[str, DphiMethod], 
        payload: str, 
        wasm_path: Optional[str] = None, 
        tier: Optional[str] = None,
        context: Optional[dict] = None,
        timeout: Optional[float] = None
    ) -> ExecutionResult:
        job_id = str(uuid.uuid4())
        active_context = self._build_context(context, timeout)
        func_name = target_func.value if isinstance(target_func, Enum) else str(target_func)

        msg_payload = {
            PayloadKey.JOB_ID: job_id, 
            PayloadKey.METHOD_FUNC: func_name, 
            PayloadKey.PAYLOAD: payload,
            PayloadKey.CONTEXT: active_context
        }
        if wasm_path: msg_payload[PayloadKey.WASM_PATH] = wasm_path
        if tier: msg_payload[PayloadKey.TIER] = tier.upper()
            
        return await self._dispatch_and_wait_async(job_id, msg_payload, timeout=timeout)

    async def execute(
        self, 
        code: Union[str, dict],  
        variables: Mapping[str, Any] | None = None, 
        tier: Optional[str] = None,
        context: Optional[dict] = None,
        timeout: Optional[float] = None
    ) -> ExecutionResult:
        job_id = str(uuid.uuid4())
        active_context = self._build_context(context, timeout)
        
        target_wasm = None
        if isinstance(code, dict):
            method_func = DphiMethod.EXECUTE_DVM.value
            payload_data = code  
            target_wasm = "dvm.wasm"
        else:
            method_func = DphiMethod.EXECUTE_CODE.value
            payload_data = {
                PayloadKey.CODE: code, 
                PayloadKey.VARS: variables or {}
            }

        msg_payload = {
            PayloadKey.JOB_ID: job_id,
            PayloadKey.METHOD_FUNC: method_func, 
            PayloadKey.PAYLOAD: payload_data,
            PayloadKey.CONTEXT: active_context
        }
        
        if target_wasm: msg_payload[PayloadKey.WASM_PATH] = target_wasm
        if tier: msg_payload[PayloadKey.TIER] = tier.upper()
            
        return await self._dispatch_and_wait_async(job_id, msg_payload, timeout=timeout)
        
    async def close(self):
        task = getattr(self, '_listener_task', None)
        if task and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    def __del__(self):
        task = getattr(self, '_listener_task', None)
        if task and not task.done():
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass