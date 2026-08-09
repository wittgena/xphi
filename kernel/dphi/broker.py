# kernel.dphi.broker
import json
import uuid
import asyncio
from enum import Enum
from typing import Optional, Any, Mapping, Union, Dict

from arch.topos.tunnel.factory import TunnelFactory
from kernel.bind.inter.protocol import ExecutionResult, ExecutionError
from kernel.dphi.method import DphiMethod
from watcher.plane.emitter import get_emitter, _flow_context

log = get_emitter("dphi.broker")

class BrokerChannel:
    EXECUTE_STREAM = "wasm:execute:stream"
    CONTROL_REQ = "wasm:control:req"
    @staticmethod
    def broker_res(broker_id: str) -> str: return f"wasm:res:broker:{broker_id}"

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
    def __init__(self, request_stream: str = BrokerChannel.EXECUTE_STREAM, timeout: float = 10.0, target_auditor=None):
        self.request_stream = request_stream
        self.control_channel = BrokerChannel.CONTROL_REQ
        self.timeout = timeout
        self.target_auditor = target_auditor
        
        # [핵심 1] 브로커 고유 ID 및 단일 응답 채널 설정
        self.broker_id = uuid.uuid4().hex[:8]
        self.response_channel = BrokerChannel.broker_res(self.broker_id)
        
        # [핵심 2] 비동기 Future 라우팅 테이블
        self._pending_jobs: Dict[str, asyncio.Future] = {}
        
        # 백그라운드 리스너 상태
        self._listener_task: Optional[asyncio.Task] = None
        self._listener_client = None

    async def _ensure_listener_started(self):
        """백그라운드 응답 리스너를 지연 초기화(Lazy init) 방식으로 단 1번만 실행합니다."""
        if self._listener_task is None:
            self._listener_task = asyncio.create_task(self._listen_responses())

    async def _listen_responses(self):
        """단 1개의 격리된 커넥션으로 수만 개의 응답을 멀티플렉싱(Multiplexing)하여 수신합니다."""
        self._listener_client = await TunnelFactory.get_isolated()
        pubsub = self._listener_client.pubsub()
        await pubsub.subscribe(self.response_channel)
        
        try:
            async for msg in pubsub.listen():
                if msg and msg["type"] == "message":
                    try:
                        result_data = json.loads(msg["data"])
                        job_id = result_data.get(PayloadKey.JOB_ID)
                        
                        # [핵심 3] 수신된 응답의 job_id로 대기 중인 Future를 찾아 결과를 주입(Set)하고 깨움
                        future = self._pending_jobs.get(job_id)
                        if future and not future.done():
                            future.set_result(result_data)
                            
                    except json.JSONDecodeError:
                        log.warning(f"[Broker] Unparseable response received on {self.response_channel}")
                    except Exception as e:
                        log.error(f"[Broker] Error processing response: {e}", exc_info=True)
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(self.response_channel)
            await pubsub.close()
            if hasattr(self._listener_client.state_store, 'aclose'):
                await self._listener_client.state_store.aclose()
            elif hasattr(self._listener_client.state_store, 'close'):
                await self._listener_client.state_store.close()

    # [수정] timeout 파라미터를 추가하여 가변적인 타임아웃 주입 허용
    async def _dispatch_and_wait_async(
        self, 
        job_id: str, 
        payload: dict, 
        target_route: str = None, 
        timeout: Optional[float] = None
    ) -> ExecutionResult:
        await self._ensure_listener_started()
        
        route = target_route or self.request_stream
        tunnel = await TunnelFactory.get_default() # 송신은 공용 풀(Pool) 사용
        
        method_name = payload.get(PayloadKey.METHOD_FUNC, 'unknown')
        
        # 페이로드에 브로커의 단일 수신 채널 명시
        payload[PayloadKey.RES_CHANNEL] = self.response_channel
        
        # [핵심 4] Future 객체를 생성하여 이벤트 루프에서 블로킹 없이 대기
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending_jobs[job_id] = future
        
        # 외부에서 주입된 타임아웃이 있으면 우선 적용, 없으면 브로커 기본 타임아웃 사용
        active_timeout = timeout if timeout is not None else self.timeout
        
        try:
            # 1. 메시지 발송
            if route == self.control_channel:
                await tunnel.publish(route, json.dumps(payload))
            else:
                await tunnel.state_store.xadd(route, {PayloadKey.DATA: json.dumps(payload)})
            
            # 2. 결과 대기 (리스너가 future.set_result()를 호출할 때까지 가변 타임아웃 적용)
            async with asyncio.timeout(active_timeout):
                result_data = await future
                
            # 3. 결과 후처리
            metrics = result_data.get(ResultKey.METRICS, {})
            if metrics and self.target_auditor and hasattr(self.target_auditor, "project_state"):
                self.target_auditor.project_state(action=method_name, metrics=metrics)
            
            if result_data.get(ResultKey.SUCCESS):
                return ExecutionResult(success=True, output=result_data.get(ResultKey.OUTPUT, ""), metrics=metrics)
            else:
                return ExecutionResult(
                    success=False, 
                    output=result_data.get(ResultKey.OUTPUT, ""),
                    error=ExecutionError(result_data.get(ResultKey.ERROR, "Unknown Execution Error")),
                    metrics=metrics
                )
                
        except asyncio.TimeoutError:
            # 에러 메시지에도 가변 타임아웃(active_timeout)을 명시하여 원인 파악 용이
            log.error(f"[{job_id[:8]}] Remote execution timeout ({active_timeout}s) on {route}")
            return ExecutionResult(success=False, error=ExecutionError(f"Remote execution timeout ({active_timeout}s)"))
        finally:
            # [안전장치] 성공/실패/타임아웃 여부와 관계없이 Future를 메모리에서 제거
            self._pending_jobs.pop(job_id, None)

    async def update_policy(self, tier: str, context: Optional[dict] = None) -> bool:
        job_id = str(uuid.uuid4())
        active_context = context if context is not None else _flow_context.get()
        payload = {
            PayloadKey.JOB_ID: job_id,
            PayloadKey.METHOD_FUNC: "update_policy",
            PayloadKey.TIER: tier.upper(),
            PayloadKey.CONTEXT: active_context
        }
        res = await self._dispatch_and_wait_async(job_id, payload, target_route=self.control_channel)
        return res.success

    # [수정] invoke에 timeout 파라미터 노출
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
        active_context = context if context is not None else _flow_context.get()
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

    # [수정] execute에 timeout 파라미터 노출 (선택적)
    async def execute(
        self, 
        code: Union[str, dict],  
        variables: Mapping[str, Any] | None = None, 
        tier: Optional[str] = None,
        context: Optional[dict] = None,
        timeout: Optional[float] = None
    ) -> ExecutionResult:
        job_id = str(uuid.uuid4())
        active_context = context if context is not None else _flow_context.get()
        
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
        """브로커 파괴 시 백그라운드 리스너를 정리합니다."""
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass