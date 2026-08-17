# watcher.tracer.transport
import uuid
import asyncio
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Any

import httpx

from watcher.plane.emitter import flow_scope, get_emitter

log = get_emitter("tracer.transport")

@dataclass
class InjectPolicy:
    """네트워크 경계에서 런타임에 개입할 능동형 제어 정책 컨테이너"""
    target_path: str
    latency_sec: float = 0.0                                          # 인위적 네트워크 지연 주입
    mock_status: Optional[int] = None                                 # 특정 HTTP 상태 코드로 강제 조기 응답(Short-circuit)
    mock_body: Optional[Any] = None                                   # 강제 페이로드 반환
    request_mutator: Optional[Callable[[httpx.Request], None]] = None # 요청 헤더/바디 동적 변조 (Tampering)


class ActiveFlowTracer(httpx.AsyncBaseTransport):
    """
    httpx.AsyncBaseTransport를 래핑하는 능동형 인터셉터.
    애플리케이션 계층의 스트림(Stream) 생명주기를 파괴하지 않고 안전하게 네트워크 패킷을 통제합니다.
    """
    def __init__(
        self, 
        underlying_transport: httpx.AsyncBaseTransport, 
        policies: Dict[str, InjectPolicy] = None
    ):
        self.underlying = underlying_transport
        self.policies = policies or {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        flow_id = f"transport_{uuid.uuid4().hex[:8]}"
        request.headers["x-active-flow-id"] = flow_id
        path_str = request.url.path

        # 1. 적용할 주입 정책(Policy) 스캔
        policy = next((p for p in self.policies.values() if p.target_path in path_str), None)

        with flow_scope(flow_id=flow_id, phase="TRANSPORT_TX", bound="injector"):
            log.info(f"[Transport:TX] {request.method} {request.url}")
            
            # 2. 패킷 변조 주입 (Mutation)
            if policy and policy.request_mutator:
                policy.request_mutator(request)
                log.warning(f"  [!] Active Mutation applied by policy for: {policy.target_path}")

            log.debug(f"  └─ Headers: {dict(request.headers)}")

        # 3. 네트워크 지연 주입 (Latency Bottleneck)
        if policy and policy.latency_sec > 0:
            log.warning(f"[Transport:Inject] Halting stream for {policy.latency_sec}s (Artificial Latency)")
            await asyncio.sleep(policy.latency_sec)

        # 자체 타이머 측정 시작 (httpx의 애플리케이션 계층 elapsed에 의존하지 않음)
        start_time = time.perf_counter()

        # 4. 강제 응답 주입 (Short-circuit Mocking)
        if policy and policy.mock_status:
            log.warning(f"[Transport:Inject] Short-circuiting! Returning mock status {policy.mock_status}")
            response = httpx.Response(
                status_code=policy.mock_status,
                json=policy.mock_body if isinstance(policy.mock_body, (dict, list)) else None,
                content=policy.mock_body if isinstance(policy.mock_body, (str, bytes)) else None,
                request=request
            )
        else:
            # 5. 실제 네트워크망으로 패킷 전송
            response = await self.underlying.handle_async_request(request)

        # 자체 타이머 종료
        elapsed_sec = time.perf_counter() - start_time

        with flow_scope(flow_id=flow_id, phase="TRANSPORT_RX", bound="injector"):
            # 주의: 여기서 response.aread()를 호출하지 않습니다. 
            # 바디 읽기는 애플리케이션 계층(HttpFlowTracer 또는 Workflow)에 위임하여 스트림을 보존합니다.
            status_log = f"[Transport:RX] {response.status_code} {response.reason_phrase} (in {elapsed_sec:.3f}s)"
            
            if response.status_code >= 400 or policy is not None:
                log.warning(f"{status_log} - Stream kept intact for upstream layer")
            else:
                log.info(status_log)

        return response

    async def aclose(self):
        """기저 Transport 객체의 비동기 자원 안전 해제"""
        await self.underlying.aclose()