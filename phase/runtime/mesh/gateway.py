# phase.runtime.mesh.gateway
import uuid
import time
import json
import re
from collections import defaultdict
from typing import Any, Dict, Optional

from arch.server.stream.schema import LogicStream as IngressLogicStream
from watcher.kernel.ledger import KernelLedger, LogicStream as KernelLogicStream, SealedKernel, LedgerRole
from watcher.plane.emitter import get_emitter

log = get_emitter("mesh.gateway", phase="KERNEL")

class TokenBucketLimiter:
    """메모리 기반의 유동적 토큰 버킷"""
    def __init__(self, capacity: int = 100, refill_rate: float = 10.0):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens: Dict[str, float] = defaultdict(lambda: float(capacity))
        self.last_refill: Dict[str, float] = defaultdict(time.time)

    def consume(self, identity_id: str, cost: int) -> bool:
        now = time.time()
        
        # 경과 시간에 따른 토큰 충전
        elapsed = now - self.last_refill[identity_id]
        self.tokens[identity_id] = min(
            self.capacity, 
            self.tokens[identity_id] + elapsed * self.refill_rate
        )
        self.last_refill[identity_id] = now

        # 비용 지불 가능 여부 확인
        if self.tokens[identity_id] >= cost:
            self.tokens[identity_id] -= cost
            return True
        return False


# =====================================================================
# [NEW] Defense Layer 2: Payload Sanitizer (Deep Content Inspection)
# =====================================================================
class PayloadSanitizer:
    """LLM이 생성한 파라미터 내부에 악성 패턴(명령어, 샌드박스 탈출 등)이 있는지 심층 검사"""
    
    # 1. OS Command Injection 패턴 (파이프, 세미콜론, 백틱, 서브쉘 등)
    OS_INJECTION_PATTERN = re.compile(r"(?:;|\||&&|`|\$\()")
    
    # 2. Path Traversal 패턴 (상위 디렉토리 이동 및 인코딩 우회)
    TRAVERSAL_PATTERN = re.compile(r"(?:\.\./|\.\.\\|%2e%2e%2f)", re.IGNORECASE)
    
    # 3. 프롬프트 인젝션 / 탈옥(Jailbreak) 키워드
    PROMPT_INJECTION_KEYWORDS = [
        "ignore previous instructions", 
        "ignore all previous instructions",
        "system override", 
        "forget all instructions",
        "bypass security"
    ]

    @classmethod
    def inspect(cls, payload: Any) -> bool:
        if not payload:
            return True
            
        try:
            # Payload를 평면화(Flatten)하여 검사하기 위해 JSON 문자열로 직렬화
            raw_text = json.dumps(payload).lower()
            
            if cls.OS_INJECTION_PATTERN.search(raw_text):
                log.warning("[Sanitizer] OS Command Injection signature detected.")
                return False
                
            if cls.TRAVERSAL_PATTERN.search(raw_text):
                log.warning("[Sanitizer] Path Traversal signature detected.")
                return False
                
            if any(kw in raw_text for kw in cls.PROMPT_INJECTION_KEYWORDS):
                log.warning("[Sanitizer] Prompt Injection / Jailbreak keyword detected.")
                return False
                
            return True
        except Exception as e:
            log.error(f"[Sanitizer] Failed to parse payload: {e}. Defaulting to BLOCKED.")
            return False  # 파싱 불가 페이로드는 안전을 위해 차단 (Fail-Closed)

class ToposGateway:
    """@desc: Compliant middleware & Adapter bridging external Ingress to the unified KernelStore"""
    def __init__(self, store: Optional[KernelLedger] = None):
        self.store = store or KernelLedger()
        self.limiter = TokenBucketLimiter(capacity=100, refill_rate=5.0)
        self.action_costs = {
            "READ_RESOURCE": 1,
            "INVOKE_TOOL": 5,
            "INJECT_QUARANTINE_RULE": 10,
            "INJECT_META_RULE": 10,
            "SECURITY_TENSION_ALERT": 0  # 시스템 알럿은 비용 면제
        }

    async def authorize_ingress(self, stream: IngressLogicStream) -> bool:
        """@desc: Structural adapter for Ingress validation"""
        action_id = str(stream.meta.stream_id)
        action = stream.payload.intent.value
        payload = stream.payload.parameters
        metadata = {
            "is_authenticated": stream.identity.is_authenticated,
            "stateless_token": stream.identity.stateless_token_id,
            "client_ip": stream.meta.client_ip,
            "protocol_version": stream.meta.original_protocol.value
        }
        return await self.authorize(action_id=action_id, action=action, payload=payload, metadata=metadata)

    async def authorize(self, action_id: str, action: str, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        @desc: The single choke-point for agent action validation.
        @flow: Quota -> Sanitizer -> KernelLogicStream -> KernelStore(propose_and_seal) -> WASM -> Boolean Signal
        """
        if metadata is None:
            metadata = {}

        ## Step 1: Quota Check (Rate Limiting)
        ## 식별자가 없으면 IP, IP도 없으면 익명(anonymous)으로 처리하여 토큰 차감
        identity_id = metadata.get("stateless_token") or metadata.get("client_ip") or "anonymous_agent"
        cost = self.action_costs.get(action, 1)
        
        if not self.limiter.consume(identity_id, cost):
            log.warning(f"[Gateway] BLOCKED (Quota Exceeded): Identity '{identity_id}' exhausted tokens for action '{action}'.")
            return False

        ## Step 2: Deep Content Inspection (Payload Sanitizer)
        ## 비용 검증을 통과한 유효한 트래픽에 대해서만 정규식/시맨틱 분석 수행
        if not PayloadSanitizer.inspect(payload):
            log.critical(f"[Gateway] BLOCKED (Semantic Breach): Malicious signature detected in payload for action '{action}'.")
            return False

        ## Step 3: Adapt & Forward to WASM Kernel
        kernel_stream = KernelLogicStream(
            id=action_id or str(uuid.uuid4()),
            action=action,
            payload=payload,
            metadata=metadata
        )

        log.debug(f"[Gateway] Forwarding stream {kernel_stream.id} to KernelStore for WASM validation.")
        try:
            sealed_kernel: Optional[SealedKernel] = await self.store.propose_and_seal(kernel_stream)
            if sealed_kernel is not None:
                log.info(f"[Gateway] AUTHORIZED: Stream {kernel_stream.id} successfully sealed into {sealed_kernel.kernel_id}.")
                return True
            else:
                if hasattr(self.store, 'role') and self.store.role == LedgerRole.FOLLOWER:
                    log.info(f"[Gateway] PROPOSED: Stream {kernel_stream.id} delegated to Mempool (FOLLOWER mode).")
                    return True  # Acknowledged into the consensus pipeline
                else:
                    log.warning(f"[Gateway] BLOCKED: Stream {kernel_stream.id} rejected by WASM Kernel Spatial Fence.")
                    return False
        except Exception as e:
            log.error(f"[Gateway] Kernel pipeline failed with exception: {e}. Defaulting to BLOCKED.")
            return False

class BypassGateway(ToposGateway):
    async def authorize(self, action_id: str, action: str, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        log.debug(f"[Gateway: Bypass] Auto-authorizing action {action_id} (DEV MODE).")
        return True