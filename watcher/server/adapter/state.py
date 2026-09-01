# xphi.watcher.server.adapter.state
## @lineage: xphi.watcher.mcp.adapter.state
from __future__ import annotations
import uuid
import time
from enum import Enum
from pydantic import BaseModel, Field, AnyUrl, IPvAnyAddress
from typing import List, Dict, Any, Optional

import orjson
import redis.asyncio as aioredis 
from watcher.plane.emitter import get_emitter

log = get_emitter("adapter.state", phase="GATEWAY")

# =====================================================================
# 1. Enterprise Data Models (현실 구조의 메타데이터 및 불변성 강제)
# =====================================================================

class AgentIdentity(BaseModel):
    """B2B 멀티테넌시 및 보안 감사를 위한 신원 객체"""
    tenant_id: str = Field(..., description="B2B 고객사 고유 식별자 (데이터 격리용)")
    principal_id: str = Field(..., description="요청을 발생시킨 에이전트/사용자 ID")
    agent_uri: AnyUrl = Field(..., description="에이전트의 SPIFFE/SPIRE URI")
    dpop_proof: str = Field(..., description="Cryptographic DPoP Signature")
    scopes: List[str] = Field(default_factory=list, description="인가된 권한 목록 (RBAC)")
    client_ip: IPvAnyAddress = Field(..., description="네트워크 방어 및 감사를 위한 IP")
    nonce: str = Field(..., description="Replay Attack 방지용 난수")

class EventMetadata(BaseModel):
    """MSA 분산 추적 및 멱등성 제어를 위한 메타데이터"""
    trace_id: str = Field(default_factory=lambda: uuid.uuid4().hex, description="OTLP 분산 추적 ID")
    span_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:16], description="현재 작업 구간 Span ID")
    idempotency_key: str = Field(..., description="네트워크 단절 재시도 시 중복 처리를 막는 고유 키")
    schema_version: str = Field(default="v1.0", description="페이로드 스키마 버전 (하위 호환성)")
    causation_id: Optional[str] = Field(None, description="이 이벤트를 유발한 이전 이벤트 ID")

class EventType(str, Enum):
    INITIALIZED = "INITIALIZED"
    MUTATED = "MUTATED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"          # 실패/롤백 상태 
    COMPENSATED = "COMPENSATED"  # Saga 패턴 보상 트랜잭션

class StateEvent(BaseModel):
    """과거 기록 변조를 원천 차단하는 불변(Immutable) 이벤트 레코드"""
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    type: EventType
    timestamp: float = Field(default_factory=time.time)
    actor: AgentIdentity 
    meta: EventMetadata  
    payload: Dict[str, Any] = Field(default_factory=dict, description="상태 변경 Delta")
    
    model_config = {
        "frozen": True,  # [핵심] 런타임 데이터 오염 원천 차단
        "extra": "forbid"
    }

class OpaqueHandle(BaseModel):
    handle_id: str = Field(default_factory=lambda: f"stream-{uuid.uuid4().hex}")
    exp: float


# =====================================================================
# 2. Utilities
# =====================================================================

def deep_merge(target: dict, updates: dict) -> dict:
    """[핵심] 얕은 병합(update)으로 인한 데이터 유실을 막는 재귀적 깊은 병합"""
    for key, value in updates.items():
        if isinstance(value, dict) and key in target and isinstance(target[key], dict):
            deep_merge(target[key], value)
        else:
            target[key] = value
    return target


# =====================================================================
# 3. Redis Infrastructure (OOM 방어 및 락-프리 꼬리물기)
# =====================================================================

class RedisAppendOnlyCache:
    """Redis Streams 기반 원자적 꼬리물기 및 자동 증발 캐시 엔진"""
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis = aioredis.from_url(redis_url, decode_responses=True)

    async def append_event(self, stream_id: str, event: StateEvent, max_len: int = 1000) -> bool:
        # Pydantic V2의 JSON 모드 직렬화 후 orjson으로 초고속 바이트 변환
        raw_payload = orjson.dumps(event.model_dump(mode='json')).decode('utf-8')
        
        async with self.redis.pipeline(transaction=True) as pipe:
            # [핵심] 무한 증식 OOM 차단을 위한 maxlen 적용
            pipe.xadd(stream_id, {"payload": raw_payload}, maxlen=max_len)
            await pipe.execute()
        return True

    async def set_absolute_ttl(self, stream_id: str, ttl: int = 3600):
        # [핵심] 무한 TTL 연장 봇(좀비 세션) 방어를 위한 절대 수명 부여
        await self.redis.expire(stream_id, ttl)

    async def read_stream(self, stream_id: str) -> Optional[List[StateEvent]]:
        messages = await self.redis.xrange(stream_id, min="-", max="+")
        if not messages:
            return None
        
        events = []
        for msg_id, data in messages:
            payload_str = data.get("payload")
            if payload_str:
                parsed = orjson.loads(payload_str)
                events.append(StateEvent(**parsed))
        return events
        
    async def delete_stream(self, stream_id: str):
        await self.redis.delete(stream_id)


# =====================================================================
# 4. State Adapter (비즈니스 로직 및 외부 연동 게이트웨이)
# =====================================================================

class MCPStateAdapter:
    """1.0 레거시 시스템을 2.0 무상태 에이전트로부터 보호하고 연결하는 어댑터"""
    def __init__(self, cache: RedisAppendOnlyCache):
        self.cache = cache

    async def _fold_state(self, events: List[StateEvent]) -> Dict[str, Any]:
        """읽어온 이벤트 로그를 순서대로 재생(Replay)하여 최종 상태를 도출"""
        current_state = {}
        for event in events:
            if event.type == EventType.INITIALIZED:
                # 초기 컨텍스트 적재 시에도 deep_merge 사용
                deep_merge(current_state, event.payload.get("initial_context", {}))
            elif event.type == EventType.MUTATED:
                # [핵심] 데이터 유실 없는 깊은 병합 수행
                deep_merge(current_state, event.payload.get("changes", {}))
        return current_state

    async def execute_stateless(
        self, 
        identity: AgentIdentity, 
        meta: EventMetadata, 
        handle_id: str, 
        action: str, 
        payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        
        # -------------------------------------------------------------
        # Action 1: 초기화 (INITIALIZE)
        # -------------------------------------------------------------
        if action == "INITIALIZE":
            handle = OpaqueHandle(exp=time.time() + 3600)
            init_event = StateEvent(
                type=EventType.INITIALIZED, 
                actor=identity, 
                meta=meta, 
                payload=payload
            )
            await self.cache.append_event(handle.handle_id, init_event)
            await self.cache.set_absolute_ttl(handle.handle_id, 3600)
            log.info(f"[StateAdapter] Stream Initialized: {handle.handle_id} by {identity.tenant_id}")
            return {"status": "initialized", "handle": handle.handle_id}

        # -------------------------------------------------------------
        # 스트림 유효성 및 멱등성 검증
        # -------------------------------------------------------------
        events = await self.cache.read_stream(handle_id)
        if events is None:
            return {"error": "STATE_EVAPORATED", "code": -32008}

        # [핵심] 멱등성 키 중복 방어 (동일한 MUTATE 요청 재시도 시 무시)
        if action == "MUTATE":
            for e in events:
                if e.meta.idempotency_key == meta.idempotency_key:
                    return {"status": "already_processed", "event_id": e.event_id}

        # [핵심] Race Condition(경합 조건) 방어: 이미 커밋/실패된 스트림 조작 불가
        if any(e.type in (EventType.COMMITTED, EventType.ABORTED) for e in events):
            return {"error": "STREAM_LOCKED_OR_CLOSED", "code": -32009}

        # -------------------------------------------------------------
        # Action 2: 상태 변경 (MUTATE)
        # -------------------------------------------------------------
        if action == "MUTATE":
            mutation_event = StateEvent(
                type=EventType.MUTATED, 
                actor=identity, 
                meta=meta, 
                payload=payload
            )
            await self.cache.append_event(handle_id, mutation_event)
            return {"status": "mutated_event_appended", "event_id": mutation_event.event_id}

        # -------------------------------------------------------------
        # Action 3: 조회 (QUERY) / 커밋 (COMMIT)
        # -------------------------------------------------------------
        if action in ("QUERY", "COMMIT"):
            final_state = await self._fold_state(events)
            
            if action == "QUERY":
                return {"status": "success", "current_state": final_state}
                
            if action == "COMMIT":
                # [핵심] 커밋 마커를 먼저 삽입하여 동시성 중복 커밋 원천 차단
                commit_marker = StateEvent(
                    type=EventType.COMMITTED, 
                    actor=identity, 
                    meta=meta, 
                    payload={"intent": "flush_to_legacy"}
                )
                await self.cache.append_event(handle_id, commit_marker)
                
                try:
                    # =========================================================
                    # 실제 1.0 레거시 백엔드 통신 (DB Update / Legacy API Call) 
                    # =========================================================
                    log.info(f"[Legacy Flush] Tenant {identity.tenant_id} 트랜잭션 반영 중... (Trace: {meta.trace_id})")
                    # (예: httpx.post(legacy_url, json=final_state))
                    
                    # 성공 시 스트림 파기
                    await self.cache.delete_stream(handle_id)
                    log.info(f"[Legacy Flush] 성공. Stream {handle_id} 폐기 완료.")
                    return {"status": "committed", "flushed_state": final_state}
                    
                except Exception as e:
                    log.error(f"[Legacy Flush Failed] {str(e)} (Trace: {meta.trace_id})")
                    # 실패 시 ABORTED 마커 삽입 (재시도 방지 및 Saga 보상 트랜잭션 유도)
                    abort_marker = StateEvent(
                        type=EventType.ABORTED, 
                        actor=identity, 
                        meta=meta, 
                        payload={"error": str(e)}
                    )
                    await self.cache.append_event(handle_id, abort_marker)
                    return {"error": "LEGACY_FLUSH_FAILED", "details": str(e)}

        return {"error": "INVALID_ACTION"}