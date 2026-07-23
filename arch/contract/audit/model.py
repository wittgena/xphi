# topos.audit.contract.model
from typing import Any, ClassVar
from pydantic import BaseModel, ConfigDict, Field

class LogstEvent(BaseModel):
    """비정형 로그 스트리밍을 위한 단일 이벤트 모델"""
    timestamp: str = Field(description="ISO 8601 format timestamp")
    level: str | None = Field(default=None, description="Log severity level")
    message: str | None = Field(default=None, description="Log message content")
    trace_id: str | None = Field(default=None, description="Trace identifier")
    
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="allow")

class LogstUsage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

class LogstResponseBody(BaseModel):
    usage: LogstUsage | None = None
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="allow")

class LogstRequest(BaseModel):
    time: str
    uri: str
    verb: str
    api_version: str | None = None
    ip_address: str | None = None
    headers: dict[str, str] | None = None
    body: Any = None

class LogstResponse(BaseModel):
    time: str
    status: int
    body: LogstResponseBody | None = None

class LogstEventPayload(BaseModel):
    """LLM 과금 및 API 트랜잭션 수집 래퍼 - 테넌트 컨텍스트 동기화 및 실시간 과금 정책 평가(Quota/Rate Limit)"""
    request: LogstRequest
    response: LogstResponse
    user_id: str | None = None
    company_id: str | None = None
    metadata: dict[str, Any] | None = None

class AuditEvent(BaseModel):
    message: str
    actor: str | None = None
    action: str | None = None
    source: str | None = None
    target: str | None = None
    status: str | None = None

class AuditLogRequest(BaseModel):
    """[Integrity 보증] 보안/규제 준수 감사 로그 요청 - 수신 직후 데이터 정규화(Canonicalization) 및 위상 기반 해싱을 거침"""
    event: AuditEvent
    verbose: bool = False
    sign_local: bool = False

class AuditEnvelope(BaseModel):
    event: AuditEvent
    received_at: str

class AuditResult(BaseModel):
    envelope: AuditEnvelope
    hash: str
    membership_proof: str | None = None
    consistency_proof: list[str] | None = None

class AuditLogResponse(BaseModel):
    """결정론적 해시 연산 및 위변조 방지 머클 증명을 포함한 응답"""
    request_id: str
    request_time: str | None = None
    response_time: str | None = None
    status: str
    result: AuditResult