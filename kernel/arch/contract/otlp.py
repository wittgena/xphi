# kernel.arch.contract.otlp
## @lineage: watcher.kernel.audit.contract.otlp
## @lineage: arch.contract.audit.otlp
from typing import Any, List, Optional, Dict
from pydantic import BaseModel, Field

class KeyValue(BaseModel):
    key: str
    value: dict[str, Any]
    
    # OTLP AnyValue 표준에 따른 헬퍼 프로퍼티
    @property
    def get_value(self) -> Any:
        # stringValue, intValue, doubleValue 등에서 실제 값을 안전하게 추출
        if not self.value: return None
        return next(iter(self.value.values()), None)

class LogRecord(BaseModel):
    timeUnixNano: str
    severityNumber: Optional[int] = None
    severityText: Optional[str] = None
    body: dict[str, Any] = Field(default_factory=dict)
    attributes: List[KeyValue] = Field(default_factory=list)
    traceId: Optional[str] = None
    spanId: Optional[str] = None

class ScopeLogs(BaseModel):
    scope: dict[str, Any] = Field(default_factory=dict)
    logRecords: List[LogRecord] = Field(default_factory=list)

class ResourceLogs(BaseModel):
    resource: dict[str, Any] = Field(default_factory=dict)
    scopeLogs: List[ScopeLogs] = Field(default_factory=list)

class ExportLogsServiceRequest(BaseModel):
    """
    [OTLP/HTTP v1.1.0 표준 로깅 페이로드]
    """
    resourceLogs: List[ResourceLogs] = Field(default_factory=list)

    def extract_genai_metrics(self) -> Dict[str, Any]:
        """
        @desc: 무거운 OTLP 페이로드를 순회하며 LLM 과금(Billing)에 필요한 핵심 메타데이터만 추출합니다.
        """
        metrics = {"usage": {}}
        for res_log in self.resourceLogs:
            # Resource 레벨 속성 (예: tenant_id)
            for attr in res_log.resource.get("attributes", []):
                if isinstance(attr, dict):
                    kv = KeyValue(**attr)
                else:
                    kv = attr
                if kv.key == "tenant.id":
                    metrics["tenant_id"] = kv.get_value

            # LogRecord 레벨 속성 (예: tokens, model)
            for scope in res_log.scopeLogs:
                for record in scope.logRecords:
                    for attr in record.attributes:
                        if attr.key == "gen_ai.request.model" or attr.key == "llm.model":
                            metrics["model"] = attr.get_value
                        elif "prompt_tokens" in attr.key or "input_tokens" in attr.key:
                            metrics["usage"]["prompt_tokens"] = attr.get_value
                        elif "completion_tokens" in attr.key or "output_tokens" in attr.key:
                            metrics["usage"]["completion_tokens"] = attr.get_value
                        elif "reasoning_tokens" in attr.key:
                            metrics["usage"]["reasoning_tokens"] = attr.get_value
        return metrics