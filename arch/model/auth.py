# xphi.arch.model.auth
from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional
from pydantic import BaseModel, Field, model_validator

class BrokerKey(str, Enum):
    """Pipeline 및 Metadata에 주입될 Key 상수 모음"""
    NODE_AUTH = "node_auth"
    FUEL_RECEIPT = "fuel_receipt"
    CLIENT_HOST = "client_host"
    FUEL_BUDGET = "fuel_budget"
    FUEL_CONSUMED = "fuel_consumed"
    AUDIT_HASH = "audit_hash"

class BrokerAction(str, Enum):
    """Broker Intent Action 상수"""
    LLM_COMPUTE = "LLM_COMPUTE"
    LLM_EMBEDDING = "LLM_EMBEDDING"

class NodeAuthPayload(BaseModel):
    """Broker에서 반환되는 Node Auth 데이터 규격"""
    tenant_id: str = Field(default="anonymous", description="부서, 프로젝트 또는 사용자 식별자")
    fuel_budget: float = Field(default=float('inf'), description="할당된 최대 토큰/연료")
    audit_hash: Optional[str] = Field(default=None, description="영수증 봉합을 위한 해시")
    is_enforced: bool = Field(default=False, description="엄격한 예산 통제 적용 여부")

    @model_validator(mode='after')
    def validate_enforcement_logic(self):
        """엄격 통제 모드(is_enforced)일 때의 정합성 검증"""
        if self.is_enforced:
            if self.tenant_id in ("anonymous", "internal_system"):
                raise ValueError(f"Strict enforcement requires a valid tenant, got: {self.tenant_id}")
            if self.fuel_budget < 0:
                raise ValueError("Fuel budget cannot be negative under strict enforcement.")
        return self

    def is_anonymous(self) -> bool:
        return self.tenant_id in ("anonymous", "internal_system")

    def has_sufficient_budget(self, requested_amount: float = 1.0) -> bool:
        """단순 예산 확인용"""
        return self.fuel_budget >= requested_amount