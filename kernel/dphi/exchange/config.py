# kernel.dphi.exchange.config
from pydantic import Field
from pydantic_settings import BaseSettings
from typing import Dict
from kernel.dphi.cgroup import Tier

class TierPolicySettings(BaseSettings):
    """[Layer 1: Infrastructure] 실행 환경 논리 자원 통제 설정 (기존과 동일)"""
    fuel_to_seconds_ratio: int = 100_000_000
    
    ## Premium (System)
    system_max_threads: int = 4
    system_max_node_capacity: int = 50
    system_max_simulation_ticks: int = 1000
    
    ## Degraded (Standard)
    standard_max_threads: int = 1
    standard_max_node_capacity: int = 3
    standard_max_simulation_ticks: int = 50

    ## Fallback
    fallback_mem_mb: int = 10
    fallback_fuel: int = 1_000_000

    class Config:
        env_prefix = "TIER_POLICY_"


class BillingSettings(BaseSettings):
    """[Layer 2: Pricing] 기본 과금 단가 및 할증(Multiplier) 설정"""
    fuel_billing_unit: int = 1_000_000
    usd_per_billing_unit: float = 0.01  # 베이스라인 원가
    
    ## tracker.billing과 연결되는 티어별 과금 할증률
    ## SYSTEM 티어는 고급 보안 자원을 쓰므로 1.5배 청구
    tier_multipliers: Dict[str, float] = Field(
        default_factory=lambda: {
            "SYSTEM": 1.5,
            "STANDARD": 1.0,
            "FALLBACK": 0.5
        }
    )

    class Config:
        env_prefix = "BILLING_"


class TreasurySettings(BaseSettings):
    """[Layer 3: Economy] 발생한 수익의 분배(Revenue Split) 비율 설정 (tracker.billing 전용)"""
    ## 합계가 1.0(100%)이 되도록 유지
    operator_share: float = 0.70      # 인프라 제공자
    gov_agent_share: float = 0.15     # 거버넌스 에이전트 풀
    sec_agent_share: float = 0.15     # 보안 검증 에이전트 풀

    class Config:
        env_prefix = "TREASURY_"

billing_config = BillingSettings()
tier_config = TierPolicySettings()
treasury_config = TreasurySettings()