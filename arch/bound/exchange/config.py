# arch.bound.exchange.config
## @lineage: arch.topos.bound.exchange.config
## @lineage: topos.bound.config
from pydantic_settings import BaseSettings
from watcher.dphi.cgroup import Tier

class BillingSettings(BaseSettings):
    """과금 관련 비즈니스 설정"""
    fuel_billing_unit: int = 1_000_000
    usd_per_billing_unit: float = 0.01

    class Config:
        env_prefix = "BILLING_"

class TierPolicySettings(BaseSettings):
    """실행 환경 논리 자원 통제 설정"""
    fuel_to_seconds_ratio: int = 100_000_000
    
    # Premium (System)
    system_max_threads: int = 4
    system_max_node_capacity: int = 50
    system_max_simulation_ticks: int = 1000
    
    # Degraded (Standard)
    standard_max_threads: int = 1
    standard_max_node_capacity: int = 3
    standard_max_simulation_ticks: int = 50

    # Fallback
    fallback_mem_mb: int = 10
    fallback_fuel: int = 1_000_000

    class Config:
        env_prefix = "TIER_POLICY_"

# 싱글톤 인스턴스 생성 (애플리케이션 전체에서 공유)
billing_config = BillingSettings()
tier_config = TierPolicySettings()