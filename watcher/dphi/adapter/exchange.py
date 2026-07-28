# watcher.dphi.adapter.exchange
from dataclasses import dataclass
from typing import Any, Dict, List
import time

from arch.topos.bound.exchange.config import billing_config
from watcher.dphi.cgroup import Tier
from watcher.plane.emitter import get_emitter

log = get_emitter("exchange.adapter")

@dataclass
class TransactionReceipt:
    job_id: str
    topos_id: str
    unified_parity_hash: int  
    clearing_signatures: List[str] 
    fuel_consumed: int
    settlement_status: str

class ExchangeAdapter:
    def __init__(self, clearing_house_pub_key: str):
        self.clearing_house_pub = clearing_house_pub_key

    def _quantize_fuel_cost(self, fuel_consumed: int, tier: Tier) -> float:
        # config에 정의된 공통 과금 비율 사용
        base_cost = (fuel_consumed / billing_config.fuel_billing_unit) * billing_config.usd_per_billing_unit
        # Tier에 따른 멀티플라이어 적용 (안전한 Enum 비교)
        multiplier = 1.5 if tier == Tier.SYSTEM else 1.0
        return base_cost * multiplier

    def finalize_settlement(
        self, 
        entangled_state: dict, 
        signatures: List[str], 
        cost_metrics: dict, 
        tier: Tier = Tier.SYSTEM  # 문자열 대신 Enum 사용
    ) -> TransactionReceipt:
        ## Parity Triplet 추출
        parity = entangled_state.get("parity", {})
        unified_topos = parity.get("topos_id", f"unknown_batch_{int(time.time())}")
        unified_phase = parity.get("phase_id", 0) 
        
        ## 자원 소모량 계량 
        fuel_consumed = cost_metrics.get("fuel_consumed", 0)
        
        # 내부적으로 달러 환산 비용을 계산하거나 로깅할 수 있음 (정렬 완료된 _quantize_fuel_cost 활용)
        estimated_usd = self._quantize_fuel_cost(fuel_consumed, tier)
        
        ## 영수증(Receipt) 발급
        log.info(f"[Exchange Adapter] Settlement Finalized. Topos: {unified_topos}, Parity: {unified_phase}, Cost: ${estimated_usd:.6f}")
        return TransactionReceipt(
            job_id=unified_topos,
            topos_id=unified_topos,
            unified_parity_hash=unified_phase,
            clearing_signatures=signatures,
            fuel_consumed=fuel_consumed,
            settlement_status="COMMITTED_TO_NEXUS"
        )
        
    def generate_settlement_payload(self, receipt: TransactionReceipt) -> dict:
        return {
            "batch_id": receipt.job_id,
            "state_root": receipt.unified_parity_hash,
            "validators": receipt.clearing_signatures,
            "gas_used": receipt.fuel_consumed,
            "timestamp": int(time.time())
        }