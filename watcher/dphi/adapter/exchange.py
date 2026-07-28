# watcher.dphi.adapter.exchange
from dataclasses import dataclass
from typing import Any, Dict, List
import time
from watcher.dphi.cgroup import Tier
from watcher.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("exchange.adapter")

@dataclass
class TransactionReceipt:
    job_id: str
    topos_id: str
    unified_parity_hash: int  # XOR 연산으로 얽힌 최종 상태 해시
    clearing_signatures: List[str] # M-of-N 서명 내역
    fuel_consumed: int
    settlement_status: str

class ExchangeAdapter:
    def __init__(self, clearing_house_pub_key: str):
        self.clearing_house_pub = clearing_house_pub_key

    def _quantize_fuel_cost(self, fuel_consumed: int, tier: str) -> float:
        base_rate = 0.00001
        multiplier = 1.5 if tier == Tier.SYSTEM.value else 1.0
        return fuel_consumed * base_rate * multiplier

    def finalize_settlement(
        self, 
        entangled_state: dict, 
        signatures: List[str], 
        cost_metrics: dict, 
        tier: str = "SYSTEM"
    ) -> TransactionReceipt:
        ## Parity Triplet 추출
        parity = entangled_state.get("parity", {})
        unified_topos = parity.get("topos_id", f"unknown_batch_{int(time.time())}")
        unified_phase = parity.get("phase_id", 0) # XOR 얽힘 해시
        
        ## 자원 소모량 계량 (가스비 산정을 위함)
        fuel_consumed = cost_metrics.get("fuel_consumed", 0)
        
        ## 영수증(Receipt) 발급
        log.info(f"[Exchange Adapter] Settlement Finalized. Topos: {unified_topos}, Parity: {unified_phase}")
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