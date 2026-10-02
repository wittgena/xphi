# xphi.kernel.wasm.quota
import logging
from dataclasses import dataclass
from enum import Enum
from xphi.watcher.plane.emitter import get_emitter

try:
    import wasmtime
except ImportError:
    wasmtime = None

log = get_emitter("wasm.quota")

class Tier(Enum):
    STANDARD = "STANDARD"   # 일반 유저 코드 및 비즈니스 로직
    SYSTEM = "SYSTEM"       # 관리자 작업 (무결성 검증, 대규모 해싱 등)
    UNLIMITED = "UNLIMITED" # 로컬 디버깅 및 프로파일링 전용

@dataclass
class QuotaPolicy:
    max_memory_bytes: int
    cpu_fuel_quota: int
    tier: Tier

    @classmethod
    def standard(cls) -> 'QuotaPolicy':
        return cls(
            max_memory_bytes=64 * 1024 * 1024,
            cpu_fuel_quota=10_000_000,
            tier=Tier.STANDARD
        )

    @classmethod
    def system(cls) -> 'QuotaPolicy':
        return cls(
            max_memory_bytes=256 * 1024 * 1024,
            cpu_fuel_quota=2_000_000_000,
            tier=Tier.SYSTEM
        )

    @classmethod
    def custom(cls, mem_mb: int, fuel: int) -> 'QuotaPolicy':
        return cls(
            max_memory_bytes=mem_mb * 1024 * 1024,
            cpu_fuel_quota=fuel,
            tier=Tier.UNLIMITED
        )

class WasmQuotaManager:
    """@desc: In-process resource controller (Data Plane) for a Wasm instance"""
    def __init__(self, policy_name: str, policy: QuotaPolicy = None):
        if wasmtime is None:
            raise ImportError("The 'wasmtime' module is required to use WasmQuotaManager.")
            
        self.policy_name = policy_name
        self.policy = policy or QuotaPolicy.standard()

    def apply_to_config(self, config: 'wasmtime.Config') -> None:
        config.consume_fuel = True

    def apply_to_store(self, store: 'wasmtime.Store') -> None:
        store.set_limits(memory_size=self.policy.max_memory_bytes)
        store.set_fuel(self.policy.cpu_fuel_quota)
        
        mb = self.policy.max_memory_bytes // 1024 // 1024
        log.info(f"[{self.policy_name}] enforced: Tier={self.policy.tier.value}, Mem={mb}MB, Fuel={self.policy.cpu_fuel_quota:,}")

    def inject_emergency_fuel(self, store: 'wasmtime.Store', additional_fuel: int) -> None:
        current_fuel = store.get_fuel()
        new_fuel = current_fuel + additional_fuel
        
        store.set_fuel(new_fuel)
        self.policy.cpu_fuel_quota += additional_fuel
        log.warning(f"[{self.policy_name}] Emergency Fuel Injected: +{additional_fuel:,} (New Total Quota: {self.policy.cpu_fuel_quota:,})")

    def inspect_metrics(self, store: 'wasmtime.Store', memory: 'wasmtime.Memory') -> dict:
        current_mem_bytes = memory.size(store) * 65536 
        try:
            fuel_remaining = store.get_fuel()
        except wasmtime.WasmtimeError:
            fuel_remaining = 0
            
        fuel_consumed = self.policy.cpu_fuel_quota - fuel_remaining
        return {
            "policy_name": self.policy_name,
            "tier": self.policy.tier.value,
            "mem_usage_bytes": current_mem_bytes,
            "mem_limit_bytes": self.policy.max_memory_bytes,
            "fuel_consumed": fuel_consumed,
            "fuel_remaining": fuel_remaining
        }