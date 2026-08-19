# kernel.dphi.adapter.utxo
import uuid
import time
from typing import Dict, Any, Optional

from kernel.dphi.ledger.consensus import KernelLedger, LogicStream
from kernel.dphi.ledger.oracle import LedgerOracle
from kernel.dphi.broker import DphiBroker
from watcher.plane.emitter import get_emitter

log = get_emitter("adapter.utxo", phase="KERNEL")

class UtxoBillingAdapter:
    """
    UTXO 기반 마이크로 과금 전용 어댑터.
    계좌(Account) 잔고를 덮어쓰는 대신, 차감 내역을 LogicStream으로 
    Mempool에 캐싱하고 암호학적 해시 영수증(SealedKernel)으로 봉인합니다.
    """
    def __init__(self, broker: DphiBroker):
        self.broker = broker
        # 합의 엔진(캐싱 및 봉인) 및 오라클(검증 및 붕괴) 인스턴스 마운트
        self.ledger = KernelLedger()
        self.oracle = LedgerOracle(broker=broker)

    async def append_charge_intent(self, tenant_id: str, fuel_consumed: int, metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        차감 의도(Intent)를 원장에 기록합니다. 
        Mempool(캐시)을 거치거나 즉각적으로 WASM 봉인을 수행합니다.
        
        Returns:
            str: 처리된 Stream ID 또는 최종 암호화된 UTXO 해시 영수증 (Commit Hash)
        """
        stream_id = f"utxo_{uuid.uuid4().hex[:8]}"
        meta = metadata or {}
        meta["timestamp_ms"] = int(time.time() * 1000)

        # 1. 상태 전이를 일으킬 입력값(LogicStream) 조립
        stream = LogicStream(
            id=stream_id,
            action="UTXO_FUEL_DEDUCTION",
            payload={"tenant": tenant_id, "deduct_fuel": fuel_consumed},
            metadata=meta
        )

        # 2. 원장에 제안(Mempool) 및 봉인(SealedKernel)
        sealed_kernel = await self.ledger.propose_and_seal(stream)

        # 3. LEADER 모드(즉시 봉인됨) vs FOLLOWER 모드(Mempool 캐싱됨) 분기 응답
        if sealed_kernel:
            utxo_hash = sealed_kernel.signature
            log.info(f"[UtxoAdapter] Billed {fuel_consumed} fuel. UTXO Hash: {utxo_hash[:8]}...")
            return utxo_hash
        else:
            log.info(f"[UtxoAdapter] Billed {fuel_consumed} fuel. Queued in Mempool Stream: {stream_id}")
            return stream_id

    async def verify_and_collapse_receipt(self, utxo_hash: str) -> Dict[str, Any]:
        """
        [지연 정산 시점]
        발행된 UTXO 해시 영수증을 WASM 엔진을 통해 결정론적으로 붕괴(Collapse)시켜
        L1 블록체인 등에 제출 가능한 불변의 상태로 확정합니다.
        """
        try:
            log.info(f"[UtxoAdapter] Requesting Oracle collapse for UTXO: {utxo_hash[:8]}...")
            collapsed_state = await self.oracle.observe_nexus(utxo_hash)
            return collapsed_state
        except Exception as e:
            log.error(f"[UtxoAdapter] Receipt collapse failed: {e}")
            raise

    async def verify_lineage(self, utxo_hash: str, depth: int = 5) -> bool:
        """
        해당 영수증이 위변조되지 않고 정당한 이전 해시들로부터 
        연결되었는지(Lineage)를 ParityTriplet을 통해 검증합니다.
        """
        try:
            res = await self.oracle.verify_kernel_lineage(utxo_hash, depth)
            is_valid = res.get("is_valid", False)
            if not is_valid:
                log.warning(f"[UtxoAdapter] UTXO Lineage verification failed for {utxo_hash[:8]}")
            return is_valid
        except Exception as e:
            log.error(f"[UtxoAdapter] Lineage verification error: {e}")
            return False

    def shutdown(self):
        self.oracle.close()