# xphi.node.rpc.handler.fuel
import asyncio
import logging
import random
from typing import Dict, Any, List, Optional

from xphi.arch.contract.protocol.rpc import WorkerContext
from xphi.arch.bound.adapter.pta import (
    PtaAdapter, 
    PtaTransaction, 
    PtaInput, 
    PtaPointer, 
    PtaOutput,
    SignStateAdapter
)
from xphi.node.runtime.wasm.broker import DphiBroker
from xphi.state.anchor.key import KeyGen
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("rpc.fuel")

_MAX_OCC_RETRIES = 4

async def _try_consume_with_occ(
    pta_adapter: PtaAdapter,
    tenant_id: str,
    consumed_fuel: int,
    trace_id: str,
    action_label: str,
) -> Dict[str, Any]:
    """
    Lock-Free Optimistic Concurrency Control (CAS) 재시도 루프:
    동시성 경쟁으로 인해 선행 트랜잭션이 PTA를 선점(`Double Spend`)하여 
    PermissionError가 발생할 경우, 짧은 지터 백오프 후 최신 거스름돈 PTA를 다시 조회하여 연쇄 차감
    """
    node_pubkey = SignStateAdapter.get_signer_pubkey()
    stream_id = KeyGen.stream_fuel(tenant_id)
    log.info(f"[Billing:{action_label}] Attempting to consume {consumed_fuel} fuel from '{tenant_id}' (Trace: {trace_id})")

    for attempt in range(_MAX_OCC_RETRIES):
        available_outputs = await pta_adapter.get_available_inputs(tenant_id, "fuel")
        total_available = sum(item["amount"] for item in available_outputs)

        if total_available <= 0:
            if attempt > 0 and attempt < _MAX_OCC_RETRIES - 1:
                await asyncio.sleep(0.02 * (attempt + 1) + random.uniform(0.005, 0.015))
                continue
            return {"status": "SUCCESS", "tx_hash": "", "consumed": 0}

        actual_consume = min(consumed_fuel, total_available)

        inputs: List[PtaInput] = []
        outputs: List[PtaOutput] = []
        accumulated = 0

        for item in available_outputs:
            canonical_payload = item["pointer"].to_key()
            real_signature = SignStateAdapter.sign_state_payload({"payload": canonical_payload})

            inputs.append(
                PtaInput(
                    pointer=item["pointer"],
                    signature=real_signature,
                    owner_address=node_pubkey,  # 노드 권한으로 시스템 대리 서명(Bypass)
                )
            )
            accumulated += item["amount"]
            if accumulated >= actual_consume:
                break

        change = accumulated - actual_consume
        if change > 0:
            outputs.append(PtaOutput(amount=change, owner=tenant_id, asset_type="fuel"))

        outputs.append(PtaOutput(amount=actual_consume, owner="system_treasury", asset_type="fuel"))
        tx = PtaTransaction(
            inputs=inputs,
            outputs=outputs,
            metadata={"action": action_label, "trace_id": trace_id},
        )

        try:
            tx_hash = await pta_adapter.execute_transaction(tx, stream_id=stream_id)
            return {"status": "SUCCESS", "tx_hash": tx_hash, "consumed": actual_consume}
        except PermissionError as e:
            if "Double Spend" in str(e) and attempt < _MAX_OCC_RETRIES - 1:
                backoff = 0.015 * (attempt + 1) + random.uniform(0.005, 0.02)
                log.debug(
                    f"[Billing:{action_label}] PTA contention for '{tenant_id}' "
                    f"(attempt {attempt + 1}/{_MAX_OCC_RETRIES}). Retrying in {backoff*1000:.1f}ms..."
                )
                await asyncio.sleep(backoff)
                continue
            log.debug(f"[Billing:{action_label}] Dropped contended tx for '{tenant_id}' after retries: {e}")
            return {"status": "DROPPED_CONTENTION", "tx_hash": "", "consumed": 0}

async def execute_direct_fuel_consume(
    tenant_id: str, 
    consumed_fuel: int, 
    trace_id: str,
    pta_adapter: Optional[PtaAdapter] = None,
    broker: Optional[DphiBroker] = None
) -> None:
    if consumed_fuel <= 0 or tenant_id in ("anonymous", "internal_system", "unknown"):
        return

    owns_broker = False
    if pta_adapter is None:
        if broker is None:
            broker = DphiBroker(timeout=5.0)
            owns_broker = True
        pta_adapter = PtaAdapter(broker=broker)
    
    try:
        res = await _try_consume_with_occ(
            pta_adapter=pta_adapter,
            tenant_id=tenant_id,
            consumed_fuel=consumed_fuel,
            trace_id=trace_id,
            action_label="direct_consume",
        )
        if res["consumed"] > 0:
            log.info(f"[Billing:Direct] Auto-consumed {res['consumed']} fuel from '{tenant_id}'. Trace: {trace_id}")
        else:
            log.warning(f"[Billing:Direct] No fuel consumed for '{tenant_id}' (Trace: {trace_id}). Status: {res['status']}")
    except Exception as e:
        log.error(f"[Billing:Direct] Background consume failed for '{tenant_id}': {e}", exc_info=True)
    finally:
        if owns_broker and broker:
            await broker.close()


# RPC Queue Handler (Daemon Mode)
def _build_error(code: int, message: str) -> dict:
    """RPC 에러 포맷 규격화"""
    return {"error": True, "code": code, "message": message}


async def handle_fuel_consume(params: dict, ctx: WorkerContext) -> dict:
    tenant_id = params.get("tenant_id")
    consumed_fuel = params.get("consumed_fuel", 0)
    trace_id = params.get("trace_id", "unknown")

    if not tenant_id or consumed_fuel <= 0:
        return _build_error(400, "Invalid consume parameters")

    try:
        res = await _try_consume_with_occ(
            pta_adapter=ctx.pta_adapter,
            tenant_id=tenant_id,
            consumed_fuel=consumed_fuel,
            trace_id=trace_id,
            action_label="queue_consume",
        )
        if res["consumed"] > 0:
            tx_hash = res["tx_hash"]
            log.info(f"[Tx:Queue] Consumed {res['consumed']} fuel from '{tenant_id}'. Trace: {trace_id} (TX: {tx_hash[:8]})")
        return res
    except Exception as e:
        log.error(f"[Tx:Queue] Fuel consume failed for '{tenant_id}': {str(e)}", exc_info=True)
        return _build_error(500, "Internal fuel consume error")