# watcher.xelog.edge.ingress
## @lineage: topos.xelog.edge.ingress
import json
import time
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Annotated
from fastapi import APIRouter, Body, Header, Response, status, Depends, BackgroundTasks, HTTPException

from arch.topos.tunnel.subs import DistributedPubSub
from kernel.topos.contract.model import AuditLogRequest, AuditLogResponse, AuditResult, AuditEnvelope
from kernel.topos.contract.otlp import ExportLogsServiceRequest
from watcher.xelog.state.schema import EdgeState, EdgeHeader
from kernel.dphi.broker import WasmBroker
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter, flow_scope
from watcher.xelog.depend import get_wasm_broker, get_pubsub
from kernel.dphi.ledger.audit import AuditLedger, get_audit_ledger

log = get_emitter("edge.ingress")
ingress_edge = APIRouter(prefix="/v1", tags=["Log Ingress"])

@ingress_edge.post("/logs", status_code=status.HTTP_200_OK)
async def otlp_logs_export(
    payload: ExportLogsServiceRequest = Body(...),
    bg_tasks: BackgroundTasks = BackgroundTasks(),
    pubsub: DistributedPubSub = Depends(get_pubsub),
    broker: WasmBroker = Depends(get_wasm_broker),
    auth_token: Annotated[str | None, Header(alias="Authorization")] = None, 
):
    """OTLP 표준 로그 수집 및 Usage Intent WASM 증명"""
    try:
        # 1. 원본 해싱
        payload_dict = payload.model_dump(exclude_none=True)
        raw_json_bytes = json.dumps(payload_dict, sort_keys=True).encode('utf-8')
        content_hash = hashlib.sha256(raw_json_bytes).hexdigest()

        # 2. 메트릭 추출
        genai_metrics = payload.extract_genai_metrics()
        
        # 3. WASM Kernel Seal (Usage Intent)
        kernel_payload = {
            "action": "seal_otlp_transaction",
            "content_hash": content_hash,
            "usage_intent": {
                "tenant_id": genai_metrics.get("tenant_id", "anonymous"),
                "model_name": genai_metrics.get("model", "default-model"),
                "usage": genai_metrics.get("usage", {})
            },
            "metrics_summary": genai_metrics
        }
        
        canonical_payload = StateAdapter.to_canonical_bytes(kernel_payload).decode('utf-8')
        res = await broker.invoke("compute_root_fingerprint", canonical_payload)
        
        if not res.success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail=f"Kernel Seal Rejected: {res.error.message}",
                headers={EdgeHeader.STATE: EdgeState.ERROR, EdgeHeader.ERROR_DETAIL: "Kernel Seal Rejected"}
            )
            
        fingerprint = json.loads(res.output).get("fingerprint")
        
        with flow_scope(phase="OTLP_INGRESS", bound="edge"):
            log.info(f"[OTLP Anchor] Secured batch. ContentHash: {content_hash[:8]}, Fingerprint: {fingerprint[:16]}")

        # 4. 브로드캐스트 (비동기 처리)
        topic_name = "otlp_global_stream" 
        bg_tasks.add_task(pubsub.publish_batch, topic=topic_name, events=[payload_dict])
        
        response_headers = {
            EdgeHeader.STATE: EdgeState.SUCCESS,
            EdgeHeader.CONTENT_HASH: content_hash,
            EdgeHeader.FINGERPRINT: fingerprint
        }
        
        return Response(status_code=status.HTTP_200_OK, headers=response_headers, content=b"{}")
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"[OTLP Anchor] Processing failed: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Internal stream processing error",
            headers={EdgeHeader.STATE: EdgeState.ERROR}
        )


# ==========================================
# 2. Custom Audit Log Ingress
# ==========================================
@ingress_edge.post("/audit/log", status_code=status.HTTP_200_OK)
async def audit_log(
    payload: AuditLogRequest,
    topos_ledger: AuditLedger = Depends(get_audit_ledger),
    broker: WasmBroker = Depends(get_wasm_broker)
) -> AuditLogResponse:
    """단건 Audit Event의 PII 마스킹 및 WASM 위변조 방지 증명 반환"""
    request_time = str(time.time())
    event_dict = payload.event.model_dump(exclude_none=True)
    
    # 1. PII 마스킹
    sanitized_event = topos_ledger._encrypt_sensitive_data(event_dict)
    
    # 2. WASM Kernel Seal
    canonical_payload = StateAdapter.to_canonical_bytes(sanitized_event).decode('utf-8')
    fp_res = await broker.invoke("compute_root_fingerprint", canonical_payload)
    
    if not fp_res.success:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to compute kernel fingerprint")
        
    event_hash = json.loads(fp_res.output)["fingerprint"]
    merkle_proof = None
    
    # 3. 추가 증명 (Verbose)
    if payload.verbose:
        proof_res = await broker.invoke("generate_proof", canonical_payload)
        if proof_res.success:
            merkle_proof = json.loads(proof_res.output).get("current_hash")

    envelope = AuditEnvelope(
        event=payload.event,
        received_at=datetime.now(timezone.utc).isoformat()
    )
    
    audit_result = AuditResult(
        envelope=envelope,
        hash=event_hash,
        membership_proof=merkle_proof,
        consistency_proof=[]
    )
    
    with flow_scope(phase="AUDIT_INGRESS", bound="edge"):
        log.info(f"[Audit Anchor] Secured event. Hash: {event_hash[:8]}")
        
    return AuditLogResponse(
        request_id=f"req_{uuid.uuid4().hex[:8]}",
        request_time=request_time,
        response_time=str(time.time()),
        status="success",
        result=audit_result
    )