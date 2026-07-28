# phase.runtime.mesh.gateway
## @lineage: phase.runtime.mesh.bridge.gateway
import uuid
from typing import Any, Dict, Optional

from phase.runtime.mesh.schema import LogicStream as IngressLogicStream
from watcher.kernel.ledger import KernelLedger, LogicStream as KernelLogicStream, SealedKernel, LedgerRole
from watcher.plane.emitter import get_emitter

log = get_emitter("mesh.gateway", phase="KERNEL")

class ToposGateway:
    """@desc: Compliant middleware & Adapter bridging external Ingress to the unified KernelStore"""
    def __init__(self, store: Optional[KernelLedger] = None):
        self.store = store or KernelLedger()

    async def authorize_ingress(self, stream: IngressLogicStream) -> bool:
        """@desc: Structural adapter for Ingress validation"""
        action_id = str(stream.meta.stream_id)
        action = stream.payload.intent.value
        payload = stream.payload.parameters
        metadata = {
            "is_authenticated": stream.identity.is_authenticated,
            "stateless_token": stream.identity.stateless_token_id,
            "client_ip": stream.meta.client_ip,
            "protocol_version": stream.meta.original_protocol.value
        }
        
        ## @action: Delegate to the core authorization choke-point
        return await self.authorize(action_id=action_id, action=action, payload=payload, metadata=metadata)

    async def authorize(self, action_id: str, action: str, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        @desc: The single choke-point for agent action validation.
        @flow: Raw Context -> KernelLogicStream -> KernelStore(propose_and_seal) -> WASM -> Boolean Signal
        """
        if metadata is None:
            metadata = {}

        ## Adapt: Translate raw context into a topological KernelLogicStream
        kernel_stream = KernelLogicStream(
            id=action_id or str(uuid.uuid4()),
            action=action,
            payload=payload,
            metadata=metadata
        )

        log.debug(f"[Gateway] Forwarding stream {kernel_stream.id} to KernelStore for WASM validation.")

        try:
            # The authoritative sealing now happens entirely within the Store -> WASM pipeline
            sealed_kernel: Optional[SealedKernel] = await self.store.propose_and_seal(kernel_stream)
            
            if sealed_kernel is not None:
                # LEADER successfully invoked WASM and sealed the state
                log.info(f"[Gateway] AUTHORIZED: Stream {kernel_stream.id} successfully sealed into {sealed_kernel.kernel_id}.")
                return True
            else:
                # If None is returned, we must distinguish between a FOLLOWER proposal and a WASM rejection.
                if hasattr(self.store, 'role') and self.store.role == LedgerRole.FOLLOWER:
                    log.info(f"[Gateway] PROPOSED: Stream {kernel_stream.id} delegated to Mempool (FOLLOWER mode).")
                    return True  # Acknowledged into the consensus pipeline
                else:
                    log.warning(f"[Gateway] BLOCKED: Stream {kernel_stream.id} rejected by WASM Kernel Spatial Fence.")
                    return False

        except Exception as e:
            ## @failure.mode: Fail-Closed. Any error in the store/WASM pipeline denies execution.
            log.error(f"[Gateway] Kernel pipeline failed with exception: {e}. Defaulting to BLOCKED.")
            return False

class BypassGateway(ToposGateway):
    """
    @desc: A development/testing dummy gateway. Auto-authorizes actions (DEV MODE).
           (Formerly BypassBridge)
    """
    async def authorize(self, action_id: str, action: str, payload: Any, metadata: Optional[Dict[str, Any]] = None) -> bool:
        log.debug(f"[Gateway: Bypass] Auto-authorizing action {action_id} (DEV MODE).")
        return True