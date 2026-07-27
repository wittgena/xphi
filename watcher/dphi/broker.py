# watcher.dphi.broker
import json
import uuid
import asyncio
from enum import Enum
from typing import Optional, Any, Mapping, Union

from arch.topos.bound.tunnel import TunnelFactory
from phase.runtime.inter.protocol import ExecutionResult, ExecutionError
from watcher.plane.emitter import get_emitter

log = get_emitter("wasm.broker")

class WasmMethod(str, Enum):
    EXECUTE_CODE = "execute_code"
    VERIFY_PACKET = "verify_packet"
    COMPUTE_ROOT_FINGERPRINT = "compute_root_fingerprint"
    EVALUATE_TENSION = "evaluate_tension"
    VALIDATE_INTENT = "validate_intent"
    GENERATE_PROOF = "generate_proof"
    GENERATE_TOPOS_ID = "generate_topos_id"
    GENERATE_PHASE_ID = "generate_phase_id"
    INIT_EPOCH = "init_epoch"
    PROCESS_EVOLUTION = "process_evolution"
    PROCESS_TOPOS_TICK = "process_topos_tick"
    INSCRIBE_ACTOR = "inscribe_actor"
    SEAL_EPOCH = "seal_epoch"
    VERIFY_BUILD_LINEAGE = "verify_build_lineage"
    VERIFY_PARITY = "verify_parity"
    EXECUTE_TRANSITION = "execute_transition"
    CONFIGURE_TOPOLOGY = "configure_topology"

class BrokerChannel:
    EXECUTE_STREAM = "wasm:execute:stream"
    CONTROL_REQ = "wasm:control:req"
    
    @staticmethod
    def execute_res(job_id: str) -> str: return f"wasm:execute:res:{job_id}"
    @staticmethod
    def control_res(job_id: str) -> str: return f"wasm:control:res:{job_id}"

class PayloadKey:
    JOB_ID = "job_id"
    METHOD_FUNC = "target_func" 
    PAYLOAD = "payload"
    RES_CHANNEL = "response_channel"
    TIER = "tier"
    WASM_PATH = "wasm_path"
    CODE = "code"
    VARS = "variables"
    DATA = "data"

class ResultKey:
    SUCCESS = "success"
    OUTPUT = "output"
    ERROR = "error"

class WasmBroker:
    def __init__(self, request_stream: str = BrokerChannel.EXECUTE_STREAM, timeout: float = 10.0):
        self.request_stream = request_stream
        self.control_channel = BrokerChannel.CONTROL_REQ
        self.timeout = timeout
        
    async def _dispatch_and_wait_async(self, job_id: str, payload: dict, response_channel: str, target_route: str = None) -> ExecutionResult:
        route = target_route or self.request_stream
        tunnel = await TunnelFactory.get_default()
        listen_client = await TunnelFactory.get_isolated()
        pubsub = listen_client.pubsub()
        
        method_name = payload.get(PayloadKey.METHOD_FUNC, 'unknown')
        
        try:
            await pubsub.subscribe(response_channel)
            
            if route == self.control_channel:
                log.info(f"[{job_id[:8]}] Broadcasting control task '{method_name}'...")
                await tunnel.publish(route, json.dumps(payload))
            else:
                log.info(f"[{job_id[:8]}] Enqueuing execution intent '{method_name}' to WASM Stream...")
                await tunnel.state_store.xadd(route, {PayloadKey.DATA: json.dumps(payload)})
            
            try:
                async with asyncio.timeout(self.timeout):
                    async for msg in pubsub.listen():
                        if msg and msg["type"] == "message":
                            try:
                                result_data = json.loads(msg[PayloadKey.DATA])
                                if result_data.get(ResultKey.SUCCESS):
                                    return ExecutionResult(success=True, output=result_data.get(ResultKey.OUTPUT, ""))
                                else:
                                    return ExecutionResult(success=False, error=ExecutionError(result_data.get(ResultKey.ERROR, "Unknown Execution Error")))
                            except json.JSONDecodeError:
                                log.warning(f"[{job_id[:8]}] Unparseable response received, skipping.")
                                continue
            except asyncio.TimeoutError:
                log.error(f"[{job_id[:8]}] Remote execution timeout ({self.timeout}s) on {route}")
                return ExecutionResult(success=False, error=ExecutionError(f"Remote execution timeout ({self.timeout}s)"))
                
        finally:
            await pubsub.unsubscribe(response_channel)
            await pubsub.close()
            if hasattr(listen_client.state_store, 'aclose'):
                await listen_client.state_store.aclose()
            elif hasattr(listen_client.state_store, 'close'):
                await listen_client.state_store.close()

    async def update_policy(self, tier: str) -> bool:
        job_id = str(uuid.uuid4())
        response_channel = BrokerChannel.control_res(job_id)
        payload = {
            PayloadKey.JOB_ID: job_id,
            PayloadKey.METHOD_FUNC: "update_policy",
            PayloadKey.TIER: tier.upper(),
            PayloadKey.RES_CHANNEL: response_channel
        }
        res = await self._dispatch_and_wait_async(job_id, payload, response_channel, target_route=self.control_channel)
        return res.success

    async def invoke(self, target_func: Union[str, WasmMethod], payload: str, wasm_path: Optional[str] = None) -> ExecutionResult:
        job_id = str(uuid.uuid4())
        response_channel = BrokerChannel.execute_res(job_id)
        
        func_name = target_func.value if isinstance(target_func, Enum) else str(target_func)
        msg_payload = {
            PayloadKey.JOB_ID: job_id, 
            PayloadKey.METHOD_FUNC: func_name, 
            PayloadKey.PAYLOAD: payload,
            PayloadKey.RES_CHANNEL: response_channel
        }
        if wasm_path:
            msg_payload[PayloadKey.WASM_PATH] = wasm_path
            
        return await self._dispatch_and_wait_async(job_id, msg_payload, response_channel)

    async def execute(self, code: str, variables: Mapping[str, Any] | None = None) -> ExecutionResult:
        job_id = str(uuid.uuid4())
        response_channel = BrokerChannel.execute_res(job_id)
        msg_payload = {
            PayloadKey.JOB_ID: job_id,
            PayloadKey.METHOD_FUNC: WasmMethod.EXECUTE_CODE.value, 
            PayloadKey.PAYLOAD: {
                PayloadKey.CODE: code, 
                PayloadKey.VARS: variables or {}
            },
            PayloadKey.RES_CHANNEL: response_channel
        }
        return await self._dispatch_and_wait_async(job_id, msg_payload, response_channel)