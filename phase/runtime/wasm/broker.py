# phase.runtime.wasm.broker
import json
import uuid
import time
from typing import Optional, Any, Mapping, Callable
from arch.xor.proto.code import ExecutionResult, ExecutionError
from arch.topos.bound.tunnel import TunnelFactory
from watcher.plane.emitter import get_emitter

log = get_emitter("wasm.broker")

class WasmBroker:
    """
    @role: Proxy client delegating execution to a remote WASM Node via MQ.
    Drop-in replacement for the local WasmInterpreter.
    """
    def __init__(self, request_channel: str = "wasm:execute:req", timeout: float = 10.0):
        self.request_channel = request_channel
        self.control_channel = "wasm:control:req"
        self.timeout = timeout
        
    def _dispatch_and_wait(self, job_id: str, payload: dict, response_channel: str, req_chan: str = None) -> ExecutionResult:
        target_channel = req_chan or self.request_channel
        tunnel = TunnelFactory.get_sync()
        
        try:
            listen_client = TunnelFactory.get_isolated_sync()
            pubsub = listen_client.pubsub()
            pubsub.subscribe(response_channel)
            
            log.info(f"[{job_id[:8]}] Dispatching task '{payload.get('target_func', payload.get('action', 'unknown'))}' to remote WASM cluster...")
            tunnel.publish(target_channel, json.dumps(payload))
            
            start_time = time.time()
            while time.time() - start_time < self.timeout:
                msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                if msg and msg["type"] == "message":
                    try:
                        result_data = json.loads(msg["data"])
                        if result_data.get("success"):
                            return ExecutionResult(success=True, output=result_data.get("output", ""))
                        else:
                            return ExecutionResult(success=False, error=ExecutionError(result_data.get("error")))
                    except json.JSONDecodeError:
                        continue
                        
            return ExecutionResult(success=False, error=ExecutionError(f"Remote execution timeout ({self.timeout}s)"))
            
        finally:
            if 'pubsub' in locals(): pubsub.close()
            if 'listen_client' in locals(): listen_client.close()

    def update_policy(self, tier: str) -> bool:
        """@desc: [Control Plane API] 원격 Worker의 WasmCgroup Policy를 동적으로 변경 - 유효값: "STANDARD", "SYSTEM" 등"""
        job_id = str(uuid.uuid4())
        response_channel = f"wasm:control:res:{job_id}"
        payload = {
            "job_id": job_id,
            "action": "update_policy",
            "tier": tier.upper(),
            "response_channel": response_channel
        }
        
        res = self._dispatch_and_wait(job_id, payload, response_channel, req_chan=self.control_channel)
        if res.success:
            log.info(f"[{job_id[:8]}] Remote policy successfully updated to {tier.upper()}.")
            return True
        else:
            log.error(f"[{job_id[:8]}] Failed to update remote policy: {res.error}")
            return False

    def invoke(self, target_func: str, payload: str) -> ExecutionResult:
        """특정 WASM FFI 함수(Target Function)를 지정하여 실행하는 명시적 엔드포인트"""
        job_id = str(uuid.uuid4())
        response_channel = f"wasm:execute:res:{job_id}"
        
        msg_payload = {
            "job_id": job_id,
            "target_func": target_func, 
            "payload": payload,         
            "response_channel": response_channel
        }
        return self._dispatch_and_wait(job_id, msg_payload, response_channel)

    def execute(
        self,
        code: str,
        variables: Mapping[str, Any] | None = None,
        callables: Mapping[str, Callable[..., Any]] | None = None,
    ) -> ExecutionResult:
        """레거시 파이썬 코드 실행용 래퍼 (WasmInterpreter와의 호환성 유지)"""
        job_id = str(uuid.uuid4())
        response_channel = f"wasm:execute:res:{job_id}"
        
        if callables:
            log.warning("[RemoteWasmBroker] 'callables' injection dropped in distributed mode.")
            
        msg_payload = {
            "job_id": job_id,
            "target_func": "execute_code", 
            "code": code,                  
            "variables": variables or {},
            "response_channel": response_channel
        }
        return self._dispatch_and_wait(job_id, msg_payload, response_channel)


class RemoteWasmWorker:
    """@role: Remote Daemon listening for MQ requests and running them in the local WASM Sandbox"""
    def __init__(self, local_interpreter, request_channel: str = "wasm:execute:req"):
        self.interpreter = local_interpreter
        self.request_channel = request_channel
        self.control_channel = "wasm:control:req"
        self.running = False
        
    def serve_forever(self):
        self.running = True
        listen_client = TunnelFactory.get_isolated_sync()
        pubsub = listen_client.pubsub()
        pubsub.subscribe(self.request_channel, self.control_channel)
        
        publish_client = TunnelFactory.get_sync()
        log.info(f"[RemoteWasmWorker] Listening on '{self.request_channel}' and '{self.control_channel}'...")
        
        try:
            for msg in pubsub.listen():
                if not self.running: break
                if msg["type"] != "message": continue
                    
                data = json.loads(msg["data"])
                raw_channel = msg["channel"]
                channel = raw_channel.decode('utf-8') if isinstance(raw_channel, bytes) else raw_channel
                
                job_id = data.get("job_id")
                response_channel = data.get("response_channel")
                if channel == self.control_channel:
                    action = data.get("action")
                    if action == "update_policy":
                        tier_name = data.get("tier", "STANDARD")
                        log.info(f"[{job_id[:8]}] Processing Control Plane request: update_policy -> {tier_name}")
                        
                        try:
                            from phase.runtime.wasm.wasmcg import CgroupPolicy
                            if tier_name == "SYSTEM":
                                new_policy = CgroupPolicy.system()
                            else:
                                new_policy = CgroupPolicy.standard()
                                
                            self.interpreter.cg.policy = new_policy
                            if self.interpreter.store is not None:
                                self.interpreter.cg.apply_to_store(self.interpreter.store)
                                
                            result = ExecutionResult(success=True, output=f"Policy updated to {tier_name}")
                        except Exception as e:
                            result = ExecutionResult(success=False, error=ExecutionError(str(e)))
                    else:
                        result = ExecutionResult(success=False, error=ExecutionError(f"Unknown control action: {action}"))

                elif channel == self.request_channel:
                    target_func = data.get("target_func", "execute_code")
                    log.info(f"[{job_id[:8]}] Processing remote execution for '{target_func}'...")
                    
                    if target_func == "execute_code" and "code" in data:
                        result = self.interpreter.execute(data.get("code", ""), variables=data.get("variables", {}))
                    else:
                        result = self.interpreter.invoke(target_func, data.get("payload", ""))

                response_payload = {
                    "success": result.success,
                    "output": result.output if result.success else "",
                    "error": str(result.error) if not result.success else ""
                }
                publish_client.publish(response_channel, json.dumps(response_payload))
                log.info(f"[{job_id[:8]}] Result dispatched to {response_channel}")
                    
        finally:
            pubsub.close()
            listen_client.close()