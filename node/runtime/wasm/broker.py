# xphi.node.runtime.wasm.broker
import os
import time
import json
import uuid
import asyncio
from enum import Enum
from typing import Optional, Any, Mapping, Union, Dict, Tuple
from contextlib import suppress

from xphi.arch.contract.config.env import config as env_config
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.arch.contract.interpreter import ExecutionResult, ExecutionError
from xphi.node.runtime.wasm.method import WasmMethod
from xphi.watcher.plane.emitter import get_emitter, _flow_context

log = get_emitter("wasm.broker")

class BrokerChannel:
    EXECUTE_STREAM = "wasm:execute:stream"
    CONTROL_REQ = "wasm:control:req"
    
    @staticmethod
    def broker_res(broker_id: str) -> str: 
        return f"wasm:res:broker:{broker_id}"

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
    CONTEXT = "context"

class ResultKey:
    SUCCESS = "success"
    OUTPUT = "output"
    ERROR = "error"
    METRICS = "metrics"

class WasmBroker:
    _shared_instance: Optional["WasmBroker"] = None
    _shared_plane: str = "local"

    @classmethod
    def _resolve_plane_and_factory(cls, explicit_factory=None) -> Tuple[str, Any]:
        if explicit_factory is not None:
            return getattr(explicit_factory, "__name__", "custom"), explicit_factory

        raw_plane = str(env_config.get("XPHI_WASM_PLANE", os.getenv("XPHI_WASM_PLANE", "local"))).strip()
        plane_lower = raw_plane.lower()

        if not plane_lower or plane_lower == "local":
            return "local", TunnelFactory

        if plane_lower.startswith("flare") or plane_lower.startswith(("http://", "https://")):
            try:
                from xphi.watcher.plane.flare.tunnel import FlareTunnelFactory
                return raw_plane, FlareTunnelFactory
            except ImportError as e:
                log.error(f"[Broker] Failed to import FlareTunnelFactory ({e}). Falling back to local TunnelFactory.")
                return "local", TunnelFactory

        log.warning(f"[Broker] Unknown XPHI_WASM_PLANE='{raw_plane}', falling back to local TunnelFactory.")
        return "local", TunnelFactory

    @classmethod
    def get_shared(cls, **kwargs) -> "WasmBroker":
        target_plane, resolved_factory = cls._resolve_plane_and_factory(kwargs.get("tunnel_factory"))

        if cls._shared_instance is None or cls._shared_plane != target_plane:
            if cls._shared_instance is not None:
                prev_task = getattr(cls._shared_instance, "_listener_task", None)
                if prev_task and not prev_task.done():
                    prev_task.cancel()

            if "timeout" not in kwargs:
                kwargs["timeout"] = float(os.getenv("XPHI_WASM_TIMEOUT", "30.0"))
            kwargs["tunnel_factory"] = resolved_factory
            cls._shared_instance = cls(**kwargs)
            cls._shared_plane = target_plane

        return cls._shared_instance

    def __init__(
        self, 
        request_stream: str = BrokerChannel.EXECUTE_STREAM, 
        timeout: float = 10.0, 
        target_auditor=None,
        tunnel_factory=None,
        **kwargs
    ):
        self.request_stream = request_stream
        self.control_channel = BrokerChannel.CONTROL_REQ
        self.timeout = timeout
        self.target_auditor = target_auditor
        
        self._plane_key, self.tunnel_factory = self._resolve_plane_and_factory(tunnel_factory)
        
        self.broker_id = uuid.uuid4().hex[:8]
        self.response_channel = BrokerChannel.broker_res(self.broker_id)
        self._pending_jobs: Dict[str, asyncio.Future] = {}
        
        self._listener_task: Optional[asyncio.Task] = None
        self._listener_client = None
        self._listener_ready = asyncio.Event()

    async def _ensure_listener_started(self):
        if self._listener_task is not None and self._listener_task.done():
            self._listener_task = None
            self._listener_ready.clear()

        if self._listener_task is None:
            self._listener_task = asyncio.create_task(self._listen_responses())

        try:
            await asyncio.wait_for(self._listener_ready.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            log.error(f"[Broker] Listener failed to start within 5s. (Plane: {self._plane_key})")
            if self._listener_task and not self._listener_task.done():
                self._listener_task.cancel()
            self._listener_task = None
            self._listener_ready.clear()
            raise RuntimeError(f"Broker Failed to connect to Pub/Sub (Plane: {self._plane_key})")

    async def _listen_responses(self):
        pubsub = None
        try:
            self._listener_client = await self.tunnel_factory.get_isolated()
            pubsub = self._listener_client.pubsub()
            await pubsub.subscribe(self.response_channel)
            self._listener_ready.set()
            async for msg in pubsub.listen():
                if msg and msg["type"] == "message":
                    try:
                        result_data = json.loads(msg["data"])
                        job_id = result_data.get(PayloadKey.JOB_ID)
                        future = self._pending_jobs.get(job_id)
                        if future and not future.done():
                            future.set_result(result_data)
                            
                    except json.JSONDecodeError:
                        log.warning(f"[Broker] Unparseable response received on {self.response_channel}")
                    except Exception as e:
                        log.error(f"[Broker] Error processing response: {e}", exc_info=True)
                        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"[Broker] Listener crashed unexpectedly: {e}")
        finally:
            self._listener_ready.clear()
            if pubsub is not None:
                with suppress(Exception):
                    await pubsub.unsubscribe(self.response_channel)
                    await pubsub.close()
                
            if self._listener_client:
                with suppress(Exception):
                    if hasattr(self._listener_client.state_store, 'aclose'):
                        await self._listener_client.state_store.aclose()
                    elif hasattr(self._listener_client.state_store, 'close'):
                        await self._listener_client.state_store.close()

    async def _dispatch_and_wait_async(
        self, 
        job_id: str, 
        payload: dict, 
        target_route: str = None, 
        timeout: Optional[float] = None
    ) -> ExecutionResult:
        await self._ensure_listener_started()
        
        route = target_route or self.request_stream
        tunnel = await self.tunnel_factory.get_default()
        method_name = payload.get(PayloadKey.METHOD_FUNC, 'unknown')
        
        payload[PayloadKey.RES_CHANNEL] = self.response_channel
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending_jobs[job_id] = future
        active_timeout = timeout if timeout is not None else self.timeout
        
        try:
            if route == self.control_channel:
                await tunnel.publish(route, json.dumps(payload))
            else:
                await tunnel.state_store.xadd(route, {PayloadKey.DATA: json.dumps(payload)})
            
            async with asyncio.timeout(active_timeout):
                result_data = await future
                
            metrics = result_data.get(ResultKey.METRICS, {})
            edge_hash = result_data.get("edge_canonical_hash") or result_data.get("edge_hash")
            if metrics and self.target_auditor and hasattr(self.target_auditor, "project_state"):
                self.target_auditor.project_state(action=method_name, metrics=metrics)
            
            if result_data.get(ResultKey.SUCCESS):
                return ExecutionResult(success=True, output=result_data.get(ResultKey.OUTPUT, ""), metrics=metrics, edge_hash=edge_hash)
            else:
                return ExecutionResult(
                    success=False, 
                    output=result_data.get(ResultKey.OUTPUT, ""),
                    error=ExecutionError(result_data.get(ResultKey.ERROR, "Unknown Execution Error")),
                    metrics=metrics,
                    edge_hash=edge_hash
                )
        except asyncio.TimeoutError:
            timeout_msg = f"Remote execution timeout ({active_timeout}s)"
            log.warning(f"[{job_id[:8]}] {timeout_msg} on {route}. (Infinite loop or payload blocked)")
            return ExecutionResult(
                success=False,
                output=timeout_msg,
                error=ExecutionError(timeout_msg)
            )
        finally:
            self._pending_jobs.pop(job_id, None)

    def _build_context(self, base_context: Optional[dict], timeout: Optional[float]) -> dict:
        ctx = dict(base_context) if base_context is not None else dict(_flow_context.get() or {})
        if "timestamp" not in ctx:
            ctx["timestamp"] = int(time.time() * 1000)
            
        ctx["timeout"] = timeout or self.timeout
        return ctx

    async def update_policy(self, tier: str, context: Optional[dict] = None) -> bool:
        job_id = str(uuid.uuid4())
        active_context = self._build_context(context, self.timeout)
        payload = {
            PayloadKey.JOB_ID: job_id,
            PayloadKey.METHOD_FUNC: "update_policy",
            PayloadKey.TIER: tier.upper(),
            PayloadKey.CONTEXT: active_context
        }
        res = await self._dispatch_and_wait_async(job_id, payload, target_route=self.control_channel)
        return res.success

    async def invoke(
        self, 
        target_func: Union[str, WasmMethod], 
        payload: str, 
        wasm_path: Optional[str] = None, 
        tier: Optional[str] = None,
        context: Optional[dict] = None,
        timeout: Optional[float] = None
    ) -> ExecutionResult:
        job_id = str(uuid.uuid4())
        active_context = self._build_context(context, timeout)
        func_name = target_func.value if isinstance(target_func, Enum) else str(target_func)

        msg_payload = {
            PayloadKey.JOB_ID: job_id, 
            PayloadKey.METHOD_FUNC: func_name, 
            PayloadKey.PAYLOAD: payload,
            PayloadKey.CONTEXT: active_context
        }
        if wasm_path: msg_payload[PayloadKey.WASM_PATH] = wasm_path
        if tier: msg_payload[PayloadKey.TIER] = tier.upper()
            
        return await self._dispatch_and_wait_async(job_id, msg_payload, timeout=timeout)

    async def execute(
        self, 
        code: Union[str, dict],  
        variables: Mapping[str, Any] | None = None, 
        tier: Optional[str] = None,
        context: Optional[dict] = None,
        timeout: Optional[float] = None
    ) -> ExecutionResult:
        job_id = str(uuid.uuid4())
        active_context = self._build_context(context, timeout)
        
        target_wasm = None
        if isinstance(code, dict):
            method_func = WasmMethod.EXECUTE_FSM.value
            payload_data = code  
            target_wasm = "fsm.wasm"
        else:
            method_func = WasmMethod.EXECUTE_CODE.value
            payload_data = {
                PayloadKey.CODE: code, 
                PayloadKey.VARS: variables or {}
            }

        msg_payload = {
            PayloadKey.JOB_ID: job_id,
            PayloadKey.METHOD_FUNC: method_func, 
            PayloadKey.PAYLOAD: payload_data,
            PayloadKey.CONTEXT: active_context
        }
        
        if target_wasm: msg_payload[PayloadKey.WASM_PATH] = target_wasm
        if tier: msg_payload[PayloadKey.TIER] = tier.upper()
            
        return await self._dispatch_and_wait_async(job_id, msg_payload, timeout=timeout)
        
    async def close(self):
        task = getattr(self, '_listener_task', None)
        if task and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        if WasmBroker._shared_instance is self:
            WasmBroker._shared_instance = None
            WasmBroker._shared_plane = "local"

    def __del__(self):
        task = getattr(self, '_listener_task', None)
        if task and not task.done():
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass