# xphi.watcher.plane.flare.bridge
import os
import json
import time
import asyncio
import httpx
from typing import Callable, Awaitable, Optional

from xphi.watcher.plane.emitter import get_emitter
from xphi.node.runtime.wasm.method import WasmMethod
from xphi.node.rpc.schema.receipt import EdgeHeader

log = get_emitter("tunnel.flare")

DEFAULT_LOCAL_EDGE_URL = "http://127.0.0.1:8787"

HEADER_CANONICAL_HASH = EdgeHeader.CANONICAL_HASH.value
HEADER_FUEL_RECEIPT = EdgeHeader.FUEL_RECEIPT.value
HEADER_EDGE_TOKEN = (
    EdgeHeader.EDGE_TOKEN.value
    if hasattr(EdgeHeader, "EDGE_TOKEN")
    else "X-Fiber-Edge-Token"
)


class PayloadKey:
    """Matches broker.PayloadKey to minimize dependencies"""
    JOB_ID = "job_id"
    METHOD_FUNC = "target_func"
    PAYLOAD = "payload"
    TIER = "tier"
    WASM_PATH = "wasm_path"
    CONTEXT = "context"


class ResultKey:
    SUCCESS = "success"
    OUTPUT = "output"
    ERROR = "error"
    EDGE_HASH = "edge_canonical_hash"


class _MockPubSub:
    """Adapter for broker's pubsub.listen() using an asyncio.Queue"""
    def __init__(self, response_queue: asyncio.Queue):
        self.q = response_queue

    async def subscribe(self, *args, **kwargs): pass
    async def unsubscribe(self, *args, **kwargs): pass
    async def close(self, *args, **kwargs): pass

    async def listen(self):
        while True:
            msg = await self.q.get()
            yield msg


class _MockStateStore:
    """Adapter to convert broker's Redis xadd() into Cloudflare HTTP POST requests"""
    def __init__(
        self,
        http_client: httpx.AsyncClient,
        response_queue: asyncio.Queue,
        mq_url: str = DEFAULT_LOCAL_EDGE_URL,
        runtime_ready_hook: Optional[Callable[[str], Awaitable[None]]] = None,
    ):
        self.http_client = http_client
        self.q = response_queue
        self.mq_url = mq_url
        self._runtime_ready_hook = runtime_ready_hook
        self._background_tasks = set()

    async def xadd(self, name: str, fields: dict, *args, **kwargs):
        job_id = "unknown"
        try:
            data_val = fields.get("data") or fields.get(b"data")
            if not data_val:
                log.debug("[FlareTunnel:xadd] No data field found in xadd fields. Returning mock id.")
                return b"0-0"

            payload = json.loads(data_val)
            job_id = payload.get(PayloadKey.JOB_ID, "unknown")

            log.debug(f"[FlareTunnel:xadd] Intercepted payload for job_id: {job_id}. Scheduling background task.")

            task = asyncio.create_task(self._process_edge_request(job_id, payload))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

            return b"edge-mock-id"

        except Exception as e:
            log.error(f"[FlareTunnel:xadd] Invalid payload format: {e}")
            return b"error-id"

    def _build_request_headers(self, context: dict) -> dict:
        """Builds canonical Edge headers for Private BYOC token and x402 fuel receipts."""
        headers = {"Content-Type": "application/json"}

        edge_token = context.get("edge_token") or os.getenv("XPHI_EDGE_TOKEN")
        if edge_token:
            headers[HEADER_EDGE_TOKEN] = str(edge_token)

        fuel_receipt = context.get("fuel_receipt") or os.getenv("XPHI_FUEL_RECEIPT")
        if fuel_receipt:
            headers[HEADER_FUEL_RECEIPT] = str(fuel_receipt)

        return headers

    async def _process_edge_request(self, job_id: str, payload: dict):
        """Dispatches JSON-RPC payload to Cloudflare Edge and enqueues the response."""
        start_time = time.time()
        log.debug(f"[FlareTunnel:_process_edge_request] Task started for job_id: {job_id}")

        try:
            if self._runtime_ready_hook is not None:
                await self._runtime_ready_hook(self.mq_url)

            method_func = payload.get(PayloadKey.METHOD_FUNC, "")
            wasm_path = payload.get(PayloadKey.WASM_PATH)
            base_payload = payload.get(PayloadKey.PAYLOAD, {})
            context = payload.get(PayloadKey.CONTEXT, {})

            requested_timeout = float(context.get("timeout", 15.0))
            dynamic_timeout = httpx.Timeout(requested_timeout + 3.0, connect=5.0)
            methods = {
                m.value for m in WasmMethod
                if m.value not in (WasmMethod.EXECUTE_CODE.value, WasmMethod.EXECUTE_FSM.value)
            }

            if wasm_path == "phase.wasm" or method_func in methods:
                vm_tgt = "PHASE"
            elif wasm_path == "fsm.wasm" or method_func == WasmMethod.EXECUTE_FSM.value:
                vm_tgt = "FSM"
            else:
                vm_tgt = "PYTHON"

            if vm_tgt == "PHASE":
                actual_edge_payload = {
                    "method": method_func,
                    "context": context,
                    "payload": base_payload,
                }
                rpc_method = "invoke_wasm"
            else:
                actual_edge_payload = base_payload
                rpc_method = method_func.replace("wasm:", "").split(":")[-1] if "wasm:" in method_func else "execute"

            edge_params = {
                "vm_target": vm_tgt,
                "payload": actual_edge_payload,
                "context": context,
            }

            if vm_tgt == "PYTHON" and isinstance(actual_edge_payload, dict):
                edge_params["code"] = actual_edge_payload.get("code", "")
                edge_params["callables"] = actual_edge_payload.get("callables", [])
                edge_params["variables"] = actual_edge_payload.get("variables", {})

            if payload.get(PayloadKey.TIER) == "SYSTEM":
                edge_params["fuel"] = None

            rpc_request = {
                "jsonrpc": "2.0",
                "method": rpc_method,
                "params": edge_params,
                "id": job_id,
            }

            req_headers = self._build_request_headers(context)
            log.info(
                f"[FlareTunnel:_process_edge_request] -> Sending POST to Edge [{vm_tgt}] "
                f"(job_id: {job_id}, method: {rpc_method}, timeout: {dynamic_timeout.read}s)"
            )

            response = await self.http_client.post(
                "/",
                json=rpc_request,
                headers=req_headers,
                timeout=dynamic_timeout,
            )

            elapsed_time = time.time() - start_time
            log.debug(
                f"[FlareTunnel:_process_edge_request] <- Received HTTP {response.status_code} "
                f"in {elapsed_time:.2f}s (job_id: {job_id})"
            )

            edge_hash = response.headers.get(HEADER_CANONICAL_HASH)
            if edge_hash:
                log.info(f"[FlareTunnel:_process_edge_request] [SEAL] Captured Edge Boundary Seal: {edge_hash}")

            response.raise_for_status()

            try:
                res_json = response.json()
            except json.JSONDecodeError:
                raise ValueError("Edge did not return valid JSON")

            broker_result = {
                PayloadKey.JOB_ID: job_id,
                ResultKey.SUCCESS: "error" not in res_json,
            }

            if edge_hash:
                broker_result[ResultKey.EDGE_HASH] = edge_hash

            if broker_result[ResultKey.SUCCESS]:
                broker_result[ResultKey.OUTPUT] = res_json.get("result", {}).get("output", "")
                log.debug(f"[FlareTunnel:_process_edge_request] Success response parsed for job_id: {job_id}")
            else:
                err_data = res_json.get("error", {})
                broker_result[ResultKey.ERROR] = err_data.get("message", "Unknown Edge Error")
                log.debug(
                    f"[FlareTunnel:_process_edge_request] Error response parsed for job_id: {job_id} | "
                    f"Error: {broker_result[ResultKey.ERROR]}"
                )

            await self._put_success(broker_result)

        except httpx.HTTPStatusError as e:
            elapsed_time = time.time() - start_time
            status = e.response.status_code
            err_msg = f"Edge HTTP {status}: {e.response.text}"
            if status in (502, 503):
                err_msg = "Wall-Time/CPU Limit Cutoff: Edge returned 502 Bad Gateway (Isolate Terminated)"
            elif status in (401, 402):
                try:
                    err_json = e.response.json()
                    err_msg = err_json.get("error", {}).get("message", err_msg)
                except Exception:
                    pass
            log.warning(f"[FlareTunnel:_process_edge_request] {err_msg} [{job_id}] (after {elapsed_time:.2f}s)")
            await self._put_error(job_id, err_msg)

        except httpx.ReadTimeout:
            elapsed_time = time.time() - start_time
            err_msg = "Wall-Time Cutoff: Edge Execution Read Timeout (V8 Isolate Hang)"
            log.warning(f"[FlareTunnel:_process_edge_request] {err_msg} [{job_id}] (after {elapsed_time:.2f}s)")
            await self._put_error(job_id, err_msg)

        except httpx.ConnectError:
            elapsed_time = time.time() - start_time
            err_msg = "Connection Refused: Local Edge Runtime (workerd/Wrangler) is unreachable."
            log.error(f"[FlareTunnel:_process_edge_request] {err_msg} [{job_id}] (after {elapsed_time:.2f}s)")
            await self._put_error(job_id, err_msg)

        except httpx.RemoteProtocolError:
            elapsed_time = time.time() - start_time
            err_msg = "Wall-Time Cutoff: Edge Connection Disconnected Forcefully (V8 Isolate Abort / EOF)"
            log.warning(f"[FlareTunnel:_process_edge_request] {err_msg} [{job_id}] (after {elapsed_time:.2f}s)")
            await self._put_error(job_id, err_msg)

        except httpx.RequestError as e:
            elapsed_time = time.time() - start_time
            log.error(
                f"[FlareTunnel:_process_edge_request] Network Error [{job_id}]: "
                f"{type(e).__name__} - {str(e)} (after {elapsed_time:.2f}s)"
            )
            await self._put_error(job_id, f"Network Error: {str(e)}")

        except Exception as e:
            elapsed_time = time.time() - start_time
            log.error(
                f"[FlareTunnel:_process_edge_request] Internal Error [{job_id}]: {e} (after {elapsed_time:.2f}s)",
                exc_info=True,
            )
            await self._put_error(job_id, str(e))

    async def _put_success(self, broker_result: dict):
        log.debug(f"[FlareTunnel:_put_success] Putting success result into queue for job_id: {broker_result.get(PayloadKey.JOB_ID)}")
        msg = {
            "type": "message",
            "data": json.dumps(broker_result).encode("utf-8"),
        }
        await self.q.put(msg)

    async def _put_error(self, job_id: str, error_msg: str):
        log.debug(f"[FlareTunnel:_put_error] Putting error result into queue for job_id: {job_id}")
        msg = {
            "type": "message",
            "data": json.dumps({
                PayloadKey.JOB_ID: job_id,
                ResultKey.SUCCESS: False,
                ResultKey.ERROR: error_msg,
            }).encode("utf-8"),
        }
        await self.q.put(msg)

    async def aclose(self):
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

    async def close(self):
        await self.aclose()