# xphi.state.phase.network.transport
from __future__ import annotations

import json
import asyncio
from typing import Dict, Any, Optional, Set
import httpx

try:
    import orjson as _json_fast
except ImportError:
    _json_fast = None

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("network.transport")


def _dumps_bytes(obj: Dict[str, Any]) -> bytes:
    """Fast serialization to UTF-8 bytes with standard json fallback."""
    if _json_fast is not None:
        return _json_fast.dumps(obj)
    return json.dumps(obj).encode("utf-8")


def _loads_obj(raw: str | bytes) -> Any:
    """Fast deserialization with standard json fallback."""
    if _json_fast is not None:
        return _json_fast.loads(raw)
    return json.loads(raw)


class DummyProcess:
    """Pseudo-process handle maintaining interface parity with asyncio.subprocess.Process."""
    def __init__(self, pid: int = 99999):
        self.pid = pid
        self.returncode: Optional[int] = None


class NetworkTransport:
    """
    Pure asynchronous HTTP/SSE multiplexing transport layer for MCPServerConnector.
    Handles direct HTTP(S) JSON-RPC dispatch, SSE streaming, and YIELD/RESUME (HTTP 202) sessions.
    """
    def __init__(self, execution_target: str, handle_id: str):
        self.execution_target = str(execution_target).strip()
        self.handle_id = handle_id

        if not self.execution_target.startswith(("http://", "https://")):
            raise ValueError(
                f"NetworkTransport [{self.handle_id}] requires an http:// or https:// endpoint, "
                f"got: '{self.execution_target}'"
            )

        self._response_queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        self._inflight_tasks: Set[asyncio.Task] = set()
        self._mcp_session_id: Optional[str] = None
        self._closed = False

        self.process = DummyProcess(pid=99999)
        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=10.0),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )

    async def start(self) -> None:
        log.info(f"[Transport:{self.handle_id}] Attached to endpoint: {self.execution_target}")

    async def send_payload(self, safe_payload: Dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError(f"NetworkTransport [{self.handle_id}] is already closed.")

        # Quarantine 또는 상위 계층에서 전달된 커스텀 HTTP 헤더 분리 (표준 JSON-RPC 오염 방지)
        payload_copy = dict(safe_payload)
        extra_headers = payload_copy.pop("_http_headers", None)
        req_id = payload_copy.get("id", "unknown")

        try:
            raw_bytes = _dumps_bytes(payload_copy)
        except (TypeError, ValueError) as e:
            log.error(f"[Transport:{self.handle_id}] Payload Serialization Failure: {e}", exc_info=True)
            raise RuntimeError(f"Serialization Exception: {e}") from e

        # Non-blocking Multiplexing (In-flight Task 추적 관리)
        task = asyncio.create_task(self._dispatch_http_request(req_id, raw_bytes, extra_headers))
        self._inflight_tasks.add(task)
        task.add_done_callback(self._inflight_tasks.discard)

    async def _enqueue_parsed_payload(self, raw_text: str) -> None:
        """
        단일 JSON(Pretty-printed 포함), NDJSON(줄바꿈 구분 다중 JSON),
        또는 SSE(data: ...) 포맷을 판별하여 개별 JSON 단위로 큐에 적재합니다.
        """
        stripped = raw_text.strip()
        if not stripped:
            return

        # 1. 전체 텍스트가 단일 유효 JSON 객체/배열인 경우
        try:
            _loads_obj(stripped)
            await self._response_queue.put(stripped)
            return
        except Exception:
            pass

        # 2. SSE(Server-Sent Events) 또는 NDJSON 멀티 프레임인 경우 라인별 파싱
        for line in stripped.splitlines():
            clean_line = line.strip()
            if not clean_line or clean_line.startswith(":"):
                continue
            if clean_line.startswith("data:"):
                clean_line = clean_line[5:].strip()
            if not clean_line or clean_line == "[DONE]":
                continue
            await self._response_queue.put(clean_line)

    async def _dispatch_http_request(
        self,
        req_id: Any,
        data: bytes,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        try:
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            }
            if self._mcp_session_id:
                headers["Mcp-Session-Id"] = self._mcp_session_id
            if isinstance(extra_headers, dict):
                headers.update(extra_headers)

            async with self.http_client.stream(
                "POST",
                self.execution_target,
                content=data,
                headers=headers,
            ) as res:
                # MCP Stateless/Stateful 세션 헤더 캡처 (YIELD/RESUME 연속성 보장)
                session_hdr = res.headers.get("Mcp-Session-Id") or res.headers.get("mcp-session-id")
                if session_hdr:
                    self._mcp_session_id = session_hdr

                if res.status_code not in (200, 202):
                    err_body = (await res.aread()).decode("utf-8", errors="replace")
                    error_fallback = json.dumps({
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": {"code": res.status_code, "message": f"HTTP {res.status_code}: {err_body}"},
                    })
                    await self._response_queue.put(error_fallback)
                    return

                content_type = res.headers.get("content-type", "").lower()
                if "text/event-stream" in content_type:
                    # 실시간 SSE 스트림 수신 (rpc_delegate 중간 위임 및 스트리밍 즉시 처리)
                    async for line in res.aiter_lines():
                        await self._enqueue_parsed_payload(line)
                else:
                    body_bytes = await res.aread()
                    full_response = body_bytes.decode("utf-8", errors="replace")
                    await self._enqueue_parsed_payload(full_response)

        except asyncio.CancelledError:
            log.debug(f"[Transport:{self.handle_id}] In-flight HTTP request [{req_id}] cancelled.")
        except Exception as e:
            log.error(f"[Transport:{self.handle_id}] Egress HTTP Dispatch Fractured [{req_id}]: {e}")
            error_fallback = json.dumps({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32000, "message": f"Transport Egress Error: {str(e)}"},
            })
            await self._response_queue.put(error_fallback)

    async def receive_raw(self) -> str:
        """Ephemeral mode extraction: Synchronously awaits the egress response."""
        raw_output = await self._response_queue.get()
        if raw_output is None:
            raise RuntimeError(f"HTTP virtual stream closed for handle: {self.handle_id}.")
        return raw_output

    async def read_egress_stream(self) -> bytes:
        """큐의 데이터를 읽어 Stdio와 동일한 바이트 스트림 반환 (다형성 지원)"""
        item = await self._response_queue.get()
        if item is None:
            return b""  # EOF
        return item.encode("utf-8") + b"\n"

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        log.info(f"[Transport:{self.handle_id}] Initiating graceful teardown of network transport...")

        for task in list(self._inflight_tasks):
            if not task.done():
                task.cancel()
        if self._inflight_tasks:
            await asyncio.gather(*self._inflight_tasks, return_exceptions=True)
        self._inflight_tasks.clear()

        if self.http_client:
            await self.http_client.aclose()

        self.process.returncode = 0
        await self._response_queue.put(None)


class StdioTransport:
    """
    Subprocess-based transport layer for standard WorkerConnector.
    Handles raw POSIX standard I/O (STDIN/STDOUT/STDERR).
    """
    def __init__(self, command: str, handle_id: str):
        self.command = command
        self.handle_id = handle_id
        self.process: Optional[asyncio.subprocess.Process] = None
        self._stderr_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        log.info(f"[Transport:{self.handle_id}] Booting stdio sandbox: {self.command}")
        self.process = await asyncio.create_subprocess_shell(
            self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._stderr_task = asyncio.create_task(self._monitor_stderr())
        log.info(f"[Transport:{self.handle_id}] Sandbox running (PID: {self.process.pid})")

    async def _monitor_stderr(self) -> None:
        """JSON 스트림을 파싱하여 중앙 로그 시스템으로 릴레이"""
        while self.process and self.process.stderr and not self.process.stderr.at_eof():
            try:
                line = await self.process.stderr.readline()
                if not line:
                    break

                raw_str = line.decode("utf-8", errors="replace").strip()
                if not raw_str:
                    continue

                try:
                    log_data = _loads_obj(raw_str)
                    if not isinstance(log_data, dict):
                        raise ValueError("Non-dict JSON log line")

                    level = str(log_data.get("level", "INFO")).upper()
                    msg = log_data.get("message", "")
                    worker_req_id = log_data.get("req_id")
                    logger_name = log_data.get("logger", "worker")

                    log_ctx = {
                        "handle_id": self.handle_id,
                        "worker_req_id": worker_req_id,
                        "worker_source": logger_name,
                    }

                    if level == "DEBUG":
                        log.debug(f"[Sandbox] {msg}", extra=log_ctx)
                    elif level in ("WARN", "WARNING"):
                        log.warning(f"[Sandbox] {msg}", extra=log_ctx)
                    elif level == "ERROR":
                        log.error(f"[Sandbox] {msg}", extra=log_ctx)
                    elif level == "CRITICAL":
                        log.critical(f"[Sandbox] {msg}", extra=log_ctx)
                    else:
                        log.info(f"[Sandbox] {msg}", extra=log_ctx)

                except Exception:
                    log.warning(f"[Sandbox:RAW] {raw_str}", extra={"handle_id": self.handle_id})

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[Transport:{self.handle_id}] STDERR Relay fractured: {e}")
                break

    async def send_payload(self, safe_payload: Dict[str, Any]) -> None:
        if not self.process or self.process.returncode is not None or not self.process.stdin:
            raise RuntimeError(f"Stdio process {self.handle_id} is dead or stdin is unavailable.")

        try:
            raw_bytes = _dumps_bytes(safe_payload) + b"\n"
        except (TypeError, ValueError) as e:
            safe_keys = list(safe_payload.keys())
            log.error(
                f"[Transport:{self.handle_id}] Payload Serialization Failed: {e}. Top-level Keys: {safe_keys}",
                exc_info=True,
            )
            raise RuntimeError(f"Serialization failed for transport payload: {e}") from e

        self.process.stdin.write(raw_bytes)
        await self.process.stdin.drain()

    async def receive_raw(self) -> str:
        """Ephemeral 모드 전용: STDOUT을 동기적으로 읽음"""
        if not self.process or not self.process.stdout:
            raise RuntimeError(f"Stdout unavailable for {self.handle_id}.")
        raw_output = await self.process.stdout.readline()
        if not raw_output:
            raise RuntimeError(f"EOF reached while reading stdout for {self.handle_id}.")
        return raw_output.decode("utf-8", errors="replace")

    async def read_egress_stream(self) -> bytes:
        """STDOUT 파이프에서 한 줄을 읽어 반환"""
        if self.process and self.process.stdout:
            return await self.process.stdout.readline()
        return b""

    async def close(self) -> None:
        if self.process and self.process.returncode is None:
            log.info(f"[Transport:{self.handle_id}] Terminating sandbox (PID: {self.process.pid})")
            if self.process.stdin and not self.process.stdin.is_closing():
                self.process.stdin.close()
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()

        if self._stderr_task and not self._stderr_task.done():
            self._stderr_task.cancel()
            await asyncio.gather(self._stderr_task, return_exceptions=True)