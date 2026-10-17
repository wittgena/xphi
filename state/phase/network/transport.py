# xphi.state.phase.network.transport
import json
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, Set
import httpx

from xphi.state.phase.network.bootstrap import WorkerRuntimeBootstrap
from xphi.watcher.plane.emitter import get_emitter
from xphi.node.space.tunnel.surface import EchoListener, SurfaceClient

log = get_emitter("network.transport")

class DummyProcess:
    def __init__(self, pid: int = -1):
        self.pid = pid
        self.returncode = None

"""Network Transport (Async HTTP Multiplexing)"""
class NetworkTransport:
    """
    Asynchronous HTTP/Network multiplexing transport layer for WorkerConnector.
    
    Supports two distinct operational models:
      1. Managed Mode: Bootstraps and manages a local binary via SurfaceClient.
      2. Unmanaged Mode: Proxies traffic directly to an external/loopback HTTP(S) endpoint.
    """
    def __init__(self, execution_target: str, handle_id: str, executable_pattern: str = "phix-*.jar"):
        self.execution_target = str(execution_target).strip()
        self.handle_id = handle_id
        
        self._response_queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
        self._inflight_tasks: Set[asyncio.Task] = set()
        self._mcp_session_id: Optional[str] = None
        self._closed = False
        
        self.process = DummyProcess()
        self.is_managed = not self.execution_target.startswith(("http://", "https://"))
        
        if self.is_managed:
            self.mq_surface = EchoListener()
            self.bootstrap = WorkerRuntimeBootstrap(
                bin_root=Path(self.execution_target),
                mq_surface=self.mq_surface,
                executable_pattern=executable_pattern
            )
            self.surface_client: Optional[SurfaceClient] = None
            self.http_client: Optional[httpx.AsyncClient] = None
        else:
            self.bootstrap = None
            self.surface_client = None
            self.http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=10.0),
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
            )

    async def start(self):
        if self.is_managed:
            log.info(f"[Transport:{self.handle_id}] Booting managed network worker node...")
            await self.bootstrap.ensure()
            
            if self.bootstrap.boundary.process_pool:
                self.process.pid = self.bootstrap.boundary.process_pool[-1].pid
                
            self.surface_client = SurfaceClient(
                bootstrap_runtime=self.bootstrap,
                mq_surface=self.mq_surface,
                source_name=f"connector.{self.handle_id}",
                fallback_url="http://localhost:8079", 
                path_prefix=""
            )
            log.info(f"[Transport:{self.handle_id}] Managed worker ready (PID: {self.process.pid}). Surface routing active.")
        else:
            log.info(f"[Transport:{self.handle_id}] Attaching to unmanaged endpoint: {self.execution_target}")
            self.process.pid = 99999  # Ephemeral pseudo-PID for remote/local HTTP targets
            log.info(f"[Transport:{self.handle_id}] Unmanaged worker attached. Direct HTTP multiplexing active.")

    async def send_payload(self, safe_payload: Dict[str, Any]):
        if self._closed:
            raise RuntimeError(f"NetworkTransport [{self.handle_id}] is already closed.")

        # Quarantine 또는 상위 계층에서 전달된 커스텀 HTTP 헤더 분리 (표준 JSON-RPC 오염 방지)
        payload_copy = dict(safe_payload)
        extra_headers = payload_copy.pop("_http_headers", None)

        try:
            raw_bytes = json.dumps(payload_copy).encode('utf-8')
            req_id = payload_copy.get("id", "unknown")
        except TypeError as e:
            log.error(f"[Transport:{self.handle_id}] Payload Serialization Failure: {e}", exc_info=True)
            raise RuntimeError(f"Serialization Exception: {e}")

        # Non-blocking Multiplexing (In-flight Task 추적 관리)
        task = asyncio.create_task(self._dispatch_http_request(req_id, raw_bytes, extra_headers))
        self._inflight_tasks.add(task)
        task.add_done_callback(self._inflight_tasks.discard)

    async def _enqueue_parsed_payload(self, raw_text: str):
        """
        단일 JSON(Pretty-printed 포함), NDJSON(줄바꿈 구분 다중 JSON), 
        또는 SSE(data: ...) 포맷을 판별하여 개별 JSON 단위로 큐에 적재합니다.
        """
        stripped = raw_text.strip()
        if not stripped:
            return

        # 1. 전체 텍스트가 단일 유효 JSON 객체/배열인 경우 (기존 동작 100% 보존)
        try:
            json.loads(stripped)
            await self._response_queue.put(stripped)
            return
        except json.JSONDecodeError:
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
        extra_headers: Optional[Dict[str, str]] = None
    ):
        try:
            if self.is_managed:
                target_path = "/mcp/invoke"
                response_buffer = []
                async for chunk in self.surface_client.request(query_path=target_path, data=data, method="POST"):
                    if isinstance(chunk, bytes):
                        response_buffer.append(chunk.decode('utf-8'))
                    else:
                        response_buffer.append(str(chunk))
                full_response = "".join(response_buffer)
                await self._enqueue_parsed_payload(full_response)
            else:
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream"
                }
                if self._mcp_session_id:
                    headers["Mcp-Session-Id"] = self._mcp_session_id
                if isinstance(extra_headers, dict):
                    headers.update(extra_headers)

                # Streamable HTTP (SSE) 및 일반 JSON 응답 동시 대응
                async with self.http_client.stream(
                    "POST",
                    self.execution_target,
                    content=data,
                    headers=headers
                ) as res:
                    # MCP Stateless/Stateful 세션 헤더 캡처 (YIELD/RESUME 연속성 보장)
                    session_hdr = res.headers.get("Mcp-Session-Id") or res.headers.get("mcp-session-id")
                    if session_hdr:
                        self._mcp_session_id = session_hdr

                    if res.status_code not in (200, 202):
                        err_body = (await res.aread()).decode('utf-8', errors='replace')
                        error_fallback = json.dumps({
                            "jsonrpc": "2.0", 
                            "id": req_id, 
                            "error": {"code": res.status_code, "message": f"HTTP {res.status_code}: {err_body}"}
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
                        full_response = body_bytes.decode('utf-8')
                        await self._enqueue_parsed_payload(full_response)

        except asyncio.CancelledError:
            log.debug(f"[Transport:{self.handle_id}] In-flight HTTP request [{req_id}] cancelled.")
        except Exception as e:
            log.error(f"[Transport:{self.handle_id}] Egress HTTP Dispatch Fractured [{req_id}]: {e}")
            error_fallback = json.dumps({
                "jsonrpc": "2.0", 
                "id": req_id, 
                "error": {"code": -32000, "message": f"Transport Egress Error: {str(e)}"}
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
        return item.encode('utf-8') + b"\n"

    async def close(self):
        if self._closed:
            return
        self._closed = True
        log.info(f"[Transport:{self.handle_id}] Initiating graceful teardown of network transport...")
        
        # 진행 중인 HTTP 태스크 정리
        for task in list(self._inflight_tasks):
            if not task.done():
                task.cancel()
        if self._inflight_tasks:
            await asyncio.gather(*self._inflight_tasks, return_exceptions=True)
        self._inflight_tasks.clear()

        if self.is_managed and self.bootstrap:
            self.bootstrap.shutdown()
        elif not self.is_managed and self.http_client:
            await self.http_client.aclose()
            
        self.process.returncode = 0
        await self._response_queue.put(None)

class StdioTransport:
    """
    Subprocess-based transport layer for standard WorkerConnector
    - Handles raw POSIX standard I/O (STDIN/STDOUT/STDERR)
    """
    def __init__(self, command: str, handle_id: str):
        self.command = command
        self.handle_id = handle_id
        self.process: Optional[asyncio.subprocess.Process] = None
        self._stderr_task: Optional[asyncio.Task] = None

    async def start(self):
        log.info(f"[Transport:{self.handle_id}] Booting legacy sandbox: {self.command}")
        self.process = await asyncio.create_subprocess_shell(
            self.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        self._stderr_task = asyncio.create_task(self._monitor_stderr())
        log.info(f"[Transport:{self.handle_id}] Sandbox running (PID: {self.process.pid})")

    async def _monitor_stderr(self):
        """JSON 스트림을 파싱하여 중앙 로그 시스템으로 릴레이"""
        while self.process and self.process.stderr and not self.process.stderr.at_eof():
            try:
                line = await self.process.stderr.readline()
                if not line:
                    break
                    
                raw_str = line.decode('utf-8', errors='replace').strip()
                if not raw_str:
                    continue
                
                try:
                    log_data = json.loads(raw_str)
                    level = log_data.get("level", "INFO").upper()
                    msg = log_data.get("message", "")
                    worker_req_id = log_data.get("req_id")
                    logger_name = log_data.get("logger", "worker")
                    
                    log_ctx = {
                        "handle_id": self.handle_id, 
                        "worker_req_id": worker_req_id, 
                        "worker_source": logger_name
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
                        
                except json.JSONDecodeError:
                    log.warning(f"[Sandbox:RAW] {raw_str}", extra={"handle_id": self.handle_id})
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[Transport:{self.handle_id}] STDERR Relay fractured: {e}")
                break

    async def send_payload(self, safe_payload: Dict[str, Any]):
        if not self.process or self.process.returncode is not None:
            raise RuntimeError(f"Legacy process {self.handle_id} is dead.")
            
        try:
            raw_msg = json.dumps(safe_payload) + "\n"
        except TypeError as e:
            safe_keys = list(safe_payload.keys())
            log.error(f"[Transport:{self.handle_id}] Payload Serialization Failed: {e}. Top-level Keys: {safe_keys}", exc_info=True)
            raise RuntimeError(f"Serialization failed for transport payload: {e}")

        self.process.stdin.write(raw_msg.encode('utf-8'))
        await self.process.stdin.drain()

    async def receive_raw(self) -> str:
        """Ephemeral 모드 전용: STDOUT을 동기적으로 읽음"""
        raw_output = await self.process.stdout.readline()
        if not raw_output:
            raise RuntimeError(f"EOF reached while reading stdout for {self.handle_id}.")
        return raw_output.decode('utf-8')

    async def read_egress_stream(self) -> bytes:
        """STDOUT 파이프에서 한 줄을 읽어 반환"""
        if self.process and self.process.stdout:
            return await self.process.stdout.readline()
        return b""

    async def close(self):
        if self.process and self.process.returncode is None:
            log.info(f"[Transport:{self.handle_id}] Terminating sandbox (PID: {self.process.pid})")
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                self.process.kill()

        if self._stderr_task and not self._stderr_task.done():
            self._stderr_task.cancel()
            await asyncio.gather(self._stderr_task, return_exceptions=True)