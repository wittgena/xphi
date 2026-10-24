# xphi.arch.contract.protocol.worker
import sys
import json
import inspect
import logging
import asyncio
import contextvars
from typing import Dict, Any, Optional

from xphi.arch.contract.protocol.binder import ToolBinderMixin, ToolError

_REAL_STDOUT = sys.stdout
sys.stdout = sys.stderr
current_request_id = contextvars.ContextVar("current_request_id", default="SYSTEM")


class JsonStderrFormatter(logging.Formatter):
    """표준 에러(stderr)로 나가는 로그를 JSON으로 구조화하는 포매터"""
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "req_id": current_request_id.get(),
            "message": record.getMessage()
        }
        if hasattr(record, "extra_ctx"):
            log_record.update(record.extra_ctx)

        return json.dumps(log_record)


def _setup_json_logger(agent_name: str) -> logging.Logger:
    logger = logging.getLogger(agent_name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonStderrFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    return logger


class WorkerProtocol(ToolBinderMixin):
    """@desc: 레거시(동기식) 워커를 위한 베이스 클래스 - 순차적 처리 및 자동 툴 바인딩 지원"""
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.log = _setup_json_logger(self.agent_name)
        self._init_tool_binder()

    def _emit_rpc_message(self, message: Dict[str, Any]):
        message["jsonrpc"] = "2.0"
        try:
            raw_out = json.dumps(message) + "\n"
            _REAL_STDOUT.write(raw_out)
            _REAL_STDOUT.flush()
        except TypeError as e:
            self.log.error(f"Payload Serialization Failed: {e}")
            if "error" not in message:
                self.send_error(message.get("id"), -32603, "Internal Serialization Error")

    def send_response(self, req_id: Any, result: Any):
        self._emit_rpc_message({"id": req_id, "result": result})

    def send_error(self, req_id: Any, code: int, message: str, data: Optional[Any] = None):
        err_obj = {"code": code, "message": message}
        if data is not None:
            err_obj["data"] = data
        self._emit_rpc_message({"id": req_id, "error": err_obj})

    def send_request(self, req_id: Any, method: str, params: Optional[Dict[str, Any]] = None):
        msg = {"id": req_id, "method": method}
        if params is not None:
            msg["params"] = params
        self._emit_rpc_message(msg)

    def serve_forever(self):
        """무한 입력 대기 루프 (동기식)"""
        self.log.info(f"Sync Agent '{self.agent_name}' Ignited. Listening on stdin...")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                self._route_request(payload)
            except json.JSONDecodeError:
                self.send_error(None, -32700, "Parse error: Invalid JSON")
            except Exception as e:
                self.log.error(f"Internal Fracture: {e}", exc_info=True)
                self.send_error(payload.get("id") if isinstance(payload, dict) else None, -32603, "Internal Server Error")

    def _route_request(self, req: Dict[str, Any]):
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})

        token = current_request_id.set(req_id)
        self.log.info(f"Incoming RPC payload recognized. Method: {method}")

        try:
            if method == "initialize":
                self.send_response(req_id, {"protocolVersion": "2026-09-04", "capabilities": {}})
            elif method == "tools/list":
                self.handle_tools_list(req_id)
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {}) or {}
                meta = params.get("_meta", {}) or {}
                self.handle_tools_call(req_id, tool_name, arguments, meta)
            else:
                self.send_error(req_id, -32601, f"Unknown method: {method}")
        except Exception as e:
            self.log.error(f"Execution Fault in '{method}': {e}", exc_info=True)
            self.send_error(req_id, -32000, str(e))
        finally:
            current_request_id.reset(token)

    """Default Auto-Bound Handlers (서브클래스에서 오버라이드하지 않으면 자동 작동)"""
    def handle_tools_list(self, req_id: Any):
        self.send_response(req_id, {"tools": self._tool_schemas})

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        func = self._bound_tools.get(tool_name)
        if not func:
            self.send_error(req_id, -32601, f"Unknown tool: {tool_name}")
            return

        try:
            kwargs = self._prepare_call_kwargs(func, arguments, meta)
            result = func(**kwargs)
            self.send_response(req_id, self._wrap_mcp_result(result, is_error=False))
        except ToolError as te:
            self.send_response(req_id, self._wrap_mcp_result(str(te), is_error=True))
        except (ValueError, TypeError, KeyError) as pe:
            self.log.warning(f"Invalid Parameters for {tool_name}: {pe}")
            self.send_error(req_id, -32602, f"Invalid params: {str(pe)}")
        except Exception as e:
            self.log.error(f"Tool Execution Error in '{tool_name}': {e}", exc_info=True)
            self.send_error(req_id, -32000, str(e))


class AsyncWorkerProtocol(ToolBinderMixin):
    """@desc: 모던(비동기) 워커를 위한 베이스 클래스 - 코루틴 라우팅, Non-blocking I/O 및 자동 툴 바인딩 지원"""
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.log = _setup_json_logger(self.agent_name)

        self._stdout_lock = asyncio.Lock()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._init_tool_binder()

    async def _initialize_streams(self):
        loop = asyncio.get_running_loop()
        self._reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(self._reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

        w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, _REAL_STDOUT)
        self._writer = asyncio.StreamWriter(w_transport, w_protocol, self._reader, loop)

    async def _emit_rpc_message_async(self, message: Dict[str, Any]):
        message["jsonrpc"] = "2.0"
        try:
            raw_out = (json.dumps(message) + "\n").encode('utf-8')
            async with self._stdout_lock:
                self._writer.write(raw_out)
                await self._writer.drain()
        except TypeError as e:
            self.log.error(f"Payload Serialization Failed: {e}")
            if "error" not in message:
                await self.send_error(message.get("id"), -32603, "Internal Serialization Error")

    async def send_response(self, req_id: Any, result: Any):
        await self._emit_rpc_message_async({"id": req_id, "result": result})

    async def send_error(self, req_id: Any, code: int, message: str, data: Optional[Any] = None):
        err_obj = {"code": code, "message": message}
        if data is not None:
            err_obj["data"] = data
        await self._emit_rpc_message_async({"id": req_id, "error": err_obj})

    async def send_request(self, req_id: Any, method: str, params: Optional[Dict[str, Any]] = None):
        msg = {"id": req_id, "method": method}
        if params is not None:
            msg["params"] = params
        await self._emit_rpc_message_async(msg)

    async def serve_forever_async(self):
        await self._initialize_streams()
        self.log.info(f"Async Agent '{self.agent_name}' Ignited. Listening on async stdin...")

        while True:
            try:
                line_bytes = await self._reader.readline()
                if not line_bytes:
                    break

                line = line_bytes.decode('utf-8').strip()
                if not line:
                    continue

                payload = json.loads(line)
                asyncio.create_task(self._route_request_async(payload))
            except json.JSONDecodeError:
                await self.send_error(None, -32700, "Parse error: Invalid JSON")
            except Exception as e:
                self.log.error(f"Stream Reader Fracture: {e}", exc_info=True)

    async def _route_request_async(self, req: Dict[str, Any]):
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})
        action = req.get("action")

        token = current_request_id.set(req_id)
        self.log.info(f"Incoming RPC payload recognized. Action/Method: {action or method}")
        try:
            if action == "RESUME":
                self.log.info("Initiating RESUME sequence for parked intent.")
                await self.handle_resume(req_id, req)
                return

            if method == "initialize":
                await self.send_response(req_id, {"protocolVersion": "2026-09-04", "capabilities": {}})
            elif method == "tools/list":
                await self.handle_tools_list(req_id)
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {}) or {}
                meta = params.get("_meta", {}) or {}
                await self.handle_tools_call(req_id, tool_name, arguments, meta)
            else:
                await self.send_error(req_id, -32601, f"Unknown method: {method}")
        except Exception as e:
            self.log.error(f"Async Routing/Execution Fault: {e}", exc_info=True)
            await self.send_error(req_id, -32000, f"Execution failed: {str(e)}")
        finally:
            current_request_id.reset(token)

    """Default Auto-Bound Async Handlers"""
    async def handle_tools_list(self, req_id: Any):
        await self.send_response(req_id, {"tools": self._tool_schemas})

    async def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        func = self._bound_tools.get(tool_name)
        if not func:
            await self.send_error(req_id, -32601, f"Method not found: Unknown tool '{tool_name}'")
            return

        try:
            kwargs = self._prepare_call_kwargs(func, arguments, meta)
            result = func(**kwargs)
            if inspect.isawaitable(result):
                result = await result
            await self.send_response(req_id, self._wrap_mcp_result(result, is_error=False))
        except ToolError as te:
            await self.send_response(req_id, self._wrap_mcp_result(str(te), is_error=True))
        except (ValueError, TypeError, KeyError) as pe:
            self.log.warning(f"Invalid Parameters for {tool_name}: {pe}")
            await self.send_error(req_id, -32602, f"Invalid params: {str(pe)}")
        except Exception as e:
            self.log.error(f"Async Tool Execution Error in '{tool_name}': {e}", exc_info=True)
            await self.send_error(req_id, -32000, str(e))

    async def handle_resume(self, req_id: Any, payload: Dict[str, Any]):
        self.log.warning(f"RESUME payload received for {req_id}, but handle_resume is not implemented.")