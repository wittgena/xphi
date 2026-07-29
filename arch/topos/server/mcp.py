# arch.topos.server.mcp
## @lineage: atoa.secure.server.mcp
import inspect
import json
from typing import Any, Optional
import httpx

from fastapi import FastAPI
from fastapi.routing import APIRoute
from starlette.responses import JSONResponse
from starlette.types import Scope, Receive, Send

from mcp_types import TextContent
from mcp.server.mcpserver.server import MCPServer
from watcher.plane.emitter import get_emitter

log = get_emitter("atoa.secure.mcp")

class SecurityFirewallMiddleware:
    """ASGI Middleware to intercept HTTP traffic, control payload sizes, and enforce access."""
    def __init__(self, app, max_body_size: int = 1024 * 1024 * 5):
        self.app = app
        self.max_body_size = max_body_size

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        # @tag: mitigation_payload_limit - Prevent Memory Exhaustion DoS
        content_length = 0
        for name, value in scope.get("headers", []):
            if name == b"content-length":
                content_length = int(value)
                break
                
        if content_length > self.max_body_size:
            response = JSONResponse({"detail": "Payload Too Large"}, status_code=413)
            return await response(scope, receive, send)

        # @tag: mitigation_auth_bypass - Custom route protection
        path = scope.get("path", "")
        if path.startswith("/custom"):
            headers = dict(scope.get("headers", []))
            if b"authorization" not in headers:
                response = JSONResponse({"detail": "Unauthorized Custom Route"}, status_code=401)
                return await response(scope, receive, send)

        await self.app(scope, receive, send)


class FastAPIMCPAdapter:
    """Automated Bridge: Scans FastAPI routes and dynamically projects them into MCP Tools."""
    def __init__(self, mcp_server: "SecureMCPServer", fastapi_app: FastAPI):
        self.mcp_server = mcp_server
        self.app = fastapi_app

    def register_routes(self, tag_filter: Optional[str] = None, exclude_paths: Optional[list[str]] = None):
        exclude_paths = exclude_paths or ["/openapi.json", "/docs", "/redoc"]
        registered_count = 0

        for route in self.app.routes:
            if not isinstance(route, APIRoute):
                continue

            if route.path in exclude_paths:
                continue

            if tag_filter and tag_filter not in (route.tags or []):
                continue

            self._project_route_to_mcp_tool(route)
            registered_count += 1

        log.info(f"[MCP Adapter] Dynamically registered {registered_count} FastAPI routes as MCP Tools.")

    def _project_route_to_mcp_tool(self, route: APIRoute):
        tool_name = route.name or route.path.strip("/").replace("/", "_")
        path = route.path
        methods = list(route.methods)
        http_method = "POST" if "POST" in methods else ("GET" if "GET" in methods else methods[0])
        
        description = route.description or route.summary or f"Automated REST tool for {http_method} {path}"
        docstring = f"[REST Endpoint: {http_method} {path}]\n{description}"

        # Internal Loopback Dispatcher
        async def dynamic_tool_handler(**kwargs) -> str:
            async with httpx.AsyncClient(app=self.app, base_url="http://internal-loopback") as client:
                try:
                    if http_method == "GET":
                        response = await client.get(path, params=kwargs)
                    elif http_method in ["POST", "PUT", "PATCH"]:
                        response = await client.request(http_method, path, json=kwargs)
                    elif http_method == "DELETE":
                        response = await client.delete(path, params=kwargs)
                    else:
                        raise ValueError(f"Unsupported HTTP method: {http_method}")

                    if response.status_code >= 400:
                        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

                    return response.text
                except Exception as e:
                    log.error(f"[MCP Adapter Error] Tool '{tool_name}' execution failed: {str(e)}")
                    raise

        dynamic_tool_handler.__name__ = tool_name
        dynamic_tool_handler.__doc__ = docstring

        # Strip FastAPI-specific dependencies (like Depends/Request) from the schema
        original_sig = inspect.signature(route.endpoint)
        clean_params = []
        for param in original_sig.parameters.values():
            if param.default.__class__.__name__ in ["Depends", "Param"] or getattr(param.annotation, "__name__", "") == "Request":
                continue
            clean_params.append(param)
            
        dynamic_tool_handler.__signature__ = original_sig.replace(parameters=clean_params)

        self.mcp_server.tool()(dynamic_tool_handler)
        log.debug(f"[MCP Adapter] Tool mapped: {tool_name} -> [{http_method}] {path}")


class SecureMCPServer(MCPServer):
    def bind_fastapi(self, app: FastAPI, tag_filter: Optional[str] = None):
        """Binds a FastAPI application and projects its routes as MCP tools."""
        adapter = FastAPIMCPAdapter(self, app)
        adapter.register_routes(tag_filter=tag_filter)

    async def _handle_call_tool(self, ctx, params):
        # @tag: mitigation_info_leak - Mask tool execution error messages
        result = await super()._handle_call_tool(ctx, params)
        
        if getattr(result, "is_error", False):
            result.content = [
                TextContent(
                    type="text", 
                    text="Internal Tool Error: The operation failed securely. Check server logs."
                )
            ]
        return result

    def sse_app(self, **kwargs) -> Any:
        # @tag: mitigation_asgi_bypass_sse
        app = super().sse_app(**kwargs)
        return SecurityFirewallMiddleware(app)

    def streamable_http_app(self, **kwargs) -> Any:
        # @tag: mitigation_asgi_bypass_http
        app = super().streamable_http_app(**kwargs)
        return SecurityFirewallMiddleware(app)