# watcher.xelog.rest
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import urlparse

from fastapi import APIRouter, FastAPI, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from arch.server.gov.mcp import SecureMCPServer, SentinelFirewallMiddleware
from arch.server.gov.middleware import WasTelemetry, LocalMiddleware
from arch.topos.tunnel.subs import DistributedPubSub
from arch.topos.tunnel.factory import UniversalFacade

from watcher.dphi.broker import WasmBroker
from watcher.kernel.log.store import LogStreamStore
from watcher.xelog.edge.a2a.router import a2a_router
from watcher.xelog.edge.ingress import ingress_edge
from watcher.xelog.edge.anchor import anchor_edge
from watcher.xelog.edge.ledger import ledger_edge
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

API_KEY_NAME = "X-XeLog-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

class Config(BaseModel):
    web_url: str = ""
    allow_cors_origins: list[str] = ["http://localhost:3000"] 
    session_api_keys: list[str] = []
    pubsub_channel: str = "xelog_audit_channel"
    wasm_timeout: float = 10.0

def get_default_config() -> Config:
    return Config()

# [FIX 3] Global Authentication Dependency
async def verify_api_key(api_key: str = Security(api_key_header), config: Config = Depends(get_default_config)):
    if not config.session_api_keys:
        # 키가 설정되지 않은 개발 환경에서의 경고 처리 (또는 강제 차단 가능)
        log.warning("No session_api_keys configured. Running in insecure mode!")
        return None
    if api_key not in config.session_api_keys:
        raise HTTPException(status_code=403, detail="Invalid or missing API Key.")
    return api_key

@asynccontextmanager
async def xelog_lifespan(app: FastAPI):
    log.info("[XeLog] Starting XeLog Hub REST API & Services...")
    config: Config = app.state.config
    
    app.state.store = LogStreamStore()
    
    tunnel = UniversalFacade() 
    pubsub = DistributedPubSub(channel=config.pubsub_channel, tunnel=tunnel)
    await pubsub.start_listening()
    app.state.pubsub = pubsub
    
    app.state.broker = WasmBroker(timeout=config.wasm_timeout)
    log.info(f"[XeLog] WasmBroker initialized (timeout: {config.wasm_timeout}s).")

    yield

    log.info("[XeLog] Shutting down XeLog Hub safely...")
    if hasattr(app.state, "pubsub"):
        await pubsub.close()
    if hasattr(app.state, "store"):
        await app.state.store.close()
    log.info("[XeLog] Teardown complete. Goodbye.")


def _get_root_path(config: Config) -> str:
    if config.web_url:
        return urlparse(config.web_url).path.rstrip("/")
    return ""


def add_api_routes(app: FastAPI, config: Config) -> None:
    app.include_router(a2a_router)
    app.include_router(ledger_edge)
    app.include_router(anchor_edge, tags=["mcp-exposed"])

    hub = APIRouter(prefix="/hub", tags=["mcp-exposed"])
    hub.include_router(ingress_edge)
    app.include_router(hub)


def create_app(config: Optional[Config] = None) -> FastAPI:
    config = config or get_default_config()

    app = FastAPI(
        title="XeLog Hub & Edge Router",
        description="Agentic Web & Immutable Ledger Interface",
        lifespan=xelog_lifespan,
        root_path=_get_root_path(config),
        dependencies=[Depends(verify_api_key)]
    )
    
    app.state.config = config
    add_api_routes(app, config)
    
    app.add_middleware(SentinelFirewallMiddleware)
    app.add_middleware(LocalMiddleware, allow_origins=config.allow_cors_origins)
    app.add_middleware(WasTelemetry)

    log.info("[XeLog] Initializing Secure MCP Server...")
    mcp = SecureMCPServer(name="XeLog-MCP-Server", version="1.0.0")
    mcp.bind_fastapi(app, allowed_tags=["mcp-exposed"])
    mcp_asgi_app = mcp.sse_app()
    app.mount("/mcp", mcp_asgi_app)
    
    return app

api = create_app()