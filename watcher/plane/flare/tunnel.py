# xphi.watcher.plane.flare.tunnel
import os
import json
import asyncio
import httpx
from urllib.parse import urlparse
from typing import Optional, Any

from xphi.arch.contract.config.env import config as env_config
from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.contract.config.tunnel import BackendProtocol, resolve_default_config, parse_connection_urls
from xphi.watcher.plane.flare.bridge import (
    DEFAULT_LOCAL_EDGE_URL,
    PayloadKey,
    ResultKey,
    _MockPubSub,
    _MockStateStore,
)

log = get_emitter("tunnel.flare")

class EdgeStateAnchor:
    """Extension Interface: Stateful Edge Binding (Durable Objects / Queues / D1)"""
    async def on_control_event(self, job_id: Optional[str], method_func: Optional[str], payload: dict) -> Optional[dict]:
        return None

    async def stream_produce(self, *args, **kwargs) -> str: return ""
    async def stream_consume(self, *args, **kwargs) -> list: return []
    async def stream_ack(self, *args, **kwargs) -> int: return 1

class FlareFacade:
    def __init__(
        self,
        state_url: str,
        mq_url: str,
        mq_protocol: BackendProtocol,
        anchor: Optional[EdgeStateAnchor] = None,
        **kwargs
    ):
        self.mq_protocol = mq_protocol
        self.mq_url = mq_url
        self.wasm_broker = None
        self.anchor = anchor or EdgeStateAnchor()

        pool_kwargs = {
            "timeout": httpx.Timeout(15.0, connect=5.0),
            "limits": httpx.Limits(max_keepalive_connections=0),
            "verify": False
        }
        self.http_client = httpx.AsyncClient(base_url=self.mq_url, http1=True, http2=False, **pool_kwargs)
        self._response_queue = asyncio.Queue()
        self.state_store = _MockStateStore(
            self.http_client,
            self._response_queue,
            mq_url=self.mq_url,
            runtime_ready_hook=FlareTunnelFactory.ensure_local_runtime_ready,
        )
        log.info(f"[FlareTunnel] Initialized Duck-Typed HTTP Bridge targeting Edge: {self.mq_url}")

    def pubsub(self):
        """Mock pubsub to satisfy broker's listener"""
        log.debug("[FlareTunnel:pubsub] Initializing mock pubsub listener")
        return _MockPubSub(self._response_queue)

    async def publish(self, channel: str, message: Any):
        """Handle Control Plane publish events to prevent Broker timeouts"""
        log.debug(f"[FlareTunnel:publish] Intercepted publish on channel: {channel}")
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            payload = json.loads(message)
            job_id = payload.get(PayloadKey.JOB_ID)
            method_func = payload.get(PayloadKey.METHOD_FUNC)

            # Optional external Edge State Anchor hook (No-op by default)
            ext_ack = await self.anchor.on_control_event(job_id, method_func, payload)
            if ext_ack is not None:
                if job_id:
                    await self._response_queue.put({'type': 'message', 'data': json.dumps(ext_ack).encode('utf-8')})
                return 1

            control_methods = {"update_policy"}
            if method_func in control_methods:
                log.debug(f"[FlareTunnel:publish] Intercepted Control Message '{method_func}'. Dropping HTTP transmission.")
                if job_id:
                    ack_msg = {
                        'type': 'message',
                        'data': json.dumps({
                            PayloadKey.JOB_ID: job_id,
                            ResultKey.SUCCESS: True,
                            ResultKey.OUTPUT: f"Policy '{method_func}' mocked successfully at edge boundary"
                        }).encode('utf-8')
                    }
                    await self._response_queue.put(ack_msg)
                return 1

            if method_func:
                task = asyncio.create_task(self.state_store._process_edge_request(job_id, payload))
                self.state_store._background_tasks.add(task)
                task.add_done_callback(self.state_store._background_tasks.discard)
            else:
                if job_id:
                    log.debug(f"[FlareTunnel:publish] Mocking successful ACK for empty method publish event (job_id: {job_id})")
                    ack_msg = {
                        'type': 'message',
                        'data': json.dumps({
                            PayloadKey.JOB_ID: job_id,
                            ResultKey.SUCCESS: True,
                            ResultKey.OUTPUT: "Event synchronized at edge boundary"
                        }).encode('utf-8')
                    }
                    await self._response_queue.put(ack_msg)

        except Exception as e:
            log.error(f"[FlareTunnel:publish] Exception processing publish mock: {e}")
        return 1

    def bind_wasm_broker(self, broker):
        self.wasm_broker = broker

    async def stream_produce(self, *args, **kwargs):
        return await self.anchor.stream_produce(*args, **kwargs)

    async def stream_consume(self, *args, **kwargs):
        return await self.anchor.stream_consume(*args, **kwargs)

    async def stream_ack(self, *args, **kwargs):
        return await self.anchor.stream_ack(*args, **kwargs)

    async def close(self):
        log.debug("[FlareTunnel:close] Closing FlareFacade and HTTP client")
        await self.state_store.aclose()
        await self.http_client.aclose()


class FlareTunnelFactory:
    _async_instance: Optional[FlareFacade] = None
    _managed_controller: Optional[Any] = None
    _runtime_lock: Optional[asyncio.Lock] = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._runtime_lock is None:
            cls._runtime_lock = asyncio.Lock()
        return cls._runtime_lock

    @staticmethod
    def _is_local_endpoint(url: str) -> bool:
        try:
            parsed = urlparse(url)
            return parsed.hostname in ("127.0.0.1", "localhost")
        except Exception:
            return False

    @staticmethod
    async def _is_port_open(url: str) -> bool:
        try:
            parsed = urlparse(url)
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=0.3)
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False

    @classmethod
    async def ensure_local_runtime_ready(cls, mq_url: str) -> None:
        if not cls._is_local_endpoint(mq_url):
            return

        if await cls._is_port_open(mq_url):
            return

        async with cls._get_lock():
            if await cls._is_port_open(mq_url):
                return

            if cls._managed_controller is None:
                log.info(f"[FlareTunnelFactory] Local V8 Isolate not detected at {mq_url}. Auto-starting dev runtime...")
                try:
                    from xphi.watcher.plane.flare.controller import FlareController
                    controller = FlareController(mode="dev")
                    cls._managed_controller = controller
                    await controller.start_runtime()
                except Exception as e:
                    cls._managed_controller = None
                    log.error(f"[FlareTunnelFactory] Auto-start of local V8 Isolate failed: {e}")
                    raise

    @staticmethod
    def _resolve_edge_url(fallback_url: str) -> str:
        raw_plane = str(env_config.get("XPHI_WASM_PLANE", os.getenv("XPHI_WASM_PLANE", ""))).strip()
        plane_lower = raw_plane.lower()

        if plane_lower.startswith("flare:"):
            target = raw_plane.split(":", 1)[1].strip()
            if not target or target.lower() in ("dev", "local"):
                return DEFAULT_LOCAL_EDGE_URL
            if target.startswith(("http://", "https://")):
                return target
            return f"http://{target}" if target.startswith(("127.0.0.1", "localhost")) else f"https://{target}"

        if plane_lower.startswith(("http://", "https://")):
            return raw_plane

        if plane_lower == "flare":
            return DEFAULT_LOCAL_EDGE_URL

        if fallback_url.startswith(("http://", "https://")):
            return fallback_url
        return DEFAULT_LOCAL_EDGE_URL

    @classmethod
    async def get_default(cls, **kwargs) -> FlareFacade:
        explicit_mq_url = kwargs.pop("mq_url", None)

        if cls._async_instance is not None:
            if explicit_mq_url and cls._async_instance.mq_url != explicit_mq_url:
                await cls.close_all()
            else:
                return cls._async_instance

        config = resolve_default_config()
        scheme, state_url, parsed_mq_url = parse_connection_urls(config.default_url)

        mq_url = explicit_mq_url or cls._resolve_edge_url(parsed_mq_url)
        state_url = kwargs.pop("state_url", state_url)
        scheme = kwargs.pop("mq_protocol", scheme)

        cls._async_instance = FlareFacade(state_url, mq_url, scheme, **kwargs)
        log.info(f"[FlareTunnelFactory] Provisioned Async Flare Tunnel: {mq_url}")
        return cls._async_instance

    @classmethod
    async def get_isolated(cls, **kwargs) -> FlareFacade:
        return await cls.get_default(**kwargs)

    @classmethod
    async def get_provenant(cls, wasm_broker, **kwargs) -> FlareFacade:
        tunnel = await cls.get_default(**kwargs)
        tunnel.bind_wasm_broker(wasm_broker)
        return tunnel

    @classmethod
    async def close_all(cls):
        if cls._async_instance:
            await cls._async_instance.close()
            cls._async_instance = None

        if cls._managed_controller is not None:
            controller = cls._managed_controller
            cls._managed_controller = None
            await controller.stop_runtime()