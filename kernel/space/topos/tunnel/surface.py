# xphi.kernel.space.topos.tunnel.surface
import json
import time
import asyncio
import urllib.parse
from typing import Optional, AsyncGenerator
import httpx

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("bound.surface")

class SurfaceMQ:
    """@role: Echolocator & Asynchronous Result Listener (MQ)"""

    async def register_state(self, key: str, value: str):
        """@flow: Asynchronously register state to the global tunnel"""
        tunnel = await TunnelFactory.get_default()
        await tunnel.sadd(key, value)

    async def listen_job(self, channel: str) -> AsyncGenerator[dict, None]:
        """@flow: Non-blocking async generator for job results"""
        # PubSub은 전용 커넥션이 필요하므로 고립(Isolated) 객체 사용
        listen_client = await TunnelFactory.get_isolated()
        pubsub = listen_client.pubsub()
        await pubsub.subscribe(channel)
        
        log.info(f"[Surface:Eye] Subscribed to MQ: {channel}")

        try:
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                try:
                    # Redis-py async는 데이터를 bytes로 반환할 수 있으므로 디코딩 안전장치 추가
                    raw_data = msg["data"]
                    if isinstance(raw_data, bytes):
                        raw_data = raw_data.decode('utf-8')
                        
                    data = json.loads(raw_data)
                    yield data
                    
                    if isinstance(data, dict) and data.get("status") in ("completed", "failed", "eof"):
                        break
                except (json.JSONDecodeError, UnicodeDecodeError):
                    log.debug(f"[Surface:Eye] Malformed payload received on {channel}")
                    continue
        finally:
            await pubsub.close()
            await listen_client.close()

    async def echolocate(self, source: str = "surface.probe", timeout: float = 2.0) -> Optional[str]:
        """@flow: Perturb system and listen for resonance without blocking the event loop"""
        listen_client = await TunnelFactory.get_isolated()
        pubsub = listen_client.pubsub()
        await pubsub.subscribe("system:echo")
        
        log.info(f"[{source}] Perturbing system to find active boundary...")
        publish_client = await TunnelFactory.get_default()
        await publish_client.publish("system:ping", json.dumps({"ts": time.time(), "source": source}))

        active_url = None
        try:
            # asyncio.timeout (Python 3.11+) 또는 wait_for를 활용한 우아한 타임아웃 제어
            async with asyncio.timeout(timeout):
                async for msg in pubsub.listen():
                    if msg["type"] == "message":
                        try:
                            raw_data = msg["data"].decode('utf-8') if isinstance(msg["data"], bytes) else msg["data"]
                            data = json.loads(raw_data)
                            
                            if "api_base" in data:
                                raw_url = data["api_base"]
                                parsed = urllib.parse.urlparse(raw_url)
                                active_url = f"{parsed.scheme}://{parsed.netloc}"
                                log.info(f"[echo] Resonance detected. Base Origin: {active_url}")
                                break
                        except json.JSONDecodeError:
                            continue
        except asyncio.TimeoutError:
            log.debug(f"[{source}] Echolocation timed out after {timeout}s")
        finally:
            await pubsub.close()
            await listen_client.close()
            
        return active_url


class SurfaceClient:
    def __init__(self, bootstrap_runtime, mq_surface: SurfaceMQ, source_name: str, fallback_url: str, path_prefix: str = ""):
        self.bootstrap_runtime = bootstrap_runtime
        self.mq = mq_surface 
        self.source_name = source_name
        self.fallback_url = fallback_url
        self.path_prefix = path_prefix 
        self._current_endpoint = None

    async def _ping(self, base_url: str) -> bool:
        """@flow: Lightweight PsiEvent validation via httpx"""
        target_url = f"{base_url.rstrip('/')}/psi"
        payload = {
            "channel": "system:ping",
            "sourceId": self.source_name,
            "data": "ping_check"
        }

        try:
            async with httpx.AsyncClient(timeout=1.5) as client:
                resp = await client.post(target_url, json=payload)
                resp.raise_for_status()
                if resp.text.strip() == "accepted":
                    log.debug(f"[{self.source_name}] Psi event accepted by {base_url}")
                    return True
                return False
        except httpx.HTTPStatusError as e:
            # HTTP 에러(예: 404, 500)는 서버가 살아있다는 증거이므로 긍정(True)으로 간주 (Bypass Bootstrap)
            log.debug(f"[{self.source_name}] HTTP {e.response.status_code} at /psi. Bypassing bootstrap.")
            return True
        except httpx.RequestError as e:
            # 네트워크 단절, 타임아웃, 커넥션 거부 등은 서버 붕괴로 판단
            log.warning(f"[{self.source_name}] Boundary collapsed (Network Error): {e}")
            return False

    async def ensure_boundary(self) -> str:
        """@flow: 1. Cache ↦ 2. Echolocation ↦ 3. Fallback ↦ 4. Bootstrap"""
        if self._current_endpoint and await self._ping(self._current_endpoint.replace(self.path_prefix, "")):
            return self._current_endpoint

        base_origin = await self.mq.echolocate(source=self.source_name, timeout=1.0)

        if not base_origin:
            parsed_fallback = urllib.parse.urlparse(self.fallback_url)
            fallback_origin = f"{parsed_fallback.scheme}://{parsed_fallback.netloc}"
            
            if await self._ping(fallback_origin):
                base_origin = fallback_origin
            else:
                log.warning(f"[{self.source_name}] Surface collapsed. Forcing runtime bootstrap...")
                # JvmRuntime의 async ensure() 정상 호출 가능
                await self.bootstrap_runtime.ensure()
                base_origin = fallback_origin

        self._current_endpoint = f"{base_origin}{self.path_prefix}"
        return self._current_endpoint

    async def request(self, query_path: str = "", data: bytes = None, method: str = "GET", headers: dict = None, **kwargs) -> AsyncGenerator[str, None]:
        """@flow: Robust Async HTTP Dispatcher (Auto-healing injected)"""
        req_headers = headers or {}
        max_retries = 2
        
        for attempt in range(max_retries):
            full_url = f"{await self.ensure_boundary()}{query_path}"
            
            try:
                # httpx를 활용한 네이티브 비동기 스트리밍 (StreamClient 의존성 제거)
                async with httpx.AsyncClient() as client:
                    async with client.stream(method=method, url=full_url, content=data, headers=req_headers, **kwargs) as response:
                        response.raise_for_status()
                        async for chunk in response.aiter_text():
                            if chunk:
                                yield chunk
                break
            except httpx.HTTPStatusError as e:
                log.error(f"[{self.source_name}] Request failed with HTTP {e.response.status_code}: {full_url}")
                raise
            except httpx.RequestError as e:
                if attempt == max_retries - 1:
                    log.error(f"[{self.source_name}] Surface completely unreachable after retries.")
                    raise
                log.warning(f"[{self.source_name}] Boundary collapsed ({e}). Realigning...")
                self._current_endpoint = None

    async def stream_job(self, query_path: str, channel_prefix: str, method: str = "POST", **kwargs) -> AsyncGenerator[tuple, None]:
        """@flow: Action ↦ Perception Unified Pipeline (Fully Async)"""
        job_id = None
        
        async for msg in self.request(query_path, method=method, **kwargs):
            yield ("http", msg)
            if isinstance(msg, str) and msg.startswith("jobId:"):
                job_id = msg.split("jobId:", 1)[1].strip()

        if job_id:
            channel = f"{channel_prefix}{job_id}"
            async for data in self.mq.listen_job(channel):
                yield ("mq", data)