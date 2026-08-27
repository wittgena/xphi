# xphi.watcher.plane.sink
## @lineage: watcher.plane.sink
## @lineage: phase.runtime.surface.sink
import os
import json
import aiohttp
from abc import ABC, abstractmethod
from typing import AsyncGenerator, Optional
from xphi.kernel.space.topos.tunnel.factory import UniversalFacade, from_url as tunnel_from_url

class EmitterSink(ABC):
    @abstractmethod
    async def set(self, key: str, payload: dict): pass

    @abstractmethod
    async def delete(self, key: str): pass

    async def get_control_flag(self, key: str): return None
    async def close(self): pass

    @abstractmethod
    async def publish(self, channel: str, payload: str): pass

    @abstractmethod
    async def subscribe(self, channel: str) -> AsyncGenerator[str, None]: pass

class TunnelSink(EmitterSink):
    """
    @role: Universal Sink utilizing the Tunnel Facade
    @flow: Replaces RedisSink to support seamless multi-protocol (Redis, Kafka) routing
    """
    def __init__(self, tunnel: Optional[UniversalFacade] = None, url: Optional[str] = None):
        self.tunnel = tunnel
        self.url = url

    async def initialize(self):
        if not self.tunnel and self.url:
            self.tunnel = await tunnel_from_url(self.url)

    async def set(self, key, payload):
        await self.tunnel.set(key, str(payload))

    async def delete(self, key):
        await self.tunnel.delete(key)

    async def get_control_flag(self, key):
        val = await self.tunnel.get(key)
        return val if isinstance(val, str) else (val.decode() if val else None)

    async def close(self):
        if self.tunnel and hasattr(self.tunnel, 'close'):
            await self.tunnel.close()
    
    async def publish(self, channel: str, payload: str):
        await self.tunnel.publish(channel, payload)

    async def subscribe(self, channel: str) -> AsyncGenerator[str, None]:
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(channel)
        
        async for message in pubsub.listen():
            if isinstance(message, dict) and message["type"] == "message":
                data = message["data"]
                yield data

class FileSink(EmitterSink):
    def __init__(self, base_dir="/tmp/psi_surface"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _path(self, key):
        return os.path.join(self.base_dir, f"{key}.json")

    async def set(self, key, payload):
        with open(self._path(key), "w") as f:
            json.dump(payload, f)

    async def delete(self, key):
        path = self._path(key)
        if os.path.exists(path):
            os.remove(path)

    async def publish(self, channel: str, payload: str):
        # File 방식에서의 Pub/Sub은 꼬리물기(tail) 등으로 구현해야 하므로 임시 패스
        pass

    async def subscribe(self, channel: str) -> AsyncGenerator[str, None]:
        # NotImplemented. 향후 watchdog 이나 inotify 로 파일 변경 스트림 구현 가능
        yield "" 

class ApiSink(EmitterSink):
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.session = aiohttp.ClientSession()

    async def set(self, key, payload):
        await self.session.post(f"{self.endpoint}/{key}", json=payload)

    async def delete(self, key):
        await self.session.delete(f"{self.endpoint}/{key}")

    async def close(self):
        await self.session.close()

    async def publish(self, channel: str, payload: str):
        ## Webhook이나 SSE(Server-Sent Events) 트리거 용도
        await self.session.post(f"{self.endpoint}/publish/{channel}", json={"payload": payload})

    async def subscribe(self, channel: str) -> AsyncGenerator[str, None]:
        ## SSE(Server-Sent Events)나 WebSocket을 통한 스트리밍 구현 자리
        yield ""