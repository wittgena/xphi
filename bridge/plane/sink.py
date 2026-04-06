# bridge.plane.sink
import aiohttp
from abc import ABC, abstractmethod
import redis.asyncio as redis_async

class EmitterSink(ABC):
    @abstractmethod
    async def set(self, key: str, payload: dict):
        pass

    @abstractmethod
    async def delete(self, key: str):
        pass

    async def get_control_flag(self, key: str):
        return None

    async def close(self):
        pass

class RedisSink(EmitterSink):
    def __init__(self, host="localhost", port=6379, db=0):
        self.redis = redis_async.Redis(host=host, port=port, db=db)

    async def set(self, key, payload):
        await self.redis.set(key, str(payload))

    async def delete(self, key):
        await self.redis.delete(key)

    async def get_control_flag(self, key):
        val = await self.redis.get(key)
        return val.decode() if val else None

    async def close(self):
        await self.redis.close()

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