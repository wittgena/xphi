# xphi.arch.bound.scope.manager
import os
import sys
import time
import socket
import asyncio
import threading
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Type, Any
from contextlib import asynccontextmanager, AsyncExitStack
import httpx

from xphi.arch.bound.scope.local.llama import LlamaServer

from xphi.arch.contract.config.env import DEFAULT_GATEWAY_PORT
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.plane.tracer.scope import scope_trace, get_current_trace_path

log = get_emitter("scope.manager")

def get_free_port(starting_port: int, max_port: int = 8999) -> int:
    for port in range(starting_port, max_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(('127.0.0.1', port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No free ports available between {starting_port} and {max_port}.")


@dataclass
class SurfaceConfig:
    surface_type: str = "local"
    host: str = "0.0.0.0"
    port: int = DEFAULT_GATEWAY_PORT
    timeout: int = 30
    show_logs: bool = True


class BaseSurface(ABC):
    @abstractmethod
    async def up(self) -> None: 
        pass

    @abstractmethod
    async def down(self) -> None: 
        pass

    @abstractmethod
    def get_engine(self) -> Any: 
        pass


class LocalSurface(BaseSurface):
    def __init__(self, config: SurfaceConfig):
        self.config = config
        self.llama_server = LlamaServer()

    async def up(self) -> None:
        log.info("[*] Initializing Local Direct Surface...")
        self.llama_server.ensure_server()
        try:
            await asyncio.sleep(0.3)
        except Exception as e:
            log.debug(f"[-] Wait interrupted during Local Surface init: {e}")

    async def down(self) -> None:
        log.info("[*] Folding Local Surface...")

    def get_engine(self) -> Any:
        return lambda agent_usage: self.llama_server


class SandboxSurface(BaseSurface):
    def __init__(self, config: SurfaceConfig):
        self.config = config
        self.process = None
        self._stop_event = threading.Event()
        self.threads = []
        self.llm_engine = LlamaServer()
        
        self.process_name = "sandbox.surface"
        self._launcher_module = None 
        self.registry_key = "system:sandbox:pids"

    def stream_output(self, pipe, prefix: str):
        try:
            for line in iter(pipe.readline, ""):
                if self._stop_event.is_set():
                    break
                if line:
                    sys.stdout.write(f"[{prefix}] {line}")
                    sys.stdout.flush()
        finally:
            pipe.close()

    async def up(self) -> None:
        if not self._launcher_module:
            raise NotImplementedError("Launcher module must be injected by subclass.")

        self.config.port = get_free_port(self.config.port)
        self.base_url = f"http://{self.config.host}:{self.config.port}"

        log.info(f"[*] Booting Sandbox Surface on {self.base_url}...")
        self.llm_engine.ensure_server()

        cmd_str = f"exec -a {self.process_name} {sys.executable} -m {self._launcher_module} --host {self.config.host} --port {self.config.port}"
        cmd = ["bash", "-c", cmd_str]
        
        env_vars = {**os.environ, "LOG_JSON": "true", "PYTHONUNBUFFERED": "1"}
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE if self.config.show_logs else subprocess.DEVNULL,
            stderr=subprocess.PIPE if self.config.show_logs else subprocess.DEVNULL,
            text=True, env=env_vars, bufsize=1
        )

        pid = self.process.pid
        
        try:
            tunnel = await TunnelFactory.get_default()
            await tunnel.sadd(self.registry_key, pid)
            log.info(f"[*] Registered Sandbox PID {pid} to {self.registry_key} via Tunnel")
        except Exception as e:
            log.warning(f"[-] Failed to register Sandbox PID to State Store: {e}")

        if self.config.show_logs and self.process.stdout and self.process.stderr:
            t1 = threading.Thread(target=self.stream_output, args=(self.process.stdout, "SURFACE:OUT"), daemon=True)
            t2 = threading.Thread(target=self.stream_output, args=(self.process.stderr, "SURFACE:LOG"), daemon=True)
            t1.start()
            t2.start()
            self.threads = [t1, t2]

        start_time = time.time()
        ready = False
        
        async with httpx.AsyncClient() as client:
            while time.time() - start_time < self.config.timeout:
                if self.process.poll() is not None:
                    raise RuntimeError(f"Server exited with code {self.process.returncode}")
                try:
                    res = await client.get(f"{self.base_url}/ready", timeout=1.0)
                    if res.status_code < 500:
                        ready = True
                        break
                except (httpx.RequestError, httpx.ConnectError):
                    pass
                
                await asyncio.sleep(0.3)

        if not ready:
            await self.down()
            raise RuntimeError("Hand failed to stabilize within timeout.")

        log.info(f"\n[+] Hand stabilized at {self.base_url}\n")

    async def down(self) -> None:
        if self.process:
            log.info("[*] Folding Sandbox Surface (Teardown)...")
            self._stop_event.set()
            self.process.terminate()
            
            try:
                await asyncio.wait_for(asyncio.to_thread(self.process.wait), timeout=5.0)
            except asyncio.TimeoutError:
                self.process.kill()
            
            try:
                tunnel = await TunnelFactory.get_default()
                await tunnel.srem(self.registry_key, self.process.pid)
                log.info(f"[+] Unregistered Sandbox PID {self.process.pid} from {self.registry_key}")
            except Exception as e:
                log.debug(f"[-] State Store cleanup skipped or failed: {e}")
                
            log.info("[+] Sandbox Surface process terminated.")

    def get_engine(self) -> Any:
        return lambda agent_usage: self.llm_engine

SURFACE_REGISTRY = {
    "local": LocalSurface,
    "sandbox": SandboxSurface
}

def get_surface_class(surface_type: str) -> Type[BaseSurface]:
    surface_class = SURFACE_REGISTRY.get(surface_type)
    if not surface_class:
        raise ValueError(f"Unknown surface type: {surface_type}")
    return surface_class

class SurfaceManager:
    def __init__(self, config: SurfaceConfig):
        self.config = config
        surface_type = getattr(config, "surface_type", "local")
        try:
            surface_class = get_surface_class(surface_type)
        except (ImportError, AttributeError, ValueError) as e:
            log.warning(f"🚨 Failed to load '{surface_type}' Surface: {e}. Fallback to default 'local' environment.")
            surface_class = get_surface_class("local")
            
        self.impl = surface_class(config)

    async def up(self):
        await self.impl.up()

    async def down(self):
        await self.impl.down()
    
    def get_engine(self):
        return self.impl.get_engine()

@asynccontextmanager
async def managed_scope(**surface_kwargs):
    config = SurfaceConfig(**surface_kwargs)
    manager = SurfaceManager(config)
    
    facet_type = "logical" if config.surface_type == "local" else "infra"
    surface_name = manager.impl.__class__.__name__.replace("Surface", "").lower()
    
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(scope_trace(name=surface_name, facet=facet_type))
        log.info(f"[*] Entered Trace Path: {get_current_trace_path()}")
        
        try:
            await manager.up()
            yield manager 
        except Exception as e:
            log.error(f"🚨 [managed_scope] pipeline exception: {type(e).__name__} - {e}")
            raise
        finally:
            log.info("[managed_scope] Triggering safe teardown sequence for infrastructure resources.")
            await asyncio.sleep(0.1)
            await manager.down()
            log.info("[+] Context Manager closed safely.")