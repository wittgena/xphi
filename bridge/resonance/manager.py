# resonance.manager
from abc import ABC, abstractmethod
import os
import subprocess
import sys
import time
import threading
import httpx
from dataclasses import dataclass
from typing import Optional
from bridge.client.local.engine import LLMEngine
from flow.surface.emitter import get_emitter

log = get_emitter("resonance.manager")

@dataclass
class SurfaceConfig:
    """실행 표면 설정을 위한 데이터 클래스"""
    use_hands: bool = False
    host: str = "0.0.0.0"
    port: int = 8000
    timeout: int = 30
    show_logs: bool = True

class BaseSurface(ABC):
    @abstractmethod
    def up(self): pass

    @abstractmethod
    def down(self): pass

    @abstractmethod
    def get_engine(self): pass

class LocalSurface(BaseSurface):
    def __init__(self):
        self.engine = LLMEngine()

    def up(self):
        log.info("[*] Initializing Local Direct Surface...")
        self.engine.ensure_server()

    def down(self):
        log.info("[*] Folding Local Surface...")

    def get_engine(self):
        return lambda agent_usage: self.engine

class HandSurface(BaseSurface):
    def __init__(self, config: SurfaceConfig):
        self.config = config
        self.base_url = f"http://{config.host}:{config.port}"
        self.process = None
        self._stop_event = threading.Event()
        self.threads = []

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

    def up(self):
        log.info(f"[*] Booting Hand Surface on {self.base_url}...")
        cmd = [sys.executable, "-m", "work.hand.launcher", "--host", self.config.host, "--port", str(self.config.port)]
        env = {**os.environ, "LOG_JSON": "true", "PYTHONUNBUFFERED": "1", "OPENHANDS_SUPPRESS_BANNER": "1"}
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE if self.config.show_logs else subprocess.DEVNULL,
            stderr=subprocess.PIPE if self.config.show_logs else subprocess.DEVNULL,
            text=True, env=env, bufsize=1
        )

        if self.config.show_logs and self.process.stdout and self.process.stderr:
            t1 = threading.Thread(target=self.stream_output, args=(self.process.stdout, "SURFACE:OUT"), daemon=True)
            t2 = threading.Thread(target=self.stream_output, args=(self.process.stderr, "SURFACE:LOG"), daemon=True)
            t1.start()
            t2.start()
            self.threads = [t1, t2]

        start_time = time.time()
        ready = False
        while time.time() - start_time < self.config.timeout:
            if self.process.poll() is not None:
                raise RuntimeError(f"Server exited with code {self.process.returncode}")
            try:
                if httpx.get(f"{self.base_url}/ready", timeout=1.0).status_code < 500:
                    ready = True
                    break
            except (httpx.RequestError, httpx.ConnectError):
                pass
            time.sleep(1)

        if not ready:
            self.down()
            raise RuntimeError("Hand failed to stabilize within timeout.")

        log.info(f"\n[+] Hand stabilized at {self.base_url}\n")

    def down(self):
        if self.process:
            log.info("[*] Folding Hand Surface (Teardown)...")
            self._stop_event.set()
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            log.info("[+] Hand Surface process terminated.")

    def get_engine(self):
        from bridge.resonance.hand.engine import OpenHandsEngine
        return lambda agent_usage: OpenHandsEngine(self.base_url, agent_usage)

class SurfaceManager:
    def __init__(self, config: SurfaceConfig):
        self.config = config
        if config.use_hands:
            self.impl = HandSurface(config)
        else:
            self.impl = LocalSurface()

    def __enter__(self):
        self.impl.up()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.impl.down()
        log.info("[+] Context Manager: Surface closed.")

    def get_engine(self):
        """위상에 맞는 '엔진 생성 함수'를 반환"""
        return self.impl.get_engine()

def managed_resonance(**kwargs):
    """기존 호출 방식을 유지하면서 내부적으로 Config 객체 생성"""
    config = SurfaceConfig(**kwargs)
    return SurfaceManager(config)

if __name__ == "__main__":
    with managed_resonance(use_hands=True, show_logs=True) as server:
        engine = server.get_engine()
        log.info(f"Surface is active. get_engine() returned: {type(engine).__name__}")
        time.sleep(2)