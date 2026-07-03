# arch.bound.sandbox.surface
## @lineage: arch.proto.sandbox.surface
"""
@desc: Unified Boundary Interface (Action & Perception)
@flow: 
  [Request] Ψ_out ↦ HTTP Stream 
  [Extract] Stream ↦ jobId
  [Listen]  jobId ↦ SurfaceMQ (Adapter) ↦ Ψ_in
"""
import urllib.request
import urllib.parse
from urllib.error import HTTPError, URLError
import json
import time
from typing import Optional, Generator

# [Architecture Align] 비동기 tunnel을 버리고, 동기식 SyncTunnelFactory 주입
from arch.bound.sandbox.sync import SyncTunnelFactory
from watcher.plane.emitter import get_emitter

log = get_emitter("client.surface")

class SurfaceMQ:
    """@role: Echolocator & Synchronous Result Listener (MQ)"""
    def __init__(self):
        # [FIX] 하드코딩된 host, port 제거. 인프라 연결은 Factory가 전담합니다.
        pass

    def listen_job(self, channel: str) -> Generator:
        """@flow: Blocking generator for asynchronous job results"""
        # [FIX] 제너레이터의 무한 루프가 전역 커넥션을 막지 못하도록 격리된 커넥션 획득
        listen_client = SyncTunnelFactory.get_isolated()
        pubsub = listen_client.pubsub()
        pubsub.subscribe(channel)
        
        log.info(f"[Surface:Eye] Subscribed to MQ: {channel}")

        try:
            for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                try:
                    data = json.loads(msg["data"])
                    yield data
                    
                    # 작업 종료 조건 (완료 또는 에러 시 리스닝 종료)
                    if data.get("status") in ("completed", "failed", "eof"):
                        break
                except Exception:
                    continue
        finally:
            pubsub.close()
            listen_client.close() # [중요] 소켓 자원 즉시 회수

    def echolocate(self, source: str = "surface.probe", timeout: float = 2.0) -> Optional[str]:
        """@flow: Perturb system and listen for resonance (Active Discovery)"""
        # 듣는 귀(Listen)는 격리된 커넥션 사용
        listen_client = SyncTunnelFactory.get_isolated()
        pubsub = listen_client.pubsub()
        pubsub.subscribe("system:echo")
        
        # 찌르는 손(Publish)은 전역 공유 커넥션 사용
        log.info(f"[{source}] Perturbing system to find active boundary...")
        publish_client = SyncTunnelFactory.get_default()
        publish_client.publish("system:ping", json.dumps({"ts": time.time(), "source": source}))

        start_time = time.time()
        active_url = None

        try:
            while time.time() - start_time < timeout:
                # [FIX] timeout 부여로 무한 블로킹 방지
                msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if msg and msg["type"] == "message":
                    try:
                        data = json.loads(msg["data"])
                        if "api_base" in data:
                            raw_url = data["api_base"]
                            parsed = urllib.parse.urlparse(raw_url)
                            active_url = f"{parsed.scheme}://{parsed.netloc}"
                            
                            log.info(f"[echo] Resonance detected. Base Origin: {active_url}")
                            break
                    except Exception:
                        continue
        finally:
            pubsub.close()
            listen_client.close() # 자원 회수
            
        return active_url


class SurfaceClient:
    """
    @role: Boundary Orchestrator (Action & Perception Pipeline)
    @desc: 외부 경계와의 통신(HTTP), 자가 치유(Echolocation), 비동기 결과 수신(MQ)을 단일 흐름으로 응집
    """
    def __init__(self, stream_client, bootstrap_runtime, mq_surface: SurfaceMQ, source_name: str, fallback_url: str, path_prefix: str = ""):
        self.stream = stream_client
        self.bootstrap_runtime = bootstrap_runtime
        self.mq = mq_surface 
        self.source_name = source_name
        self.fallback_url = fallback_url
        self.path_prefix = path_prefix 
        self._current_endpoint = None

    def _ping(self, base_url: str) -> bool:
        """@flow: Lightweight PsiEvent validation"""
        target_url = f"{base_url.rstrip('/')}/psi"
        ping_payload = json.dumps({
            "channel": "system:ping",
            "sourceId": self.source_name,
            "data": "ping_check"
        }).encode('utf-8')

        try:
            req = urllib.request.Request(
                target_url, 
                data=ping_payload, 
                method="POST",
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=1.5) as response:
                result = response.read().decode('utf-8').strip()
                if result == "accepted":
                    log.debug(f"[{self.source_name}] Psi event accepted by {base_url}")
                    return True
                return True # 200 OK
        except HTTPError as e:
            log.debug(f"[{self.source_name}] HTTP {e.code} at /psi. Bypassing bootstrap.")
            return True
        except URLError as e:
            log.warning(f"[{self.source_name}] Boundary collapsed (URLError): {e.reason}")
            return False
        except Exception as e:
            log.error(f"[{self.source_name}] Unexpected ping anomaly: {e}")
            return False

    def ensure_boundary(self) -> str:
        """@flow: 1. Cache ↦ 2. Echolocation ↦ 3. Fallback ↦ 4. Bootstrap"""
        if self._current_endpoint and self._ping(self._current_endpoint.replace(self.path_prefix, "")):
            return self._current_endpoint

        base_origin = self.mq.echolocate(source=self.source_name, timeout=1.0)

        if not base_origin:
            fallback_origin = f"{urllib.parse.urlparse(self.fallback_url).scheme}://{urllib.parse.urlparse(self.fallback_url).netloc}"
            if self._ping(fallback_origin):
                base_origin = fallback_origin
            else:
                log.warning(f"[{self.source_name}] Surface collapsed. Forcing runtime bootstrap...")
                self.bootstrap_runtime.ensure()
                base_origin = fallback_origin

        self._current_endpoint = f"{base_origin}{self.path_prefix}"
        return self._current_endpoint

    def request(self, query_path: str = "", data: bytes = None, method: str = "GET", headers: dict = None, **kwargs) -> Generator:
        """@flow: Robust HTTP Dispatcher (Auto-healing injected)"""
        full_url = f"{self.ensure_boundary()}{query_path}"
        req_headers = headers or {}
        req = urllib.request.Request(full_url, data=data, method=method, headers=req_headers)
        
        try:
            yield from self.stream.stream(req, **kwargs)
        except HTTPError as e:
            log.error(f"[{self.source_name}] Request failed with HTTP {e.code}: {full_url}")
            raise 
        except URLError as e:
            log.warning(f"[{self.source_name}] Boundary collapsed ({e.reason}). Realigning...")
            self._current_endpoint = None 
            full_url = f"{self.ensure_boundary()}{query_path}"
            req = urllib.request.Request(full_url, data=data, method=method, headers=req_headers)
            yield from self.stream.stream(req, **kwargs)
        except Exception as e:
            log.error(f"[{self.source_name}] Unexpected stream anomaly: {e}")
            raise

    def stream_job(self, query_path: str, channel_prefix: str, method: str = "POST", **kwargs) -> Generator:
        """
        @flow: Action ↦ Perception Unified Pipeline
        호출자는 스레드를 분리할 필요 없이 이 파이프라인 하나로 HTTP 응답과 MQ(Redis/Kafka) 결과를 순차적으로 획득합니다.
        """
        job_id = None
        
        ## 1. Action (Dispatch via HTTP)
        for msg in self.request(query_path, method=method, **kwargs):
            yield ("http", msg)
            if isinstance(msg, str) and msg.startswith("jobId:"):
                job_id = msg.split("jobId:")[1].strip()

        ## 2. Perception (Listen via MQ Adapter)
        if job_id:
            channel = f"{channel_prefix}{job_id}"
            for data in self.mq.listen_job(channel):
                yield ("mq", data)