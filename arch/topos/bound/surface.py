# arch.topos.bound.surface
## @lineage: arch.topos.bound.sandbox.surface
"""
@flow:
-> request: Ψ_out ↦ HTTP Stream 
-> extract: Stream ↦ jobId
-> listen: jobId ↦ SurfaceMQ (Tunnel) ↦ Ψ_in
"""

import urllib.request
import urllib.parse
from urllib.error import HTTPError, URLError
import json
import time
from typing import Optional, Generator

from arch.topos.bound.tunnel import TunnelFactory
from watcher.plane.emitter import get_emitter

log = get_emitter("bound.surface")

class SurfaceMQ:
    """@role: Echolocator & Synchronous Result Listener (MQ)"""
    
    def listen_job(self, channel: str) -> Generator:
        """@flow: Blocking generator for asynchronous job results"""
        listen_client = TunnelFactory.get_isolated_sync()
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
                    if data.get("status") in ("completed", "failed", "eof"):
                        break
                except json.JSONDecodeError:
                    # [개선] 단순 JSON 파싱 실패만 무시하고, 다른 치명적 에러는 방치하지 않음
                    log.debug(f"[Surface:Eye] Invalid JSON payload on {channel}")
                    continue
        finally:
            pubsub.close()
            listen_client.close()

    def echolocate(self, source: str = "surface.probe", timeout: float = 2.0) -> Optional[str]:
        """@flow: Perturb system and listen for resonance (Active Discovery)"""
        listen_client = TunnelFactory.get_isolated_sync()
        pubsub = listen_client.pubsub()
        pubsub.subscribe("system:echo")
        
        log.info(f"[{source}] Perturbing system to find active boundary...")
        publish_client = TunnelFactory.get_sync()
        publish_client.publish("system:ping", json.dumps({"ts": time.time(), "source": source}))

        start_time = time.time()
        active_url = None

        try:
            while time.time() - start_time < timeout:
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
                    except json.JSONDecodeError:
                        continue
        finally:
            pubsub.close()
            listen_client.close()
            
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
            parsed_fallback = urllib.parse.urlparse(self.fallback_url)
            fallback_origin = f"{parsed_fallback.scheme}://{parsed_fallback.netloc}"
            
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
        req_headers = headers or {}
        max_retries = 2
        for attempt in range(max_retries):
            full_url = f"{self.ensure_boundary()}{query_path}"
            req = urllib.request.Request(full_url, data=data, method=method, headers=req_headers)
            
            try:
                yield from self.stream.stream(req, **kwargs)
                break  # 성공 시 루프 탈출
                
            except HTTPError as e:
                log.error(f"[{self.source_name}] Request failed with HTTP {e.code}: {full_url}")
                raise  # HTTP 에러는 엔드포인트 무효화 사유가 아니므로 즉각 실패 처리
                
            except URLError as e:
                if attempt == max_retries - 1:
                    log.error(f"[{self.source_name}] Surface completely unreachable after retries.")
                    raise
                log.warning(f"[{self.source_name}] Boundary collapsed ({e.reason}). Realigning...")
                self._current_endpoint = None  # 무효화. 다음 루프(attempt)에서 ensure_boundary()가 재탐색 유도
                
            except Exception as e:
                log.error(f"[{self.source_name}] Unexpected stream anomaly: {e}")
                raise

    def stream_job(self, query_path: str, channel_prefix: str, method: str = "POST", **kwargs) -> Generator:
        """
        @flow: Action ↦ Perception Unified Pipeline
        호출자는 스레드를 분리할 필요 없이 이 파이프라인 하나로 HTTP 응답과 MQ(Redis/Kafka) 결과를 순차적으로 획득합니다.
        """
        job_id = None
        
        ## Action (Dispatch via HTTP)
        for msg in self.request(query_path, method=method, **kwargs):
            yield ("http", msg)
            if isinstance(msg, str) and msg.startswith("jobId:"):
                job_id = msg.split("jobId:", 1)[1].strip()  # [개선] split 제한(1)을 두어 값에 ':'가 포함되어도 안전하게 파싱

        ## Perception (Listen via MQ Adapter)
        if job_id:
            channel = f"{channel_prefix}{job_id}"
            for data in self.mq.listen_job(channel):
                yield ("mq", data)