# exam.hands.flow
"""
@flow: Φ(config) → Ψ₀(genesis) → Ψᵢ(injection) → Ψ(execution) → Φ′(observation)
@focus: Autopoietic loop closure, live contract probing, topological degradation
"""
import uuid
import subprocess
import sys
import time
import json
import httpx
from contextlib import contextmanager
from bridge.plane.emitter import get_logger

SERVER_CMD = [sys.executable, "-m", "openhands.agent_server"]
BASE_URL = "http://0.0.0.0:8000"
OPENAPI_URL = f"{BASE_URL}/openapi.json"

log = get_logger("flow.runtime")

@contextmanager
def managed_server(timeout: int = 30):
    """서버 프로세스의 생명주기를 관리하는 컨텍스트 (Φ_surface)"""
    process = None
    try:
        log.info("[*] Booting OpenHands Execution Surface...")
        process = subprocess.Popen(
            SERVER_CMD,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        start_time = time.time()
        ready = False
        while time.time() - start_time < timeout:
            try:
                if httpx.get(f"{BASE_URL}/ready", timeout=1.0).status_code < 500:
                    ready = True
                    break
            except httpx.RequestError:
                pass
            time.sleep(1)
            
        if not ready:
            raise RuntimeError("Surface failed to stabilize within timeout.")
            
        log.info("[+] Surface stabilized. Ready for inversion.\n")
        yield process
        
    finally:
        if process:
            log.info("\n[*] Folding surface gracefully (Teardown)...")
            process.terminate()
            process.wait(timeout=5)


class TopologicalRuntime:
    """오류가 발생해도 가상 상태를 유지하며, 라이브 스펙을 기반으로 기댓값을 투영합니다."""
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.Client(base_url=base_url, timeout=15.0)
        self.conversation_id: str = str(uuid.UUID(int=0)) 
        self.is_healthy: bool = True
        self.openapi_spec: dict = self._fetch_live_openapi_spec()

    def _fetch_live_openapi_spec(self) -> dict:
        """@phase: Φ_probe (Fetch live contract from the active surface)"""
        log.info("[Φ:probe] Extracting live OpenAPI contract from surface...")
        try:
            response = httpx.get(OPENAPI_URL, timeout=5.0)
            response.raise_for_status()
            log.info("  └ [Φ:bound] Contract successfully synchronized.")
            return response.json()
        except Exception as e:
            log.warning(f"  └ [∂Φ:warn] Failed to synchronize live spec: {e}")
            return {}

    def _log_http_error(self, phase: str, e: httpx.HTTPStatusError, api_path: str):
        """HTTP 에러 발생 시 라이브 스펙을 대조하여 기댓값을 가시화합니다."""
        status = e.response.status_code
        method = e.request.method
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text

        log.error(f"  └ [∂Φ:error] {phase} rejected ({status}): {detail}")
        self.is_healthy = False

        # Live OpenAPI 스펙 출력 로직 (Contract Projection)
        if self.openapi_spec:
            # 원본 OpenAPI 스펙의 구조는 paths -> 경로 -> HTTP메서드 순입니다.
            route_spec = self.openapi_spec.get("paths", {}).get(api_path, {}).get(method.lower())
            
            if route_spec:
                log.info(f"    [Φ:contract] Required Spec for {method.upper()} {api_path}:")
                
                params = route_spec.get("parameters")
                body = route_spec.get("requestBody")
                
                if params:
                    params_str = json.dumps(params, indent=2, ensure_ascii=False)
                    indented_params = "\n".join(f"      {line}" for line in params_str.splitlines())
                    log.info(f"      - Params:\n{indented_params}")
                    
                if body:
                    body_str = json.dumps(body, indent=2, ensure_ascii=False)
                    indented_body = "\n".join(f"      {line}" for line in body_str.splitlines())
                    log.info(f"      - Body:\n{indented_body}")
            else:
                log.info(f"    [Φ:contract] No spec found for {method.upper()} {api_path} in live contract.")

    def trigger_genesis(self):
        """@phase: Ψ₀ (Conversation Creation)"""
        log.info("[Ψ₀:genesis] Initiating conversation space...")
        api_path = "/api/conversations"
        try:
            payload = {
                "agent": "CodeActAgent",
                "workspace": "./"        
            }
            response = self.client.post(api_path, json=payload)
            response.raise_for_status()
            
            data = response.json()
            extracted_id = data.get("id") or data.get("conversation_id")
            
            if extracted_id:
                self.conversation_id = extracted_id
                log.info(f"  └ [Φ:bound] Space created: {self.conversation_id}")
            else:
                log.warning("  └ [∂Φ:warn] Genesis succeeded but returned no ID.")
                self.is_healthy = False
                
        except httpx.HTTPStatusError as e:
            self._log_http_error("Genesis", e, api_path)
        except Exception as e:
            log.error(f"  └ [∂Φ:error] Genesis exception: {e}")
            self.is_healthy = False

    def trigger_injection(self):
        """@phase: Ψᵢ (Message Injection)"""
        log.info(f"[Ψᵢ:injection] Stimulating loop at /events (ID: {self.conversation_id})...")
        api_path = "/api/conversations/{conversation_id}/events"
        try:
            payload = {
                "action": "message",
                "args": {"content": "Hello, topology probe."}
            }
            response = self.client.post(
                f"{self.base_url}/api/conversations/{self.conversation_id}/events", 
                json=payload
            )
            response.raise_for_status()
            log.info("  └ [Φ:bound] Injection successful.")
        except httpx.HTTPStatusError as e:
            self._log_http_error("Injection", e, api_path)
        except Exception as e:
            log.error(f"  └ [∂Φ:error] Injection exception: {e}")

    def trigger_activation(self):
        """@phase: Ψ (Execution Trigger)"""
        log.info(f"[Ψ:activation] Firing execution kernel at /run (ID: {self.conversation_id})...")
        api_path = "/api/conversations/{conversation_id}/run"
        try:
            response = self.client.post(f"{self.base_url}/api/conversations/{self.conversation_id}/run")
            response.raise_for_status()
            log.info("  └ [Φ:bound] Kernel active.")
        except httpx.HTTPStatusError as e:
            self._log_http_error("Activation", e, api_path)
        except Exception as e:
            log.error(f"  └ [∂Φ:error] Activation exception: {e}")

    def observe_closure(self):
        """@phase: Φ′ (Result Observation & Closure Detection)"""
        log.info(f"[Φ′:observation] Tracing world line at /events/search (ID: {self.conversation_id})...")
        api_path = "/api/conversations/{conversation_id}/events/search"
        try:
            response = self.client.get(f"{self.base_url}/api/conversations/{self.conversation_id}/events/search")
            response.raise_for_status()
            events = response.json()
            
            if isinstance(events, list):
                log.info(f"  └ [Φ′:stable] Loop closed. Observed {len(events)} events.")
            else:
                log.info(f"  └ [Φ′:warn] Invalid event structure returned.")
        except httpx.HTTPStatusError as e:
            self._log_http_error("Observation", e, api_path)
        except Exception as e:
            log.error(f"  └ [∂Φ:error] Observation broken: {e}")


def spin_wheel():
    with managed_server():
        runtime = TopologicalRuntime(BASE_URL)
        
        runtime.trigger_genesis()
        runtime.trigger_injection()
        runtime.trigger_activation()
        runtime.observe_closure()
        
        if runtime.is_healthy:
            log.info("\n[SUCCESS] Autopoietic execution loop fully functional.")
        else:
            log.info("\n[FAIL] Topology traversed, but structural fractures were detected.")

if __name__ == "__main__":
    spin_wheel()