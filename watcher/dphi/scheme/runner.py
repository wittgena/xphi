# watcher.dphi.scheme.runner
import time
from typing import Any
import hashlib
import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from watcher.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("scheme.runner")

class BaseRunner:
    """테스트 상태 관리 및 결과 리포팅을 담당하는 최상위 추상화 클래스"""
    def __init__(self):
        self.success_count = 0
        self.fail_count = 0
        self.last_failed_context = []

    def report(self):
        log.info(f"\n=== [DONE] Scenarios Completed: {self.success_count} Passed, {self.fail_count} Failed ===")
        if self.fail_count > 0:
            log.warning("Review the following failed contexts:")
            for ctx in self.last_failed_context:
                log.warning(f" - {ctx}")

    def _record_success(self, elapsed_ms: float, msg: str):
        self.success_count += 1
        log.info(f"  [PASS] Time: {elapsed_ms:.2f}ms | Output: {msg[:150]}")

    def _record_fail(self, elapsed_ms: float, error_msg: str, context: str):
        self.fail_count += 1
        log.error(f"  [FAIL] Time: {elapsed_ms:.2f}ms | Details: {error_msg}")
        self.last_failed_context.append(context)


class SchemeRunner(BaseRunner):
    """내부 WASM Broker 인보크(Invoke) 전용 러너"""
    def __init__(self, broker):
        super().__init__()
        self.broker = broker

    async def _set_worker_policy(self, tier_name: str):
        log.info(f"\n[Control Plane] Shifting WasmCgroup Policy Tier -> {tier_name}")
        try:
            if hasattr(self.broker, "update_policy"):
                await self.broker.update_policy(tier=tier_name)
                log.info(f"  └─ Policy successfully enforced to {tier_name}.")
            else:
                log.warning(f"  └─ Broker missing 'update_policy' API.")
        except Exception as e:
            log.error(f"  └─ Failed to update policy: {e}")

    async def _run_case(self, title: str, target_func: str, payload: Any, expected_success: bool):
        log.info(f"\n[TEST] {title} (Func: {target_func})")
        start_time = time.time()
        result = await self.broker.invoke(target_func=target_func, payload=payload)
        elapsed_ms = (time.time() - start_time) * 1000
        
        if result.success == expected_success:
            self._record_success(elapsed_ms, str(result.output) if result.success else str(result.error))
        else:
            err_msg = str(result.error) if not result.success else str(result.output)
            safe_payload_str = str(payload)[:50]
            self._record_fail(
                elapsed_ms, 
                f"Expected success={expected_success}, Got success={result.success}. {err_msg}",
                f"Function: {target_func} | Input: {safe_payload_str}... | Error: {err_msg}"
            )


class TrustlessWebRunner(BaseRunner):
    """외부 HTTP API 및 암호화 서명 전용 러너"""
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=base_url, timeout=10.0)
        
        self.committee_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.committee_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.committee_keys
        ]

    async def _run_api_case(self, title: str, method: str, endpoint: str, payload: dict, expected_status: int = 200) -> httpx.Response | None:
        log.info(f"\n[TEST] {title} ({method} {endpoint})")
        start_time = time.time()
        
        try:
            res = await self.client.request(method, endpoint, json=payload)
            elapsed_ms = (time.time() - start_time) * 1000
            
            if res.status_code == expected_status:
                self._record_success(elapsed_ms, res.text)
                return res
            else:
                self._record_fail(
                    elapsed_ms,
                    f"Expected: {expected_status}, Got: {res.status_code}. {res.text}",
                    f"Endpoint: {endpoint} | Error: {res.text}"
                )
                return res
        except Exception as e:
            self.fail_count += 1
            log.error(f"  [CRITICAL FAIL] Network/Execution Error: {str(e)}")
            return None

    def _sign_payload(self, signers: list, payload_dict: dict) -> list:
        raw_json_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        commit_hash = hashlib.sha256(raw_json_bytes).digest()
        return [k.sign(commit_hash).hex() for k in signers]