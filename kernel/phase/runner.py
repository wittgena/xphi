# kernel.phase.runner
import time
import hashlib
from typing import Any, Dict, List, Optional, Callable
import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from kernel.dphi.adapter.state import StateAdapter
from watcher.plane.emitter import get_emitter

log = get_emitter("scheme.runner")

class BaseRunner:
    def __init__(self):
        self.success_count = 0
        self.fail_count = 0
        self.last_failed_context: List[str] = []

    def report(self):
        log.info(f"\n=== [DONE] Scenarios Completed: {self.success_count} Passed, {self.fail_count} Failed ===")
        if self.fail_count > 0:
            log.warning("Review the following failed contexts:")
            for ctx in self.last_failed_context:
                log.warning(f" - {ctx}")

    def _record_success(self, elapsed_ms: float, msg: str):
        self.success_count += 1
        safe_msg = str(msg).replace('\n', ' ')[:150]
        log.info(f"  [PASS] Time: {elapsed_ms:.2f}ms | Output: {safe_msg}")

    def _record_fail(self, elapsed_ms: float, error_msg: str, context: str):
        self.fail_count += 1
        log.error(f"  [FAIL] Time: {elapsed_ms:.2f}ms | Details: {error_msg}")
        self.last_failed_context.append(context)

class SchemeRunner(BaseRunner):
    def __init__(self, broker: Any):
        super().__init__()
        self.broker = broker

    async def _set_worker_policy(self, tier_name: str):
        log.info(f"\n[Control Plane] Shifting WasmCgroup Policy Tier -> {tier_name}")
        if hasattr(self.broker, "update_policy"):
            await self.broker.update_policy(tier=tier_name)
            log.info(f"  └─ Policy successfully enforced to {tier_name}.")
        else:
            log.warning(f"  └─ Broker missing 'update_policy' API.")

    async def _run_case(
        self, 
        title: str, 
        target_func: str, 
        payload: Any, 
        expected_success: bool, 
        expected_match: Optional[str] = None,
        custom_validator: Optional[Callable[[str], bool]] = None
    ):
        """
        확장된 테스트 검증 엔진:
        - expected_success: 성공(True) 또는 실패(False) 기대 여부
        - expected_match: 결과 문자열(에러 포함)에 특정 키워드가 포함되어야 하는지 검증
        - custom_validator: 결과 출력값을 기반으로 사용자 정의 검증 로직 수행
        """
        log.info(f"\n[TEST] {title} (Func: {target_func})")
        start_time = time.time()
        result = await self.broker.invoke(target_func=target_func, payload=payload)
        elapsed_ms = (time.time() - start_time) * 1000
        
        output_str = str(result.output) if result.success else str(result.error)
        safe_payload_str = str(payload)[:50]

        # 1. 성공/실패 여부 검증
        if result.success != expected_success:
            self._record_fail(
                elapsed_ms, 
                f"Expected success={expected_success}, Got success={result.success}. Output: {output_str}",
                f"Function: {target_func} | Input: {safe_payload_str}..."
            )
            return

        # 2. 특정 문자열 매치 검증 (주로 실패 시 에러 타입 확인에 유용)
        if expected_match and expected_match.lower() not in output_str.lower():
            self._record_fail(
                elapsed_ms, 
                f"Expected string '{expected_match}' not found in output. Output: {output_str}",
                f"Function: {target_func} | Input: {safe_payload_str}..."
            )
            return

        # 3. 커스텀 로직 검증 (결과 파싱이 필요한 경우)
        if custom_validator and not custom_validator(output_str):
            self._record_fail(
                elapsed_ms, 
                f"Custom validation failed for output: {output_str}",
                f"Function: {target_func} | Input: {safe_payload_str}..."
            )
            return

        self._record_success(elapsed_ms, output_str)

class WebRunner(BaseRunner):
    def __init__(self, base_url: str, client: Optional[httpx.AsyncClient] = None):
        super().__init__()
        self.base_url = base_url
        self._is_injected_client = client is not None
        self.client = client or httpx.AsyncClient(base_url=base_url, timeout=10.0)
        self.committee_keys = [ed25519.Ed25519PrivateKey.generate() for _ in range(3)]
        self.committee_pubs = [
            k.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            ).hex() for k in self.committee_keys
        ]

    async def teardown(self):
        """WebRunner가 자체 생성한 클라이언트인 경우 리소스를 정리합니다."""
        if not self._is_injected_client and not self.client.is_closed:
            await self.client.aclose()

    async def _run_api_case(self, title: str, method: str, endpoint: str, payload: Dict[str, Any], expected_status: int = 200) -> Optional[httpx.Response]:
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
                    f"Expected: {expected_status}, Got: {res.status_code}. Response: {res.text[:100]}",
                    f"Endpoint: {endpoint} | Error: {res.text[:200]}"
                )
                return res
        except Exception as e:
            self.fail_count += 1
            log.error(f"  [CRITICAL FAIL] Network/Execution Error: {str(e)}")
            return None

    def _sign_payload(self, signers: List[ed25519.Ed25519PrivateKey], payload_dict: Dict[str, Any]) -> List[str]:
        raw_json_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        commit_hash = hashlib.sha256(raw_json_bytes).digest()
        return [k.sign(commit_hash).hex() for k in signers]