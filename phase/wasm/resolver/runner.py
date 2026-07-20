# phase.wasm.resolver.runner
import json
import time
from typing import Any
from watcher.plane.emitter import get_emitter

log = get_emitter("scheme.runner")

class SchemeRunner:
    def __init__(self, broker):
        self.broker = broker
        self.success_count = 0
        self.fail_count = 0
        self.last_failed_context = []

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

    # payload는 시나리오에 따라 문자열(str) 또는 딕셔너리(dict)가 들어올 수 있으므로 Any로 처리합니다.
    async def _run_case(self, title: str, target_func: str, payload: Any, expected_success: bool):
        log.info(f"\n[TEST] {title} (Func: {target_func})")
        
        start_time = time.time()
        result = await self.broker.invoke(target_func=target_func, payload=payload)
        elapsed_ms = (time.time() - start_time) * 1000
        
        if result.success == expected_success:
            output_msg = str(result.output)[:150] if result.success else str(result.error)
            log.info(f"  [PASS] Time: {elapsed_ms:.2f}ms | Output: {output_msg}")
            self.success_count += 1
        else:
            log.error(f"  [FAIL] Time: {elapsed_ms:.2f}ms | Expected success={expected_success}, Got success={result.success}")
            err_msg = str(result.error) if not result.success else str(result.output)
            log.error(f"    Details: {err_msg}")
            
            self.fail_count += 1
            safe_payload_str = str(payload)
            self.last_failed_context.append(f"Function: {target_func} | Input: {safe_payload_str[:50]}... | Error: {err_msg}")

    def report(self):
        log.info(f"\n=== [DONE] Scenarios Completed: {self.success_count} Passed, {self.fail_count} Failed ===")
        if self.fail_count > 0:
            log.warning("Review the following failed contexts:")
            for ctx in self.last_failed_context:
                log.warning(f" - {ctx}")