# xphi.kernel.space.runner.phase
## @lineage: xphi.kernel.dphi.runner.phase
## @lineage: kernel.dphi.runner.phase
import asyncio
import time
import hashlib
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Callable, Union
from enum import Enum

import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from xphi.arch.xor.parser.block.contract import Contract, CoherenceState
from xphi.kernel.space.sandbox.executor import SandboxExecutor, TaskContext, EffectResolver
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.ledger.consensus import KernelLedger, KernelCommit
from xphi.kernel.dphi.method import DphiMethod
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("phase.runner")

class RecoveryMethod(str, Enum):
    SEAL_VOID_EPOCH = "seal_void_epoch"

class BaseRunner:
    def __init__(self):
        self.success_count = 0
        self.fail_count = 0
        self.last_failed_context: List[str] = []
        self.failed_cases: List[Dict[str, str]] = []

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

    def _record_fail(self, elapsed_ms: float, error_msg: str, context: str, title: str = "Unknown Test Case"):
        self.fail_count += 1
        log.error(f"  [FAIL] Time: {elapsed_ms:.2f}ms | Details: {error_msg}")
        self.last_failed_context.append(context)
        self.failed_cases.append({
            "title": title,
            "error": f"[{context}] {error_msg}"
        })

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
            log.warning("  └─ Broker missing 'update_policy' API.")

    async def _run_case(
        self, 
        title: str, 
        target_func: Union[str, DphiMethod, RecoveryMethod],
        payload: Any, 
        expected_success: bool, 
        expected_match: Optional[str] = None,
        custom_validator: Optional[Callable[[str], bool]] = None,
        tier: Optional[str] = None
    ):
        func_name = target_func.value if isinstance(target_func, Enum) else target_func
        log.info(f"\n[TEST] {title} (Func: {func_name})")
        start_time = time.time()
        
        # 1회성 Tier 주입 실행
        result = await self.broker.invoke(target_func=func_name, payload=payload, tier=tier)
        elapsed_ms = (time.time() - start_time) * 1000
        
        output_str = str(result.output) if result.success else str(result.error)
        safe_payload_str = str(payload)[:50]

        if result.success != expected_success:
            self._record_fail(
                elapsed_ms, 
                f"Expected success={expected_success}, Got success={result.success}. Output: {output_str}",
                f"Function: {func_name} | Input: {safe_payload_str}...",
                title=title
            )
            return

        if expected_match and expected_match.lower() not in output_str.lower():
            self._record_fail(
                elapsed_ms, 
                f"Expected string '{expected_match}' not found in output. Output: {output_str}",
                f"Function: {func_name} | Input: {safe_payload_str}...",
                title=title
            )
            return

        if custom_validator and not custom_validator(output_str):
            self._record_fail(
                elapsed_ms, 
                f"Custom validation failed for output: {output_str}",
                f"Function: {func_name} | Input: {safe_payload_str}...",
                title=title
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
                    f"Endpoint: {endpoint} | Error: {res.text[:200]}",
                    title=title
                )
                return res
        except Exception as e:
            self.fail_count += 1
            log.error(f"  [CRITICAL FAIL] Network/Execution Error: {str(e)}")
            self.failed_cases.append({"title": title, "error": f"[CRITICAL] {str(e)}"})
            return None

    def _sign_payload(self, signers: List[ed25519.Ed25519PrivateKey], payload_dict: Dict[str, Any]) -> List[str]:
        raw_json_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        commit_hash = hashlib.sha256(raw_json_bytes).digest()
        return [k.sign(commit_hash).hex() for k in signers]

class RuntimeRunner(ABC):
    def __init__(self, broker: DphiBroker, resolvers: Optional[Dict[str, EffectResolver]] = None):
        self.broker = broker
        self.executor = SandboxExecutor(resolvers=resolvers)
        self.is_running = False

    async def watch_and_react(self, initial_context: TaskContext):
        self.is_running = True
        log.info(f"[RuntimeRunner] Activated pattern for task: {initial_context.task_type}")
        
        async for contract in self.executor.execute_stream(initial_context):
            if not self.is_running:
                break
            await self.on_contract_emitted(contract)

    @abstractmethod
    async def on_contract_emitted(self, contract: Contract):
        pass

    def stop(self):
        self.is_running = False