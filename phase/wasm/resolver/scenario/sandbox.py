# phase.wasm.resolver.scenario.sandbox
import time
import asyncio
from phase.wasm.resolver.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("scenario.sandbox")

class SandboxScenarios(SchemeRunner):
    """@desc: Core execution engine, resource isolation, and state generation scenarios"""
    async def run_all(self):
        log.info("\n=== [START] Executing Sandbox & Compute Scenarios ===")
        await self._set_worker_policy("SYSTEM")
        
        """샌드박스 보안 및 자원 격리"""
        await self._test_wasmcg_resilience()
        await self._test_legacy_execution()
        await self._test_guardrail_validation()
        
        """인과성 제어 및 의존성 역전"""
        await self._test_topos_and_phase()
        await self._test_tripartite_parity()
        await self._test_dependency_injection()
        await self._test_ffi_robustness()
        
        """연료(Fuel) 통제 및 구조적 한계 테스트"""
        await self._test_fuel_linear_profiling()
        await self._test_dynamic_policy_and_exhaustion()
        await self._test_concurrency_and_pool_scaling()
        await self._test_fault_isolation()
        
        self.report()

    async def _test_wasmcg_resilience(self):
        log.info("\n--- Running Suite: WasmCG Resilience & Resource Control ---")
        await self._run_case("WasmCG: Unregistered API Call (O(1) Guard)", "hack_system_memory", {}, expected_success=False)

    async def _test_fuel_linear_profiling(self):
        log.info("\n--- Running Suite: Deterministic Fuel Profiling ---")
        await self._run_case("Profile: 10KB Payload Hashing", "compute_root_fingerprint", {"dummy_data": "A" * 10_000}, expected_success=True)
        await self._run_case("Profile: 50KB Payload Hashing", "compute_root_fingerprint", {"dummy_data": "A" * 50_000}, expected_success=True)

    async def _test_dynamic_policy_and_exhaustion(self):
        log.info("\n--- Running Suite: Dynamic Policy & Resource Exhaustion ---")
        await self._set_worker_policy("STANDARD")
        large_payload = {"dummy_data": "A" * 150_000}
        await self._run_case("Boundary Test: 150KB Hashing under STANDARD Tier (Expect Trap)", "compute_root_fingerprint", large_payload, expected_success=False)

    async def _test_legacy_execution(self):
        log.info("\n--- Running Suite: Legacy Code Execution ---")
        await self._run_case("Legacy: Normal Execution", "execute_code", "x = 10\nprint(x)", expected_success=True)

    async def _test_guardrail_validation(self):
        log.info("\n--- Running Suite: Guardrail Validation ---")
        await self._run_case("Guardrail: Missing Files", "verify_packet", {"packet_id": "123"}, expected_success=False)
        await self._run_case("Guardrail: Valid Packet", "verify_packet", {"packet_id": "123", "files": {"model.bin": "hash"}}, expected_success=True)

    async def _test_topos_and_phase(self):
        log.info("\n--- Running Suite: Topos & Phase Generation (Epoch-Tick) ---")
        current_ts = int(time.time() * 1000)
        await self._run_case("Event: Generate Topos Anchor ID", "generate_topos_id", {"ts": current_ts}, expected_success=True)

        payload_phase = {"topo": 50, "press": -10, "rupture": False}
        await self._run_case("Event: Generate Phase & Nexus ID (Tick 0)", "generate_phase_id", payload_phase, expected_success=True)
        await self._run_case("Event: Generate Phase & Nexus ID (Tick 1)", "generate_phase_id", payload_phase, expected_success=True)

    async def _test_tripartite_parity(self):
        log.info("\n--- Running Suite: Tripartite XOR Parity (Nexus Recovery) ---")
        t_id, p_id, n_id = 101010, 999999, 907049 
        await self._run_case("Parity: Validate All 3 IDs (Integrity)", "verify_parity", {"topos_id_low32": t_id, "phase_id": p_id, "nexus_id": n_id}, expected_success=True)
        await self._run_case("Parity: Recover Missing Phase ID", "verify_parity", {"topos_id_low32": t_id, "nexus_id": n_id}, expected_success=True)
        await self._run_case("Parity: Recover Missing Topos ID", "verify_parity", {"phase_id": p_id, "nexus_id": n_id}, expected_success=True)
        await self._run_case("Parity: Error on Insufficient Info (1 ID)", "verify_parity", {"nexus_id": n_id}, expected_success=False)

    async def _test_dependency_injection(self):
        log.info("\n--- Running Suite: Dependency Injection (State Override) ---")
        injected_payload = {"topo": 100, "press": 200, "rupture": True, "injected_anchor": 999999, "injected_tick": 77}
        await self._run_case("DI: Phase Generation with Injected State", "generate_phase_id", injected_payload, expected_success=True)

    async def _test_ffi_robustness(self):
        log.info("\n--- Running Suite: FFI Robustness & Zero-Copy Guard ---")
        malformed_payload = '{"topo": 50, "press": -10, "rupture": '
        await self._run_case("FFI Guard: Malformed JSON Payload (Expect Clean Error)", "generate_phase_id", malformed_payload, expected_success=False)

    async def _test_concurrency_and_pool_scaling(self):
        log.info("\n--- Running Suite: Concurrency & Interpreter Pool Scaling ---")
        request_count = 10
        target_func = "compute_root_fingerprint"
        payload = {"dummy_data": "Parallel Test"}
        log.info(f"Firing {request_count} concurrent requests to trigger dynamic spawning...")
        
        start_time = time.time()
        tasks = [self.broker.invoke(target_func, payload) for _ in range(request_count)]
        results = await asyncio.gather(*tasks)
        elapsed_ms = (time.time() - start_time) * 1000
        successes = sum(1 for r in results if r.success)
        
        if successes == request_count:
            log.info(f"  [PASS] Successfully handled {request_count} concurrent WASM executions in {elapsed_ms:.2f}ms.")
            self.success_count += 1
        else:
            log.error(f"  [FAIL] Concurrency test failed. Only {successes}/{request_count} succeeded.")
            self.fail_count += 1

    async def _test_fault_isolation(self):
        log.info("\n--- Running Suite: Supervisor Fault Isolation ---")
        
        toxic_result = await self.broker.invoke("non_existent_function", {})
        if not toxic_result.success:
            log.info("  [EXPECTED] Toxic request failed cleanly.")
        else:
            self.fail_count += 1
            return
            
        recovery_result = await self.broker.invoke("execute_code", "print('I survived')")
        if recovery_result.success:
            log.info("  [PASS] System survived the crash and isolated the fault.")
            self.success_count += 1
        else:
            self.fail_count += 1