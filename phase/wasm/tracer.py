# phase.wasm.tracer
import sys
import asyncio
import base64
import json
from pathlib import Path
from typing import Tuple, Optional

from phase.bind.resolver import resolve_path
from phase.runtime.inter.wasm import WasmInterpreter
from phase.wasm.wasmcg import CgroupPolicy

from watcher.tracer.bound import BaseTracer
from watcher.kernel.store import KernelStore, KernelCommit
from watcher.plane.emitter import get_emitter

log = get_emitter("wasm.tracer")

TIME_ROOT = resolve_path("time")
DEST_WASM_FILE = TIME_ROOT / "dphi.wasm"
REGISTRY_FILE = TIME_ROOT / "registry.json"
STREAM_ID = "wasm_binary_lineage"

class WasmTracer(BaseTracer):
    def __init__(self, timeout: int = 300, tester=None):
        super().__init__(tracer_name="wasm.tracer", timeout=timeout)
        self.store = KernelStore()
        self.tester = tester

    async def verify_cache_or_build(self) -> Tuple[bool, str]:
        head_hash = self.store.get_head_hash(STREAM_ID) or "0000000"
        registry = {}
        if REGISTRY_FILE.exists():
            with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
                registry = json.load(f)

        needs_build = True

        if DEST_WASM_FILE.exists() and registry.get("lineage_hash") == head_hash:
            self.log.info("[Ledger] Local registry matches KernelStore. Verifying binary integrity...")
            try:
                with open(DEST_WASM_FILE, "rb") as f:
                    wasm_b64 = base64.b64encode(f.read()).decode('utf-8')

                sys_policy = CgroupPolicy.system()
                temp_interp = WasmInterpreter(str(DEST_WASM_FILE), policy=sys_policy)
                
                payload_data = json.dumps({"parent_hash": registry.get("parent_hash", "0000000"), "wasm_base64": wasm_b64})
                result = temp_interp.invoke("verify_build_lineage", payload_data)

                if result.success:
                    res_data = json.loads(result.output)
                    if res_data.get("lineage_hash") == head_hash:
                        self.log.info(f"[Ledger] Integrity verified. Hash: {head_hash[:8]}")
                        needs_build = False
            except Exception as e:
                self.log.error(f"[ERROR] Verification crashed: {e}")

        if needs_build:
            self.log.info("[STATUS] Cache miss/invalid. Initiating Builder...")
            from phase.wasm.builder import WasmBuilder

            builder = WasmBuilder()
            await builder.execute()
            if builder.rupture_confirmed:
                return False, builder.build_error

            self.log.info("[Ledger] Registering new WASM lineage...")
            with open(DEST_WASM_FILE, "rb") as f:
                new_wasm_b64 = base64.b64encode(f.read()).decode('utf-8')

            sys_policy = CgroupPolicy.system()
            temp_interp = WasmInterpreter(str(DEST_WASM_FILE), policy=sys_policy)
            payload_data = json.dumps({"parent_hash": head_hash, "wasm_base64": new_wasm_b64})
            result = temp_interp.invoke("verify_build_lineage", payload_data)

            if result.success:
                res_data = json.loads(result.output)
                new_hash = res_data["lineage_hash"]
                commit = KernelCommit(
                    stream_id=STREAM_ID, executable_payload="WASM Build Transition",
                    tension_at_seal=0.0, blob_hashes=[], parent_hash=head_hash
                )
                self.store.save_kernel(commit)
                self.store.update_head(STREAM_ID, new_hash)

                with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
                    reg_data = json.load(f)
                reg_data["parent_hash"] = head_hash
                reg_data["lineage_hash"] = new_hash
                with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
                    json.dump(reg_data, f, indent=4)
                self.log.info(f"[Ledger] New WASM lineage sealed: {new_hash[:8]}")
            else:
                return False, f"Lineage generation failed: {result.error}"

        return True, ""

    async def orchestrate_tests(self) -> Tuple[bool, str]:
        self.log.info("\n--- [START] Delegating Orchestration to WasmTester ---")
        try:
            if not self.tester:
                from phase.wasm.tester.dphi import WasmTester
                self.log.info("[SYSTEM] Initializing default WasmTester...")
                self.tester = WasmTester(
                    wasm_module_path=str(DEST_WASM_FILE),
                    sandbox_root=str(TIME_ROOT)
                )
            else:
                self.log.info("[SYSTEM] Using injected WasmTester instance.")
            return await self.tester.execute()
        except Exception as e:
            self.log.error(f"[FATAL] Failed to delegate orchestration: {e}")
            return False, str(e)

    def _gather_telemetry_context(self) -> dict:
        context = {}
        if not self.tester or not hasattr(self.tester, 'auditors'):
            return context
            
        for auditor in self.tester.auditors:
            if auditor.__class__.__name__ == "WasmTelemetryAuditor":
                context["max_recursion_depth"] = auditor.max_depth
                context["health_flag"] = auditor.health_flag
            elif auditor.__class__.__name__ == "WasmEntropyAuditor":
                context["fuel_consumed"] = auditor.total_fuel_used
                
        return context

    async def execute(self) -> None:
        self.log.crit("## @trace.init: WASM Lifecycle (AgentBot Removed)")
        build_ok, build_err = await self.verify_cache_or_build()
        
        if build_ok:
            test_ok, test_err = await self.orchestrate_tests()
            if test_ok:
                self.log.info("## @trace.success: System stabilized.")
                return
            else:
                last_error = test_err
        else:
            last_error = build_err

        self.log.crit("## @trace.collapse: COMPILATION OR TEST FAILED.")
        self.log.error(f"[FATAL] Module: wasm.tracer | Reason: Process failed without self-healing agent.")
        self.log.error(f"Last Error: {last_error[-1000:]}")
        self.log.error(f"Final Telemetry: {self._gather_telemetry_context()}")
        self.rupture_confirmed = True