# watcher.flare.tracer
import os
import json
import asyncio
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, Type, Tuple, List

from xphi.kernel.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.watcher.tracer.bound import (
    BaseStreamAuditor, 
    SystemBound,
    log_streamer
)
from xphi.watcher.wasm.auditor import CanonicalProofAuditor

TIME_ROOT = resolve_path("time")

"""METADATA & CONFIGURATION"""
META_INFO = {
    "VERSION": "3.0.0 (Absolute Hologram & Tail Edition)",
    "SYSTEM": "Cloudflare V8 Isolate Boundary Tracer & Orchestrator"
}

log = get_emitter("tracer.flare")

# =====================================================================
# 1. AUDITORS (Dual observation networks based on physical location)
# =====================================================================

class WranglerDevAuditor(BaseStreamAuditor):
    """@desc: [Local Hologram] Real-time observation of V8 engine logs and crashes on the local PC."""
    def __init__(self, workspace: Path, boundary: SystemBound = None):
        super().__init__(target="wrangler_dev", boundary=boundary, delay=1)
        self.workspace = workspace
        self.log = get_emitter("auditor.flare.dev", phase="agent")
        
        self.hit_cpu_limit = False
        self.is_ready = False
        
        # [ADD] Buffer to store startup logs for automated self-diagnosis
        self.startup_logs: List[str] = []

    @log_streamer(["npx", "wrangler", "dev", "index.ts", "--port", "8787", "--local"], cwd="{workspace}")
    async def run_stream(self, line: str) -> None:
        if not line: return
        
        line_stripped = line.strip()
        
        # Keep the last 50 lines in the buffer to prevent memory leaks
        self.startup_logs.append(line_stripped)
        if len(self.startup_logs) > 50:
            self.startup_logs.pop(0)
        
        if "Ready on http://127.0.0.1:8787" in line or "Ready on http://localhost:8787" in line:
            self.is_ready = True
            self.log.info("  [LOCAL_EDGE] ⚡ V8 Isolate Hologram Materialized (Port 8787).")
            
        elif "1102" in line or "CPU time limit exceeded" in line:
            self.hit_cpu_limit = True
            self.log.warning("  [V8_RUPTURE] Kinetic Trap Triggered! Local CPU Limit 50ms Exceeded.")
            
        elif "error" in line.lower() or "exception" in line.lower():
            self.log.error(f"  [EDGE_FAULT] {line_stripped}")
            
        else:
            self.log.debug(f"  [EDGE_STREAM] {line_stripped}")


class WranglerTailAuditor(BaseStreamAuditor):
    """@desc: [Global Edge] Intercepts global logs from cloud-deployed workers to the local environment."""
    def __init__(self, worker_name: str, boundary: SystemBound = None):
        super().__init__(target=worker_name, boundary=boundary, delay=1)
        self.worker_name = worker_name
        self.log = get_emitter(f"auditor.flare.tail.{worker_name}", phase="agent")
        
        self.hit_cpu_limit = False
        self.is_ready = True  # Tail stream is instantly considered ready

    @log_streamer(["npx", "wrangler", "tail", "{target}", "--format", "json"])
    async def run_stream(self, line: str) -> None:
        if not line: return
        try:
            payload = json.loads(line)
            exceptions = payload.get("exceptions", [])
            
            # Print standard logs
            for l in payload.get("logs", []):
                self.log.info(f"  [GLOBAL_EDGE] {l.get('message')}")
                
            # Detect V8 collapse (Kinetic Trap)
            for exc in exceptions:
                name = exc.get("name", "")
                msg = exc.get("message", "")
                
                if "1102" in msg or "CPU time limit exceeded" in msg:
                    self.hit_cpu_limit = True
                    self.log.warning("  [V8_RUPTURE] Kinetic Trap Triggered at Global Edge (Error 1102)")
                else:
                    self.log.error(f"  [EDGE_FAULT] {name}: {msg}")
                    
        except json.JSONDecodeError:
            self.log.debug(f"  [TAIL_SYS] {line.strip()}")


# =====================================================================
# 2. INFRA MIXIN (Dynamic Runtime Mutation & Deployment Control)
# =====================================================================

class FlareInfraMixin:
    def _get_runner_path(self) -> str:
        return os.path.join(TIME_ROOT, "pysand.ts")

    def _mutate_pysand_for_cloudflare(self, workspace: Path, worker_name: str):
        pysand_path = Path(self._get_runner_path())
        if not pysand_path.exists():
            raise FileNotFoundError(f"Core sandbox not found at {pysand_path}")

        original_code = pysand_path.read_text(encoding="utf-8")
        
        # 1. Strip Deno-specific dependencies
        core_logic = original_code.split("// Main Event Loop")[0]
        core_logic = core_logic.replace(
            'import pyodideModule from "npm:pyodide/pyodide.js";', 
            'import { loadPyodide } from "pyodide";\nconst pyodideModule = { loadPyodide };'
        ).replace(
            'import { readLines } from "https://deno.land/std@0.186.0/io/mod.ts";', 
            '// Deno io removed for CF Edge.'
        )

        # 2. Generate Cloudflare Fetch API wrapper
        cf_fetch_wrapper = """
// ==========================================================
// CLOUDFLARE EDGE PLANE: FETCH API INJECTION
// ==========================================================
let pyodideReadyPromise;

export default {
  async fetch(request, env, ctx) {
    if (!pyodideReadyPromise) {
      pyodideReadyPromise = pyodideModule.loadPyodide();
    }
    const pyodide = await pyodideReadyPromise;
    
    if (!globalThis._setupDone) {
        pyodide.runPython(PYTHON_SETUP_CODE);
        globalThis._setupDone = true;
    }

    if (request.method !== "POST") return new Response("Method Not Allowed", { status: 405 });
    
    const input = await request.json();
    const { method, params = {}, id: requestId } = input;
    
    if (method === "execute") {
        const code = params.code || "";
        const context = params.context || {};
        let setupCompleted = false;
        
        try {
            if (code.includes("import ") || code.includes("from ")) {
                await pyodide.loadPackagesFromImports(code);
            }
            
            const ts = context.timestamp !== undefined ? context.timestamp : 'None';
            const seed = context.seed !== undefined ? JSON.stringify(context.seed) : 'None';
            pyodide.runPython(`_apply_execution_context(${ts}, ${seed})`);
            pyodide.runPython("_prepare_execution()");
            setupCompleted = true;
            
            const result = await pyodide.runPythonAsync(code);
            let output = result === null || result === undefined 
                         ? pyodide.runPython("buf_stdout.getvalue()") 
                         : (typeof result.toJs === 'function' ? result.toJs({ dict_converter: Object.fromEntries }) : result);
            
            if (result && typeof result.destroy === 'function') result.destroy();
            
            return new Response(JSON.stringify({ jsonrpc: "2.0", result: { output }, id: requestId }), {
                headers: { "Content-Type": "application/json" }
            });
        } catch (error) {
            return new Response(JSON.stringify({ 
                jsonrpc: "2.0", 
                error: { message: error.message, data: { type: error.type || "Error" } }, 
                id: requestId 
            }), {
                headers: { "Content-Type": "application/json" }
            });
        } finally {
            if (setupCompleted) pyodide.runPython("_restore_execution()");
        }
    }
    return new Response("Unknown method", { status: 400 });
  }
};
"""
        # 3. Merge and save source files
        (workspace / "index.ts").write_text(core_logic + "\n" + cf_fetch_wrapper)
        
        # 4. Generate wrangler.toml
        wrangler_toml = f"""
name = "{worker_name}"
main = "index.ts"
compatibility_date = "2024-01-01"
"""
        (workspace / "wrangler.toml").write_text(wrangler_toml.strip())

    async def provision_workspace(self, worker_name: str):
        self.workspace = Path("/tmp/dphi_flare_workspace")
        self.workspace.mkdir(exist_ok=True, parents=True)
        log.info(f"## @flare.provision: Mutating local core into Edge format at {self.workspace}...")
        self._mutate_pysand_for_cloudflare(self.workspace, worker_name)

    async def deploy_to_global_edge(self):
        log.info("## @flare.deploy: Pushing code to Global Cloudflare Edge...")
        process = await asyncio.create_subprocess_exec(
            "npx", "wrangler", "deploy",
            cwd=str(self.workspace),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        stdout, _ = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"Wrangler deploy failed:\n{stdout.decode()}")
        log.info("## @flare.deploy: Successfully deployed to Global Edge.")

    async def destroy_global_edge(self, worker_name: str):
        log.info("## @flare.teardown: Erasing Global Edge Instance...")
        process = await asyncio.create_subprocess_exec(
            "npx", "wrangler", "delete", worker_name, "--force",
            cwd=str(self.workspace),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        await process.communicate()

    async def teardown_workspace(self):
        if self.workspace and self.workspace.exists():
            shutil.rmtree(self.workspace)
            log.info("## @flare.teardown: Local workspace dismantled.")