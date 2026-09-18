# xphi.kernel.space.time.flare.engine
from js import Response, Headers
import json
import sys
import io
import time
import os
import hashlib
import random
import traceback

print("[PythonEngine] 🚀 Pyodide Environment Initialized (Cold Boot Finished).")

"""Strict Host Environment Isolation (Prevent Env Leakage)"""
class RestrictedEnviron:
    """Block os.environ access with Exception to enforce sandbox isolation."""
    def __getitem__(self, key): raise PermissionError("Isolated")
    def __setitem__(self, key, value): raise PermissionError("Isolated")
    def __delitem__(self, key): raise PermissionError("Isolated")
    def __contains__(self, key): raise PermissionError("Isolated")
    def get(self, key, default=None): raise PermissionError("Isolated")
    def pop(self, key, default=None): raise PermissionError("Isolated")
    def keys(self): raise PermissionError("Isolated")
    def values(self): raise PermissionError("Isolated")
    def items(self): raise PermissionError("Isolated")
    def update(self, *args, **kwargs): raise PermissionError("Isolated")
    def clear(self): pass # Allow internal framework calls
    def __getattr__(self, name): raise PermissionError("Isolated")

os.environ = RestrictedEnviron()

# =====================================================================
# [Security 2] Virtual Time & Deterministic PRNG (Anti-Spectre & Idempotency)
# =====================================================================
_virtual_context = {
    "time": 0.0,
    "perf_time": 0.0,
    "seed_counter": 0,
    "base_seed": b"dphi_default_seed"
}

def _mock_time(): 
    return _virtual_context["time"]
    
def _mock_perf():
    _virtual_context["perf_time"] += 0.001
    return _virtual_context["perf_time"]

def _mock_sleep(secs):
    # Prevent actual thread blocking; only advance virtual time
    _virtual_context["time"] += secs
    _virtual_context["perf_time"] += secs

time.time = _mock_time
time.perf_counter = time.monotonic = time.process_time = _mock_perf
time.sleep = _mock_sleep

_original_urandom = os.urandom
def _mock_urandom(size):
    _virtual_context["seed_counter"] += 1
    state = _virtual_context["base_seed"] + str(_virtual_context["seed_counter"]).encode()
    res = b""
    while len(res) < size:
        state = hashlib.sha256(state).digest()
        res += state
    return res[:size]
os.urandom = _mock_urandom

def _apply_execution_context(ts, seed_string):
    """Completely reset V8 Isolate global state per request (Enforce Determinism)."""
    _virtual_context["time"] = float(ts) if ts is not None else 0.0
    _virtual_context["perf_time"] = 0.0
    
    # Force a fallback seed to guarantee PRNG idempotency even without input
    actual_seed = seed_string.encode('utf-8') if seed_string else b"dphi_secure_fallback_seed"
    _virtual_context["base_seed"] = actual_seed
    _virtual_context["seed_counter"] = 0
    
    det_hash = hashlib.sha256(actual_seed).hexdigest()
    random.seed(int(det_hash, 16))

# =====================================================================
# [Core] Cloudflare Edge Request Handler
# =====================================================================
async def on_fetch(request, env):
    try:
        req_text = await request.text()
        input_data = json.loads(req_text)
        params = input_data.get("params", {})
        request_id = input_data.get("id", "unknown")
        
        # [LOG ADDED] Captures the exact moment the Python engine starts processing the request
        print(f"[PythonEngine] 📥 Received Request ID: {request_id}")
        
        context = params.get("context", {})
        ts = context.get("timestamp", None)
        seed = context.get("seed", None)
        
        # [Defense] Initialize execution context (Time & PRNG seed reset)
        _apply_execution_context(ts, json.dumps(seed) if seed else None)
        
        # [Defense] Prevent Warm-Start bleeding (Clear sys attributes from prior runs)
        if hasattr(sys, 'FLARE_BLEED_TEST'):
            delattr(sys, 'FLARE_BLEED_TEST')
            
        old_stdout, old_stderr = sys.stdout, sys.stderr
        buf_stdout, buf_stderr = io.StringIO(), io.StringIO()
        
        # Hijack standard streams
        sys.stdout, sys.stderr = buf_stdout, buf_stderr
        
        code = params.get("code", "")
        variables = params.get("variables", {})
        
        # Separate global/local namespaces and inject external variables
        global_env = dict(variables)
        local_env = {}
        
        # [METRICS] 시작 가상 시간 측정
        start_perf = _virtual_context["perf_time"]
        
        try:
            # Compile first to isolate SyntaxErrors
            print(f"[PythonEngine] ⚙️ Compiling & Executing payload for ID: {request_id}...", file=old_stdout) # [LOG ADDED]
            compiled_code = compile(code, "", "exec")
            exec(compiled_code, global_env, local_env)
            output = buf_stdout.getvalue()
            
            # [METRICS] 종료 가상 시간 및 환경 메모리 사이즈 측정 (Fuel 환산)
            end_perf = _virtual_context["perf_time"]
            fuel_consumed = int((end_perf - start_perf) * 1_000_000) # 마이크로초 기반 정수 Fuel
            mem_usage = sys.getsizeof(local_env)
            
            print(f"[PythonEngine] ✅ Execution successful. Formatting Response...", file=old_stdout) # [LOG ADDED]
            
            headers = Headers.new({"Content-Type": "application/json"}.items())
            return Response.new(
                json.dumps({
                    "jsonrpc": "2.0", 
                    "result": {
                        "output": output,
                        # [ADDED] router.ts가 Canonical Hash 생성을 위해 읽어들일 메트릭 데이터
                        "metrics": {
                            "fuel_consumed": fuel_consumed,
                            "mem_usage_bytes": mem_usage,
                            "tier": "EDGE_PYTHON"
                        }
                    }, 
                    "id": request_id
                }),
                headers=headers
            )
            
        except BaseException as e:
            # [METRICS] 에러 발생 시점의 가상 시간 및 환경 메모리 사이즈 측정
            end_perf = _virtual_context["perf_time"]
            fuel_consumed = int((end_perf - start_perf) * 1_000_000)
            mem_usage = sys.getsizeof(local_env)

            # Standardize native exceptions (Strict JSON-RPC compliance)
            error_output = buf_stdout.getvalue()
            error_type = type(e).__name__
            
            print(f"[PythonEngine] ⚠️ Execution Fault Caught: {error_type} | Formatting Error Response...", file=old_stdout) # [LOG ADDED]
            
            # Replicate local Python error string formats exactly
            formatted_error_msg = f"{error_type}: {str(e)}"
            
            headers = Headers.new({"Content-Type": "application/json"}.items())
            return Response.new(
                json.dumps({
                    "jsonrpc": "2.0", 
                    "error": {
                        "message": formatted_error_msg, 
                        "data": {
                            "type": error_type,
                            "stdout": error_output,
                            "traceback": traceback.format_exc(),
                            # [ADDED] 실패 트랜잭션도 해싱할 수 있도록 메트릭 주입
                            "metrics": {
                                "fuel_consumed": fuel_consumed,
                                "mem_usage_bytes": mem_usage,
                                "tier": "EDGE_PYTHON"
                            }
                        }
                    },
                    "id": request_id
                }),
                headers=headers
            )
            
        finally:
            # Restore standard streams
            sys.stdout, sys.stderr = old_stdout, old_stderr
            print(f"[PythonEngine] 🏁 Request ID: {request_id} execution phase finalized.") # [LOG ADDED]
            
    except Exception as e:
        # Handle framework-level exceptions (e.g., JSON parsing)
        print(f"[PythonEngine] 🚨 Framework Internal Error: {str(e)}") # [LOG ADDED]
        return Response.new(json.dumps({"error": str(e)}), status=400)