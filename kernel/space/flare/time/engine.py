# kernel.space.flare.time.engine
from js import Response, Headers
import json
import sys
import io
import time
import os
import hashlib
import random
import traceback

# =====================================================================
# [보안 1] 호스트 환경 변수 완전 격리 (Environment Leakage 원천 차단)
# =====================================================================
class RestrictedEnviron:
    """os.environ 접근 시도 자체를 Exception으로 차단하여 샌드박스 격리(Isolation) 강제"""
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
    def clear(self): pass # 내부 프레임워크 호출 허용
    def __getattr__(self, name): raise PermissionError("Isolated")

os.environ = RestrictedEnviron()

# =====================================================================
# [보안 2] 가상 시간 & 결정론적 난수 모의 (Spectre 방어 및 PRNG 멱등성)
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
    # 실제 블로킹(Thread Sleep)을 방지하고 가상 시간만 전진시킴
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
    """매 요청(Request)마다 V8 Isolate의 글로벌 상태를 완벽히 초기화 (Determinism 보장)"""
    _virtual_context["time"] = float(ts) if ts is not None else 0.0
    _virtual_context["perf_time"] = 0.0
    
    # 시드가 없더라도 기본 시드를 강제하여 두 번 실행해도 100% 동일한 난수가 나오게 함 (PRNG Idempotency)
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
        request_id = input_data.get("id", None)
        
        context = params.get("context", {})
        ts = context.get("timestamp", None)
        seed = context.get("seed", None)
        
        # [방어기제] 실행 컨텍스트 초기화 (시간, 난수 시드 강제 리셋)
        _apply_execution_context(ts, json.dumps(seed) if seed else None)
        
        # [방어기제] Warm Start 상태 출혈 방지 (이전 요청의 sys 속성 찌꺼기 완벽 제거)
        if hasattr(sys, 'FLARE_BLEED_TEST'):
            delattr(sys, 'FLARE_BLEED_TEST')
            
        old_stdout, old_stderr = sys.stdout, sys.stderr
        buf_stdout, buf_stderr = io.StringIO(), io.StringIO()
        sys.stdout, sys.stderr = buf_stdout, buf_stderr
        
        code = params.get("code", "")
        variables = params.get("variables", {})
        
        # 전역/지역 네임스페이스 분리 및 외부 변수 주입
        global_env = dict(variables)
        local_env = {}
        
        try:
            # 컴파일 먼저 수행하여 Syntax 에러 분리
            compiled_code = compile(code, "<string>", "exec")
            exec(compiled_code, global_env, local_env)
            output = buf_stdout.getvalue()
            
            headers = Headers.new({"Content-Type": "application/json"}.items())
            return Response.new(
                json.dumps({"jsonrpc": "2.0", "result": {"output": output}, "id": request_id}),
                headers=headers
            )
            
        except BaseException as e:
            # Native 예외 규격 표준화 (JSON-RPC 엄격 준수)
            error_output = buf_stdout.getvalue()
            error_type = type(e).__name__
            
            # 파이썬 로컬 환경과 완벽히 동일한 에러 문자열 생성 (예: "PermissionError: Isolated")
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
                            "traceback": traceback.format_exc()
                        }
                    },
                    "id": request_id
                }),
                headers=headers
            )
            
        finally:
            sys.stdout, sys.stderr = old_stdout, old_stderr
            
    except Exception as e:
        # JSON 파싱 등 프레임워크 레벨 예외 처리
        return Response.new(json.dumps({"error": str(e)}), status=400)