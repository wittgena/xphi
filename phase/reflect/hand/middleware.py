# phase.reflect.hand.middleware
import time
import hashlib
import json
from typing import Dict, List
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from topos.plane.emitter import get_emitter

log = get_emitter("resonance.middleware")

class ResonanceMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, window_size: int = 10, threshold: int = 3):
        super().__init__(app)
        self.window_size = window_size
        self.threshold = threshold
        self.residue: Dict[str, List[dict]] = {}

    async def dispatch(self, request: Request, call_next):
        conv_id = self._extract_conversation_id(request.url.path)
        if not conv_id:
            return await call_next(request)

        ## Body 추출 (비파괴적 방식 유지)
        body_bytes = await request.body()
        
        ## 다시 읽을 수 있도록 request._receive를 override
        async def receive(): return {"type": "http.request", "body": body_bytes}
        request._receive = receive

        now = time.time()
        
        ## 위상 이력(Residue) 컨테이너 초기화
        if conv_id not in self.residue:
            self.residue[conv_id] = []
        history = self.residue[conv_id]

        ## 내용이 매번 바뀌어도 'think'라는 행동 자체를 카운팅
        is_think_action = False
        if "/events" in request.url.path and request.method == "POST":
            try:
                payload = json.loads(body_bytes)
                if payload.get("action") == "think":
                    is_think_action = True
                    think_count = sum(1 for h in history if h.get("is_think"))
                    
                    if think_count >= (self.threshold - 1): # 3번째 요청이 들어오면 실행하지 않고 즉각 단절(UAA)
                        log.error(f"[@rupture] Server Bridge blocking excessive thoughts in {conv_id}")
                        return self._project_rupture_response(conv_id, "cognitive_livelock")
            except Exception:
                pass # JSON 파싱 실패 시 무시하고 다음 방어선으로 이동

        ## 동일한 URL, 파라미터, Body가 완전히 똑같이 반복되는 경우
        fingerprint = self._generate_fingerprint(request, body_bytes)
        
        ## think 액션이 아닐 때만 지문(Fingerprint) 검사 수행
        if not is_think_action:
            if self._check_structural_stuck(history, fingerprint):
                log.warning(f"[@rupture] Structural loop detected in {conv_id}")
                return self._project_rupture_response(conv_id, fingerprint)

        ## 실행 및 응답 관측
        response = await call_next(request)
        
        ## 응답 위상 기록 (다음 판단을 위한 Residue 축적)
        history.append({
            "fp": fingerprint,
            "is_think": is_think_action, # think 여부 기록
            "ts": now,
            "status": response.status_code
        })
        self.residue[conv_id] = history[-self.window_size:]
        return response

    def _check_structural_stuck(self, history: List[dict], current_fp: str) -> bool:
        if not history: return False
        
        ## 최근 threshold 회수의 호출이 지문(FP)까지 모두 동일한지 확인
        recent_matches = [h for h in history[-self.threshold:] if h.get("fp") == current_fp]
        if len(recent_matches) >= self.threshold:
            return True
        return False

    def _extract_conversation_id(self, path: str) -> str:
        parts = path.strip("/").split("/")
        if len(parts) >= 2 and parts[0] == "conversations":
            res = parts[1]
            return res
        return ""

    def _generate_fingerprint(self, request: Request, body_bytes: bytes) -> str:
        ## URL, Query Param, 그리고 Body의 순수 텍스트를 모두 조합하여 완벽한 상태 지문을 생성
        body_text = body_bytes.decode('utf-8', 'ignore')
        raw_data = f"{request.method}:{request.url.path}:{request.query_params}:{body_text}"
        return hashlib.md5(raw_data.encode()).hexdigest()

    def _project_rupture_response(self, conv_id: str, fingerprint: str):
        """에이전트에게 반사된 자신의 위상을 투영 (UAA Hard Stop)"""
        log.error(f"[@theoria] Projecting rupture response to {conv_id} (FP: {fingerprint})")
        return JSONResponse(
            status_code=508,
            content={
                "error": "Structural Recursion Detected",
                "phase": "interference",
                "residue_fingerprint": fingerprint,
                "suggestion": "Identity loop or cognitive livelock found. Shift your reasoning substrate.",
                "theoria_projection": f"Reflected call in {conv_id}"
            }
        )