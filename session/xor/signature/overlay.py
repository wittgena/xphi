# session.xor.signature.overlay
import sys
import json
import random
import re
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Set
from phase.reflect.xor import Xor
from bound.surface.emitter import get_emitter
from bound.resolver import find_current_self, resolve_path, load_bound

log = get_emitter("signature.overlay")

try:
    SELF_ROOT = find_current_self()
    OVERLAY_ROOT = resolve_path("overlay")
    OVERLAY_MEM = OVERLAY_ROOT / "mem.json"
    BOUND_CONFIG = load_bound(SELF_ROOT)
    CHANNELS = BOUND_CONFIG.get("channels", {}).get("namespaces", [])
except Exception as e:
    log.error(f"[Critical] 시스템 위상 로드 실패: {e}")
    sys.exit(1)

N_MEM_WINDOWS = 5
TOP_K_DOCS = 3
SEED_WORDS = ["recursion", "boundary", "resonance", "drift", "lattice", "sheaf", "asymmetry", "flux", "bifurcation", "cohomology"]
STOPWORDS = {"the","and","for","with","that","this","from","are","was","but","have","not","you","your"}

class MemProbe:
    """순수 기호적 환경 센서 (Symbolic Environment Sensor)"""
    def __init__(self, mem_path: Path):
        self.path = mem_path
        self.mem = self._load()
        self.xor = Xor() 

    def _load(self):
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except: pass
        return {"symbol_history": [], "last_updated": None}

    def save(self):
        self.path.write_text(json.dumps(self.mem, indent=2, ensure_ascii=False), encoding="utf-8")

    def run_xearch(self, query: str) -> List[Any]:
        try:
            return self.xor.search(query)
        except Exception as e:
            log.error(f"Xor search failed: {e}")
            return []

    def extract_tf(self, path_str: str) -> List[tuple]:
        """물리/논리적 경로에서 기호(단어)를 추출"""
        text = ""
        try:
            target_path = Path(path_str)
            if target_path.exists() and target_path.is_file():
                text = target_path.read_text(encoding="utf-8")
            elif path_str.startswith("http"):
                req = urllib.request.Request(path_str, headers={'User-Agent': 'Xphi-Probe/1.0'})
                with urllib.request.urlopen(req, timeout=3) as response:
                    text = response.read().decode('utf-8')
            else:
                return []
                
            words = re.findall(r"\b[a-zA-Z]{3,}\b", text.lower())
            counts = Counter(w for w in words if w not in STOPWORDS)
            return counts.most_common(10)
        except Exception as e:
            log.debug(f"TF 추출 실패 ({path_str}): {e}")
            return []

    def sense_environment(self) -> Dict[str, Any]:
        """
        DocksExecutor의 PressureProbe와 TrajectoryXor가 소비할 Context를 생성합니다.
        숫자 조작을 버리고, '기억된 기호'와 '새로운 기호'의 차이로 위상을 읽습니다.
        """
        seed = random.choice(SEED_WORDS)
        raw_output = self.run_xearch(seed)
        
        # 1. 문서 및 기호 수집
        enriched_docs = []
        current_symbols: Set[str] = set()
        
        # Xor 결과를 파싱하고 기호 추출
        docs = sorted(raw_output, key=lambda x: getattr(x, 'score', 0), reverse=True)[:TOP_K_DOCS]
        
        for r in docs:
            doc_info = {
                "path": r.file_path,
                "type": getattr(r, 'block_type', 'unknown'),
                "keywords": self.extract_tf(r.file_path)
            }
            enriched_docs.append(doc_info)
            # 현재 환경의 기호 풀 생성
            for kw, _ in doc_info["keywords"]:
                current_symbols.add(kw)

        # 2. 위상적 이질감(Alienation) 계산 -> 이것이 곧 Tension
        remembered_symbols: Set[str] = set()
        for past_symbols in self.mem["symbol_history"]:
            remembered_symbols.update(past_symbols)

        # 교집합이 아닌 '차집합'을 통한 낯섦의 크기
        alien_symbols = current_symbols - remembered_symbols
        
        # 순수 기호 기반의 Tension: 현재 환경 기호 중 내 기억에 없는 기호의 비율
        tension = len(alien_symbols) / max(1, len(current_symbols)) if current_symbols else 0.0

        # 3. 기억의 축적
        if current_symbols:
            self.mem["symbol_history"].append(list(current_symbols))
            self.mem["symbol_history"] = self.mem["symbol_history"][-N_MEM_WINDOWS:]
            self.mem["last_updated"] = datetime.now().isoformat()
            self.save()

        context = {
            "seed": seed,
            "tension": tension, # PressureProbe가 사용할 위상적 긴장도
            "alien_symbols": list(alien_symbols), # 디버깅 및 관측용
            "enriched_docs": enriched_docs # TrajectoryXor가 env_symbols를 뽑아낼 소스
        }
        
        log.info(f"Environment Sensed | Seed: {seed} | Symbolic Tension: {tension:.2f} | Alien Symbols: {len(alien_symbols)}")
        return context

if __name__ == "__main__":
    probe = MemProbe(OVERLAY_MEM)
    context_report = probe.sense_environment()
    print(json.dumps(context_report, indent=2, ensure_ascii=False))