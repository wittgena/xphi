# arch.bound.sensor
## @lineage: arch.topos.bound.sensor
## @lineage: arch.bound.pos.sensor
"""@role: Class-based Boundary-driven Model Binder"""
import re
import json
import sys
from pathlib import Path
from collections import defaultdict, Counter

class BoundSensor:
    """∂Φ(Bound) 감지 및 Φ seed 추출을 담당하는 센서 계층"""
    
    ## axis.map 
    POS_MAP = {
        "적": {"group": "structural", "pos": "XSN", "group_desc": "phi_x 구조 귀속자 - 개념 고정 / 안정화"},
        "의": {"group": "possessive", "pos": "JKG", "group_desc": "dPhi 경계 귀속 - 소속 / 종속 구조"},
        "을": {"group": "objective", "pos": "JKO", "group_desc": "psi_i 작용 대상 - 의미 흐름 목적지"},
        "를": {"group": "objective", "pos": "JKO", "group_desc": "psi_i 작용 대상 - 의미 흐름 목적지"},
        "이": {"group": "subject", "pos": "JKS", "group_desc": "psi_i 발생원 - 작용 주체"},
        "가": {"group": "subject", "pos": "JKS", "group_desc": "psi_i 발생원 - 작용 주체"},
        "은": {"group": "topic", "pos": "JX", "group_desc": "위상 attractor - 문맥 중심점"},
        "는": {"group": "topic", "pos": "JX", "group_desc": "위상 attractor - 문맥 중심점"},
        "에서": {"group": "ablative", "pos": "JKB", "group_desc": "출발 경계 (from)"},
        "에": {"group": "locative", "pos": "JKB", "group_desc": "위치 고정점 (at / in)"},
        "으로": {"group": "directional", "pos": "JKB", "group_desc": "방향 유도 (to / toward)"},
        "로": {"group": "directional", "pos": "JKB", "group_desc": "방향 유도 (to / toward)"},
        "와": {"group": "instrumental", "pos": "JC", "group_desc": "수단 / 매개 / 동반"},
        "과": {"group": "instrumental", "pos": "JC", "group_desc": "수단 / 매개 / 동반"},
        "로써": {"group": "instrumental", "pos": "JKB", "group_desc": "수단 / 매개 / 동반"},
        "까지": {"group": "terminative", "pos": "JX", "group_desc": "종착점 (endpoint)"},
        "부터": {"group": "originative", "pos": "JX", "group_desc": "시작점 (source)"},
    }

    def __init__(self):
        # 길이 순 정렬 (최장 일치 보장)
        suffixes = sorted(self.POS_MAP.keys(), key=len, reverse=True)
        
        # 정규식 패턴: (한글/영문/숫자 1자 이상) + (맵에 정의된 조사 중 하나) + (문자열 끝)
        # 예: ^([가-힣a-zA-Z0-9]+)(에서|으로|로써|까지|부터|...)$ 
        pattern = rf"^([가-힣a-zA-Z0-9]+)({'|'.join(suffixes)})$"
        self.regex = re.compile(pattern)
        self.punctuation = """.,!?"'()[]{}>"""

    def sense(self, text):
        candidates = []
        words = text.split()
        
        for word in words:
            clean_word = word.strip(self.punctuation)
            match = self.regex.match(clean_word)
            
            if match:
                stem = match.group(1)   # 명사(추정) 부분
                suffix = match.group(2) # 매칭된 조사 부분
                
                normalized_node = stem.strip().lower()
                meta = self.POS_MAP[suffix]
                candidates.append((normalized_node, meta["group"]))
                
        return candidates