# receptor.lang.binder
"""
@role: Class-based Boundary-driven Model Binder
@semantics:
- Entity-Component-System (ECS) inspired architecture
- BoundarySensor: Detects ∂Φ
- ModelManifold: Maintains Φ nodes and edge coupling
- ModelBinder: Orchestrates the field formation
"""
import json
import sys
from pathlib import Path
from collections import defaultdict, Counter
from konlpy.tag import Mecab
from tqdm import tqdm
from bound.emitter import get_logger
from bound.resolver import find_current_self, resolve_path
from phase.model import PhaseGraph, PhaseNode, PhaseRelation

log = get_logger("lang.pos")

class PosSensor:
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

    def __init__(self, dic_path="/opt/homebrew/lib/mecab/dic/mecab-ko-dic"):
        try:
            self.mecab = Mecab(dic_path)
        except Exception as e:
            log.warn(f"Mecab load failed: {e}")
            self.mecab = None

    def sense(self, text):
        """텍스트에서 (phi_seed, boundary_group) 쌍을 추출"""
        if not self.mecab: return []
        tokens = self.mecab.pos(text)
        candidates = []
        for i in range(len(tokens) - 1):
            cur_word, cur_tag = tokens[i]
            next_word, next_tag = tokens[i+1]
            meta = self.POS_MAP.get(next_word)
            if cur_tag.startswith("NN") and meta and next_tag == meta["pos"]:
                normalized_node = cur_word.strip().lower()
                candidates.append((cur_word, meta["group"]))
        return candidates

class LangManifold:
    """추출된 Φ 노드와 이들 간의 결합(Edge)을 관리하는 데이터 필드"""
    
    def __init__(self):
        self.node_intensity = Counter()
        self.node_support = defaultdict(set)
        self.node_boundaries = defaultdict(Counter)
        self.edge_field = Counter()

    def bind(self, seed, b_group, doc_path):
        """노드를 매니폴드에 결속"""
        self.node_intensity[seed] += 1
        self.node_support[seed].add(doc_path)
        self.node_boundaries[seed][b_group] += 1

    def couple(self, nodes):
        """노드들 간의 위상적 결합(Edge) 형성"""
        sorted_nodes = sorted(list(nodes))
        for i in range(len(sorted_nodes)):
            for j in range(i + 1, len(sorted_nodes)):
                edge = tuple(sorted([sorted_nodes[i], sorted_nodes[j]]))
                self.edge_field[edge] += 1

    def get_invariants(self, threshold=7):
        """다양한 경계 속성을 가진 불변 노드 식별"""
        return [n for n, b_counts in self.node_boundaries.items() if len(b_counts) >= threshold]

class LangBinder:
    """모델을 순회하며 위상 필드를 구축하고 투영(Projection)을 생성하는 오케스트레이터"""

    def __init__(self):
        self.sensor = PosSensor()
        self.manifold = LangManifold()
        self.model_root = resolve_path('model')
        self.output_path = resolve_path("xor") / "node" / "model.bound.json"

    def execute(self):
        log.info(f"Binding Model Field from: {self.model_root}")
        
        files = list(self.model_root.rglob("*.md"))
        for path in tqdm(files, desc="Processing Documents"):
            try:
                text = path.read_text(encoding="utf-8")
                candidates = self.sensor.sense(text)
                
                doc_nodes = set()
                for seed, b_group in candidates:
                    self.manifold.bind(seed, b_group, str(path))
                    doc_nodes.add(seed)
                
                self.manifold.couple(doc_nodes)
            except Exception as e:
                log.error(f"Error in {path.name}: {e}")

        self._project()

    def _project(self, top_k=100):
        invariants = self.manifold.get_invariants()
        top_seeds = [n for n, _ in self.manifold.node_intensity.most_common(top_k)]
        
        # 1. 모델 클래스 기반 노드 초기화
        nodes_dict = {}
        for seed in top_seeds:
            nodes_dict[seed] = PhaseNode(
                id=seed,
                intensity=self.manifold.node_intensity[seed],
                is_invariant=(seed in invariants),
                boundaries=dict(self.manifold.node_boundaries[seed]),
                support_manifold=sorted(list(self.manifold.node_support[seed]))[:3]
            )

        # 2. 엣지 데이터를 모델 클래스로 주입
        for (u, v), weight in self.manifold.edge_field.items():
            if weight >= 2 and u in top_seeds and v in top_seeds:
                nodes_dict[u].relations.append(PhaseRelation(target=v, strength=weight))
                nodes_dict[v].relations.append(PhaseRelation(target=u, strength=weight))

        # 3. 최종 그래프 객체 생성
        topos_graph = PhaseGraph(
            invariants=invariants,
            nodes=nodes_dict
        )

        # 4. JSON 투영 (to_dict 활용)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(topos_graph.to_dict(), f, ensure_ascii=False, indent=2)
            
        log.info(f"Model Manifold Projection completed: {self.output_path}")

if __name__ == "__main__":
    binder = LangBinder()
    binder.execute()