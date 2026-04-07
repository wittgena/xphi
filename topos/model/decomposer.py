# topos.model.decomposer
"""
@role: Theoria Role Decomposer (Projection Override)
@semantics:
- Inherits the Manifold construction from TheoriaBinder.
- Overrides the Projection phase to apply Purity & Coupling math.
- Acts as a mathematical filter over the raw topological field.
"""
import json
from plane.emitter import get_logger
from topos.theoria.binder import TheoriaBinder

log = get_logger("theoria.decomposer")

class TheoriaDecomposer(TheoriaBinder):
    """
    TheoriaBinder를 상속받아, Purity(순도)와 Coupling(결합도) 기반의
    Variant 분해 투영을 수행하는 클래스
    """
    
    def __init__(self, tau=0.7):
        # 1. 부모 클래스(Binder)의 초기화 (Sensor, Manifold 로드)
        super().__init__()
        
        # 2. Decomposer만의 고유 속성 추가
        self.purity_tau = tau
        
        # 3. 출력 경로 덮어쓰기 (binder와 충돌 방지)
        self.output_path = self.output_path.parent / "topos.decomposed.json"

    def _project(self, top_k=None):
        """
        [오버라이드] 부모 클래스의 단순 요약 투영을 무시하고,
        위상 수학적 역할 분해(Role Decomposition) 로직을 적용합니다.
        """
        log.info(f"Applying Decomposition Math (tau={self.purity_tau})...")
        
        # 1. Purity 기반 역할 분해 (Variant 식별)
        roles_map = {}
        for node, b_counts in self.manifold.node_boundaries.items():
            total = sum(b_counts.values())
            if total == 0: continue
            
            primary_group, count = b_counts.most_common(1)[0]
            purity = count / total
            
            if purity >= self.purity_tau:
                roles_map[node] = {
                    "role": primary_group,
                    "purity": round(purity, 3),
                    "intensity": total
                }

        # 2. Normalized Coupling 계산
        edges_data = []
        for (u, v), w in self.manifold.edge_field.items():
            if u in roles_map and v in roles_map:
                intensity_u = self.manifold.node_intensity[u]
                intensity_v = self.manifold.node_intensity[v]
                
                # 강도가 약한 노드 기준으로 결합력을 정규화
                strength = w / min(intensity_u, intensity_v)
                
                if strength > 0.1: # 유의미한 임계점
                    edges_data.append({
                        "source": u, "target": v,
                        "coupling": round(strength, 3),
                        "raw_weight": w
                    })

        # 결합 강도(Coupling) 기준으로 정렬하여 상위 500개만 투영
        edges_data = sorted(edges_data, key=lambda x: x["coupling"], reverse=True)[:500]

        # 3. 결과 직렬화
        projection = {
            "metadata": {
                "type": "topos.decomposed_field",
                "purity_threshold": self.purity_tau,
                "variant_nodes_count": len(roles_map),
                "coupled_edges_count": len(edges_data)
            },
            "nodes": [{"id": node, **attr} for node, attr in roles_map.items()],
            "edges": edges_data
        }

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(projection, f, ensure_ascii=False, indent=2)
            
        ## Decomposer 콘솔 요약 (Summary Report)
        print(f"## [Theoria Decomposer] Role Decomposition Summary (τ={self.purity_tau})")
        print(f"- Contextual Variants (특수 역할 노드): {len(roles_map)}개 도출")
        
        ## 순도(Purity)와 강도(Intensity)가 높은 상위 5개 역할 출력
        sorted_roles = sorted(roles_map.items(), key=lambda x: (x[1]['purity'], x[1]['intensity']), reverse=True)
        print("\n- Top Pure Roles (순도 상위 5):")
        for i, (node, attr) in enumerate(sorted_roles[:5], 1):
            print(f"  {i}. {node:<10} → [{attr['role']}] (Purity: {attr['purity']:.2f}, Intensity: {attr['intensity']})")

        ## 결합력(Coupling) 기준 상위 5개 관계 출력
        print(f"\n- Strongest Theoria Couplings (결합 강도 상위 5):")
        for i, edge in enumerate(edges_data[:5], 1):
            print(f"  {i}. {edge['source']} ↔ {edge['target']} (Strength: {edge['coupling']:.2f})")
        print("="*50)

        log.info(f"Topological Decomposition saved to: {self.output_path}")

if __name__ == "__main__":
    decomposer = TheoriaDecomposer(tau=0.7)
    decomposer.execute()