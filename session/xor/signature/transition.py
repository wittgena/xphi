# session.xor.signature.transition
"""@flow: ResidueStore(rocks.db) → XeCrystallizer → ScriptCompiler(Φ') → ScriptProjector(Ψ)"""
import json
from typing import Dict, Any, List
from bound.surface.emitter import get_logger
from session.xor.store import ResidueStore, ResidueSnapshot
from session.xor.signature.compiler import SignatureCompiler, SignatureProjector

log = get_logger("signature.transition")

class SignatureTransition:
    """
    [Phase: xe(Snapshot) → Φ⁺(Signature State) → Φ′(Topology) → Ψ(Actuator)]
    RocksDB에 쌓인 ResidueSnapshot을 읽어들여 Action Compiler로 밀어넣는 위상 전이 브릿지
    """
    def __init__(self, target_module_id: str = "ator_dynamic_core"):
        self.target_module_id = target_module_id
        self.store = ResidueStore()
        self.compiler = SignatureCompiler()
        self.projector = SignatureProjector()

    def process_latest_snapshot(self) -> Dict[str, Any]:
        """DB에서 가장 최근의 응결된 스냅샷을 가져와 상전이를 트리거한다."""
        keys = list(self.store.db.keys())
        if not keys:
            log.info("No residue snapshots found. Topology is stable.")
            return {}

        ## 가장 최근에 적재된 스냅샷 키 가져오기 (단순화를 위해 마지막 키 사용)
        latest_key = keys[-1]
        raw_json = self.store.db[latest_key]
        
        try:
            snapshot_dict = json.loads(raw_json)
            snapshot = ResidueSnapshot(**snapshot_dict)
        except Exception as e:
            log.error(f"Failed to parse snapshot: {e}")
            return {}

        ## 스냅샷(xe)을 Signature State로 결정화
        signature_state = self._crystallize(snapshot, basis_ref=f"basis::{latest_key.decode()[:8]}")
        log.signal(f"Crystallized Signature State with Tension: {signature_state['tension']}")

        ## 컴파일 (Φ⁺ → Φ')
        try:
            phase_script = self.compiler.compile(signature_state)
            log.signal("Topological Graph (Φ') woven successfully.")
            
            ## 런타임 투영 (Φ' → Ψ)
            runtime_specs = self.projector.project(phase_script)
            log.signal("Runtime Specs projected. Ready for execution surface.")
            
            ## 처리 완료된 스냅샷은 DB에서 제거 (혹은 상태 변경)하여 중복 컴파일 방지
            del self.store.db[latest_key]
            return {
                "phase_script": phase_script.export_boundary(),
                "runtime_specs": runtime_specs
            }
        except Exception as e:
            log.error(f"Phase transition failed during compilation: {e}")
            return {}

    def _crystallize(self, snapshot: ResidueSnapshot, basis_ref: str) -> Dict[str, Any]:
        """ResidueSnapshot의 토폴로지 노드와 기호들을 컴파일러용 Mutated Rules로 변환"""
        mutated_rules = []
        
        ## 기호(Symbols)들을 위상적 긴장 요인으로 치환하여 Rule 생성
        for idx, symbol in enumerate(snapshot.symbols):
            rule_pressure = min(snapshot.tension * 1.5, 1.0)
            mutated_rules.append({
                "logic": f"Absorb or deflect unintegrated symbol: {symbol}",
                "aspect": f"aspect_{symbol[:10]}",
                "pressure": rule_pressure,
                "basis_ref": basis_ref,
                "target_module": self._determine_target_by_pressure(rule_pressure)
            })
        return {
            "module_id": self.target_module_id,
            "base_instructions": "Dynamic projection field generated from structural residues.",
            "version": int(snapshot.timestamp), # Timestamp를 버저닝에 활용
            "basis_snapshot": basis_ref,
            "tension": snapshot.pressure, # 누적되었던 최종 Pressure를 Tension으로 인계
            "input_fields": ["payload", "context_vector"],
            "mutated_rules": mutated_rules
        }

    def _determine_target_by_pressure(self, pressure: float) -> str:
        """압력(Pressure)에 따른 바운더리 라우팅 결정"""
        if pressure >= 0.85:
            return "ator_nullifier"   # 고압축/파괴
        elif pressure >= 0.60:
            return "ator_compressor"  # 구조적 압축
        else:
            return "ator_processor"   # 일반 흡수