# arch.topos.edge.trajectory
import time
import hashlib
import json
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set
from watcher.plane.emitter import get_logger
from pathlib import Path
from dataclasses import dataclass, field

from phase.bind.resolver import find_current_self
from watcher.plane.emitter import get_emitter

TARGET_EXTENSIONS = {".md", ".py", ".kt"}
FILE_FORMATS = {
    ".md": {"lineage": "@lineage:"},
    ".py": {"lineage": "## @lineage:"},
    ".kt": {"lineage": "// @lineage:"},
}
EXCLUDED_DIRS = {"__pycache__", ".git"}

log = get_logger("edge.trajectory")

@dataclass
class ClosureDelta:
    """기호적/구조적 변이 규칙 (Δ)"""
    lineage_ref: Optional[str] = None
    tension_level: float = 0.0
    break_symbols: Set[str] = field(default_factory=set)
    align_symbols: Set[str] = field(default_factory=set)
    bias_shift: float = 0.0

@dataclass(kw_only=True)
class SignatureBound:
    """위상 공간 (Φ) - Lineage 기반 폐쇄성 관리"""
    module_id: str
    base_instructions: str
    input_fields: List[str]
    output_fields: List[str]

    ref_topo: int = 0 
    ref_press: int = 0
    
    active_repulsion: Dict[str, float] = field(default_factory=dict)
    active_attraction: Dict[str, float] = field(default_factory=dict)

    lineage: List[str] = field(default_factory=list)
    delta_history: List[ClosureDelta] = field(default_factory=list)
    version: int = 1

    def mutate(self, rule: ClosureDelta):
        """Φ -> Φ⁺: 감쇄와 강화를 통한 위상적 안정화"""
        self.delta_history.append(rule)
        strength = rule.tension_level + rule.bias_shift
        
        for sym in rule.break_symbols:
            self.active_repulsion[sym] = self.active_repulsion.get(sym, 0.0) + strength
            
        for sym in rule.align_symbols:
            self.active_attraction[sym] = self.active_attraction.get(sym, 0.0) + strength

        decay_factor = 0.9
        self._apply_decay(self.active_repulsion, decay_factor)
        self._apply_decay(self.active_attraction, decay_factor)

        if rule.lineage_ref and rule.lineage_ref not in self.lineage:
            self.lineage.append(rule.lineage_ref)

        self.version += 1
        log.info(f"[{self.module_id}] Field Stabilized v{self.version} (Lineage: {rule.lineage_ref})")

    def _apply_decay(self, field_map: Dict[str, float], factor: float):
        for k in list(field_map.keys()):
            field_map[k] *= factor
            if field_map[k] < 0.05:
                del field_map[k]

    def dump_state(self) -> dict:
        return {
            "module_id": self.module_id,
            "version": self.version,
            "lineage": self.lineage,
            "repulsion_field": self.active_repulsion,
            "attraction_field": self.active_attraction
        }

class TrajectoryXor:
    def __init__(self, tension_threshold: float = 0.5):
        self.tension_threshold = tension_threshold

    def synth(self, recent_bases: List[Dict]) -> Optional[ClosureDelta]:
        if not recent_bases: return None

        bad_symbols, env_symbols = set(), set()
        max_tension = 0.0
        ref_lineage = recent_bases[0].get("lineage", ["unknown"])[-1]

        for basis in recent_bases:
            traces = basis.get("traces", [])
            for t in traces:
                for step in t.get("steps", []):
                    payload = str(step.get("inputs", {}).get("payload", ""))
                    tokens = {word.lower() for word in payload.split() if len(word) > 3}
                    bad_symbols.update(tokens)

            context = basis.get("context", {})
            tension = context.get("tension", 0.0)
            if tension > max_tension: max_tension = tension
            if tension > self.tension_threshold:
                for doc in context.get("enriched_docs", []):
                    for kw, count in doc.get("keywords", []):
                        env_symbols.add(kw)

        return ClosureDelta(
            lineage_ref=ref_lineage,
            tension_level=max_tension,
            break_symbols=bad_symbols - env_symbols,
            align_symbols=env_symbols,
            bias_shift=max_tension * 0.1
        )

@dataclass(frozen=True)
class PhaseState:
    """현실과 맞닿은 경계에서의 에너지 및 수용체 파동 (Ψ)"""
    membrane_bound: bool
    axp_ratio: float
    ctla_4_expression: float
    cd28_expression: float
    lineage_path: str

@dataclass(frozen=True)
class FrameLog:
    frame_id: str
    lineage_path: str
    tension_snapshot: float  
    suppress_ratio: float    
    reason: str
    timestamp: float = field(default_factory=time.time)

class FrameRegistry:
    def __init__(self):
        self._frames: List[FrameLog] = []

    def commit_frame(self, lineage: str, tension: float, suppress_ratio: float, reason: str) -> FrameLog:
        raw_id = f"{lineage}:{tension}:{time.time()}"
        frame_id = hashlib.sha256(raw_id.encode()).hexdigest()[:8]
        
        frame = FrameLog(
            frame_id=f"frame.treg.{frame_id}",
            lineage_path=lineage,
            tension_snapshot=tension,
            suppress_ratio=suppress_ratio,
            reason=reason
        )
        self._frames.append(frame)
        log.info(f"[theoria.registry] 닫힘 프레임 등재 완료: {frame.frame_id}")
        log.info(f"  ↳ @lineage: {frame.lineage_path}")
        log.info(f"  ↳ @reason:  {frame.reason}\n")
        return frame

    def get_historical_tension(self, lineage: str) -> float:
        return sum(f.tension_snapshot for f in self._frames if lineage.startswith(f.lineage_path))

class TregEdge:
    TENSION_THRESHOLD = 1.0
    SUPPRESS_DOMINANCE = 0.5
    MEMORY_THRESHOLD = 1.2 

    def __init__(self, registry: FrameRegistry, 
                 signature: Optional[SignatureBound] = None, 
                 xor_engine: Optional[TrajectoryXor] = None):
        self.registry = registry
        self.signature = signature
        self.xor_engine = xor_engine

    def traverse(self, state: PhaseState) -> Dict[str, Any]:
        historical_tension = self.registry.get_historical_tension(state.lineage_path)
        if historical_tension > self.MEMORY_THRESHOLD:
            return {"status": "rejected_by_memory", "message": f"과거 붕괴 이력 누적 (Tension: {historical_tension:.2f})"}

        if state.axp_ratio > self.TENSION_THRESHOLD:
            return self._seal_topology(state, "AxP_saturation_energy_depleted")

        co_stim_ratio = state.ctla_4_expression / max(state.cd28_expression, 0.01)
        if co_stim_ratio > self.SUPPRESS_DOMINANCE:
            return self._seal_topology(state, f"CTLA4_suppression_dominance_{co_stim_ratio:.2f}")

        return {"status": "traversed", "next_node": "phi_x_activation"}

    def _seal_topology(self, state: PhaseState, reason: str) -> Dict[str, Any]:
        frame = self.registry.commit_frame(
            lineage=state.lineage_path,
            tension=state.axp_ratio,
            suppress_ratio=(state.ctla_4_expression / max(state.cd28_expression, 0.01)),
            reason=reason
        )
        
        if self.xor_engine and self.signature:
            synthetic_basis = {
                "lineage": [state.lineage_path],
                "context": {"tension": state.axp_ratio, "enriched_docs": []},
                "traces": [{"steps": [{"inputs": {"payload": reason}}]}]
            }
            delta = self.xor_engine.synth([synthetic_basis])
            if delta:
                self.signature.mutate(delta)
        
        return {
            "status": "closed",
            "frame_ref": frame.frame_id,
            "message": "흐름이 억제되었으며 Theoria 레지스트리에 닫힘이 증명됨."
        }

class TopologicalScanner:
    """디렉토리를 순회하며 Lineage를 추출하고 TregEdge를 통해 위상을 시뮬레이션하는 궤적 스캐너"""
    
    def __init__(self, base_root: str, target_rel_path: str):
        self.base_root = Path(base_root)
        self.target_dir = self.base_root / target_rel_path
        
        # 시스템 브릿지 초기화 (Composition)
        self.registry = FrameRegistry()
        self.xor_engine = TrajectoryXor(tension_threshold=0.5)
        self.signature = SignatureBound(
            module_id="meta.self.treg_scanner",
            base_instructions="Dynamic multi-ext path-based lineage traversal",
            input_fields=[], output_fields=[]
        )
        self.gate = TregEdge(registry=self.registry, signature=self.signature, xor_engine=self.xor_engine)

    def _extract_lineage(self, filepath: Path) -> str:
        """단일 파일에서 확장자에 맞는 Lineage 추출 또는 폴백 반환"""
        ext = filepath.suffix
        lineage_prefix = FILE_FORMATS.get(ext, FILE_FORMATS[".md"])["lineage"]

        try:
            with filepath.open('r', encoding='utf-8') as f:
                for _ in range(10):
                    line = f.readline()
                    if not line: break
                    if lineage_prefix in line:
                        return line.split(lineage_prefix)[1].strip()
        except Exception as e:
            log.warning(f"[extract_fail] {filepath}: {e}")
        
        rel_path = filepath.relative_to(self.base_root)
        return str(rel_path.with_suffix('')).replace("/", ".").replace("\\", ".")

    def scan(self) -> List[str]:
        """지정된 디렉토리를 순회하여 유효한 Lineage 목록을 반환"""
        if not self.target_dir.exists() or not self.target_dir.is_dir():
            log.error(f"[Error] 지정된 경로가 존재하지 않거나 디렉토리가 아닙니다: {self.target_dir}")
            return []

        log.info(f">>> 위상 스캔 시작: {self.target_dir}")
        extracted_lineages = []
        
        for file_path in self.target_dir.rglob("*"):
            if (
                file_path.is_file() 
                and file_path.suffix in TARGET_EXTENSIONS 
                and not file_path.name.startswith(".") 
                and not any(p in EXCLUDED_DIRS for p in file_path.parts)
            ):
                lineage = self._extract_lineage(file_path)
                extracted_lineages.append(lineage)
                
        return extracted_lineages

    def simulate(self, lineages: List[str], stress_target: str = None):
        """추출된 Lineage들을 대상으로 Traversal 시뮬레이션 실행"""
        if not lineages:
            log.info("발견된 Lineage 대상 파일이 없습니다.")
            return

        log.info(f"\n--- 총 {len(lineages)}개의 대상 Lineage 확보. 순차 Traversal 실행 ---")
        
        for lin in lineages:
            log.info(f"\n[Traverse Request] {lin}")
            
            # 스트레스 타겟이 지정되었고, 현재 lineage가 그 타겟을 포함하면 과부하 발생
            current_axp = 1.5 if stress_target and stress_target in lin else 0.5
            
            state = PhaseState(
                membrane_bound=True, 
                axp_ratio=current_axp, 
                ctla_4_expression=0.1, 
                cd28_expression=0.8, 
                lineage_path=lin
            )
            res = self.gate.traverse(state)
            log.info(f"  ↳ 결과: {res}")

        log.info("\n>>> 최종 위상 공간(Φ) 상태 스냅샷")
        log.info(self.signature.dump_state())