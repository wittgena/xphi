# contract.task.proposer
"""
@desc: 
- Fold an external proposal surface into the runtime manifold.
- Recognizes polymorphic λ-ator shapes (Active, Reflective, Anchor).
- Admits nodes by local shape fitness, verifies global continuity,
- and binds survivable nodes into the live registry.
- [NEW] Orchestrates legacy bootstrap sequences via @bootstrap_contract decorators.
"""
import json
import importlib
import sys
from typing import Dict, Any, List, Set, Optional, Callable
from pathlib import Path
from collections import defaultdict
from bound.emitter import get_logger
from bound.resolver import resolve_path

OVERLAY_ROOT = resolve_path("overlay")

log = get_logger('task.proposer')

## [Phase 0] Legacy Bootstrap Registry & Discovery
BOOTSTRAP_REGISTRY = defaultdict(list)

def bootstrap_contract(group: str, args: List[str] = None):
    """해당 모듈이 속한 위상 그룹과, python -m 실행 시 필요한 CLI 인자를 등록합니다."""
    def decorator(func: Callable):
        # 함수 자체가 아니라, 함수가 속한 '모듈 이름'을 저장합니다.
        BOOTSTRAP_REGISTRY[group].append({
            "module_fqn": func.__module__,
            "args": args or []
        })
        return func
    return decorator

def discover_modules(root: Path):
    """
    하위 모듈들을 1회 import 하여 @bootstrap_contract 데코레이터를 레지스트리에 적재합니다.
    """
    root_parent = root.parent
    if str(root_parent) not in sys.path:
        sys.path.insert(0, str(root_parent))

    for py_file in root.rglob("*.py"):
        if py_file.name.startswith("_"): continue
        relative = py_file.relative_to(root_parent)
        module_path = str(relative.with_suffix("")).replace("/", ".").replace("\\", ".")
        try:
            importlib.import_module(module_path)
        except Exception as e:
            # 부트스트랩 스캔 중 발생하는 의존성 에러는 무시 (실제 실행은 subprocess에서 하므로 안전함)
            pass

# =========================================================
# [Phase 1] Lambda ator Topology (Form & Structure)
# =========================================================
class LambdaShape:
    """위상 공간에서 ator(-er)가 발현될 수 있는 필연적인 결합 형태들"""
    ACTIVE = "ActiveNode"         # λx.transform(x) : 입력 -> 변환 -> 출력
    REFLECTIVE = "ReflectiveNode" # λx.reflect(x)   : 입력 -> 투영 (변환 없음)
    ANCHOR = "AnchorNode"         # λx.anchor(x)    : 공간의 닻, 중력장 (상태 선언)
    UNKNOWN = "Amorphous"         # 형태 불명

class FoldJudger:
    def __init__(self, proposal_path: Optional[Path] = None, initial_surface_keys: Optional[Set[str]] = None):
        self.proposal_path = proposal_path
        self.initial_surface_keys = initial_surface_keys or set()
        self.phase_manifold: Dict[str, Any] = {
            "admitted_nodes": {},
            "phase_drift_log": [],
            "boundary_report": {}
        }

    def run_bootstrap_sequence(self, group_order: List[str]):
        """
        Activator의 요청에 따라, 등록된 모듈들을 기존과 동일하게 'python -m' 서브프로세스로 격리 실행합니다.
        """
        print(f"\n=== [FoldJudger] Dynamic Subprocess Bootstrap ===")
        for group in group_order:
            print(f"\n## @phase.group: {group}")
            
            targets = BOOTSTRAP_REGISTRY.get(group, [])
            if not targets:
                print(f"  └── (No scripts registered for {group})")
                continue

            for target in targets:
                module_fqn = target["module_fqn"]
                cli_args = target["args"]
                
                # 기존 하드코딩 명령어를 동적으로 완벽히 복원
                cmd = [sys.executable, "-m", module_fqn] + cli_args
                cmd_str = " ".join(cmd)
                
                print(f"  └── [exec] {cmd_str}")
                
                try:
                    # 프로세스 격리, 현재 작업 디렉토리(/usr/local/self) 기준 실행
                    subprocess.run(cmd, check=True, text=True)
                except subprocess.CalledProcessError as e:
                    log.error(f"[fatal] Bootstrap execution failed: {cmd_str}")
                    sys.exit(1)

    ## [Phase 2] Topological Folding (Proposal JSON Parsing)
    def _load_proposal(self) -> Dict[str, List[Dict[str, Any]]]:
        if not self.proposal_path or not self.proposal_path.exists():
            raise FileNotFoundError(f"[fold] Proposal surface not found: {self.proposal_path}")
        with open(self.proposal_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _determine_lambda_shape(self, methods: Set[str], contract: Dict[str, Any]) -> str:
        """메서드 구조와 위상 계약을 분석하여 노드의 λ-ator 형태를 판별합니다."""
        if {"align", "transform", "project"}.issubset(methods):
            return LambdaShape.ACTIVE
        elif {"align", "project"}.issubset(methods):
            return LambdaShape.REFLECTIVE
        elif contract.get("requires") and contract.get("emits"):
            return LambdaShape.ANCHOR
        return LambdaShape.UNKNOWN

    def _judge_local_structure(self, proposals: Dict[str, List[Dict[str, Any]]]) -> None:
        """phase.1: judge local admissibility and infer topological shape"""
        for group, nodes in proposals.items():
            for node in nodes:
                fqn = node["fqn"]
                methods = set(node.get("methods", []))
                contract = node.get("proposed_contract", {})

                if not contract.get("requires") or not contract.get("emits"):
                    self._record_drift(fqn, group, "MISSING_LAMBDA_BINDING (계약 누락)", contract)
                    continue

                shape = self._determine_lambda_shape(methods, contract)

                if shape == LambdaShape.UNKNOWN:
                    self._record_drift(fqn, group, "AMORPHOUS_STRUCTURE (형태 불명)", contract, list(methods))
                    continue

                self.phase_manifold["admitted_nodes"][fqn] = {
                    "group": group,
                    "contract": contract,
                    "lambda_shape": shape
                }

    def _verify_global_continuity(self) -> None:
        """phase.2: verify that admitted nodes can inhabit one continuous field"""
        available_states = set(self.initial_surface_keys)
        for data in self.phase_manifold["admitted_nodes"].values():
            available_states.update(data["contract"]["emits"])

        unreachable_nodes = []
        for fqn, data in self.phase_manifold["admitted_nodes"].items():
            requires = set(data["contract"]["requires"])
            unmet = requires - available_states
            if unmet:
                unreachable_nodes.append((fqn, unmet))

        for fqn, unmet in unreachable_nodes:
            data = self.phase_manifold["admitted_nodes"].pop(fqn)
            self._record_drift(fqn, data["group"], "TOPOLOGICAL_DEAD_END", data["contract"], list(unmet))

    def _fold_into_runtime(self) -> int:
        """phase.3: fold globally valid nodes into the live runtime manifold."""
        folded_count = 0
        for fqn, data in self.phase_manifold["admitted_nodes"].items():
            module_path, class_name = fqn.rsplit('.', 1)
            try:
                module = importlib.import_module(module_path)
                node_class = getattr(module, class_name)
                
                contract_spec = ContractSpec(
                    requires=frozenset(data["contract"]["requires"]),
                    emits=frozenset(data["contract"]["emits"])
                )
                
                node_class.__manifold_contract__ = contract_spec
                node_class.__lambda_shape__ = data["lambda_shape"]
                
                NODE_REGISTRY[fqn] = {
                    "contract": contract_spec,
                    "lambda_shape": data["lambda_shape"],
                    "node_class": node_class
                }
                folded_count += 1
            except Exception as e:
                self._record_drift(fqn, data["group"], "FOLDING_ERROR", data["contract"], str(e))
                
        return folded_count

    def _record_drift(self, fqn: str, group: str, reason: str, contract: Dict[str, Any], details: Any = None) -> None:
        self.phase_manifold["phase_drift_log"].append({
            "fqn": fqn,
            "group": group,
            "reason": reason,
            "details": details,
            "evidence": contract.get("evidence", None)
        })

    def execute_folding(self) -> Dict[str, Any]:
        """@flow: ingress -> shape inference -> continuity -> runtime bind."""
        if not self.proposal_path:
            log.warning("No proposal path provided. Skipping JSON folding.")
            return self.phase_manifold

        print(f"[fold] Polymorphic folding started from → {self.proposal_path.name}\n")

        proposals = self._load_proposal()
        self._judge_local_structure(proposals)
        self._verify_global_continuity()
        folded_count = self._fold_into_runtime()

        drifted_count = len(self.phase_manifold["phase_drift_log"])
        total = folded_count + drifted_count

        self.phase_manifold["boundary_report"] = {
            "total_candidates": total,
            "successfully_folded": folded_count,
            "drifted": drifted_count
        }

        print(f"[UGA] Folded into Runtime: {folded_count} (Active/Reflective/Anchor) | Drifted: {drifted_count}")
        return self.phase_manifold


if __name__ == "__main__":
    proposal_file = OVERLAY_ROOT / "contracts.json"
    
    ## @anchor.bootstrap: initial states present before any fold begins
    initial_surface = {"raw_data", "timestamp", "ext.phase"} 
    
    judger = FoldJudger(proposal_path=proposal_file, initial_surface_keys=initial_surface)
    final_manifold = judger.execute_folding()
    
    print("\n## Living Runtime Registry (Folded State by Shape)")
    
    for shape in [LambdaShape.ACTIVE, LambdaShape.REFLECTIVE, LambdaShape.ANCHOR]:
        nodes_of_shape = {k: v for k, v in NODE_REGISTRY.items() if v.get("lambda_shape") == shape}
        
        if nodes_of_shape:
            print(f"\n[{shape}s]")
            for fqn, meta in nodes_of_shape.items():
                req = ", ".join(meta["contract"].requires)
                emi = ", ".join(meta["contract"].emits)
                print(f"  - {fqn}: [{req}] ➔ [{emi}]")

    legacy_nodes = {k: v for k, v in NODE_REGISTRY.items() if "lambda_shape" not in v}
    if legacy_nodes:
        print(f"\n[Legacy Nodes (Decorated)]")
        for fqn, meta in legacy_nodes.items():
            req = ", ".join(meta["contract"].requires)
            emi = ", ".join(meta["contract"].emits)
            print(f"  - {fqn}: [{req}] ➔ [{emi}]")