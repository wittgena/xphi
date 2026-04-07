# contract.folder
"""
@flow: Proto(Metadata) + Contract(Execution) -> UnifiedRegistry -> ContractFolder
"""
import sys
import importlib
from pathlib import Path
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple, Callable, Optional
from plane.emitter import get_logger
from contract.proto.col import Proto, proto, get_proto
from contract.registry import (
    PhaseNode, ContractSpec, NodeMeta, 
    UnifiedRegistry, registry as unified_registry
)

try:
    from anchor.resolver import find_current_self, resolve_path
except ImportError:
    def find_current_self(): return Path.cwd()
    def resolve_path(p): return Path.cwd() / p

log = get_logger('contract.folder')

def fold_contract(*, requires: List[str], emits: List[str]):
    """
    기존 Phase contract 데코레이터를 유지하되, 새로운 UnifiedRegistry로 연결하는 어댑터 역할
    """
    def decorator(cls: type):
        if not issubclass(cls, PhaseNode):
            raise TypeError(f"'{cls.__name__}' must satisfy PhaseNode protocol.")
            
        p_meta = get_proto(cls)
        fqn = f"{cls.__module__}.{cls.__qualname__}"
        
        # UnifiedRegistry 규격에 맞춰 등록
        unified_registry.register_node(
            name=fqn,
            cls=cls,
            requires=requires,
            emits=emits,
            proto=p_meta
        )
        return cls
    return decorator

@fold_contract(requires=["raw_data", "timestamp"], emits=["parsed_event"])
@proto(Proto(sequence=("Φ_in", "extract", "Φ_out"), kind="sensor"))
class Watcher:
    def align(self, surface: Dict[str, Any]):
        self.data = surface["raw_data"]
    def transform(self):
        self.result = f"Event parsed: {self.data}"
    def project(self):
        return {"parsed_event": self.result}

@fold_contract(requires=["parsed_event"], emits=["action_plan"])
@proto(Proto(sequence=("parsed_event", "evaluate", "plan"), kind="logic"))
class Launcher:
    def align(self, surface: Dict[str, Any]):
        self.event = surface["parsed_event"]
    def transform(self):
        self.plan = f"Action ready for {self.event}"
    def project(self):
        return {"action_plan": self.plan}

def discover_modules(root: Path):
    """파일 시스템 탐색 및 자동 임포트 (기존 로직 유지)"""
    print(f"[Scanner] recursive scan → {root}")
    root_parent = root.parent
    if str(root_parent) not in sys.path:
        sys.path.insert(0, str(root_parent))
        
    for py_file in root.rglob("*.py"):
        if py_file.name.startswith("_"):
            continue
        relative = py_file.relative_to(root_parent)
        module_path = str(relative.with_suffix("")).replace("/", ".").replace("\\", ".")
        try:
            importlib.import_module(module_path)
            print(f"  └── Loaded: {module_path}")
        except Exception as e:
            log.error(f"[module load fail] {module_path} : {e}")

class FoldFlow:
    """Runtime Engine Topology"""
    def __init__(self, target_registry: UnifiedRegistry):
        self.registry = target_registry
        self.producer_index = defaultdict(list)
        
        for name, meta in self.registry.nodes.items():
            for e in meta.contract.emits:
                self.producer_index[e].append(name)

    def possible_nodes(self, surface_keys: Set[str]) -> List[str]:
        candidates = []
        for name, meta in self.registry.nodes.items():
            if meta.contract.requires.issubset(surface_keys):
                candidates.append(name)
        return candidates

class ContractFolder:
    def __init__(self, initial_surface: Dict[str, Any], target_registry: UnifiedRegistry = unified_registry):
        self.surface = dict(initial_surface)
        self.graph = FoldFlow(target_registry)
        self.executed: Set[str] = set()

    def step(self) -> bool:
        surface_keys = set(self.surface.keys())
        candidates = self.graph.possible_nodes(surface_keys)
        
        for node_name in candidates:
            if node_name in self.executed:
                continue
                
            meta = self.graph.registry.nodes[node_name]
            node = meta.node_class()
            
            ## 메타데이터 로깅 (proto 활용)
            p_kind = meta.contract.proto.kind if meta.contract.proto else "generic"
            print(f"[SurfaceLoop] activate <{p_kind}> node → {node_name}")
            
            # [유지] 점진적 리팩토링을 위해 기존의 동기 실행 로직을 그대로 둡니다.
            node.align(self.surface)
            node.transform()
            output = node.project()
            
            self.surface.update(output)
            print(f"  └── [Surface] update → {list(output.keys())}")
            
            self.executed.add(node_name)
            return True
            
        return False

    def run(self):
        print("\n## FoldFlow Start")
        while self.step():
            pass
        print("\n## Complete")
        print("Final Surface:", self.surface)

if __name__ == "__main__":
    try:
        SELF_ROOT = find_current_self()
        for repo in ['ion', 'phase', 'tixe']:
            discover_modules(SELF_ROOT / repo)
    except Exception as e:
        print(f"[Skip] 동적 탐색 건너뜀: {e}")

    print("\n[Registry] Discovered Nodes & Protos:")
    for name, meta in unified_registry.nodes.items():
        req_str = ", ".join(meta.contract.requires)
        emit_str = ", ".join(meta.contract.emits)
        proto_info = f" | proto={meta.contract.proto.sequence}" if meta.contract.proto else ""
        print(f"  - {name}: requires=[{req_str}] -> emits=[{emit_str}]{proto_info}")
    
    initial_surface = {"raw_data": "SYSTEM_BOOT", "timestamp": "2026-03-27"}
    folder = ContractFolder(initial_surface)
    folder.run()