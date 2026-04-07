# contract.scanner
from dataclasses import dataclass
from typing import Dict, Any, Optional, Set, runtime_checkable

@dataclass
class ExecutionPlan:
    node_name: str
    node_class: Type
    inputs: Dict[str, Any]
    contract: ContractSpec

class ContractScanner:
    def __init__(self, initial_surface: Dict[str, Any], registry: UnifiedRegistry):
        self.surface = dict(initial_surface)
        self.registry = registry
        self.executed: Set[str] = set()

    def step(self) -> Optional[ExecutionPlan]:
        surface_keys = set(self.surface.keys())
        
        for name, meta in self.registry.nodes.items():
            if name in self.executed:
                continue
                
            # 요구 조건이 Surface에 모두 만족되는지 확인
            if meta.contract.requires.issubset(surface_keys):
                plan = ExecutionPlan(
                    node_name=name,
                    node_class=meta.node_class,
                    inputs={k: self.surface[k] for k in meta.contract.requires},
                    contract=meta.contract
                )
                self.executed.add(name)
                return plan
                
        return None # 더 이상 활성화될 노드가 없음 (Attractor 도달)