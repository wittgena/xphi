# arch.contract.registry
import sys
import importlib
from types import SimpleNamespace
from typing import Any, Dict, List, Set, Type, Protocol, runtime_checkable, FrozenSet, Callable, Optional, Mapping
from pathlib import Path
from dataclasses import dataclass
from collections import defaultdict
from arch.contract.protocol import Proto

@runtime_checkable
class PhaseNode(Protocol):
    def align(self, surface: Dict[str, Any]) -> None: ...
    def transform(self) -> None: ...
    def project(self) -> Dict[str, Any]: ...

@dataclass(frozen=True)
class ContractSpec:
    requires: FrozenSet[str]
    emits: FrozenSet[str]
    proto: Optional[Proto] = None

@dataclass(frozen=True)
class NodeMeta:
    node_class: Type[PhaseNode]
    contract: ContractSpec

class UnifiedRegistry:
    """단일 진실 공급원 (SSOT) 레지스트리"""
    def __init__(self):
        self._nodes: Dict[str, NodeMeta] = {}
        self._cli_tasks: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._kernels: Dict[str, Type] = {} 
        self._fields: Dict[str, Type] = {}
        self._watchers: Dict[str, Type] = {}
        self._regimes: Dict[str, Type] = {}
        self._ators: Dict[str, Type] = {}

    def __len__(self) -> int:
        """등록된 위상 노드(PhaseNode)의 개수를 반환"""
        return len(self._nodes)

    @property
    def registered_nodes(self) -> Mapping[str, NodeMeta]:
        return self._nodes

    @property
    def registered_cli_tasks(self) -> Mapping[str, List[Dict[str, Any]]]:
        return self._cli_tasks

    def register_node(self, name: str, cls: Type[PhaseNode], requires: list, emits: list, proto: Optional[Proto] = None):
        contract = ContractSpec(
            requires=frozenset(requires), emits=frozenset(emits), proto=proto
        )
        cls.__manifold_contract__ = contract
        self._nodes[name] = NodeMeta(node_class=cls, contract=contract)
    
    def register_cli_task(self, name: str, module_fqn: str, args: Optional[List[str]] = None, 
        tags: Optional[List[str]] = None, entry: str = "", recept: Optional[List[str]] = None):
        self._cli_tasks[name].append({
            "module_fqn": module_fqn, 
            "args": args or [],
            "tags": tags or [],
            "entry": entry,
            "recept": recept or []
        })

    def register_component(self, category: str, name: str, cls: Type):
        target = getattr(self, f"_{category}s")
        target[name.lower()] = cls
        print(f"[Registry] Component Registered: [{category}] {name} -> {cls.__name__}")

    def create_component(self, category: str, config: Any, **extra_kwargs):
        """범용 컴포넌트 팩토리"""
        # 1. 'type' 추출 (Dict/Object 공통 처리)
        if isinstance(config, dict):
            c_type = config.get('type', '').lower()
            # 딕셔너리일 경우 params를 기본적으로 extra_kwargs에 병합
            params = config.get('params', {})
            extra_kwargs = {**params, **extra_kwargs}
        else:
            c_type = getattr(config, 'type', '').lower()
            
        target_map = getattr(self, f"_{category}s")
        
        if c_type not in target_map:
            raise ValueError(f"[Registry] Unknown {category}: {c_type}")

        node_class = target_map[c_type]
        try:
            return node_class(**extra_kwargs)
        except TypeError as e:
            ## 주입된 kwargs와 클래스의 __init__ 시그니처가 맞지 않을 때 명확한 에러 제공
            raise TypeError(f"[Registry] Failed to init {node_class.__name__} due to signature mismatch: {e}")

registry = UnifiedRegistry()

def manifold_node(name: str, *, requires: List[str] = None, emits: List[str] = None):
    requires = requires or []
    emits = emits or []
    def decorator(cls: Type):
        proto_meta = getattr(cls, "__proto_meta__", None) 
        registry.register_node(name, cls, requires, emits, proto_meta)
        return cls
    return decorator

def cli_contract(name: str, args: List[str] = None, tags: List[str] = None, entry: str = "entry_task", recept: List[str] = None):
    def decorator(func: Callable):
        registry.register_cli_task(name, func.__module__, args, tags, entry, recept)
        return func
    return decorator

def kernel_contract(name: str):
    def decorator(cls: Type): registry.register_component("kernel", name, cls); return cls
    return decorator

def field_contract(name: str):
    def decorator(cls: Type): registry.register_component("field", name, cls); return cls
    return decorator

def watcher_contract(name: str):
    def decorator(cls: Type): registry.register_component("watcher", name, cls); return cls
    return decorator

def regime_contract(name: str):
    def decorator(cls: Type): registry.register_component("regime", name, cls); return cls
    return decorator

def ator_contract(name: str):
    def decorator(cls: Type): registry.register_component("ator", name, cls); return cls
    return decorator

contract = SimpleNamespace(
    cli=cli_contract,
    kernel=kernel_contract,
    field=field_contract,
    watcher=watcher_contract,
    regime=regime_contract,
    ator=ator_contract,
    node=manifold_node
)