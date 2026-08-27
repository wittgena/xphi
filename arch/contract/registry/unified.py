# xphi.arch.contract.registry.unified
## @lineage: arch.contract.registry.unified
"""
@module: arch.contract.registry.unified
@desc: Single Source of Truth (SSOT) Registry supporting Runtime Plasticity.
"""
import sys
from types import SimpleNamespace
from typing import Any, Dict, List, Type, Callable, Mapping, FrozenSet, Optional
from collections import defaultdict
from dataclasses import dataclass

try:
    from xphi.arch.contract.protocol import Proto
except ImportError:
    Proto = Any

# =========================================================================
# CORE METADATA STRUCTURES
# =========================================================================
@dataclass(frozen=True)
class ContractSpec:
    requires: FrozenSet[str] = frozenset()
    emits: FrozenSet[str] = frozenset()
    proto: Optional[Proto] = None

@dataclass(frozen=True)
class NodeMeta:
    node_class: Type
    contract: ContractSpec


# =========================================================================
# UNIFIED REGISTRY
# =========================================================================
class UnifiedRegistry:
    """단일 진실 공급원 (SSOT) 레지스트리 - Ator 대통합 구조"""
    def __init__(self):
        # 파편화된 카테고리를 삭제하고 단일 물리적 저장소로 통합 (Primary Key: name)
        self._ators: Dict[str, Type] = {}
        self._daemons: Dict[str, Type] = {}
        self._cli_tasks: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    @property
    def registered_nodes(self) -> Mapping[str, NodeMeta]:
        """
        하위 호환성 및 메타데이터 조회를 위한 프로퍼티.
        내부적으로는 메모리 최적화를 위해 Type을 직접 관리하지만, 
        외부 순회 시에는 NodeMeta 껍데기로 감싸서 제공하여 에러를 방지합니다.
        """
        wrapped_nodes = {}
        for name, cls in self._ators.items():
            contract_spec = getattr(cls, "__manifold_contract__", ContractSpec())
            wrapped_nodes[name] = NodeMeta(node_class=cls, contract=contract_spec)
        return wrapped_nodes

    @property
    def registered_daemons(self) -> Mapping[str, Type]:
        return self._daemons

    @property
    def registered_cli_tasks(self) -> Mapping[str, List[Dict[str, Any]]]:
        return self._cli_tasks

    def __len__(self) -> int:
        return len(self._ators)

    def register_component(self, name: str, cls: Type, role: str = "ator"):
        """
        - name: 컴포넌트의 고유 식별자 (ex: "sensor.kuramoto")
        - role: 시스템 내에서의 논리적 역할 (ex: "kernel", "watcher", "regime")
        """
        target_map = self._daemons if role == "daemon" else self._ators
        name_key = name.lower()

        # 1. 핫 리로드 시 동일 클래스명 덮어쓰기 허용 (Idempotency 보장)
        if name_key in target_map and target_map[name_key].__name__ == cls.__name__:
            target_map[name_key] = cls
            return
            
        # 2. 클래스 메타데이터에 역할(role) 주입 (향후 런타임 검증용)
        cls.__ator_role__ = role
        
        # 3. 빈 ContractSpec을 주입하여 하위 호환성을 보장
        if not hasattr(cls, "__manifold_contract__"):
            cls.__manifold_contract__ = ContractSpec()
        
        # 4. 통합 풀에 고유 이름(name)으로 저장
        target_map[name_key] = cls
        log_role = role.upper()
        print(f"[Registry] {log_role} Registered: {name} -> {cls.__name__}")

    def create_component(self, config: Any, **extra_kwargs):
        """
        만능 팩토리. 
        config 내부의 'name' 또는 'type'(레거시) 필드를 추출하여 탐색합니다.
        """
        if isinstance(config, dict):
            # JSON에 'name'이 명시되어 있으면 우선 사용, 없으면 레거시 'type' 사용
            target_name = config.get('name', config.get('type', '')).lower()
            extra_kwargs = {**config.get('params', {}), **extra_kwargs}
        else:
            target_name = getattr(config, 'name', getattr(config, 'type', '')).lower()
            
        # _ators 풀을 먼저 찾고, 없으면 _daemons 풀에서 탐색 (O(1) 룩업)
        node_class = self._ators.get(target_name) or self._daemons.get(target_name)
        
        if not node_class:
            raise ValueError(f"[Registry] Component name '{target_name}' not found in registry.")

        try:
            return node_class(**extra_kwargs)
        except TypeError as e:
            raise TypeError(f"[Registry] Signature mismatch initializing {node_class.__name__}: {e}")

# 싱글톤 인스턴스
registry = UnifiedRegistry()

# =========================================================================
# UNIVERSAL DECORATORS
# =========================================================================
def ator_contract(name: str, role: str = "ator"):
    """
    모든 시스템 컴포넌트를 수용하는 범용 데코레이터
    사용 예: @contract.ator("topos.watcher", role="watcher")
    """
    def decorator(cls: Type): 
        registry.register_component(name, cls, role=role)
        return cls
    return decorator

def daemon_contract(name: str):
    """백그라운드 무한 루프 워커용 데코레이터"""
    def decorator(cls: Type): 
        registry.register_component(name, cls, role="daemon")
        return cls
    return decorator

def cli_contract(name: str, task_type: str = "cli", **kwargs):
    def decorator(func: Callable):
        registry._cli_tasks[name].append({"module_fqn": func.__module__, "type": task_type, **kwargs})
        return func
    return decorator

# 통합 Export 네임스페이스
contract = SimpleNamespace(
    cli=cli_contract,
    ator=ator_contract,
    daemon=daemon_contract
)