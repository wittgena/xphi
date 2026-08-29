# xphi.arch.contract.registry.unified
import sys
from types import SimpleNamespace
from typing import Any, Dict, List, Type, Callable, Mapping, FrozenSet, Optional
from collections import defaultdict
from dataclasses import dataclass

class UnifiedRegistry:
    """단일 진실 공급원 (SSOT) 레지스트리 - Ator 대통합 구조"""
    def __init__(self):
        # 파편화된 카테고리를 삭제하고 단일 물리적 저장소로 통합 (Primary Key: name)
        self._ators: Dict[str, Type] = {}
        self._daemons: Dict[str, Type] = {}
        self._cli_tasks: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    @property
    def registered_daemons(self) -> Mapping[str, Type]:
        return self._daemons

    @property
    def registered_cli_tasks(self) -> Mapping[str, List[Dict[str, Any]]]:
        return self._cli_tasks

    def __len__(self) -> int:
        return len(self._ators)

    def register_component(self, name: str, cls: Type, role: str = "ator"):
        target_map = self._daemons if role == "daemon" else self._ators
        name_key = name.lower()

        # 1. 핫 리로드 시 동일 클래스명 덮어쓰기 허용 (Idempotency 보장)
        if name_key in target_map and target_map[name_key].__name__ == cls.__name__:
            target_map[name_key] = cls
            return
            
        # 2. 클래스 메타데이터에 역할(role) 주입 (향후 런타임 검증용)
        cls.__ator_role__ = role
        
        # 4. 통합 풀에 고유 이름(name)으로 저장
        target_map[name_key] = cls
        log_role = role.upper()
        print(f"[Registry] {log_role} Registered: {name} -> {cls.__name__}")

    def create_component(self, config: Any, **extra_kwargs):
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