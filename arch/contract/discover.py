# arch.contract.discover
## @lineage: topos.contract.discover
## @lineage: phase.runtime.contract.discover
import sys
import importlib
import ast
from pathlib import Path
from typing import Optional, Set

def _contains_forbidden_imports(py_file: Path, forbidden_libs: Set[str]) -> bool:
    """@internal: 모듈을 실제 import 하기 전에 AST(추상구문트리)를 분석하여 특정 외부 패키지를 참조하는지 정적으로 검사"""
    if not forbidden_libs:
        return False
        
    try:
        with open(py_file, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=py_file.name)
            
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root_module = alias.name.split('.')[0]
                    if root_module in forbidden_libs:
                        return True
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    root_module = node.module.split('.')[0]
                    if root_module in forbidden_libs:
                        return True
    except Exception as e:
        print(f"[Discover] AST Parsing failed for {py_file.name}: {e}")
        return False

    return False

def discover_modules(
    root: Path, 
    forbidden_libs: Optional[Set[str]] = None,
    exclude_files: Optional[Set[str]] = None,
    force_reload: bool = False
) -> None:
    """
    고정된 root를 기준으로 모듈을 탐색하여 일관된 FQN(Fully Qualified Name)을 생성
    :param root: 탐색을 시작할 최상위 디렉토리 경로
    :param exclude_files: 로드에서 명시적으로 제외할 파일명 목록
    """
    if not root.exists():
        print(f"[Discover] Root path {root} does not exist.")
        return

    print(f"[Discover] Start module discovery from: {root}")
    
    ## 필터링 정책 초기화 (의존성 주입이 없으면 기본값 사용)
    forbidden_set = forbidden_libs or {"openhands", "dspy"}
    exclude_set = exclude_files or {"registry.py", "scanner.py", "discover.py"}

    ## Base Path 설정
    root_path_str = str(root.resolve())
    if root_path_str not in sys.path:
        sys.path.insert(0, root_path_str)
        print(f"[Discover] Base path added: {root_path_str}")

    ## 재귀적 모듈 탐색 및 안전한 로드
    for py_file in root.rglob("*.py"):
        ## @rule.A: Private 모듈 및 명시적 제외 파일 패스
        if (py_file.name.startswith("_") and py_file.name != "__init__.py") or \
           py_file.name in exclude_set:
            continue

        ## @rule.B: AST 정적 분석을 통한 무거운/예제 모듈 사전 차단 (Zero Side-effect)
        if _contains_forbidden_imports(py_file, forbidden_set):
            print(f"[Discover] Ignored (heavy dependency found): {py_file.name}")
            continue

        # ## @rule.C: 모듈 동적 로딩 및 핫 리로딩(Hot Reloading) 지원
        # try:
        #     relative = py_file.relative_to(root)
        #     module_path = ".".join(relative.with_suffix("").parts)
        #     if module_path:
        #         if module_path in sys.modules:
        #             # 이미 캐시된 위상(모듈)인 경우, 파일의 수정 사항을 반영하기 위해 강제 리로드
        #             importlib.reload(sys.modules[module_path])
        #         else:
        #             # 최초 진입하는 위상인 경우 정상 로드
        #             importlib.import_module(module_path)
        
        ## @rule.C: 모듈 동적 로딩 및 핫 리로딩(Hot Reloading) 지원
        try:
            relative = py_file.relative_to(root)
            module_path = ".".join(relative.with_suffix("").parts)
            if module_path:
                if module_path in sys.modules:
                    # 플래그가 켜져 있을 때만 명시적 리로드 수행
                    if force_reload:
                        importlib.reload(sys.modules[module_path])
                    else:
                        # 이미 로드된 모듈은 안전하게 패스 (멱등성 보장)
                        pass 
                else:
                    # 최초 진입하는 위상인 경우 정상 로드
                    importlib.import_module(module_path)
        except Exception as e:
            print(f"[Discover] Failed to load {py_file}: {e}")