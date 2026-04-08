# topos.code.inspect
import os
import ast
import importlib.util
import sys
from pathlib import Path
from typing import List, Dict, Any

class ProjectMapper:
    """프로젝트의 위상과 경로를 관리합니다."""
    @staticmethod
    def setup_workspace(target_path: Path):
        """임포트가 가능하도록 최상위 경로를 sys.path에 등록합니다."""
        base_dir = str(target_path if target_path.is_dir() else target_path.parent)
        if base_dir not in sys.path:
            sys.path.insert(0, base_dir)
        return base_dir

    @staticmethod
    def collect_python_files(target_path: Path) -> List[Path]:
        """분석 대상 파일들을 수집합니다."""
        if target_path.is_file():
            return [target_path]
        return list(target_path.rglob("*.py"))

class ImportLens:
    """AST를 통해 파일 내부의 임포트 좌표를 추출합니다."""
    @staticmethod
    def get_targets(file_path: Path) -> List[Dict[str, str]]:
        targets = []
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=file_path.name)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    for alias in node.names:
                        targets.append({
                            "module": node.module,
                            "target_name": alias.name
                        })
        except Exception:
            pass
        return targets

class IntegrityChecker:
    """모듈이 실제로 로드 가능한지(Rupture) 확인합니다."""
    @staticmethod
    def check_loadable(module_name: str) -> bool:
        try:
            spec = importlib.util.find_spec(module_name)
            return spec is not None
        except:
            return False