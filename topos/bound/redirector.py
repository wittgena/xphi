# arch.bound.redirector
import sys
import importlib.util
from pathlib import Path
from typing import Optional, Union

class ModuleRedirector:
    def __init__(self, target_package: str, local_dir: Union[str, Path], clear_cache: bool = True):
        self.target_package = target_package
        self.local_dir = Path(local_dir).resolve()
        self.clear_cache = clear_cache
        self._is_installed = False

    def find_spec(self, fullname, path, target=None):
        ## 대상 패키지 또는 그 하위 패키지인지 확인
        if fullname == self.target_package or fullname.startswith(f"{self.target_package}."):
            ## 상대 경로 계산 (예: openhands.agent_server.sub -> sub)
            rel_path = fullname[len(self.target_package):].lstrip(".").replace(".", "/")
            target_path = self.local_dir / rel_path

            ## 패키지 형태 (__init__.py 존재) 확인
            if target_path.is_dir():
                init_file = target_path / "__init__.py"
                if init_file.exists():
                    return importlib.util.spec_from_file_location(
                        fullname,
                        str(init_file),
                        submodule_search_locations=[str(target_path)]
                    )
            
            ## 단일 파일 형태 (.py 존재) 확인
            py_file = target_path.with_suffix(".py")
            if py_file.exists():
                return importlib.util.spec_from_file_location(fullname, str(py_file))

        return None

    def install(self):
        """커스텀 파인더를 sys.meta_path의 최우선 순위로 등록"""
        if self._is_installed:
            return

        if self.clear_cache:
            self._clear_sys_modules()

        sys.meta_path.insert(0, self)
        self._is_installed = True
        print(f"[Redirector] '{self.target_package}' -> '{self.local_dir}' mapping installed.")

    def uninstall(self):
        """등록된 커스텀 파인더를 제거"""
        if self in sys.meta_path:
            sys.meta_path.remove(self)
        
        if self.clear_cache:
            self._clear_sys_modules()
            
        self._is_installed = False
        print(f"[Redirector] '{self.target_package}' mapping uninstalled.")

    def _clear_sys_modules(self):
        """기존에 로드된 캐시 모듈을 삭제하여 재로드를 강제"""
        keys_to_del = [
            key for key in sys.modules.keys() 
            if key == self.target_package or key.startswith(f"{self.target_package}.")
        ]
        for key in keys_to_del:
            del sys.modules[key]

    ## 컨텍스트 매니저 지원 (with 문 사용 가능)
    def __enter__(self):
        self.install()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.uninstall()

def main():
    LOCAL_PATH = Path("meta/local")

    with ModuleRedirector("openhands.agent_server", LOCAL_PATH):
        import openhands.agent_server.core as core
        print(f"Loaded from: {core.__file__}")

if __name__ == "__main__":
    main()