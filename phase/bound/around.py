# phase.bound.around
## @lineage: around
import os
import sys
import shutil
import json
from pathlib import Path
import site
import logging
import subprocess

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger("around")

CURRENT_SCRIPT = Path(__file__).absolute()
CURRENT_DIR = CURRENT_SCRIPT.parent
SELF_ROOT = Path.cwd()
PTH_FILENAME = "self.around.pth"

def ignore_hidden(dir, files):
    """@helper: 숨김 파일 및 디렉토리를 복사 대상에서 제외"""
    return [f for f in files if f.startswith('.')]

def replicate_and_relaunch() -> None:
    if os.getenv("PYTH_REPLICATED") == "1":
        return

    ## 논리적 경로: 심볼릭 링크를 추적하지 않은 '실행 시점'의 경로
    logical_script = Path(__file__).absolute()
    logical_dir = logical_script.parent
    
    ## 물리적 경로: 심볼릭 링크를 추적한 원본 파일의 경로 (필요시 참고용)
    ## physical_dir = Path(__file__).resolve().parent 

    if logical_dir.name == "anchor":   
        ## [Case A] self/meta/anchor/around.py로 실행된 경우 -> 복사 실행
        if logical_dir.parent.name == "meta":
            self_dir = logical_dir.parent.parent
            dst_dir = self_dir / "anchor"
            
            log.info(f"[Phase: Copy] Replicating: {logical_dir} -> {dst_dir}")
            try:
                ## symlinks=True는 심볼릭 링크 자체를 복사
                shutil.copytree(logical_dir, dst_dir, dirs_exist_ok=True, ignore=ignore_hidden, symlinks=True)
            except Exception as e:
                log.error(f"[Error] Unexpected error during copy: {e}")
                sys.exit(1)

            log.info(f"[Phase: Relaunch] Executing `pyth.py` from {dst_dir}...\n")
            os.environ["PYTH_REPLICATED"] = "1"
            new_script = dst_dir / logical_script.name
            os.execvp(sys.executable, [sys.executable, str(new_script)] + sys.argv[1:])
            
        ## [Case B] self/anchor/around.py 로 실행된 경우 (symlink 포함) -> 복사 생략
        else:
            log.info("[Phase: Skip Copy] Executed from 'anchor' directly (or via symlink).")
            pass

def discover_repos(base_dir: Path, max_depth: int = 1) -> list[Path]:
    """@flow: 주변 디렉토리를 스캔하여 Git 레포지토리를 탐색"""
    found = []
    exclude = {'.git', 'node_modules', 'venv', '__pycache__', 'build', '.idea', '.vscode'}
    def _scan(current: Path, depth: int):
        if depth > max_depth:
            return
        try:
            for child in current.iterdir():
                if child.is_dir() and child.name not in exclude:
                    ## .git이 있거나 특정 기준을 만족하면 레포로 간주
                    if (child / '.git').exists():
                        found.append(child)
                    else:
                        _scan(child, depth + 1)
        except PermissionError:
            pass

    _scan(base_dir, 0)
    return sorted(list(set(found)))

def update_bound_config(repos: list[Path]) -> None:
    """@task: anchor/bound.json 파일이 있고 'around' 키가 존재할 경우 repo 경로 업데이트"""
    bound_path = SELF_ROOT / "anchor" / "bound.json"
    if not bound_path.exists():
        return

    try:
        with open(bound_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        if "around" in config:
            config["around"] = [str(p.resolve()) for p in repos]
            with open(bound_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            log.info(f"[Phase: Sync] Updated 'around' paths in: {bound_path}")
        else:
            log.info(f"[Phase: Sync] Skip: 'around' key not found in {bound_path}")

    except (json.JSONDecodeError, Exception) as e:
        log.warning(f"[Warning] Failed to update bound.json: {e}")

def project_self() -> list[Path]:
    """@install: 동적으로 탐색된 경로를 site-packages에 투영하고 발견된 repos 리스트 반환"""
    try:
        log.info(f"[Phase: Discovery] Scanning around: {SELF_ROOT}")
        repos = discover_repos(SELF_ROOT, max_depth=2)
        if not repos:
            log.error("[Error] No valid repositories found.")
            return []
        
        update_bound_config(repos)
        pth_content = "\n".join(str(p.resolve()) for p in repos)

        ## .pth 파일 쓰기 - site.getsitepackages()가 여러 개일 수 있으므로 첫 번째(보통 시스템/유저 메인) 사용
        sp_paths = site.getsitepackages()
        site_packages = Path(sp_paths[0])
        pth_path = site_packages / PTH_FILENAME
        pth_path.write_text(pth_content, encoding="utf-8")

        log.info(f"[Phase: Bootstrap] {len(repos)} topos projected to: {pth_path}")
        for r in repos:
            log.info(f"  + {r.name}")
        return repos # 찾은 경로 리스트 반환
    except PermissionError:
        log.error("[Error] Permission denied: Run with elevated privileges (sudo/admin).")
        sys.exit(1)
    except Exception as e:
        log.error(f"[Error] Bootstrap failed: {e}")
        raise

def verify_projection(repos: list[Path]) -> None:
    log.info("\n[Phase: Verification] Projected sys.path (filtered):")
    
    resolved_paths = [str(p.resolve()) for p in repos]
    script = f"""
import sys
valid_starts = {resolved_paths + [str(SELF_ROOT)]}
for p in sys.path:
    if any(p.startswith(v) for v in valid_starts):
        print(p)
"""
    subprocess.run([sys.executable, "-c", script], check=True)

if __name__ == "__main__":
    ## 복사 및 재실행 판단 (meta/anchor -> anchor)
    replicate_and_relaunch()
    
    ## 복사된 위치(또는 원래 위치)에서 본래 작업 수행
    found_repos = project_self()
    if found_repos:
        verify_projection(found_repos)