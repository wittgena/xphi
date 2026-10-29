# xphi.node.space.bind.around
import os
import sys
import copy
import shutil
import json
import site
import logging
import subprocess
import importlib.util
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger("around")

CURRENT_SCRIPT = Path(__file__).absolute()
CURRENT_DIR = CURRENT_SCRIPT.parent
PTH_FILENAME = "xphi.pth"
BOUND = "bound.json"
CORES = ["fiber", "xphi"]

## Default minimum skeleton to prevent resolver crashes if bound.json doesn't exist
DEFAULT_BOUND_SKELETON = {
    "name": "bound",
    "version": "1.5.1",
    "identity": {
        "manifold_id": 1,
        "vertex_id": 5
    },
    "substitution": {
        "io": "anchor/io",
        "memory": "anchor/memory",
        "workspace": "anchor/ext/workspace",
        "xor": "anchor/io/xor",
        "contract": "anchor/io/contract",
        "node": "xphi/node",
        "phase": "fiber/phase"
    },
    "paths": {
        "theoria": "theoria",
        "abc": ":phase:/abc",
        "fiber": "fiber",
        "io": ":io:",
        "ext": ":anchor:/ext",
        "chat": ":io:/chat",
        "memory": ":anchor:/memory",
        "adb": ":anchor:/adb",
        "sign": ":anchor:/sign",
        "log": ":io:/log",
        "ailog": ":io:/log/ailog",
        "fixture": ":io:/fixture",
        "sandbox": ":workspace:",
        "origin": ":node:/space/bind/origin",
        "time": ":node:/space/time",
        "flaretime": ":node:/space/time/flare",
        "surface": ":io:/surface",
        "contract": ":contract:",
        "spec": ":contract:/spec",
        "scheme": ":contract:/scheme",
        "xor": ":xor:",
        "lib": ":contract:/lib",
        "workspace": ":workspace:",
        "template": ":workspace:/template",
        "code": ":xor:/code",
        "res": ":io:/res"
    },
    "channels": {
        "namespaces": [
            "theoria", "brane", "nexus", "xphi", "ext", "surgent", "psi", "delta", "xor", "watcher"
        ],
        "xphi": {
            "pattern": "^(theoria|psi|delta|meta|xphi|ion|ex|xe|xor|loop|field|watcher):"
        }
    },
    "quarantine": ["venv", "env", "node_modules", "dev", "ex", "tests"]
}


def ignore_hidden(dir, files):
    """@helper: Exclude hidden files and directories from the copy target."""
    return [f for f in files if f.startswith('.')]


def determine_execution_mode() -> str:
    if any(part in ["site-packages", "dist-packages"] for part in CURRENT_SCRIPT.parts):
        return "USER"
    return "DEV"


def resolve_workspace_root() -> Path:
    current = CURRENT_DIR
    while current.parent != current:
        if current.name in CORES:
            return current.parent
        current = current.parent
    
    # Fallback to current working directory if core marker is not found
    return Path.cwd()


def replicate_and_relaunch(workspace_root: Path, anchor_dir: Path) -> None:
    """@flow: Replicate the script to the root anchor directory and relaunch (DEV Mode Only)"""
    if os.getenv("PYTH_REPLICATED") == "1":
        return

    target_dir = anchor_dir / CURRENT_DIR.name
    
    # Skip if already running from the replicated target path
    if CURRENT_SCRIPT.is_relative_to(anchor_dir):
        log.info("[Phase: Skip Copy] Executed from target 'anchor' directly.")
        return

    log.info(f"[Phase: Copy] Replicating: {CURRENT_DIR} -> {target_dir}")
    try:
        shutil.copytree(CURRENT_DIR, target_dir, dirs_exist_ok=True, ignore=ignore_hidden, symlinks=True)
    except Exception as e:
        log.error(f"[Error] Unexpected error during copy: {e}")
        sys.exit(1)

    log.info(f"[Phase: Relaunch] Executing relaunched script from {target_dir}...\n")
    os.environ["PYTH_REPLICATED"] = "1"
    new_script = target_dir / CURRENT_SCRIPT.name
    os.execvp(sys.executable, [sys.executable, str(new_script)] + sys.argv[1:])


def discover_repos(base_dir: Path, max_depth: int = 1) -> list[Path]:
    """@flow: Scan surrounding directories to discover local Git repositories (DEV Mode)."""
    found = []
    exclude = {'.git', 'node_modules', 'venv', '__pycache__', 'build', '.idea', '.vscode'}
    def _scan(current: Path, depth: int):
        if depth > max_depth:
            return
        try:
            for child in current.iterdir():
                if child.is_dir() and child.name not in exclude:
                    if (child / '.git').exists() or (child / 'pyproject.toml').exists():
                        found.append(child)
                    else:
                        _scan(child, depth + 1)
        except PermissionError:
            pass

    _scan(base_dir, 0)
    return sorted(list(set(found)))


def discover_user_repos() -> list[Path]:
    """@flow: Resolve installed package paths using importlib metadata (USER Mode)."""
    found = []
    for core in CORES:
        spec = importlib.util.find_spec(core)
        if spec and spec.submodule_search_locations:
            pkg_path = Path(spec.submodule_search_locations[0])
            found.append(pkg_path)
    return sorted(list(set(found)))


def update_bound_config(anchor_dir: Path, repos: list[Path]) -> None:
    """
    @task: Update the bound.json file dynamically.
    @desc: Ensures skeleton integrity (self-healing) and updates the 'around' topology with privilege info.
    """
    bound_path = anchor_dir / BOUND
    anchor_dir.mkdir(parents=True, exist_ok=True)
    
    # Start with a clean deep copy of the default skeleton to guarantee all required keys ('adb', etc.) exist
    config = copy.deepcopy(DEFAULT_BOUND_SKELETON)

    if bound_path.exists():
        try:
            with open(bound_path, 'r', encoding='utf-8') as f:
                existing = json.load(f)
            if isinstance(existing, dict):
                for k, v in existing.items():
                    if isinstance(v, dict) and isinstance(config.get(k), dict):
                        config[k].update(v)
                    else:
                        config[k] = v
        except json.JSONDecodeError:
            log.warning(f"[Warning] Failed to parse {bound_path}, overwriting with default skeleton.")
    else:
        log.info(f"[Phase: Sync] {BOUND} not found. Bootstrapping with default skeleton.")

    new_around_map = {}
    for p in repos:
        repo_name = p.name
        is_core = repo_name in CORES
        
        # Persist data in a dictionary structure
        new_around_map[repo_name] = {
            "path": str(p.resolve()),
            "is_core": is_core,
            "allow_side_effects": is_core  # Core repositories are exempt from AST side-effect checks
        }
        
    config["around"] = new_around_map
    
    try:
        with open(bound_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        log.info(f"[Phase: Sync] Updated 'around' topology (Dict Mode) in: {bound_path}")
    except Exception as e:
        log.warning(f"[Warning] Failed to update {BOUND}: {e}")


def project_dev_mode(workspace_root: Path, anchor_dir: Path) -> list[Path]:
    """@install: Discover local repos, project paths into site-packages, and bind (DEV Mode)."""
    log.info(f"[Phase: Discovery] Scanning workspace around: {workspace_root} (DEV Mode)")
    repos = discover_repos(workspace_root, max_depth=2)
    
    if not repos:
        log.error("[Error] No valid local repositories found.")
        return []
    
    update_bound_config(anchor_dir, repos)
    
    # Write .pth file (based on path list)
    pth_content = "\n".join(str(p.resolve()) for p in repos)
    sp_paths = site.getsitepackages()
    site_packages = Path(sp_paths[0])
    pth_path = site_packages / PTH_FILENAME
    
    try:
        pth_path.write_text(pth_content, encoding="utf-8")
        log.info(f"[Phase: Bootstrap] {len(repos)} topos projected to: {pth_path}")
    except PermissionError:
        log.error("[Error] Permission denied: Run with elevated privileges (sudo/admin) to link .pth file.")
        sys.exit(1)

    for r in repos:
        is_core = "*" if r.name in CORES else " "
        log.info(f"  + [{is_core}] {r.name}")
        
    return repos


def project_user_mode(anchor_dir: Path | None = None) -> list[Path]:
    """@install: Discover installed packages and setup user-local workspace (USER Mode)."""
    if anchor_dir is None:
        anchor_dir = Path.home() / ".anchor"

    log.info("[Phase: Discovery] Resolving installed packages (USER Mode)")
    repos = discover_user_repos()
    
    if not repos:
        log.warning("[Warning] No core packages found in the python environment.")
    
    update_bound_config(anchor_dir, repos)
    log.info(f"[Phase: Bootstrap] User environment configured at: {anchor_dir}")
    
    for r in repos:
        is_core = "*" if r.name in CORES else " "
        log.info(f"  + [{is_core}] {r.name}")
        
    return repos


def verify_projection(repos: list[Path], root_context: Path) -> None:
    """@helper: Verify if the projected paths exist within sys.path."""
    log.info("\n[Phase: Verification] Projected sys.path (filtered):")
    
    resolved_paths = [str(p.resolve()) for p in repos]
    script = f"""
import sys
valid_starts = {resolved_paths + [str(root_context)]}
for p in sys.path:
    if any(p.startswith(v) for v in valid_starts):
        print(p)
"""
    subprocess.run([sys.executable, "-c", script], check=True)

if __name__ == "__main__":
    MODE = determine_execution_mode()
    
    if MODE == "DEV":
        # Local source environment workflow
        WORKSPACE_ROOT = resolve_workspace_root()
        ANCHOR_DIR = WORKSPACE_ROOT / "anchor"
        
        replicate_and_relaunch(WORKSPACE_ROOT, ANCHOR_DIR)
        found_repos = project_dev_mode(WORKSPACE_ROOT, ANCHOR_DIR)
        if found_repos:
            verify_projection(found_repos, WORKSPACE_ROOT)
    else:
        # Installed package user environment workflow (Always lock to ~/.anchor)
        USER_ROOT = Path.home()
        ANCHOR_DIR = USER_ROOT / ".anchor"
        
        found_repos = project_user_mode(ANCHOR_DIR)
        if found_repos:
            verify_projection(found_repos, USER_ROOT)