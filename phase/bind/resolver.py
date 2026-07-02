# phase.bind.resolver
import os
import json
import re
import argparse
from pathlib import Path
from functools import lru_cache

ANCHOR_DIR = "self"
BOUND = "bound.json"

def is_self_root(path: Path) -> bool:
    """Verify if the path is the self root - checks if the directory name is 'self' and contains an 'anchor' subdirectory."""
    return path.name == ANCHOR_DIR and (path / "anchor").is_dir()

def find_current_self(start: Path | None = None) -> Path:
    """Find the nearest 'self' root - does not perform cascading searches."""
    if start is None:
        start = Path.cwd()

    start = start.resolve()
    for parent in [start] + list(start.parents):
        if is_self_root(parent):
            return parent

    raise RuntimeError(f"No self root found from {start}")

@lru_cache(maxsize=1)
def resolve_identity(start: Path | None = None) -> tuple[int, int]:
    """
    @desc: Parse and return the system's unique topological identifiers (Manifold, Vertex).
    @returns: (manifold_id, vertex_id)
    """
    self_root = find_current_self(start)
    bound = load_bound(self_root)
    identity = bound.get("identity", {})
    manifold_id = identity.get("manifold_id", 1) & 0x1F
    vertex_id = identity.get("vertex_id", 1) & 0x1F
    
    return manifold_id, vertex_id

def get_invoker(path: Path):
    try:
        rel = path.relative_to(find_current_self()).with_suffix("")
        parts = rel.parts[1:] if rel.parts and rel.parts[0] == "self" else rel.parts
        invoker = ".".join(parts)
        command = ".".join(parts[-2:])
    except Exception as e: 
        print(f"## get_invoker fail: {e}")
        return "", ""
    return invoker, command

def load_bound(self_root: Path) -> dict:
    """Load self/anchor/bound.json - includes minimal validation."""
    bound = self_root / "anchor" / BOUND 
    if not bound.exists():
        return {}

    try:
        data = json.loads(bound.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise RuntimeError("bound.json must be a JSON object")
        return data
    except Exception as e:
        raise RuntimeError(f"Invalid bound in {bound}: {e}")

def _clean_subpath(root_name: str, sub_path_str: str) -> str:
    """Prevent duplication of the root directory name in subpaths."""
    pure_path = Path(sub_path_str)
    if pure_path.parts and pure_path.parts[0] == root_name:
        return os.path.join(*pure_path.parts[1:]) if len(pure_path.parts) > 1 else "."
    return sub_path_str

def _track_io_usage(name: str, target_path: Path):
    """Runtime IO tracking hook"""
    try:
        from arch.contract.registry.path import path_registry
        ## Unify method name to log_access
        path_registry.log_access(name, target_path)
    except ImportError:
        pass

@lru_cache(maxsize=32)
def resolve_path(name: str, start: Path | None = None) -> Path:
    ## Normalize the 'start' argument to improve lru_cache hit rate
    effective_start = start.resolve() if start else Path.cwd().resolve()
    self_root = find_current_self(effective_start)
    
    clean_name = _clean_subpath(self_root.name, name)
    direct_candidate = (self_root / clean_name).resolve()
    if direct_candidate.exists() and direct_candidate.is_dir():
        return direct_candidate

    bound = load_bound(self_root)
    paths = bound.get("paths", {})
    
    if name in paths:
        raw_mapped = paths[name]
        
        ## Fully normalize the substitution dictionary into Path objects based on self_root
        substitutions = {
            "self": self_root,
            "anchor": self_root / "anchor"
        }
        
        ## Merge substitution from bound.json and align with self_root
        for prefix_key, rel_path in bound.get("substitution", {}).items():
            safe_rel_path = rel_path.lstrip("/") if isinstance(rel_path, str) else str(rel_path)
            substitutions[prefix_key] = (self_root / safe_rel_path).resolve()
        
        ## Modified regex: Optionally match the slash (/) and sub-path
        ## @ex: ":io:/log" -> prefix='io', sub_path='log'
        ## @ex: ":io:" -> prefix='io', sub_path=''
        match = re.match(r"^:([^:]+):(?:/(.*))?$", raw_mapped)
        if match:
            prefix = match.group(1)
            sub_path = match.group(2) or ""
            if prefix in substitutions:
                target_path = (substitutions[prefix] / sub_path).resolve()
            else:
                raise RuntimeError(f"Unknown substitution prefix ':{prefix}:' in paths mapping '{name}: {raw_mapped}'")
        else:
            ## Handling case of a normal path mapping without a prefix (e.g., "brane")
            mapped_subpath = _clean_subpath(self_root.name, raw_mapped)
            target_path = (self_root / mapped_subpath).resolve()
    else:
        target_path = direct_candidate

    target_path.mkdir(parents=True, exist_ok=True)
    _track_io_usage(name, target_path)
    return target_path

def resolve_channel(name: str, start: Path | None = None) -> str:
    """
    @phase: Redis channel resolver
    @order:
    - anchor mapping
    - namespace validation
    - return original
    """
    self_root = find_current_self(start)
    bound = load_bound(self_root)

    channels = bound.get("channels", {})
    anchor = channels.get("anchor", {})
    namespaces = channels.get("namespaces", [])
    if not namespaces:
        raise RuntimeError("channels.namespaces not defined in bound.json")

    ## anchor override
    if name in anchor:
        return anchor[name]

    ## namespace validation
    if ":" not in name:
        raise RuntimeError(
            f"Invalid channel '{name}'. Channel must follow '<namespace>:<name>'"
        )

    prefix = name.split(":")[0]
    if prefix not in namespaces:
        raise RuntimeError(
            f"Channel namespace '{prefix}' not allowed. "
            f"Allowed: {', '.join(namespaces)}"
        )
    return name

def resolve_pattern(start: Path | None = None) -> str:
    """Return the Redis pattern that xphi should subscribe to."""
    self_root = find_current_self(start)
    bound = load_bound(self_root)
    channels = bound.get("channels", {})
    xphi = channels.get("xphi", {})
    pattern = xphi.get("pattern")
    if not pattern:
        raise RuntimeError("xphi pattern not defined in bound.json")

    return pattern

def around(base_dir: Path, max_depth: int = 2) -> dict:
    """
    @flow: Φ(base) → ∂Φ(local scan) → Φ_git_map{}
    @desc: Returns the privilege state of each Repo, rather than just a simple list.
    """
    found_repos = {}
    def _search(current_path: Path, current_depth: int):
        ## @phase: bounded recursion (local ∂Φ)
        if current_depth > max_depth:
            return

        ## @detect: Φ_git emergence
        if (current_path / '.git').exists():
            repo_name = current_path.name
            ## Grant privileges if the repository name is included in CORE_REPOS
            is_core = repo_name in CORE_REPOS
            found_repos[repo_name] = {
                "path": str(current_path),
                "is_core": is_core,
                "allow_side_effects": is_core
            }

        try:
            for child in current_path.iterdir():
                ## @filter: exclude non-relevant Φ
                if child.is_dir():
                    if not child.name.startswith('.') and child.name not in ('node_modules', 'venv', '__pycache__', 'build'):
                        _search(child, current_depth + 1)
        except PermissionError:
            pass

    _search(base_dir, 0)
    return found_repos

def run_around():
    try:
        self_root = find_current_self()
    except RuntimeError as e:
        print(f"[Error] {e}")
        return

    base_dir = self_root.parent 
    json_path = self_root / "anchor" / BOUND
    if not json_path.exists():
        print(f"[Error] 파일을 찾을 수 없습니다: {json_path}")
        return

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError:
        print(f"[Error] JSON 형식이 올바르지 않습니다: {json_path}")
        return

    ## @flow: Φ → ∂Φ → Φ_local
    repos = around(base_dir, max_depth=2)
    data['around'] = repos

    updated_json = json.dumps(data, indent=2, ensure_ascii=False)
    print(updated_json)
    with open(json_path, 'w', encoding='utf-8') as f:
        f.write(updated_json + '\n')

def run_test():
    """@flow: test logic integration"""
    try:
        print("## bound RESOLVER TEST")
        self_root = find_current_self(Path("."))
        print(f"[SELF ROOT] {self_root}")

        bound = load_bound(self_root)
        if bound:
            print("\n[BOUND FOUND]")
            print(json.dumps(bound, indent=2))
        else:
            print("\n[NO BOUND FOUND]")

        print("\n## @path.resolution.test")
        paths = bound.get("paths", {})
        for name in paths.keys():
            try:
                resolved = resolve_path(name)
                print(f" ✔ {name} → {resolved}")
            except Exception as e:
                print(f" ✘ {name} → ERROR: {e}")

        print("\n## @channel.resolution.test")
        channels = bound.get("channels", {})
        anchor = channels.get("anchor", {})

        tested = set()
        for name in anchor.keys():
            tested.add(name)

        for name in tested:
            try:
                resolved = resolve_channel(name)
                print(f" ✔ {name} -> {resolved}")
            except Exception as e:
                print(f" ✘ {name} -> ERROR: {e}")

        namespace_tests = [
            "psi:test_signal",
            "delta:new_branch",
            "execution:done",
            "xor:score",
            "loop:trigger"
        ]

        for name in namespace_tests:
            try:
                resolved = resolve_channel(name)
                print(f" ✔ {name} → {resolved}")
            except Exception as e:
                print(f" ✘ {name} → ERROR: {e}")

        invalid_tests = ["unknown:test", "badformat", "psi"]
        print("\n## @invalid.channel.test")
        for name in invalid_tests:
            try:
                resolved = resolve_channel(name)
                print(f" ✘ {name} → SHOULD FAIL but returned {resolved}")
            except Exception as e:
                print(f" ✔ {name} → correctly rejected ({e})")

        print("\n## @xphi.pattern")
        pattern = resolve_pattern()
        print(f"xphi pattern -> {pattern}")

        test_channels = [
            "psi:intensity",
            "delta:generated",
            "execution:completed",
            "xor:similarity_score",
            "loop:stabilized",
            "xphi:heartbeat"
        ]
        compiled = re.compile(pattern)
        for ch in test_channels:
            match = bool(compiled.match(ch))
            print(f"   {ch} → {'MATCH' if match else 'NO MATCH'}")

        print("\n## TEST COMPLETE")
    except Exception as e:
        print(f"[FATAL ERROR] {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Anchor Resolver CLI")
    parser.add_argument("--around", action="store_true", help="Run around script to update repository bounds")
    parser.add_argument("--test", action="store_true", help="Run manual verification tests")
    
    args = parser.parse_args()
    if args.around:
        run_around()
    elif args.test:
        run_test()
    else:
        parser.print_help()