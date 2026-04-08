# align.path
import os
import sys
import argparse
import yaml
from typing import List, Dict, Any
from pathlib import Path
from plane.log import get_logger
from anchor.resolver import find_current_self, get_invoker
from bridge.executor.cli import execute_cli_task, CliTaskAdapter
from flow.surface.aligner import FieldAligner, AlignRecord

log = get_logger("align.path")

try:
    SELF_ROOT = find_current_self()
except Exception as e:
    log.error(f"[error] 기준면(.self)을 찾을 수 없음: {e}")
    sys.exit(1)

TARGET_EXTENSIONS = {".md", ".py"}
EXCLUDED_DIRS = {"__pycache__"}

## @grouping.axes
def key_path(item: AlignRecord) -> str:
    namespace = item["namespace"]
    parts = namespace.split(".")
    return namespace if len(parts) <= 1 else ".".join(parts[:-1])

def key_status(item: AlignRecord) -> str:
    return item.get("status", "unknown")

def key_type(item: AlignRecord) -> str:
    return "missing" if item.get("missing") else "mismatch"

GROUP_KEYS = {
    "path": key_path,
    "status": key_status,
    "type": key_type,
}

class PathAligner(FieldAligner):
    def scan(self, only_dirs: List[str] | None = None):
        root_dir = Path(self.root_dir)
        mismatches: List[AlignRecord] = []
        matched_count = 0
        mismatched_count = 0

        log.info(f"[scan] start (root={root_dir})")
        try:
            repo_dirs = [
                d
                for d in root_dir.iterdir()
                if d.is_dir()
                and not d.name.startswith(".")
                and d.name not in EXCLUDED_DIRS
                and (only_dirs is None or d.name in only_dirs)
            ]
        except FileNotFoundError:
            log.error(f"[error] 루트 디렉토리 '{root_dir}'를 찾을 수 없습니다.")
            return [], 0, 0

        for repo_dir in repo_dirs:
            log.info(f"[scan] repo={repo_dir.name}")
            for file_path in repo_dir.rglob("*"):
                if (
                    not file_path.is_file()
                    or file_path.suffix not in TARGET_EXTENSIONS
                    or file_path.name.startswith(".")
                    or any(p in EXCLUDED_DIRS for p in file_path.parts)
                ):
                    continue

                try:
                    with file_path.open("r", encoding="utf-8") as f:
                        lines = f.readlines()

                    first_line = lines[0].strip() if lines else ""
                    relative_path = file_path.relative_to(repo_dir)
                    expected = (
                        relative_path.with_suffix("")
                        .as_posix()
                        .replace("/", ".")
                    )
                    namespace = (
                        file_path.relative_to(root_dir)
                        .with_suffix("")
                        .as_posix()
                        .replace("/", ".")
                    )

                    if not first_line.startswith("# "):
                        log.warning(f"[missing] {namespace} (expected={expected})")
                        mismatched_count += 1
                        mismatches.append(
                            {
                                "file_path": str(file_path),
                                "namespace": namespace,
                                "expected": expected,
                                "actual": "",
                                "missing": True,
                            }
                        )
                        continue
                    
                    actual = first_line[2:]
                    if actual != expected:
                        log.warning(f"[mismatch] {namespace} (actual={actual}, expected={expected})")
                        mismatched_count += 1
                        mismatches.append(
                            {
                                "file_path": str(file_path),
                                "namespace": namespace,
                                "expected": expected,
                                "actual": actual,
                                "missing": False,
                            }
                        )
                    else:
                        matched_count += 1

                except Exception as e:
                    log.error(f"[error] {file_path}: {e}")

        log.info(f"[scan] done matched={matched_count}, mismatched={mismatched_count}")
        return mismatches, matched_count, mismatched_count

    def align(self, mismatches: List[AlignRecord], prefixes: List[str]):
        log.info(f"[align] start (total={len(mismatches)})")

        results = []
        for item in mismatches:
            namespace = item["namespace"]
            fixable = any(namespace.startswith(p) for p in prefixes)
            status = "mismatch"
            if fixable:
                try:
                    path = Path(item["file_path"])
                    with path.open("r+", encoding="utf-8") as f:
                        lines = f.readlines()
                        if item.get("missing"):
                            lines.insert(0, f"# {item['expected']}\n")
                        else:
                            lines[0] = f"# {item['expected']}\n"

                        f.seek(0)
                        f.writelines(lines)
                        f.truncate()

                    status = "fixed"
                except Exception as e:
                    status = "fail"
                    item["error"] = str(e)

            item["status"] = status
            results.append(item)

        log.info(f"[align] done")
        return results

def normalize_prefix(prefix: str) -> str:
    return prefix.strip().replace("/", ".").lstrip(".")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", action="append")
    parser.add_argument(
        "--group-by",
        default="path",
        choices=["path", "status", "type"],
    )
    args = parser.parse_args()
    prefixes = [normalize_prefix(p) for p in (args.dir or [])]
    only_dirs = args.dir or None

    aligner = PathAligner(SELF_ROOT)
    run_kwargs = {
        "axis": args.group_by,
        "group_keys": GROUP_KEYS,
        "scan_kwargs": {"only_dirs": only_dirs},
        "fix_kwargs": {"prefixes": prefixes},
    }
    
    adapted_task = CliTaskAdapter(aligner.run, **run_kwargs)
    invoker, command = get_invoker(Path(__file__))
    payload={"_context": {"invoker": str(invoker), "command": command, "cli_args": sys.argv[1:]}}
    execute_cli_task(task_instance=adapted_task, command_name="align.path", payload=payload)

if __name__ == "__main__":
    main()