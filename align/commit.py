# align.commit
import argparse
import sys
import subprocess
from typing import List
from pathlib import Path
from plane.emitter import get_emitter
from anchor.resolver import find_current_self, get_invoker
from topos.repo.node import RepoNode, AnchorNode, align_commit_protocol
from topos.repo.scanner import RepoScanner, GitRepo
from bridge.executor.cli import execute_cli_task, CliTaskAdapter

log = get_emitter("commit.aligner", mode="SLIM")

def git_commit_runner(path: Path, message: str, apply: bool) -> str:
    """
    @role: Physical State Finalizer
    실제 Git 저장소의 상태를 확정하고 결과 해시를 반환함
    """
    if not apply:
        return "dry-run-id"
        
    try:
        # 1. 변화 감지 (Dirty check)
        status = subprocess.run(
            ["git", "status", "--porcelain"], 
            cwd=path, capture_output=True, text=True
        ).stdout.strip()
        
        if not status:
            # 변경 사항이 없으면 현재 HEAD 반환
            res = subprocess.run(
                ["git", "rev-parse", "HEAD"], 
                cwd=path, capture_output=True, text=True
            )
            return res.stdout.strip()

        # 2. 물리적 집행 (Add & Commit)
        subprocess.run(["git", "add", "-A"], cwd=path, check=True)
        subprocess.run(["git", "commit", "-m", message], cwd=path, check=True)
        
        # 3. 결과 관측 (New Hash)
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"], 
            cwd=path, capture_output=True, text=True
        )
        return res.stdout.strip()
        
    except Exception as e:
        log.error(f"Git execution failed at {path}: {e}")
        return "0000000"

class CommitAligner:
    """
    @topos: Coordinate physical runners with logical nodes
    """
    def __init__(self, apply: bool, message: str):
        self.apply = apply
        self.message = message
        try:
            self.root = find_current_self()
        except Exception as e:
            log.crit(f"failed to resolve self root: {e}")
            sys.exit(1)

    def run(self):
        log.info(f"## execution mode: {'APPLY' if self.apply else 'DRY-RUN'}")
        scanner = RepoScanner(self.root)
        found_repos: List[GitRepo] = scanner.scan()

        if not found_repos:
            log.warning("no physical repositories discovered")
            return

        nodes: List[RepoNode] = [
            RepoNode(name=repo.name, path=str(repo.path), runner=git_commit_runner) 
            for repo in found_repos if repo.path.resolve() != self.root.resolve()
        ]
        
        anchor = AnchorNode(name="self", path=str(self.root), runner=git_commit_runner)
        log.info(f"initiating protocol for {len(nodes)} nodes under anchor: {anchor.name}")
        align_commit_protocol(
            repos=nodes, 
            anchor=anchor, 
            message=self.message, 
            apply=self.apply
        )

def main():
    parser = argparse.ArgumentParser(description="Era-based Alignment Orchestrator")
    parser.add_argument("-m", "--message", required=True, help="Commit message")
    parser.add_argument("--apply", action="store_true", help="Actually execute state closure")
    args = parser.parse_args()

    aligner = CommitAligner(apply=args.apply, message=args.message)
    adapted_task = CliTaskAdapter(aligner.run)
    invoker, command = get_invoker(Path(__file__))
    payload = {
        "_context": {
            "invoker": str(invoker), 
            "command": command, 
            "cli_args": sys.argv[1:]
        }
    }
    execute_cli_task(task_instance=adapted_task, command_name=command, payload=payload)

if __name__ == "__main__":
    main()