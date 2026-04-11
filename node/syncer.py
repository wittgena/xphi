# node.syncer
import argparse
import sys
import subprocess
from typing import List, Callable, Any
from pathlib import Path
from plane.emitter import get_emitter
from anchor.resolver import find_current_self, get_invoker
from node.repo.scanner import NodeScanner, NodeCommit
from contract.registry import cli_contract
from bridge.proto.repo import RepoNode, AnchorNode, anchor_commit_protocol, git_commit_runner
from contract.executor.cli import execute_cli_task, CliTaskAdapter, parse_local, dispatch_cli

log = get_emitter("node.syncer", mode="SLIM")

def git_commit_runner(path: Path, message: str, apply: bool) -> str:
    """@role: Physical State Finalizer - 실제 Git 저장소의 상태를 확정하고 결과 해시를 반환"""
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

class NodeSyncer:
    """Coordinate physical runners with logical nodes based on injected protocols"""
    
    def __init__(self, apply: bool, runner: Callable, protocol: Callable, **protocol_kwargs):
        """
        :param apply: 실제 실행 여부
        :param runner: 노드에서 물리적 작업을 수행할 함수 (ex: git_commit_runner)
        :param protocol: 노드들을 조율할 프로토콜 함수 (ex: align_commit_protocol)
        :param protocol_kwargs: 프로토콜 실행에 필요한 추가 인자들 (ex: message="...")
        """
        self.apply = apply
        self.runner = runner
        self.protocol = protocol
        self.protocol_kwargs = protocol_kwargs
        
        try:
            self.root = find_current_self()
        except Exception as e:
            log.crit(f"failed to resolve self root: {e}")
            sys.exit(1)

    def run(self):
        log.info(f"## execution mode: {'APPLY' if self.apply else 'DRY-RUN'}")
        scanner = NodeScanner(self.root)
        found_nodes: List[NodeCommit] = scanner.scan()

        if not found_nodes:
            log.warning("no physical nodes discovered")
            return

        nodes: List[RepoNode] = [
            RepoNode(name=node.name, path=str(node.path), runner=self.runner) 
            for node in found_nodes if node.path.resolve() != self.root.resolve()
        ]
        
        anchor = AnchorNode(name="self", path=str(self.root), runner=self.runner)
        log.info(f"initiating protocol for {len(nodes)} nodes under anchor: {anchor.name}")
        self.protocol(repos=nodes, anchor=anchor, apply=self.apply, **self.protocol_kwargs)

def entry_task(args):
    parser = argparse.ArgumentParser(description="Era-based Alignment Orchestrator")
    parser.add_argument("-m", "--message", required=True, help="Commit message")
    parser.add_argument("--apply", action="store_true", help="Actually execute state closure")
    parsed_args = parser.parse_args(args)

    syncer = NodeSyncer(
        apply=parsed_args.apply,
        runner=git_commit_runner,
        protocol=anchor_commit_protocol,
        message=parsed_args.message
    )
    return CliTaskAdapter(syncer.run)

@cli_contract(name="node.syncer", recept=[])
def main():
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli("node.syncer", entry_task, __file__)

if __name__ == "__main__":
    main()