# phase.hub.swarm.messenger
## @lineage: hub.swarm.messenger
## @lineage: nexus.swarm.manager.messenger
## @lineage: nexus.manager.messenger
## @lineage: nexus.messenger.alone
## @lineage: messenger.alone
from __future__ import annotations
import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Optional, Union, Literal
import tyro
from watcher.plane.emitter import get_emitter
from phase.bind.resolver import find_current_self, resolve_path
from arch.contract.exp.atomic import sha256_file, read_json, atomic_write_text, atomic_write_json, now_iso, now_compact

log = get_emitter("messenger.alone")

MESSENGER_SCHEMA_VERSION = 1
TRANSCRIPT_EXCLUDED: frozenset[str] = frozenset({"transcript.json"})

ROOT = find_current_self()
TEMPLATE_ROOT = resolve_path("template")

## Survival Kit (최후의 보루: 하드코딩 폴백)
FALLBACK_REQUIREMENTS = """\
torch>=2.0
unsloth
transformers
trl
peft
datasets
accelerate
bitsandbytes
safetensors
pyyaml
tyro
"""

FALLBACK_RUN_SH = """\
#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

echo "=== ribos run ==="
if [[ "${SKIP_INSTALL:-0}" != "1" ]]; then
    pip install -q -r requirements.txt
fi

OUT_DIR="${OUT_DIR:-$HERE/_output}"
python ribos.py run --packet "$HERE" --out "$OUT_DIR"

echo "=== Done. Adapter at: $OUT_DIR ==="
"""

def resolve_template(cli_path: Optional[Path], filename: str, fallback_content: str) -> str:
    """우선순위: 1. CLI 명시 지정 -> 2. 외부 템플릿 폴더 -> 3. 내장 폴백"""
    # 1. CLI 파라미터로 커스텀 파일이 들어온 경우
    if cli_path and cli_path.exists():
        log.info(f"Using CLI provided {filename}")
        return cli_path.read_text(encoding="utf-8")
    
    # 2. 시스템 템플릿 디렉토리에서 파일을 찾는 경우 (가장 이상적인 깔끔한 구조)
    system_template = TEMPLATE_ROOT / filename
    if system_template.exists():
        log.info(f"Using system template for {filename}")
        return system_template.read_text(encoding="utf-8")
        
    # 3. 둘 다 없으면 스크립트 내부의 생존 배낭(Fallback) 사용
    log.warning(f"Template {filename} not found externally. Using embedded fallback.")
    return fallback_content

# ============================================================================
# 2. Transport Strategies (전송 계층 캡슐화)
# ============================================================================
class TransportStrategy:
    def send(self, packet_dir: Path, dest: str) -> None: raise NotImplementedError
    def fetch(self, src: str, inbox: Path) -> Path: raise NotImplementedError

class LocalTransport(TransportStrategy):
    def send(self, packet_dir: Path, dest: str) -> None:
        dest_dir = Path(dest)
        dest_dir.mkdir(parents=True, exist_ok=True)
        target = dest_dir / packet_dir.name
        shutil.copytree(packet_dir, target, symlinks=False)
        log.info(f"sent (local) → {target}")

    def fetch(self, src: str, inbox: Path) -> Path:
        src_dir = Path(src)
        inbox.mkdir(parents=True, exist_ok=True)
        target = inbox / src_dir.name
        if target.exists():
            target = inbox / f"{src_dir.name}.{int(time.time())}"
        shutil.copytree(src_dir, target, symlinks=False)
        log.info(f"fetched (local) → {target}")
        return target

class RsyncTransport(TransportStrategy):
    def __init__(self, extra_args: list[str]):
        self.extra_args = extra_args

    def send(self, packet_dir: Path, dest: str) -> None:
        target = f"{dest.rstrip('/')}/{packet_dir.name}/"
        cmd = ["rsync", "-av", "--progress"] + self.extra_args + [f"{packet_dir.as_posix()}/", target]
        subprocess.run(cmd, check=True)
        log.info(f"rsync sent → {target}")

    def fetch(self, src: str, inbox: Path) -> Path:
        src_basename = src.rstrip("/").rsplit("/", 1)[-1]
        target = inbox / src_basename
        cmd = ["rsync", "-av", "--progress"] + self.extra_args + [f"{src.rstrip('/')}/", f"{target.as_posix()}/"]
        subprocess.run(cmd, check=True)
        return target

class ShellTransport(TransportStrategy):
    def __init__(self, cmd_template: str, expect_name: Optional[str] = None):
        self.cmd_template = cmd_template
        self.expect_name = expect_name

    def send(self, packet_dir: Path, dest: str) -> None:
        cmd_str = self.cmd_template.format(
            src=shlex.quote(str(packet_dir.resolve())),
            name=shlex.quote(packet_dir.name),
            dest=shlex.quote(dest)
        )
        subprocess.run(cmd_str, shell=True, check=True)

    def fetch(self, src: str, inbox: Path) -> Path:
        cmd_str = self.cmd_template.format(
            src=shlex.quote(src),
            dest=shlex.quote(str(inbox.resolve())),
            name=shlex.quote(self.expect_name or "")
        )
        subprocess.run(cmd_str, shell=True, check=True)
        children = [p for p in inbox.iterdir() if p.is_dir()]
        return max(children, key=lambda p: p.stat().st_mtime)

# ============================================================================
# 3. Commands (비즈니스 로직)
# ============================================================================
@dataclass
class PackCommand:
    """assemble a packet (called by nexus.transcribe)"""
    corpus: Path
    config: Path
    ribos: Path
    out: Path
    nexus_version: str
    messenger_version: str
    ribos_version: str
    schema_version: int
    eval_set: Optional[Path] = None
    prev_adapter: Optional[Path] = None
    requirements: Optional[Path] = None
    run_sh: Optional[Path] = None
    notes: Optional[str] = None
    tarball: bool = False

    def execute(self):
        corpus_sha = sha256_file(self.corpus)
        packet_id = f"{now_compact()}-{corpus_sha[:6]}"
        packet_dir = self.out / f"packet-{packet_id}"
        packet_dir.mkdir(parents=True)

        try:
            # 1. 핵심 파일 복사
            shutil.copy2(self.ribos, packet_dir / "ribos.py")
            shutil.copy2(self.corpus, packet_dir / "corpus.jsonl")
            shutil.copy2(self.config, packet_dir / self.config.name)

            if self.eval_set and self.eval_set.exists():
                shutil.copy2(self.eval_set, packet_dir / "eval_holdout.jsonl")
            
            if self.prev_adapter:
                shutil.copytree(self.prev_adapter, packet_dir / "prev_adapter", symlinks=False)

            # 2. 템플릿 폴백 시스템 가동 (우선순위 자동 판별)
            req_content = resolve_template(self.requirements, "requirements.txt", FALLBACK_REQUIREMENTS)
            run_content = resolve_template(self.run_sh, "run.sh", FALLBACK_RUN_SH)

            atomic_write_text(packet_dir / "requirements.txt", req_content)
            atomic_write_text(packet_dir / "run.sh", run_content, mode=0o755)
            os.chmod(packet_dir / "run.sh", 0o755)

            # 3. 매니페스트 (Transcript) 작성
            transcript = {
                "packet_id": packet_id,
                "created_at": now_iso(),
                "messenger_schema": MESSENGER_SCHEMA_VERSION,
                "schema_version": self.schema_version,
                "nexus_version": self.nexus_version,
                "messenger_version": self.messenger_version,
                "ribos_version": self.ribos_version,
                "notes": self.notes,
            }
            atomic_write_json(packet_dir / "transcript.json", transcript)

        except Exception:
            shutil.rmtree(packet_dir, ignore_errors=True)
            raise

        if self.tarball:
            tar_path = packet_dir.with_suffix(".tar.gz")
            with tarfile.open(tar_path, "w:gz") as tar:
                tar.add(packet_dir, arcname=packet_dir.name)

        log.info(f"packed → {packet_dir}")
        print(json.dumps({"packet": str(packet_dir)}, indent=2))

@dataclass
class TransportConfig:
    """전송 설정 명세 (Send / Fetch 공유)"""
    method: Literal["local", "rsync", "cmd"] = "local"
    rsync_args: list[str] = field(default_factory=list)
    cmd_template: Optional[str] = None
    
    def get_strategy(self, expect_name: Optional[str] = None) -> TransportStrategy:
        if self.method == "rsync":
            return RsyncTransport(self.rsync_args)
        elif self.method == "cmd":
            if not self.cmd_template: raise ValueError("cmd_template required")
            return ShellTransport(self.cmd_template, expect_name)
        return LocalTransport()

@dataclass
class SendCommand:
    """transport a packet to the cloud"""
    packet: Path
    dest: str
    transport: TransportConfig = field(default_factory=TransportConfig)

    def execute(self):
        strategy = self.transport.get_strategy()
        strategy.send(self.packet, self.dest)
        print(json.dumps({"sent": self.packet.name}, indent=2))

@dataclass
class FetchCommand:
    """retrieve a returned adapter into ./inbox/"""
    src: str
    inbox: Path = Path("./inbox")
    transport: TransportConfig = field(default_factory=TransportConfig)
    expect_name: Optional[str] = None

    def execute(self):
        strategy = self.transport.get_strategy(self.expect_name)
        target = strategy.fetch(self.src, self.inbox)
        print(json.dumps({"fetched": str(target)}, indent=2))

# ============================================================================
# 4. Entrypoint
# ============================================================================
MessengerApp = Union[
    Annotated[PackCommand, tyro.conf.subcommand("pack")],
    Annotated[SendCommand, tyro.conf.subcommand("send")],
    Annotated[FetchCommand, tyro.conf.subcommand("fetch")],
]

def main(argv: Optional[list[str]] = None) -> None:
    try:
        command = tyro.cli(MessengerApp, args=argv)
        command.execute()
    except Exception as e:
        log.error(f"messenger error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()