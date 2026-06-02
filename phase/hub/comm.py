# meta.gov.comm.hub
import json
import sys
import time
import shutil
from dataclasses import dataclass, field
from typing import Annotated, Union, Optional
from pathlib import Path
import tyro
from phase.bind.resolver import find_current_self, resolve_path
from watcher.plane.emitter import get_emitter
from arch.contract.exp.atomic import parse_iso
from arch.contract.registry.static import static_registry
from watcher.gateway.check.lineage import LineageManager
from watcher.gateway.check.iso import (
    SystemVersion, RibosValidator, NEXUS_OUTPUT_SCHEMA,
    receive_adapter, transcribe, self_check
)
from phase.hub.swarm.harvester import ResidueHarvester

log = get_emitter('comm.hub')

ROOT = find_current_self()
IO_ROOT = resolve_path("io")
TEMPLATE_ROOT = resolve_path("template")

PATHS = {
    "nexus_script": Path(__file__).resolve(),
    "messenger_script": ROOT / "messenger.py",
    "ribos_canon": TEMPLATE_ROOT / "ribos.py",
    "ribos_backup": TEMPLATE_ROOT / ".backup",
    "io_state": IO_ROOT / "state",
    "harvest_cursor": IO_ROOT / "state" / "harvest_cursor.json",
    "corpus_dir": IO_ROOT / "export",
    "eval_set": IO_ROOT / "eval" / "holdout.jsonl",
    "configs": IO_ROOT / "configs",
    "quarantine": IO_ROOT / "quarantine",
    "adapters": ROOT / "adapters",
    "latest_link": ROOT / "adapters" / "latest",
    "packets": ROOT / "packets",
    "inbox": ROOT / "inbox",
}

def get_system_version() -> SystemVersion:
    """런타임에 파일 해시를 읽어 시스템 버전 객체를 생성하는 팩토리 함수"""
    return SystemVersion.load(
        nexus_script=PATHS["nexus_script"],
        messenger_script=PATHS["messenger_script"],
        ribos_canon=PATHS["ribos_canon"]
    )

def bootstrap_environment():
    """임포트 타임 에러를 막기 위해, 런타임에 데코레이터 역할을 대신하여 계약을 동적 등록합니다."""
    static_registry.register_contract(
        name="ribos_canon",
        path=PATHS["ribos_canon"],
        requires=["def main", "def run_genesis"],
        schema_symbol="ribos_OUTPUT_SCHEMA",
        expected_schema=NEXUS_OUTPUT_SCHEMA,
        backup_dir=PATHS["ribos_backup"],
        validator_cls=RibosValidator
    )

# ==========================================
# 2. Command Configs (명령어 명세 및 실행 제어)
# ==========================================

@dataclass
class HarvestCommand:
    """extract residue from local store"""
    # default_factory를 사용해 런타임에 경로를 안전하게 할당
    store: Path = field(default_factory=lambda: PATHS["io_state"].parent / "metadata" / "native_storage")
    out: Optional[Path] = None
    min_reward: float = 0.5
    max_per_prompt: int = 3
    full: bool = False

    def execute(self):
        h = ResidueHarvester(store_path=self.store, cursor_path=PATHS["harvest_cursor"])
        out_path = self.out or PATHS["corpus_dir"] / f"corpus-{int(time.time())}.jsonl"
        manifest = h.harvest(
            out_path=out_path,
            min_reward=self.min_reward,
            since_cursor=not self.full,
            max_per_prompt=self.max_per_prompt,
        )
        print(json.dumps({"out": str(out_path), **manifest}, indent=2, ensure_ascii=False))


@dataclass
class TranscribeCommand:
    """build an messenger packet for the ribos"""
    corpus: Path
    config: Path
    out: Optional[Path] = None
    prev_adapter: Optional[Path] = None

    def execute(self):
        # 의존성 주입(DI): 모델 내부에서 경로를 찾지 않도록 CLI가 모두 주입
        packet = transcribe(
            corpus_path=self.corpus,
            config_path=self.config,
            out_dir=self.out or PATHS["packets"],
            sys_version=get_system_version(),
            messenger_script=PATHS["messenger_script"],
            ribos_canon=PATHS["ribos_canon"],
            prev_adapter=self.prev_adapter,
        )
        print(json.dumps({"packet": str(packet)}, indent=2))


@dataclass
class ReceiveCommand:
    """ingest a freshly-trained adapter directory"""
    path: Path
    auto_promote: Optional[float] = None

    def execute(self):
        lineage = LineageManager(PATHS["adapters"])
        # 의존성 주입(DI)
        report = receive_adapter(
            incoming_path=self.path,
            lineage=lineage,
            corpus_dir=PATHS["corpus_dir"],
            quarantine_dir=PATHS["quarantine"],
            sys_version=get_system_version(),
            promote_threshold=self.auto_promote,
        )
        print(json.dumps({
            "gen_id": report.gen_id, "ok": report.ok, 
            "issues": report.issues, "details": report.details
        }, indent=2, ensure_ascii=False))
        
        if not report.ok:
            sys.exit(2)


@dataclass
class VerifyRibosCommand:
    """check or install canonical ribos"""
    candidate: Optional[Path] = None

    def execute(self):
        # bootstrap_environment()에서 이미 등록되었으므로 안전하게 호출 가능
        if self.candidate:
            result = static_registry.install_asset("ribos_canon", self.candidate)
        else:
            result = static_registry.static_check("ribos_canon")
        print(json.dumps(result, indent=2))
        if not result.get("ok"):
            sys.exit(2)


# --- Lineage Subcommands Grouping ---
@dataclass
class LineageListCommand:
    """list all adapter lineages"""
    def execute(self):
        lineage = LineageManager(PATHS["adapters"])
        rows = [{
            "gen_id": r.gen_id, "created_at": r.created_at, "promoted": r.promoted,
            "eval_score": r.eval_score, "parent_adapter": r.parent_adapter,
        } for r in lineage.list()]
        print(json.dumps(rows, indent=2, ensure_ascii=False))

@dataclass
class LineageShowCommand:
    """show manifest for a specific adapter"""
    gen_id: str
    def execute(self):
        lineage = LineageManager(PATHS["adapters"])
        print(json.dumps(lineage.get(self.gen_id).manifest, indent=2, ensure_ascii=False))

@dataclass
class LineageDiffCommand:
    """compare two adapter manifests"""
    a: str
    b: str
    def execute(self):
        lineage = LineageManager(PATHS["adapters"])
        manifest_a, manifest_b = lineage.get(self.a).manifest, lineage.get(self.b).manifest
        diff = {k: {"a": manifest_a.get(k), "b": manifest_b.get(k)} 
                for k in sorted(set(manifest_a) | set(manifest_b)) if manifest_a.get(k) != manifest_b.get(k)}
        print(json.dumps(diff, indent=2, ensure_ascii=False))

@dataclass
class LineageCommand:
    """inspect adapter lineage (list, show, diff)"""
    action: Union[
        Annotated[LineageListCommand, tyro.conf.subcommand("list")],
        Annotated[LineageShowCommand, tyro.conf.subcommand("show")],
        Annotated[LineageDiffCommand, tyro.conf.subcommand("diff")],
    ]
    def execute(self):
        self.action.execute()


@dataclass
class PromoteCommand:
    """mark adapter as promoted and update latest symlink"""
    gen_id: str
    def execute(self):
        lineage = LineageManager(PATHS["adapters"])
        lineage.set_promoted(self.gen_id, True)
        lineage.update_latest_symlink(self.gen_id, PATHS["latest_link"])
        print(json.dumps({"promoted": self.gen_id}, indent=2))


@dataclass
class RollbackCommand:
    """revert latest to the previous promoted adapter"""
    def execute(self):
        lineage = LineageManager(PATHS["adapters"])
        prev = lineage.previous_promoted(PATHS["latest_link"])
        if prev is None:
            log.error("no previous promoted adapter to roll back to")
            sys.exit(1)
        lineage.update_latest_symlink(prev.gen_id, PATHS["latest_link"])
        print(json.dumps({"rolled_back_to": prev.gen_id}, indent=2))


@dataclass
class GcCommand:
    """remove old non-promoted adapters"""
    older_than_days: int = 30
    keep_promoted: bool = True

    def execute(self):
        lineage = LineageManager(PATHS["adapters"])
        cutoff = time.time() - self.older_than_days * 86400
        removed = []
        for r in lineage.list():
            if r.promoted and self.keep_promoted: continue
            ts = parse_iso(r.created_at)
            if ts is None or ts >= cutoff: continue
            shutil.rmtree(r.path)
            removed.append(r.gen_id)
        print(json.dumps({"removed": removed}, indent=2))


@dataclass
class SelfCheckCommand:
    """verify 3-way system integrity"""
    def execute(self):
        # 의존성 주입: 시스템 해시, 환경 경로, 리니지 매니저를 넘겨줌
        lineage = LineageManager(PATHS["adapters"])
        report = self_check(
            sys_version=get_system_version(), 
            paths=PATHS, 
            lineage=lineage
        )
        print(json.dumps(report, indent=2, ensure_ascii=False))
        if not report["ok"]:
            sys.exit(2)

NexusApp = Union[
    Annotated[HarvestCommand, tyro.conf.subcommand("harvest")],
    Annotated[TranscribeCommand, tyro.conf.subcommand("transcribe")],
    Annotated[ReceiveCommand, tyro.conf.subcommand("receive")],
    Annotated[VerifyRibosCommand, tyro.conf.subcommand("verify-ribos")],
    Annotated[LineageCommand, tyro.conf.subcommand("lineage")],
    Annotated[PromoteCommand, tyro.conf.subcommand("promote")],
    Annotated[RollbackCommand, tyro.conf.subcommand("rollback")],
    Annotated[GcCommand, tyro.conf.subcommand("gc")],
    Annotated[SelfCheckCommand, tyro.conf.subcommand("self-check")],
]

def main(argv: Optional[list[str]] = None) -> None:
    try:
        bootstrap_environment()
        command = tyro.cli(NexusApp, args=argv)
        command.execute()
    except SystemExit:
        raise
    except Exception as e:
        log.error(f"nexus error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()