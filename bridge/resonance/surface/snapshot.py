# bridge.resonance.surface.snapshot
import sys
import json
import hashlib
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Tuple
from bridge.event.psi import PsiCarrier
from flow.surface.emitter import get_logger

log = get_logger("ext.trace.delta")

## Anchor Metadata
VERS = "6"  # version inc
STATE_FILE = ".bound.snapshot.json"
FULL_REBUILD_RATIO = 0.6

## Snapshot (Bound Storage)
def get_snapshot_path(root: Path) -> Path:
    return root / STATE_FILE

def load_snapshot_state(root: Path) -> Dict[str, str]:
    path = get_snapshot_path(root)
    if not path.exists():
        return {}
    return json.loads(path.read_text()).get("files", {})

def persist_snapshot_state(root: Path, token: str, files: Dict[str, str]) -> None:
    get_snapshot_path(root).write_text(
        json.dumps({"token": token, "files": files}, indent=2)
    )

def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()

## @scan
def scan_files(root: Path) -> Dict[str, str]:
    files = {}
    for p in root.rglob("*"):
        if p.is_file() and p.name != STATE_FILE:
            files[str(p.relative_to(root))] = file_hash(p)
    return files

def compute_root_fingerprint(files: Dict[str, str]) -> str:
    h = hashlib.sha256()
    for k in sorted(files.keys()):
        h.update(k.encode())
        h.update(files[k].encode())
    return h.hexdigest()

## @delta.classify
def compute_delta(
    prev: Dict[str, str],
    curr: Dict[str, str]
) -> Tuple[list, list, list]:

    prev_keys = set(prev.keys())
    curr_keys = set(curr.keys())

    added = list(curr_keys - prev_keys)
    removed = list(prev_keys - curr_keys)

    modified = [
        k for k in (prev_keys & curr_keys)
        if prev[k] != curr[k]
    ]

    return added, removed, modified

def classify_rebuild_scope(prev_count: int, changed_count: int) -> str:
    if prev_count == 0:
        return "full"

    ratio = changed_count / prev_count
    return "full" if ratio >= FULL_REBUILD_RATIO else "partial"

## @entry (Meso Trigger Engine)
def entry(root: Path, last_token: str = "") -> Dict:
    curr = scan_files(root)
    prev = load_snapshot_state(root)
    token = compute_root_fingerprint(curr)

    if last_token and last_token != token:
        state = "full"
        added = list(curr.keys())
        removed = []
        modified = []
    else:
        added, removed, modified = compute_delta(prev, curr)
        changed = len(added) + len(removed) + len(modified)
        state = classify_rebuild_scope(len(prev), changed)

    persist_snapshot_state(root, token, curr)

    psi = PsiCarrier(
        kind="bound.delta",
        tag=state,
        payload=f"source:{str(root)}"
    )

    return {
        "version": VERS,
        "token": token,
        "state": state,
        "added": added,
        "removed": removed,
        "modified": modified,
        "psi": psi
    }

## @test.valid
def test_valid() -> None:
    """
    @intent: Meso Structural Validation
    @transition: full → partial → residue
    @goal: Bound Drift Classification Verification
    """
    log.info("[TEST] Bound Meso Self-Check Start")
    temp_dir = tempfile.mkdtemp()
    bound_root = Path(temp_dir)

    alpha = bound_root / "alpha.txt"
    beta = bound_root / "beta.txt"

    try:
        ## phase.1: Genesis (Full Rebuild)
        log.info("\n[PHASE] Genesis -> Initial Snapshot")
        alpha.write_text("seed")
        beta.write_text("anchor")

        first = entry(bound_root)
        log.info(f"[STATE] classification={first['state']}")
        log.info(f"[DELTA] added={len(first['added'])}, removed={len(first['removed'])}, modified={len(first['modified'])}")
        log.info(f"[Ψ] meso_signal={first['psi'].symbol}")

        assert first["state"] == "full"

        ## phase.2: Incremental Change
        log.info("\n[PHASE] Incremental Mutation -> Partial Drift")
        alpha.write_text("seed evolved")

        second = entry(bound_root)

        log.info(f"[STATE] classification={second['state']}")
        log.info(f"[DELTA] added={len(second['added'])}, removed={len(second['removed'])}, modified={len(second['modified'])}")
        log.info(f"[Ψ] meso_signal={second['psi'].symbol}")

        assert second["state"] == "partial"

        ## phase.3: @residue.detection
        log.info("\n[PHASE] Residue Removal → Structural Contraction")
        alpha.unlink()

        third = entry(bound_root)

        log.info(f"[STATE] classification={third['state']}")
        log.info(f"[DELTA] added={len(third['added'])}, removed={len(third['removed'])}, modified={len(third['modified'])}")
        log.info(f"[Ψ] meso_signal={third['psi'].symbol}")

        assert len(third["removed"]) == 1
        log.info("\n[RESULT] Meso Layer Validation Complete ✔")

    finally:
        shutil.rmtree(temp_dir)
    log.info("[TEST] Completed Successfully")

## CLI
def main() -> None:
    if "--test" in sys.argv:
        test_valid()
        return

    if len(sys.argv) < 2:
        log.info("usage: python -m metaflow.bound.tracer <root> [last_token]")
        sys.exit(1)

    root = Path(sys.argv[1])
    last_token = sys.argv[2] if len(sys.argv) > 2 else ""

    result = entry(root, last_token)

    log.info(json.dumps({
        "version": result["version"],
        "token": result["token"],
        "state": result["state"],
        "delta_summary": {
            "added": len(result["added"]),
            "removed": len(result["removed"]),
            "modified": len(result["modified"])
        },
        "meso_signal": result["psi"].symbol
    }, indent=2))


if __name__ == "__main__":
    main()