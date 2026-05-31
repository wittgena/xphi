# nexus.harvester.residue
from __future__ import annotations
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from xe.xor.store import ResidueStore
from watcher.plane.emitter import get_emitter
from nexus.exp.atomic import sha256_file, sha256_text, atomic_write_json, atomic_write_jsonl, read_json, now_iso, parse_iso

log = get_emitter("residue.harvester")

class ResidueHarvester:
    """Pulls residue from the local Surgent store"""
    def __init__(self, store_path: Path, cursor_path: Path):
        self.store_path = store_path
        self.cursor_path = cursor_path
        self._store = None

    def _open_store(self):
        if self._store is not None:
            return self._store

        self._store = ResidueStore(path=str(self.store_path))
        return self._store

    def _load_cursor(self) -> dict:
        if self.cursor_path.exists():
            return read_json(self.cursor_path)
        return {"last_snapshot_id": None, "last_harvest_at": None}

    def _save_cursor(self, cursor: dict) -> None:
        atomic_write_json(self.cursor_path, cursor)

    def harvest(
        self,
        out_path: Path,
        min_reward: float = 0.5,
        since_cursor: bool = True,
        max_per_prompt: int = 3,
    ) -> dict:
        cursor = self._load_cursor() if since_cursor else {"last_snapshot_id": None}
        last_id = cursor.get("last_snapshot_id")
        store = self._open_store()
        snapshots = list(store.retrieve_all())

        if last_id is not None and since_cursor:
            seen = False
            filtered = []
            for s in snapshots:
                if seen:
                    filtered.append(s)
                elif getattr(s, "id", None) == last_id:
                    seen = True
            snapshots = filtered if seen else snapshots

        traces: list[dict] = []
        prompt_count: dict[str, int] = {}
        dedup: set[str] = set()
        rejected = {"low_reward": 0, "empty": 0, "duplicate": 0, "per_prompt_cap": 0}

        for snap in snapshots:
            for block in getattr(snap, "blocks", []):
                reward = float(block.get("reward", 0.0))
                prompt = (block.get("instruction") or "").strip()
                completion = (block.get("successful_action") or "").strip()

                if reward < min_reward:
                    rejected["low_reward"] += 1
                    continue
                if not prompt or not completion:
                    rejected["empty"] += 1
                    continue

                key = sha256_text(prompt + "\x1f" + completion)
                if key in dedup:
                    rejected["duplicate"] += 1
                    continue
                dedup.add(key)

                pk = sha256_text(prompt)
                if prompt_count.get(pk, 0) >= max_per_prompt:
                    rejected["per_prompt_cap"] += 1
                    continue
                prompt_count[pk] = prompt_count.get(pk, 0) + 1

                traces.append({
                    "prompt": prompt,
                    "completion": completion,
                    "reward": reward,
                    "topology_context": block.get("context", ""),
                })

        if not traces:
            raise RuntimeError("Harvest yielded zero traces — refusing to write empty corpus.")

        n = atomic_write_jsonl(out_path, traces)
        manifest = {
            "created_at": now_iso(),
            "n_traces": n,
            "min_reward": min_reward,
            "max_per_prompt": max_per_prompt,
            "cursor_from": last_id,
            "cursor_to": getattr(snapshots[-1], "id", None) if snapshots else last_id,
            "rejected": rejected,
            "corpus_sha256": sha256_file(out_path),
            "nucleus_version": SystemVersion.current().nucleus_sha,
        }
        atomic_write_json(out_path.with_suffix(out_path.suffix + ".manifest.json"), manifest)
        if since_cursor and snapshots:
            self._save_cursor({
                "last_snapshot_id": getattr(snapshots[-1], "id", None),
                "last_harvest_at": now_iso(),
            })

        log.info(f"Harvested {n} traces → {out_path}")
        log.info(f"  rejected: {rejected}")
        return manifest