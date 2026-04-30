# agent.workflow.field.projector
import os
import json
import ast
import asyncio
import time
from pathlib import Path
import networkx as nx
import redis.asyncio as redis_async
from bound.resolver import find_current_self, resolve_path
from flow.surface.emitter import get_logger
from node.runtime import NodeRuntime

log = get_logger("surface.projector")

class ToposMapper:
    """@topos.role: boundary coordinate mapper (Φ_path → ∂Φ_prefix)"""
    @staticmethod
    def to_prefix(root: Path, path: Path) -> str:
        # @phase: filesystem Φ → symbolic ∂Φ (dot-prefix)
        rel = path.relative_to(root)
        dot_path = ".".join(rel.with_suffix('').parts)
        return dot_path  # namespace base (no ":")


class ToposSyncer:
    """@topos.role: Φ → Surface projection (static topology → Redis field)"""
    def __init__(self, redis_conn, root: Path):
        self.r = redis_conn
        self.root = root

    async def sync(self, field_graph: nx.MultiDiGraph):
        # @phase: Φ(graph) → ∂Φ(surface adjacency field)
        log.info("[surface] projecting topology to dot-prefix space")
        pipe = self.r.pipeline()

        for node in field_graph.nodes:
            # @phase: node ∈ Φ → coordinate ∂Φ(prefix)
            prefix = ToposMapper.to_prefix(self.root, self.root / node)
            key = f"{prefix}:adj"   # @surface.key: adjacency field at ∂Φ

            # @phase: neighbor relation preserved in same coordinate space
            neighbors = [
                ToposMapper.to_prefix(self.root, self.root / n)
                for n in field_graph.neighbors(node)
            ]

            # @effect: overwrite local adjacency slice (Φ_local)
            pipe.delete(key)
            if neighbors:
                pipe.sadd(key, *neighbors)

        # @commit: Φ → Surface synchronization
        await pipe.execute()


class ToposExecutor:
    """@topos.role: Ψ emitter (event → surface binding at ∂Φ)"""
    def __init__(self, redis_conn):
        self.r = redis_conn

    async def __call__(self, task: dict):
        # @input: Ψ_task (runtime intent)
        command = task.get("command", "IDLE")
        intensity = task.get("intensity", "normal")
        ts = int(time.time() * 1000)

        # @binding: Ψ anchored to ∂Φ via prefix (closure condition)
        origin = task.get("origin_prefix", "unknown").rstrip(":")

        # @surface.key: Ψ written inside Φ coordinate space
        event_key = f"{origin}:event:{intensity}:{command.lower()}:{ts}"
        latest_key = f"{origin}:event:{intensity}:{command.lower()}:latest"

        log.info(f"[bus] emitting signal -> {event_key}")

        # @payload: Ψ carrier (no structural mutation, pure emission)
        payload = json.dumps({
            "task": task,
            "origin": origin,
            "timestamp": ts
        })

        # @phase: Ψ → Surface (write)
        await self.r.set(event_key, payload)
        await self.r.set(latest_key, event_key)

        # @return: emission acknowledgment (no Φ change)
        return {"status": "dispatched", "key": event_key}


async def bootstrap():
    # @phase: system anchoring (self → root coordinate)
    root = find_current_self()
    log.info(f"[bootstrap] root identified: {root}")

    # @infra: Surface binding (Redis as ∂Φ field)
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
    r = redis_async.from_url(REDIS_URL, decode_responses=True)

    # @phase: Φ construction (static code topology)
    field_graph = nx.MultiDiGraph()

    for path in root.rglob("*.py"):
        if ".git" in str(path):
            continue

        # @node: Φ element (file-level unit)
        rel = path.relative_to(root).as_posix()
        field_graph.add_node(rel)

        # @edge: Φ relation (import dependency)
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for n in ast.walk(tree):
                if isinstance(n, (ast.Import, ast.ImportFrom)):
                    target = getattr(n, 'module', None) or n.names[0].name
                    field_graph.add_edge(rel, target, type="import")
        except:
            pass

    # @projection: Φ → ∂Φ (surface embedding)
    projector = ToposSyncer(r, root)
    await projector.sync(field_graph)

    # @binding: Ψ executor attached to runtime node
    executor = ToposExecutor(r)
    node = NodeRuntime(executor=executor)

    log.info("NodeRuntime is ready with unified prefix-based surface.")
    return node


async def main():
    # @entry: system activation (Ψ loop start)
    node = await bootstrap()
    # @loop: Ψ ↔ ∂Φ feedback runtime
    await node.start()


if __name__ == "__main__":
    asyncio.run(main())