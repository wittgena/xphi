# devops.kube.ator.projector
import os
import re
import ast
import json
import asyncio
from pathlib import Path
from typing import Dict, Any, List
import networkx as nx
import redis.asyncio as redis_async
from fastapi import FastAPI
import uvicorn
from anchor.resolver import find_current_self, resolve_path
from bridge.plane.emitter import get_logger
from node.runtime import NodeRuntime

log = get_logger("field.projector")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

def parse_key(key: str) -> str:
    """
    space:adj:surface/xor.py → surface/xor.py
    """
    if not key or not key.startswith("space:adj:"):
        return None
    return key[len("space:adj:"):]


class PathSensor:
    """filesystem → Ψ"""
    def scan(self, root: Path):
        for path in root.rglob("*"):
            if path.suffix in [".py", ".md"]:
                yield {
                    "path": path,
                    "suffix": path.suffix
                }

class ToposField:
    """@role: Φ (topological field)"""

    def __init__(self, root: Path):
        self.root = root
        self.g = nx.MultiDiGraph()

    def apply(self, psi: Dict[str, Any]):
        path: Path = psi["path"]
        rel = path.relative_to(self.root).as_posix()

        self.g.add_node(rel, suffix=psi["suffix"])

        ## containment
        parent = (
            path.parent.relative_to(self.root).as_posix()
            if path.parent != self.root else "root"
        )
        self.g.add_edge(parent, rel, type="contain")

    def enrich_links(self):
        """md + python dependency"""
        for node in list(self.g.nodes):
            full = self.root / node
            if not full.exists():
                continue

            ## markdown
            if full.suffix == ".md":
                try:
                    content = full.read_text(encoding="utf-8", errors="ignore")
                    links = re.findall(r'\[\[(.*?)\]\]|\[.*?\]\((.*?)\)', content)

                    for l in links:
                        tgt = l[0] or l[1]
                        if tgt:
                            self.g.add_edge(node, tgt, type="link")
                except:
                    pass
            ## python import
            elif full.suffix == ".py":
                try:
                    tree = ast.parse(full.read_text(encoding="utf-8"))
                    for n in ast.walk(tree):
                        if isinstance(n, ast.Import):
                            for alias in n.names:
                                self.g.add_edge(node, alias.name, type="import")
                        elif isinstance(n, ast.ImportFrom):
                            if n.module:
                                self.g.add_edge(node, n.module, type="import")
                except:
                    pass

    def neighbors(self, node: str) -> List[str]:
        return list(self.g.neighbors(node))

    def to_json(self):
        return nx.node_link_data(self.g)

    def load_json(self, data):
        self.g = nx.node_link_graph(data)

class FieldProjector:
    """Φ → Redis key-space"""

    def __init__(self, redis_conn):
        self.r = redis_conn

    async def sync(self, field: ToposField):
        log.info("[redis] syncing topology")

        for node in field.g.nodes:
            adj = list(field.g.neighbors(node))
            key = f"space:adj:{node}"

            await self.r.delete(key)
            if adj:
                await self.r.sadd(key, *adj)


class ToposAPI:
    """single-key API"""
    def __init__(self, field: ToposField):
        self.app = FastAPI()
        self.field = field

        @self.app.get("/query")
        async def query(key: str):
            path = parse_key(key)

            if not path:
                return {"error": "invalid_key"}

            return {
                "key": key,
                "neighbors": self.field.neighbors(path)
            }

class ToposExecutor:
    """Φ′ resolver (key-only)"""

    def __init__(self, field: ToposField):
        self.field = field

    async def __call__(self, task):
        key = task.get("key")

        path = parse_key(key)
        if not path:
            return {"error": "invalid_key"}

        return {
            "key": key,
            "neighbors": self.field.neighbors(path)
        }

async def bootstrap():
    root = find_current_self()
    anchor = resolve_path("anchor")
    cache_file = anchor / "key.space.json"

    sensor = PathSensor()
    field = ToposField(root)

    ## build or load
    if cache_file.exists():
        log.info("[bootstrap] load cached topology")
        with open(cache_file, "r", encoding="utf-8") as f:
            field.load_json(json.load(f))
    else:
        log.info("[bootstrap] build topology")

        for psi in sensor.scan(root):
            field.apply(psi)

        field.enrich_links()

        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(field.to_json(), f, indent=2, ensure_ascii=False)

        log.info(f"[bootstrap] saved → {cache_file}")

    ## redis
    r = redis_async.from_url(REDIS_URL, decode_responses=True)
    projector = FieldProjector(r)
    await projector.sync(field)

    ## api + runtime
    api = ToposAPI(field)
    executor = ToposExecutor(field)
    node = NodeRuntime(executor=executor)
    return api.app, node

async def main():
    app, node = await bootstrap()
    log.info("topology key-space server ready")

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
    server = uvicorn.Server(config)

    await asyncio.gather(
        node.start(),
        server.serve()
    )

if __name__ == "__main__":
    asyncio.run(main())