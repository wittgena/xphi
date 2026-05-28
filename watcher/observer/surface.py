# topos.watcher.surface
import asyncio
import time
import json
import argparse
import sys
import redis.asyncio as redis_async
from typing import Optional, Dict, Any, List
from watcher.plane.emitter import get_emitter
from phase.bind.resolver import find_current_self
from arch.contract.registry.unified import contract
from phase.runtime.cli.executor import CliTaskAdapter, parse_local, dispatch_cli

class SurfaceObserver:
    """
    @role: Topology Steward / Manifold Manager
    @desc: 위상 공간(Redis)의 엔트로피를 제어하는 선택적 백그라운드 관리자
    """
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.redis: Optional[redis_async.Redis] = None
        self.log = get_emitter("topos.manager", phase="SYSTEM")
        
        self.config = {
            "node_ttl_sec": 30.0,
            "max_log_stream_len": 10000,
            "max_global_queue_len": 5000,
            "digest_interval": 3600,
        }

        self.task_registry = {
            "gc": {
                "coro": self._reap_ghost_nodes,
                "desc": "좀비 노드 및 고아 상태 청소 (Topology GC)",
                "interval": "15s"
            },
            "trim": {
                "coro": self._trim_entropy,
                "desc": "엔트로피 관리 및 스트림/큐 크기 제한",
                "interval": "60s"
            },
            "quarantine": {
                "coro": self._inspect_quarantine,
                "desc": "격리된 '독성 이벤트' 수거 및 분석 (DLQ)",
                "interval": "5s"
            },
            "digest": {
                "coro": self._broadcast_digest,
                "desc": "위상 공간 밀도 측정 및 요약 브로드캐스트",
                "interval": f"{self.config['digest_interval']}s"
            }
        }

    async def connect(self):
        self.redis = await redis_async.from_url(self.redis_url, decode_responses=True)

    def print_task_plan(self, active_tasks: List[str]):
        """@desc: 실행 전 시스템이 수행할 개입(Intervention) 계획을 시각적으로 선언"""
        print("\n" + "="*60)
        print(" [ Phase: task.plan ] Topology Stewardship Matrix")
        print("="*60)
        
        for name, info in self.task_registry.items():
            is_active = name in active_tasks
            status = "[\033[92m ACTIVE \033[0m]" if is_active else "[\033[90m BYPASS \033[0m]"
            print(f" {status} {name:<12} : {info['desc']} (Cycle: {info['interval']})")
            
        print("="*60 + "\n")

    async def _reap_ghost_nodes(self):
        while True:
            try:
                keys = await self.redis.keys("runtime:node:*")
                for key in keys:
                    node_id = key.split(":")[-1]
                    last_ping = await self.redis.get(f"runtime:heartbeat:{node_id}")
                    if not last_ping or (time.time() - float(last_ping) > self.config["node_ttl_sec"]):
                        await self.redis.delete(key)
                        await self.redis.delete(f"runtime:heartbeat:{node_id}")
                        self.log.warn(f"Reaped ghost node: {node_id}")
            except Exception: pass
            await asyncio.sleep(15.0)

    async def _trim_entropy(self):
        while True:
            try:
                await self.redis.xtrim("system:logs:stream", maxlen=self.config["max_log_stream_len"], approximate=True)
                q_len = await self.redis.llen("runtime:queue")
                if q_len > self.config["max_global_queue_len"]:
                    await self.redis.ltrim("runtime:queue", 0, self.config["max_global_queue_len"] - 1)
            except Exception: pass
            await asyncio.sleep(60.0)

    async def _inspect_quarantine(self):
        while True:
            try:
                toxic_event = await self.redis.rpop("runtime:quarantine")
                if toxic_event:
                    self.log.crit(f"Toxic Psi Event harvested: {json.loads(toxic_event).get('symbol')}")
            except Exception: pass
            await asyncio.sleep(5.0)

    async def _broadcast_digest(self):
        while True:
            await asyncio.sleep(self.config["digest_interval"])
            try:
                node_keys = await self.redis.keys("runtime:node:*")
                self.log.signal(f"[Topology Digest] Active Resonance Nodes: {len(node_keys)}")
            except Exception: pass

    async def start_stewardship(self, active_tasks: List[str]):
        """선택된 태스크들만 모아서 비동기 데몬으로 구동"""
        tasks = []
        for name in active_tasks:
            if name in self.task_registry:
                tasks.append(asyncio.create_task(self.task_registry[name]["coro"]()))
        
        if not tasks:
            self.log.warn("No active tasks selected. Observer is dormant.")
            return

        self.log.info("Initiating selected background stewardship...")
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_async(self, active_tasks: List[str]):
        """비동기 실행 진입점"""
        await self.connect()
        await self.start_stewardship(active_tasks)

    def run(self, active_tasks: List[str], plan_only: bool = False):
        """@desc: 어댑터에 바인딩되는 동기 메서드. 실행 환경(plan/apply) 통제 및 루프 생성"""
        self.print_task_plan(active_tasks)
        
        if plan_only:
            self.log.info("Plan printed. Bypassing execution due to --plan-only.")
            return

        try:
            ## 스스로 이벤트 루프를 생성하여 비동기 도메인을 실행
            asyncio.run(self._run_async(active_tasks))
        except KeyboardInterrupt:
            self.log.signal("Topos Manager gracefully stopped.")

def entry_task(args):
    parser = argparse.ArgumentParser(description="Topos Manager: Manifold Stewardship CLI")
    parser.add_argument("--tasks", type=str, default="all", help="Comma-separated list of tasks to run (gc, trim, quarantine, digest) or 'all'")
    parser.add_argument("--plan-only", action="store_true", help="Print the task.plan and exit without executing anything")
    parsed_args = parser.parse_args(args)
    manager = SurfaceObserver()
    
    if parsed_args.tasks.lower() == "all":
        active_list = list(manager.task_registry.keys())
    else:
        active_list = [t.strip() for t in parsed_args.tasks.split(",") if t.strip() in manager.task_registry]

    run_kwargs = {
        "active_tasks": active_list,
        "plan_only": parsed_args.plan_only
    }
    return CliTaskAdapter(manager.run, **run_kwargs)

@contract.cli(name="observer.surface", recept=[])
def main():
    bound_args, remain = parse_local(sys.argv[1:])
    dispatch_cli("observer.surface", entry_task, __file__)

if __name__ == "__main__":
    main()