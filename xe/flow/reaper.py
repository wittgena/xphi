"""
@desc: orphan process collector via echo-resonance
@flow:
Ψ (ping emission)
  -> Ψ_echo (node self-report)
  -> Φ (port → pid projection)
  -> ∂Φ (process boundary identification)
  -> termination actuation
  -> residue (system stabilization)
"""
import asyncio
import json
import urllib.parse
import os
import argparse
from redis import Redis
from watcher.plane.emitter import get_emitter
from phase.runtime.node import NodeRuntime

log = get_emitter("flow.reaper", phase="BOOT")

class Reaper:
    """@flow: Ψ -> mode select -> {strike | force | clean} -> process termination"""
    def __init__(self, redis_conn, tag: str = "xphi"):
        self.redis = redis_conn
        self.tag = tag
        self.registry_key = f"system:{self.tag}:pids"
        self.process_pattern = f"{self.tag}"
        self.log = get_emitter("flow.reaper", phase="EXEC")

    async def command_listener(self):
        pubsub = self.redis.pubsub()
        pubsub.subscribe("system:reaper:command")

        while True:
            msg = pubsub.get_message(ignore_subscribe_messages=True)
            if msg:
                data = json.loads(msg["data"])

                if data.get("task") == "strike":
                    await self.targeted_strike(data.get("source_id"))

            await asyncio.sleep(0.1)

    async def targeted_strike(self, source_id: str):
        pubsub = self.redis.pubsub()
        pubsub.subscribe("system:echo")
        self.redis.publish("system:ping", json.dumps({"source": "reaper"}))

        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < 2.0:
            msg = pubsub.get_message(ignore_subscribe_messages=True)
            if msg:
                try:
                    data = json.loads(msg["data"])

                    if data.get("node_id") != source_id:
                        continue

                    port = urllib.parse.urlparse(data["api_base"]).port
                    pid = await self.get_pid_from_port(port)
                    if pid:
                        await self.kill_pid(pid)
                        break

                except Exception:
                    continue

            await asyncio.sleep(0.05)

        pubsub.unsubscribe("system:echo")


    async def get_pid_from_port(self, port: int):
        """port → pid resolution"""
        try:
            proc = await asyncio.create_subprocess_exec(
                "lsof", "-t", f"-i:{port}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            return stdout.decode().strip() if stdout else None
        except Exception as e:
            self.log.error(f"lsof failed: {e}")
            return None

    async def kill_pid(self, pid: str, force: bool = True):
        signal = "-9" if force else "-15"
        proc = await asyncio.create_subprocess_exec(
            "kill", signal, pid,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
        await proc.wait()
        
        # 0이면 성공적으로 죽임, 그 외는 이미 죽었거나 권한 없음
        if proc.returncode == 0:
            self.log.warn(f" [Killed] PID {pid} with signal {signal}")

    async def surgical_strike(self, wait_time: float = 2.0):
        """@flow: Ψ (ping) -> Ψ_echo (collect) -> Φ (resolve endpoint) -> ∂Φ (pid detection) -> selective kill"""
        self.log.info("🦇 Broadcasting system:ping...")
        
        pubsub = self.redis.pubsub()
        pubsub.subscribe("system:echo")
        self.redis.publish("system:ping", json.dumps({"ts": asyncio.get_event_loop().time(), "source": "reaper_node"}))

        found_nodes = 0
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < wait_time:
            msg = pubsub.get_message(ignore_subscribe_messages=True)
            if msg:
                try:
                    data = json.loads(msg["data"])
                    if "api_base" in data:
                        port = urllib.parse.urlparse(data["api_base"]).port
                        pid = await self.get_pid_from_port(port)
                        if pid:
                            await self.kill_pid(pid)
                            found_nodes += 1
                except Exception:
                    continue
            await asyncio.sleep(0.1)
        
        pubsub.unsubscribe("system:echo")
        return found_nodes

    async def scorched_earth(self):
        """
        @flow: 
        1. 2안(OS 레벨 프로세스명 매칭) -> 2. 3안(Redis PID 레지스트리 잔당 처리) -> 3. 레지스트리 포맷
        """
        self.log.warn(f"Initiating Scorched Earth for target [{self.tag}]...")

        ## OS 레벨 태그 기반 일괄 종료
        proc = await asyncio.create_subprocess_exec("pkill", "-9", "-f", self.process_pattern)
        await proc.wait()
        self.log.info(f" [Phase 1] pkill sweep completed for pattern: {self.process_pattern}")

        ## Redis Registry 기반 잔당 확인사살
        try:
            pids = self.redis.smembers(self.registry_key)
            if pids:
                self.log.info(f" [Phase 2] Checking {len(pids)} PIDs in registry {self.registry_key}")
                for pid in pids:
                    ## pkill로 죽지 않았거나 태그가 유실된 PID 직접 타격
                    await self.kill_pid(str(pid), force=True)
            
            ## 정리 완료 후 레지스트리 비우기
            self.redis.delete(self.registry_key)
            self.log.info(f" [Phase 3] Registry {self.registry_key} cleared.")
        except Exception as e:
            self.log.error(f" Registry cleanup failed: {e}")

        self.log.info("Scorched Earth protocol completed.")

    async def __call__(self, task_type: str = "clean"):
        if task_type == "strike":
            return await self.surgical_strike()
        elif task_type == "force":
            await self.scorched_earth()
        else: ## clean
            count = await self.surgical_strike()
            await self.scorched_earth()
            return count

async def main():
    """@flow: BOOT -> arg parse -> Direct Execution -> Exit"""
    parser = argparse.ArgumentParser(description="Reaper Process Collector")
    parser.add_argument("--tag", type=str, default="xphi", help="Target component tag (default: xphi)")
    parser.add_argument("--task", type=str, default="clean", choices=["strike", "force", "clean"], help="Task to run")
    args = parser.parse_args()

    redis_conn = Redis(host=os.getenv("REDIS_HOST", "localhost"), decode_responses=True)
    reaper = Reaper(redis_conn, tag=args.tag)
    
    log.info(f"Executing Reaper [{args.task}] directly on local plane...")
    
    # NodeRuntime으로 감싸지 않고 __call__을 직접 호출!
    count = await reaper(task_type=args.task)
    
    log.info(f"Reaper execution finished. Process completed.")

if __name__ == "__main__":
    asyncio.run(main())

# async def main():
#     """@flow: BOOT -> arg parse -> bind surface -> attach executor -> activate RuntimeNode"""
    
#     parser = argparse.ArgumentParser(description="Reaper Process Collector")
#     parser.add_argument("--tag", type=str, default="xphi", help="Target component tag (default: xphi)")
#     args = parser.parse_args()

#     redis_conn = Redis(host=os.getenv("REDIS_HOST", "localhost"), decode_responses=True)
#     reaper_executor = Reaper(redis_conn, tag=args.tag)
#     node = NodeRuntime(executor=reaper_executor)
    
#     log.info(f"Reaper Node [{args.tag}] is now online. Monitoring for cleanup commands...")
#     await node.start()

# if __name__ == "__main__":
#     asyncio.run(main())