# xphi.reflect.reaper
"""
@desc: orphan process collector via echo-resonance

@phase:
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
from redis import Redis
from flow.surface.emitter import get_emitter
from bridge.node.runtime import NodeRuntime

class Reaper:
    """
    @role: Φ′ executor (cleanup operator)
    @flow: Ψ -> mode select -> {strike | force | clean} -> process termination
    """
    def __init__(self, redis_conn):
        self.redis = redis_conn
        self.log = get_emitter("surface.reaper", phase="EXEC")

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

                    ## source_id 기준으로 필터링
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
        proc = await asyncio.create_subprocess_exec("kill", signal, pid)
        await proc.wait()
        self.log.warn(f" [Killed] PID {pid} with signal {signal}")

    async def surgical_strike(self, wait_time: float = 2.0):
        """
        @flow: Ψ (ping) -> Ψ_echo (collect) -> Φ (resolve endpoint) -> ∂Φ (pid detection) -> selective kill
        """
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
            await asyncio.sleep(0.1) # 루프 효율화
        
        pubsub.unsubscribe("system:echo")
        return found_nodes

    async def scorched_earth(self):
        """
        @flow: Ψ (pattern scan) -> ∂Φ (process match) -> global kill
        """
        self.log.warn("Initiating Scorched Earth (pkill)...")
        proc = await asyncio.create_subprocess_exec("pkill", "-9", "-f", "xphi-.*\\.jar")
        await proc.wait()
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
    """
    @flow: BOOT -> bind surface -> attach executor (Φ′) -> activate RuntimeNode (Ψ loop)
    """
    log = get_emitter("reaper.launcher", phase="BOOT")
    redis_conn = Redis(host=os.getenv("REDIS_HOST", "localhost"), decode_responses=True)
    reaper_executor = Reaper(redis_conn)
    node = NodeRuntime(executor=reaper_executor)
    log.info("Reaper Node is now online. Monitoring for cleanup commands...")
    await node.start()

if __name__ == "__main__":
    asyncio.run(main())