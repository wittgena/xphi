# watcher.plane.flow.executor
## @lineage: phase.dynamics.flow.executor
import os
import sys
import json
import uuid
import asyncio
import argparse
import redis.asyncio as redis_async
from pathlib import Path
from typing import List, Dict, Any, Callable
from dataclasses import asdict

from arch.contract.executor import BaseExecutor
from arch.contract.registry.unified import registry
from arch.contract.event.next import next_id
from arch.contract.event.psi import PsiEvent, PsiCarrier

from phase.runtime.surface.sensor import REDIS_URL
from phase.bind.resolver import find_current_self
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("flow.executor")

class FlowExecutor(BaseExecutor):
    """
    @role: Subprocess Delegation & Stream Broker
    @desc: Spawns an isolated worker subprocess via stdio pipes to protect the main Node runtime.
    """
    def __init__(self, completion_signal: asyncio.Event):
        super().__init__()
        self.completion_signal = completion_signal
        self.node = None

    async def execute(self, psi) -> list:
        if not hasattr(psi, 'carrier') or psi.carrier.kind != "COMMAND":
            return []

        # Extract context routing parameters
        context = psi.carrier.payload.get("_context", {})
        command = context.get("command") or psi.carrier.tag
        cli_args = context.get("cli_args", [])
        task_id = getattr(psi, 'event_id', None) or f"flow-{next_id()}"
        response_channel = psi.context.get("response_channel")
        
        # Enforce dynamic timeout from context, default to 300 seconds
        timeout_seconds = context.get("timeout", 300.0)

        if not command:
            self.log.error(f"[Isolated] Cannot resolve command from payload or tag. Task ID: {task_id}")
            return []

        # Resolve target module metadata from unified registry
        task_info_list = registry.registered_cli_tasks.get(command)
        if not task_info_list:
            self.log.error(f"[Isolated] No registered task found for command: {command}")
            return []

        task_info = task_info_list[0]
        module_fqn = task_info.get("module_fqn")

        with flow_scope(flow_id=task_id, phase="SUPERVISION"):
            self.log.info(f"[Isolated] Spawning isolated worker for '{command}' (Timeout: {timeout_seconds}s)")
            process = None
            
            try:
                # 1. Enforce strict isolation timeout boundary
                async with asyncio.timeout(timeout_seconds):
                    
                    # 2. Build execution command forcing --local routing within the subprocess
                    cmd = [sys.executable, "-m", module_fqn] + cli_args + ["--local"]
                    
                    self_root = find_current_self()
                    # 3. Spawn subprocess with redirected stdio pipes
                    process = await asyncio.create_subprocess_exec(
                        *cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.STDOUT,
                        cwd=str(self_root),
                        env=os.environ.copy()
                    )

                    # 4. Stream reading loop (Asynchronous Non-blocking I/O)
                    while True:
                        line = await process.stdout.readline()
                        if not line:
                            break
                        
                        decoded_line = line.decode('utf-8').strip()
                        if not decoded_line:
                            continue
                        
                        # 5. Broadcast real-time stream frames back to Redis manifold
                        if self.node and self.node.redis and response_channel:
                            stream_payload = {
                                "type": "FLOW",
                                "phase": "STREAMING",
                                "boundary": decoded_line
                            }
                            await self.node.redis.publish(response_channel, json.dumps(stream_payload))

                    await process.wait()
                    
                    if process.returncode != 0:
                        raise RuntimeError(f"Subprocess terminated with abnormal exit code: {process.returncode}")

                    # 6. Publish final collapse event upon successful convergence
                    if self.node and self.node.redis and response_channel:
                        collapse_payload = {
                            "type": "COLLAPSE",
                            "status": "SUCCESS",
                            "summary": f"Flow converged successfully via isolated worker process."
                        }
                        await self.node.redis.publish(response_channel, json.dumps(collapse_payload))

            except TimeoutError:
                self.log.error(f"⚠️ [SUPERVISOR KILLED] Flow task '{command}' exceeded {timeout_seconds}s limit.")
                if process and process.returncode is None:
                    try:
                        process.terminate()
                        await process.wait()
                    except Exception as e:
                        self.log.error(f"Failed to kill timed-out subprocess cleanly: {e}")
                
                if self.node and self.node.redis and response_channel:
                    error_payload = {
                        "type": "ERROR",
                        "status": "FAILED",
                        "summary": f"Task timed out after enforcement of {timeout_seconds}s boundary."
                    }
                    await self.node.redis.publish(response_channel, json.dumps(error_payload))
                    
            except Exception as e:
                self.log.error(f"[Isolated] Supervised execution pipeline failed: {e}")
                if self.node and self.node.redis and response_channel:
                    error_payload = {
                        "type": "ERROR",
                        "status": "FAILED",
                        "summary": str(e)
                    }
                    await self.node.redis.publish(response_channel, json.dumps(error_payload))
            finally:
                self.completion_signal.set()

        return []


# -------------------------------------------------------------------------
# CLI Router & Dedicated Flow Receiver (Proxy)
# -------------------------------------------------------------------------

def dispatch_flow_cli(command_name: str, entry_func: Callable, file_path: str):
    """
    @role: Universal Flow Execution Router
    @desc: Analyzes CLI arguments to decide between local execution or remote topological injection.
    """
    def parse_local(argv):
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--local", action="store_true")
        parser.add_argument("--timeout", type=float, default=300.0)
        return parser.parse_known_args(argv)

    bound_args, remain = parse_local(sys.argv[1:])
    
    from phase.bind.resolver import get_invoker
    invoker, command = get_invoker(Path(file_path))
    
    payload = { 
        "_context": {
            "invoker": str(invoker), 
            "command": command, 
            "cli_args": remain,
            "timeout": bound_args.timeout
        } 
    }

    if bound_args.local:
        # [ Dimension 1: Local Stream Execution ]
        log.info(f"[Router] Routing {command_name} to Local Flow Stream.")
        flow_instance = entry_func(remain)
        asyncio.run(_local_stream_runner(flow_instance, command_name))
    else:
        # [ Dimension 2: Topological Injection ]
        log.info(f"[Router] Injecting {command_name} Flow to Topological Manifold.")
        execute_flow_cli_task(command_name=command_name, payload=payload)


async def _local_stream_runner(flow_instance, command_name):
    """Executes the flow instance locally rendering standard output frames."""
    print(f"\n[\033[94mLOCAL STREAM\033[0m] Starting {command_name}...")
    try:
        # [핵심 수정] flow_instance가 execute_flow를 가지면 그것을, 아니면 execute를 호출
        stream_generator = flow_instance.execute_flow() if hasattr(flow_instance, "execute_flow") else flow_instance.execute()
        
        async for event in stream_generator:
            # Gracefully handle PseudoField objects or strings
            boundary = getattr(event, 'boundary', 'active')
            if hasattr(boundary, 'name'):
                boundary = boundary.name
            
            phase = getattr(event, 'phase', 'STREAM')
            print(f" ├─ [FLOW:{phase}] {boundary}")
            
        print(f" └─ [\033[92mCOLLAPSE\033[0m] Local stream converged.")
    except Exception as e:
        print(f" └─ [\033[91mERROR\033[0m] Local stream failed: {e}")

async def _async_run_flow_proxy(command_name: str, payload: dict):
    """
    @role: Dedicated Flow Receiver
    @desc: Listens to the Redis stream until a COLLAPSE or ERROR event is received.
    """
    r = redis_async.from_url(REDIS_URL, decode_responses=True)
    task_id = f"flow-{uuid.uuid4().hex[:8]}"
    response_channel = f"res:{task_id}"
    log_channel = f"log:{task_id}"

    pubsub = r.pubsub()
    await pubsub.subscribe(response_channel, log_channel)

    # Inject Ψ into the manifold
    trigger_event = PsiEvent(
        event_id=task_id,
        source_id=f"{command_name}:proxy",
        scope="GLOBAL",
        parent_id=None,
        tick=1,
        carrier=PsiCarrier(kind="COMMAND", tag=command_name, payload=payload),
        context={"response_channel": response_channel}
    )
    
    try:
        event_data = asdict(trigger_event)
    except TypeError:
        event_data = trigger_event.__dict__
        
    await r.lpush("runtime:queue", json.dumps(event_data))

    timeout_sec = payload.get("_context", {}).get("timeout", 300.0)

    try:
        # Maintain loop until collapse or timeout
        async with asyncio.timeout(timeout_sec):
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue

                channel = msg["channel"]
                if channel == response_channel:
                    data = json.loads(msg["data"])
                    msg_type = data.get("type", "UNKNOWN")

                    # [Core Fix] Do NOT return on FLOW events; keep streaming
                    if msg_type == "FLOW":
                        phase_name = data.get("phase", "STREAM")
                        boundary = data.get("boundary", "")
                        print(f" ├─ [FLOW:{phase_name}] {boundary}")
                        continue 
                        
                    # Return only on terminating conditions
                    elif msg_type == "COLLAPSE":
                        print(f" └─ [\033[92mCOLLAPSE\033[0m] {data.get('summary', 'Flow converged.')}")
                        return
                        
                    elif msg_type == "ERROR":
                        print(f" └─ [\033[91mERROR\033[0m] {data.get('summary', 'Flow failed.')}")
                        return

    except TimeoutError:
        print(f" └─ [\033[91mTIMEOUT\033[0m] Node failed to converge within {timeout_sec}s.")
    finally:
        await pubsub.unsubscribe(response_channel)
        await r.close()

def execute_flow_cli_task(command_name: str, payload: dict):
    """Triggers the remote proxy and monitors the flow stream."""
    try:
        asyncio.run(_async_run_flow_proxy(command_name, payload))
    except KeyboardInterrupt:
        log.info("[CLI] Flow interrupted by user.")