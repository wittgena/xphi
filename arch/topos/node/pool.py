# arch.topos.node.pool
import time
import uuid
import asyncio
import random
import json
from typing import Optional, Any

from arch.contract.event.psi import PsiEvent, PsiCarrier
from arch.model.phase.flow import PhaseFlow
from kernel.phase.daemon.bootstrap import TOPIC_BUS_STREAM, KEY_HEARTBEAT_PREFIX
from kernel.phase.runtime.node import runtime_keys
from watcher.plane.emitter import get_emitter

log = get_emitter("node.pool")

class NodeProxy:
    def __init__(self, role: str, target_node_id: str, runtime, main_loop: asyncio.AbstractEventLoop):
        self.role = role
        self.target_node_id = target_node_id
        self.runtime = runtime
        self.main_loop = main_loop

    async def _dispatch_async(self, flow: PhaseFlow):
        try:
            carrier = PsiCarrier(
                kind="DELEGATION",
                tag=self.role,
                payload={
                    "target_node": self.target_node_id,
                    "flow_payload": flow.payload,
                    "flow_aspect": getattr(flow, "aspect", "default")
                }
            )
            event = PsiEvent(
                event_id=f"proxy-{uuid.uuid4().hex[:8]}",
                source_id=self.runtime.node_id if hasattr(self.runtime, 'node_id') else "unknown",
                scope="GLOBAL",
                parent_id=None,
                tick=1,
                carrier=carrier
            )
            
            event_json = json.dumps(event.to_dict() if hasattr(event, 'to_dict') else event.__dict__)
            tunnel = self.runtime.tunnel
            if not tunnel:
                raise RuntimeError("NodeProxy requires a connected tunnel.")
                
            await tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": event_json})
            log.debug(f"[RemoteProxy] Successfully dispatched flow to {self.target_node_id} via Stream (Role: {self.role})")
        except Exception as e:
            log.error(f"[RemoteProxy] Dispatch failed for {self.target_node_id}: {e}")
            raise

    def __call__(self, flow: PhaseFlow):
        future = asyncio.run_coroutine_threadsafe(
            self._dispatch_async(flow),
            self.main_loop
        )
        return future

class NodePool:
    def __init__(self, runtimeNode):
        self.runtime = runtimeNode
        try:
            self.main_loop = asyncio.get_running_loop()
        except RuntimeError:
            self.main_loop = asyncio.get_event_loop()

    @property
    def tunnel(self):
        if not self.runtime or not hasattr(self.runtime, 'tunnel') or self.runtime.tunnel is None:
            log.warning("[NodePool] Accessing tunnel before node.start().")
            return None
        return self.runtime.tunnel

    def get(self, role: str) -> NodeProxy:
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._resolve_with_retry_or_spawn(role), 
                self.main_loop
            )
            target_node_id = future.result(timeout=15.0) 
            return NodeProxy(role, target_node_id, self.runtime, self.main_loop)
        except Exception as e:
            log.error(f"NodePool totally failed to provide capability '{role}': {e}")
            raise

    async def _resolve_with_retry_or_spawn(self, role: str, max_retries=3, delay=2.0) -> str:
        for attempt in range(max_retries):
            target = await self._resolve_target(role)
            if target:
                return target
                
            log.warning(f"[Registry Miss] No agents for '{role}'. Retrying in {delay}s... ({attempt+1}/{max_retries})")
            await asyncio.sleep(delay)

        log.warning(f"[Auto-Provision] Exhausted retries for '{role}'. Spawning a local fallback agent...")
        return await self._spawn_fallback_agent(role)

    async def _spawn_fallback_agent(self, role: str) -> str:
        """요구되는 역량을 수행할 수 있는 임시 워커를 생성하여 Redis에 등록"""
        fallback_id = f"node-fallback-{role}-{uuid.uuid4().hex[:6]}"
        
        tunnel = self.tunnel
        if not tunnel:
            raise RuntimeError("Cannot spawn fallback agent without an active tunnel.")
            
        capability_key = f"capability:{role}"
        emit_idx_key = runtime_keys.get("index_emits", emit_key=capability_key)
        await tunnel.sadd(emit_idx_key, fallback_id)
        
        await tunnel.set(f"{KEY_HEARTBEAT_PREFIX}{fallback_id}", int(time.time()), ex=60)
        log.info(f"[Auto-Provision] Fallback agent '{fallback_id}' successfully spawned and registered.")
        return fallback_id

    async def _resolve_target(self, role: str) -> Optional[str]:
        tunnel = self.tunnel
        
        retry_count = 0
        while tunnel is None and retry_count < 5:
            log.info("[NodePool] Waiting for Tunnel to be initialized...")
            await asyncio.sleep(1.0)
            tunnel = self.tunnel
            retry_count += 1

        if tunnel is None:
            log.error("[NodePool] Tunnel is not available after wait.")
            return None

        capability_key = f"capability:{role}"
        index_key = runtime_keys.get("index_emits", emit_key=capability_key)
        capable_nodes = await tunnel.smembers(index_key)
        if not capable_nodes:
            return None
            
        alive_nodes = []
        for n_id in capable_nodes:
            n_id_str = n_id.decode('utf-8') if isinstance(n_id, bytes) else n_id
            if await tunnel.exists(f"{KEY_HEARTBEAT_PREFIX}{n_id_str}"):
                alive_nodes.append(n_id_str)
                
        if not alive_nodes:
            return None

        return random.choice(alive_nodes)