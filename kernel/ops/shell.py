# xphi.kernel.ops.shell
import sys
import time
import asyncio
import json
import uuid
import random
from dataclasses import asdict
from typing import Optional, Any

from xphi.arch.dev.wasm.builder import WasmBuilder
from xphi.arch.bound.event.psi import PsiEvent, PsiCarrier, CarrierType
from xphi.arch.bound.event.next import next_id
from xphi.arch.bound.adapter.state import StateAdapter

from xphi.kernel.space.tunnel.factory import TunnelFactory
from xphi.kernel.ops.daemon.bootstrap import KEY_HEARTBEAT_PATTERN, TOPIC_BUS_STREAM
from xphi.kernel.wasm.broker import DphiBroker
from xphi.kernel.wasm.method import DphiMethod

from xphi.state.anchor.nexus import ActorIdentity
from xphi.watcher.plane.emitter import get_emitter

log_surge = get_emitter("attach.surge")
log_inject = get_emitter("attach.inject")
log = get_emitter("shell.entry")

class ComputeStressSurge:
    def __init__(self, broker: Any, capacity: int):
        self.broker = broker
        self.capacity = capacity

    async def ignite(self):
        log_surge.info(f"\n[Compute Stress] Initiating Compute Market Load Generation (Capacity: {self.capacity})")
        code_normal = "print('NORMAL_OK')"
        code_oom_trap = "arr = []\nwhile True: arr.append('A' * 1024 * 1024)"
        code_fp_math = "import math\nprint(sum(math.sin(i)*math.cos(i) for i in range(1000)))"
        
        # Deserialization Bomb (Nested JSON with a depth of 100)
        json_bomb = {"level_0": "payload"}
        for i in range(100): json_bomb = {f"level_{i+1}": json_bomb}

        total_requests = max(200, self.capacity * 4)
        log_surge.info(f" └─ Injecting {total_requests} mixed tasks (Valid vs Resource-Exhaustive/Malicious)...")
        
        tasks = []
        expected_valid = 0
        
        for _ in range(total_requests):
            choice = random.randint(1, 4)
            if choice == 1:
                tasks.append(self.broker.execute(code=code_normal, tier="STANDARD", timeout=5.0))
                expected_valid += 1
            elif choice == 2:
                tasks.append(self.broker.execute(code=code_fp_math, tier="STANDARD", timeout=5.0))
                expected_valid += 1
            elif choice == 3:
                tasks.append(self.broker.execute(code=code_oom_trap, tier="STANDARD", timeout=5.0))
            else:
                tasks.append(self.broker.invoke(DphiMethod.VERIFY_PACKET.value, json_bomb, tier="STANDARD", timeout=5.0))
                
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.time() - start_time) * 1000
        
        successes, trapped = 0, 0
        for res in results:
            if isinstance(res, Exception) or not getattr(res, 'success', False):
                trapped += 1
            else:
                successes += 1
                
        log_surge.info(f" └─ Demand Mix -> Valid Compute: {expected_valid} | Malicious/Exhaustive: {total_requests - expected_valid}")
        log_surge.info(f" └─ Result     -> Processed: {successes} | Isolated/Dropped: {trapped}")
        
        if successes == expected_valid:
            log_surge.info(f" └─ 🟢 [System Resilience] {elapsed:.2f}ms | Perfect Resilience. Malicious tasks isolated instantly.")
        else:
            log_surge.info(f" └─ 🔴 [Degradation] {elapsed:.2f}ms | System bottlenecked. Valid tasks dropped.")

class LedgerSurge:
    def __init__(self, broker: Any, capacity: int):
        self.broker = broker
        self.capacity = capacity
        self.committee = [ActorIdentity(f"Com_{i}") for i in range(3)]
        self.rogue = ActorIdentity("Rogue")

    async def ignite(self):
        log_surge.info(f"\n[Consensus Stress] Bombarding Consensus Engine (Capacity: {self.capacity})")
        
        burst_size = max(150, self.capacity * 3)
        log_surge.info(f" └─ Injecting {burst_size} concurrent SEAL_EPOCH intents (Valid vs Sybil/Rogue)...")
        
        tasks = []
        expected_sealed = 0
        
        for i in range(burst_size):
            parity = StateAdapter.build_parity_triplet("surge_topos", 1, i)
            commit = StateAdapter.build_anchor_commit(parity, 0, "gen", {"repo": "hash"}, {})
            
            choice = random.randint(1, 3)
            if choice == 1: # Valid (2-of-3)
                signers = [self.committee[0], self.committee[1]]
                expected_sealed += 1
            elif choice == 2: # Threshold Fail (1-of-3)
                signers = [self.committee[0]]
            else: # Sybil Attack (Duplicate signatures)
                signers = [self.committee[0], self.committee[0]]
                
            sig_pubs = [s.pubkey_hex for s in signers]
            sigs = [s.sign(commit) for s in signers]
            
            payload = StateAdapter.build_seal_epoch_payload(
                parity, 0, "gen", {"repo": "hash"}, {}, int(time.time()),
                sig_pubs, sigs, 2, [c.pubkey_hex for c in self.committee]
            )
            tasks.append(self.broker.invoke(DphiMethod.SEAL_EPOCH.value, payload, tier="SYSTEM", timeout=10.0))

        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.time() - start_time) * 1000
        
        sealed, rejected = 0, 0
        for res in results:
            if isinstance(res, Exception) or not getattr(res, 'success', False): rejected += 1
            else: sealed += 1
                
        log_surge.info(f" └─ Intent Mix -> Valid Seals: {expected_sealed} | Sybil/Invalid: {burst_size - expected_sealed}")
        log_surge.info(f" └─ Result     -> Sealed: {sealed} | Rejected: {rejected}")
        
        if sealed == expected_sealed:
            log_surge.info(f" └─ 🟢 [Security] {elapsed:.2f}ms | Byzantine defense held up perfectly under load.")
        else:
            log_surge.info(f" └─ 🔴 [Contention] {elapsed:.2f}ms | Ledger deadlock or valid seals dropped.")

class TrafficStressSurge:
    def __init__(self, broker: Any, capacity: int):
        self.broker = broker
        self.capacity = capacity

    async def ignite(self):
        log_surge.info(f"\n[Traffic Stress] Flooding P2P Ingress Pipeline (Capacity: {self.capacity})")
        
        burst_size = max(200, self.capacity * 5)
        log_surge.info(f" └─ Injecting {burst_size} high-frequency RPC Ingress Intents...")
        tasks = [
            self.broker.invoke(DphiMethod.INIT_EPOCH.value, {
                "ts": int(time.time() * 1000) + i, "topo": 777, "press": 5, "rupture": False, "injected_tick": None
            }, tier="SYSTEM", timeout=10.0)
            for i in range(burst_size)
        ]
        
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.time() - start_time) * 1000
        
        successes = sum(1 for r in results if getattr(r, 'success', False))
        overloads = sum(1 for r in results if not getattr(r, 'success', False) and "OVERLOADED" in str(getattr(r, 'error', '')))
        
        handled = successes + overloads
        if handled >= burst_size * 0.9: 
            log_surge.info(f" └─ 🟢 [Throughput] {elapsed:.2f}ms | Ingress Processed {successes} | Shed {overloads}")
        else:
            log_surge.info(f" └─ 🔴 [Overload] {elapsed:.2f}ms | Ingress Processed: {successes}/{burst_size}")

class ChaosEngine:
    def __init__(self, tunnel: Any):
        self.tunnel = tunnel

    async def inject_volumetric_load(self, size: int):
        log_inject.info(f"☄️ [Chaos] Injecting volumetric payload (Size: {size}) into Event Stream...")
        massive_payload = [{"mass": i, "impact": random.random()} for i in range(size)]
        trigger_event = PsiEvent(
            event_id=next_id(), source_id="debug.shell", scope="NETWORK", parent_id=None, tick=1, phase_id=0,
            carrier=PsiCarrier(kind="COMMAND", tag="flow.dynamics", payload={"_context": {"command": "flow.absorb"}}),
            context={"payload": massive_payload}
        )
        
        event_dict = asdict(trigger_event) if hasattr(trigger_event, '__dataclass_fields__') else trigger_event.__dict__
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(event_dict)})
        log_inject.info(" └─ 🌊 Load spike injected. Observe system telemetry and latency metrics in boot logs.")

    async def mutate_node_state(self, node_id: str, new_state: str):
        valid_states = ["ATTRACTOR", "REFLECTOR", "NORMAL"]
        if new_state not in valid_states:
            log_inject.info(f"⚠️ State must be one of: {valid_states}")
            return
            
        log_inject.info(f"🌀 [State] Mutating Node '{node_id}' -> {new_state}...")
        trigger_event = PsiEvent(
            event_id=next_id(), source_id="debug.shell", scope="SYSTEMIC", parent_id=None, tick=1, phase_id=0,
            carrier=PsiCarrier(kind="MUTATE", tag="ATOR_STATE", payload={"ator_id": node_id, "state": new_state}),
            context={}
        )
        
        event_dict = asdict(trigger_event) if hasattr(trigger_event, '__dataclass_fields__') else trigger_event.__dict__
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(event_dict)})
        log_inject.info(" └─ 🌌 State mutation applied. Local consensus adjustment expected.")

    async def trigger_system_overload(self):
        log_inject.info("💥 [Chaos: Overload] Forcing systemic tension spike (Tension = 1.5)...")
        
        trigger_event = PsiEvent(
            event_id=next_id(), source_id="debug.shell", scope="SYSTEMIC", parent_id=None, tick=1, phase_id=0,
            carrier=PsiCarrier(kind="INJECT", tag="TENSION_SPIKE", payload={"target_node": "0", "tension": 1.5}),
            context={}
        )
        
        event_dict = asdict(trigger_event) if hasattr(trigger_event, '__dataclass_fields__') else trigger_event.__dict__
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(event_dict)})
        log_inject.info(" └─ ⚡ Overload injected. Waiting for BoundObserver -> Epoch.flip (Forced Transition) log.")

class ClusterManager:
    def __init__(self, tunnel):
        self.tunnel = tunnel
        self.broker = DphiBroker()
        self.worker_count = 0
        self.total_capacity = 0

    async def refresh_cluster_state(self) -> bool:
        active_keys = await self.tunnel.keys(KEY_HEARTBEAT_PATTERN)
        if not active_keys:
            self.worker_count, self.total_capacity = 0, 0
            return False
            
        self.worker_count, self.total_capacity = 0, 0
        for key in active_keys:
            raw_meta = await self.tunnel.get(key)
            if not raw_meta: continue
            try:
                meta_str = raw_meta.decode('utf-8') if isinstance(raw_meta, bytes) else raw_meta
                meta = json.loads(meta_str)
                if meta.get("role") == "worker":
                    self.worker_count += 1
                    self.total_capacity += int(meta.get("capacity", 0))
            except Exception: 
                pass
            
        return self.total_capacity > 0

    async def close(self):
        await self.broker.close()
        log.info("🔌 [Manifold] Broker detached. System resources released.")

class ShellEntry:
    def __init__(self, tunnel):
        self.tunnel = tunnel
        self.manifold = ClusterManager(tunnel)
        self.running = True
        self.physics_injector = ChaosEngine(tunnel)

    async def _print_header(self):
        await self.manifold.refresh_cluster_state()
        log.info("\n" + "="*75)
        log.info(" 🌌 [\033[95mPHASE Cluster Administration & Diagnostics Console\033[0m]")
        log.info(f" ⚙️  Status: \033[92mOnline\033[0m | Workers: {self.manifold.worker_count} | Max Capacity: {self.manifold.total_capacity}")
        log.info("="*75)
        log.info(" [Pre-flight & Observation]")
        log.info("  \033[96mbuild\033[0m        : Compile WASM artifacts and trace integrity")
        log.info("  \033[96mnodes\033[0m        : Scan cluster heartbeats and available capacity")
        log.info("  \033[96mping\033[0m         : Broadcast echo (system:ping)")
        log.info(" [Administrative Actions]")
        log.info("  \033[93mreload \033[0m : Inject hot-reload intent (e.g., reload eco.mesh)")
        log.info("  \033[93mexec \033[0m   : Dispatch CLI intent to Master Node (e.g., exec align)")
        log.info(" [Application Layer Stress Testing (Surge)]")
        log.info("  \033[91msurge \033[0m : Simulate burst traffic on specific mesh (type: \033[90meco, ledger, market\033[0m)")
        log.info(" [System Level Fault Injection (Chaos)]")
        log.info("  \033[95minject \033[0m: Inject systemic chaos / structural anomalies (type: \033[90mkinetic, ator, rupture\033[0m)")
        log.info("  \033[90mexit / quit\033[0m  : Terminate console")
        log.info("-" * 75)

    async def run(self):
        await self._print_header()
        while self.running:
            cmd_line = await asyncio.to_thread(input, "\033[93madmin>\033[0m ")
            if not cmd_line.strip(): 
                continue
            await self.process_command(cmd_line.strip())

    async def process_command(self, cmd_line: str):
        parts = cmd_line.split()
        cmd = parts[0].lower()

        try:
            if cmd in ["exit", "quit"]:
                self.running = False
                
            elif cmd == "build":
                log.info("🔨 [Pre-flight] Initiating WasmBuilder Trace...")
                builder = WasmBuilder()
                await builder.trace()
                if builder.rupture_confirmed: 
                    log.error("❌ WASM Artifact build failed. Structural rupture confirmed.")
                else: 
                    log.info("✅ WASM Artifacts (phase.wasm, dvm.wasm) are armed and ready.")
            elif cmd == "nodes":
                await self.manifold.refresh_cluster_state()
                log.info(f"📊 Active Compute Market: {self.manifold.worker_count} Workers / {self.manifold.total_capacity} Slots.")
            elif cmd == "surge":
                if len(parts) < 2:
                    log.info("⚠️ Usage: surge ")
                    return
                target = parts[1].lower()
                await self.manifold.refresh_cluster_state()
                cap = max(self.manifold.total_capacity, 40)
                
                if target == "eco":
                    await self.manifold.broker.update_policy("SYSTEM")
                    await TrafficStressSurge(self.manifold.broker, cap).ignite()
                elif target == "ledger":
                    await self.manifold.broker.update_policy("SYSTEM")
                    await LedgerSurge(self.manifold.broker, cap).ignite()
                elif target == "market":
                    await self.manifold.broker.update_policy("STANDARD")
                    await ComputeStressSurge(self.manifold.broker, cap).ignite()
                    await self.manifold.broker.update_policy("SYSTEM") # Revert to SYSTEM policy
                else:
                    log.info(f"⚠️ Unknown surge target: {target}")
            elif cmd == "inject":
                if len(parts) < 2:
                    log.info("⚠️ Usage: inject  [args]")
                    return
                target = parts[1].lower()
                
                if target == "kinetic":
                    size = int(parts[2]) if len(parts) > 2 else 100
                    await self.physics_injector.inject_volumetric_load(size)
                elif target == "ator":
                    if len(parts) < 4:
                        log.info("⚠️ Usage: inject ator  ")
                        return
                    await self.physics_injector.mutate_node_state(parts[2], parts[3].upper())
                elif target == "rupture":
                    await self.physics_injector.trigger_system_overload()
                else:
                    log.info(f"⚠️ Unknown inject target: {target}")
            elif cmd == "reload":
                if len(parts) < 2: 
                    log.info("⚠️ Usage: reload ")
                    return
                await self._inject_reload(parts[1])
            elif cmd == "exec":
                if len(parts) < 2: 
                    log.info("⚠️ Usage: exec  [args...]")
                    return
                await self._inject_cli_command(parts[1], parts[2:])
            elif cmd == "ping":
                await self._inject_ping()
            else:
                log.info(f"⚠️ Unknown command: {cmd}")
        except Exception as e:
            log.info(f"❌ Error executing command: {e}")

    async def _inject_reload(self, module_fqn: str):
        sync_event = PsiEvent(
            event_id=next_id(), parent_id=None, source_id="debug.shell", scope="GLOBAL", tick=0, phase_id=0, context={},
            carrier=PsiCarrier(kind="system:topology", tag="reload", payload={"module_fqn": module_fqn}, carrier_type=CarrierType.FIXED)
        )
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(sync_event.__dict__)})
        log.info(f"📡 Broadcasted topology reload for '\033[96m{module_fqn}\033[0m'")

    async def _inject_ping(self):
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe("system:echo")
        log.info("🦇 Emitting system:ping...")
        await self.tunnel.publish("system:ping", json.dumps({"source": "debug.shell"}))
        try:
            async with asyncio.timeout(2.0):
                async for msg in pubsub.listen():
                    if msg["type"] == "message":
                        data = json.loads(msg["data"])
                        log.info(f"  [ECHO] Received from: {data}")
        except asyncio.TimeoutError:
            log.info("⏳ Echo collection timeout (2.0s).")
        finally:
            await pubsub.close()

    async def _inject_cli_command(self, command: str, args: list):
        """Publish COMMAND event to Master Node's Control Bus and track execution logs."""
        task_id = f"debug-{uuid.uuid4().hex[:8]}"
        response_channel = f"res:{task_id}"
        
        payload = { 
            "_context": {
                "command": command, 
                "cli_args": args,
                "timeout": 30.0
            } 
        }
        
        trigger_event = PsiEvent(
            event_id=task_id, source_id="debug.shell", scope="GLOBAL", parent_id=None, tick=1, phase_id=0,
            carrier=PsiCarrier(kind="COMMAND", tag=command, payload=payload),
            context={"response_channel": response_channel}
        )
        
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(response_channel)
        
        event_dict = asdict(trigger_event) if hasattr(trigger_event, '__dataclass_fields__') else trigger_event.__dict__
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(event_dict)})
        log.info(f"🚀 Injected COMMAND '{command}' -> Stream. Listening on '{response_channel}'...")
        
        try:
            async with asyncio.timeout(30.0):
                async for msg in pubsub.listen():
                    if msg["type"] == "message":
                        result = json.loads(msg["data"])
                        status = result.get("status", "UNKNOWN")
                        color = "\033[92m" if status == "SUCCESS" else "\033[91m"
                        log.info(f"\n{color}[{status}]\033[0m {result.get('summary', '')}")
                        break
        except asyncio.TimeoutError:
            log.info("⏳ Execution timeout (Node did not reply within 30s).")
        finally:
            await pubsub.close()


async def main():
    tunnel = await TunnelFactory.get_default()
    shell = ShellEntry(tunnel)
    
    try:
        await shell.run()
    finally:
        await shell.manifold.close()
        await tunnel.close()
        log.info("System resources released.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("\nExiting Console...")