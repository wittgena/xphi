# rhythm.heartbeat
"""
@desc: 정렬된 자율 진동 하트비트 루프
@metaphor: SA Node $\rightarrow$ Cardiac Cycle $\rightarrow$ External ECG Transduction
"""
import asyncio
import uuid
import time
import random
import json
from typing import Dict, Any, Optional
import redis.asyncio as redis_async
from bridge.event.psi import PsiEvent, PsiCarrier
from bound.plane import BoundPlane
from bridge.rhythm import BridgeRhythm
from flow.surface.emitter import get_emitter

class PhaseField(type):
    """
    @scale: macro-field
    @registry: Track all active heart cell instances for shock propagation
    @flow: pulse generation → conduction → ventricular contraction → inversion → reflow
    """
    MAX_LIMIT = 50
    _semaphore = None
    _instances = [] 

    gap_node = asyncio.Queue()      
    conduction_flow = asyncio.Queue()  
    ventricular_field = asyncio.Queue() 
    global_tick = 0

    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        PhaseField._instances.append(instance)
        
        if cls._semaphore is None:
            cls._semaphore = asyncio.Semaphore(cls.MAX_LIMIT)

        async def managed_exist():
            async with cls._semaphore:
                try:
                    await instance.exist()
                finally:
                    if instance in PhaseField._instances:
                        PhaseField._instances.remove(instance)

        asyncio.create_task(managed_exist())
        return instance

class Xe(metaclass=PhaseField):
    """@scale: cellular-unit"""
    def __init__(self, phase_name="HEART", bridge=None):
        self.trace_id = f"{self.__class__.__name__}.{uuid.uuid4().hex[:4]}"
        self.bridge = bridge
        self.log = get_emitter(self.trace_id, phase=phase_name)
        self.potential = 0.0

    async def shock_reset(self):
        """@phase: phase reset (Defibrillation)"""
        self.potential = 0.0
        self.log.warn(f"[⚡] Shock applied: Phase reset for {self.trace_id}")

    async def emit_external(self, kind: str, tag: str, payload: dict = None, parent_id: str = None) -> PsiEvent:
        """@phase: transduction (Internal -> External)"""
        PhaseField.global_tick += 1
        carrier = PsiCarrier(kind=kind, tag=tag, payload=payload or {"timestamp": time.time()})
        event = PsiEvent(
            event_id=f"evt-{uuid.uuid4().hex[:6]}",
            parent_id=parent_id,
            source_id=self.trace_id,
            scope="GLOBAL",
            tick=PhaseField.global_tick,
            carrier=carrier,
            context={"domain": "heartbeat", "phase": "rhythm"}
        )
        if self.bridge:
            await self.bridge.emit(event)
        return event

    async def exist(self): pass

class SAPacemaker(Xe):
    """@phase: SA Node (Self-oscillation)"""
    def __init__(self, threshold=1.0, leakage=0.15, **kwargs):
        super().__init__(phase_name="SA_NODE", **kwargs)
        self.threshold = threshold
        self.leakage = leakage

    async def exist(self):
        while True:
            self.potential += self.leakage + random.uniform(-0.01, 0.01)
            if self.potential >= self.threshold:
                self.potential = 0.0
                pulse_id = f"beat.{uuid.uuid4().hex[:4]}"
                
                # 이벤트 발행 후 해당 ID를 인과성(parent_id) 추적을 위해 큐에 함께 담음
                event = await self.emit_external(kind="PULSE", tag="HEART:SA_NODE", payload={"pulse_id": pulse_id})
                
                # [튜닝 2] 다음 노드(AV)가 불응기(큐가 참)라면 펄스를 억지로 밀어넣지 않고 소멸시킴
                # await PhaseField.gap_node.put({"id": pulse_id, "parent_id": event.event_id})
                try:
                    PhaseField.gap_node.put_nowait({"id": pulse_id, "parent_id": event.event_id})
                    BoundPlane.record(time.time(), "SA_NODE", f"[♥] Pulse Fired: {pulse_id}", "SYS")
                except asyncio.QueueFull:
                    # AV Node가 아직 이전 맥박을 처리 중임 -> 기외수축 무시 (Refractory)
                    self.log.debug(f"Pulse {pulse_id} dropped (Refractory Period)")

                BoundPlane.record(time.time(), "SA_NODE", f"[♥] Pulse Fired: {pulse_id}", "SYS")
            await asyncio.sleep(0.1)

class AVConduction(Xe):
    """@phase: Conduction delay"""
    async def exist(self):
        while True:
            data = await PhaseField.gap_node.get()
            await asyncio.sleep(0.2) 
            
            vector_id = f"vector({data['id']})"
            event = await self.emit_external(
                kind="CONDUCTION", tag="HEART:AV_NODE", 
                payload={"vector_id": vector_id}, parent_id=data["parent_id"]
            )
            try:
                PhaseField.conduction_flow.put_nowait({"id": vector_id, "parent_id": event.event_id})
            except asyncio.QueueFull:
                pass

class VentricleSystole(Xe):
    """@phase: Systole (Contraction)"""
    async def exist(self):
        while True:
            data = await PhaseField.conduction_flow.get()
            await asyncio.sleep(0.4) 
            
            phi_id = f"Phi({uuid.uuid4().hex[:3]})"
            event = await self.emit_external(
                kind="SYSTOLE", tag="HEART:VENTRICLE", 
                payload={"phi_id": phi_id}, parent_id=data["parent_id"]
            )
            
            try:
                PhaseField.ventricular_field.put_nowait({"id": phi_id, "parent_id": event.event_id})
            except asyncio.QueueFull:
                pass
            BoundPlane.record(time.time(), "SYSTOLE", f"[♥] Contraction: {phi_id}", "INFO")

class DiastoleInversion(Xe):
    """@phase: Diastole (Recovery)"""
    async def exist(self):
        while True:
            data = await PhaseField.ventricular_field.get()
            await asyncio.sleep(0.3)

            # [튜닝 3] 재진입(Re-entry) 억제:
            # for _ in range(2):
            #     reflow_id = f"reflow.{uuid.uuid4().hex[:4]}"
            #     await PhaseField.gap_node.put({"id": reflow_id, "parent_id": data["parent_id"]})
            BoundPlane.record(time.time(), "DIASTOLE", "[♥] Inversion complete.", "SYS")

async def run_heartbeat(redis_url: str):
    """@scale: macro-orchestration"""
    print("## Topos Autonomous Heart Online")
    bridge = BridgeRhythm(redis_url, "rhythm.heart")

    SAPacemaker(bridge=bridge)
    AVConduction(bridge=bridge)
    VentricleSystole(bridge=bridge)
    DiastoleInversion(bridge=bridge)

    while True:
        await asyncio.sleep(1)
        print(f"[Monitor] Heartbeat active. Population: {len(PhaseField._instances)} cells | Tick: {PhaseField.global_tick}")

async def run_on_signal(redis_url: str):
    """@phase: External signal integration (Shock / Pacing / Run)"""
    redis = redis_async.from_url(redis_url, decode_responses=True)
    pubsub = redis.pubsub()
    await pubsub.subscribe("runtime:signal")
    
    print("[System] Heartbeat Signal Listener Online. Waiting for triggers...")
    heart_task = None

    async for msg in pubsub.listen():
        if msg["type"] != "message": continue
        
        try:
            data = json.loads(msg["data"])
            sig_type = data.get("type")

            ## Shock: Global Phase Reset
            if sig_type == "heart:shock":
                BoundPlane.record(time.time(), "SHOCK", "[⚡] EMERGENCY SHOCK: Global Phase Reset", "CRIT")
                tasks = [inst.shock_reset() for inst in PhaseField._instances]
                if tasks: await asyncio.gather(*tasks)
                
                for q in (PhaseField.gap_node, PhaseField.conduction_flow, PhaseField.ventricular_field):
                    while not q.empty(): q.get_nowait()

            ## Pace: External demand pacing
            elif sig_type == "heart:pace":
                BoundPlane.record(time.time(), "PACER", "[External] Demand Pacing", "WARN")
                await PhaseField.gap_node.put({
                    "id": f"pulse.pacer.{uuid.uuid4().hex[:4]}", 
                    "parent_id": "ext-pacer-event"
                })

            ## Run: Initial start
            elif sig_type == "origin:run":
                if not PhaseField._instances and heart_task is None:
                    heart_task = asyncio.create_task(run_heartbeat(redis_url))
                else:
                    print("[System] Heart is already beating.")

        except Exception as e:
            print(f"[Signal Error] {e}")

async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true", help="Start heartbeat immediately")
    parser.add_argument("--redis", default="redis://localhost:6379")
    args = parser.parse_args()

    if args.run:
        listener_task = asyncio.create_task(run_on_signal(args.redis))
        heart_task = asyncio.create_task(run_heartbeat(args.redis))
        await asyncio.gather(listener_task, heart_task)
    else:
        await run_on_signal(args.redis)

if __name__ == "__main__":
    asyncio.run(main())