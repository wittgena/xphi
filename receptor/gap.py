# receptor.gap
"""@flow: ψ → attempt → Φ → limit → cancel → inversion → ψ'"""
import time
import asyncio
import uuid
import random
import redis.asyncio as redis_async
from bound.emitter import get_emitter
from bridge.psi import PsiType
from rhythm.heartbeat import BridgeRhythm

tick_counter = [0]

class Phase(type):
    """
    Phase field container
    queues:
    - void_gap: raw ψ emission field
    - attempt_flow: directional attempt vectors
    - rupture_field: canceled Φ structures
    """
    MAX_LIMIT = 50
    _semaphore = None
    
    void_gap = asyncio.Queue()     
    attempt_flow = asyncio.Queue() 
    rupture_field = asyncio.Queue() 

    def __new__(mcs, name, bases, namespace):
        namespace['bound'] = f"origin.{uuid.uuid4().hex[:4]}"
        return super().__new__(mcs, name, bases, namespace)

    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        try: loop = asyncio.get_running_loop()
        except RuntimeError: return instance

        if cls._semaphore is None: cls._semaphore = asyncio.Semaphore(cls.MAX_LIMIT)

        async def managed_exist():
            async with cls._semaphore: await instance.exist()

        loop.create_task(managed_exist())
        return instance

class Xe(metaclass=Phase):
    """Runtime phase actor"""
    def __init__(self, phase_name="SYSTEM"):
        self.trace_id = f"{self.__class__.__name__}.{uuid.uuid4().hex[:4]}"
        self.bonds = []
        self.log = get_emitter(self.trace_id, phase=phase_name)

    async def exist(self): pass

class GenesisGap(Xe):
    """
    coherence gap detector
    condition: ψᵢ ∉ Φ.commit
    """
    def __init__(self):
        super().__init__(phase_name="GENESIS")
        
    async def exist(self):
        self.log.crit("T0 is unreachable. Coherence gap detected. Bootstrapping.", tick=tick_counter[0])
        for i in range(1, 4):
            await Phase.void_gap.put(f"psi.0.{i}")

class Seeker(Xe):
    """directional attempt node: transforms ψ signals into directional vectors"""
    def __init__(self):
        super().__init__(phase_name="ATTEMPT")
        
    async def exist(self):
        while True:
            psi = await Phase.void_gap.get()
            self.log.info(f"Attempt vector created from '{psi}'", tick=tick_counter[0])
            await asyncio.sleep(0.3)
            await Phase.attempt_flow.put(f"vector({psi})")

class LimitDetector(Xe):
    """
    structural limit detector

    accumulation rule:
    - vectors → Φ structure

    cancellation condition:
    - Φ cannot equal reference anchor (T0)
    """
    def __init__(self):
        super().__init__(phase_name="LIMIT_DETECTOR")
        self.limit_mass = 3
        
    async def exist(self):
        while True:
            vector = await Phase.attempt_flow.get()
            self.bonds.append(vector)
            
            if len(self.bonds) == self.limit_mass:
                phi_structure = f"Phi({uuid.uuid4().hex[:3]})"
                self.log.warn(f"Structure {phi_structure} formed. Testing equality with T0.", tick=tick_counter[0])
                await asyncio.sleep(0.5)
                
                self.log.crit(f"+cancel: {phi_structure} cannot close to reference.", tick=tick_counter[0])
                await Phase.rupture_field.put(phi_structure)
                self.bonds.clear()

class InversionBound(Xe):
    def __init__(self, bridge):
        super().__init__(phase_name="INVERSION_BOUND")
        self.bridge = bridge
        
    async def exist(self):
        while True:
            failed_phi = await Phase.rupture_field.get()

            self.log.crit(f"+rupture: Collapse at center 0. Shattering {failed_phi}", tick=tick_counter[0])
            await asyncio.sleep(0.8)
            
            for _ in range(5): 
                new_psi = f"psi.{uuid.uuid4().hex[:4]}"
                await Phase.void_gap.put(new_psi)

                ## runtime.inject.psi 
                await self.bridge.emit(
                    PsiType(
                        kind="inversion:trigger",
                        tag=new_psi
                    )
                )
            self.log.signal("Inversion completed. New ψ flow emitted.", tick=tick_counter[0])

async def main():
    print("## Topos Genesis (Autopoietic Phase Loop)")

    bridge = BridgeRhythm(redis_url, "origin.inversion")

    ## bootstrap nodes
    GenesisGap()  
    for _ in range(3): Seeker()
    for _ in range(2): LimitDetector()
    for _ in range(2): InversionBound(bridge)
    
    ## system clock
    while True:
        tick_counter[0] += 1
        await asyncio.sleep(1.0)

async def run_on_signal(redis_url):
    redis = redis_async.from_url(redis_url, decode_responses=True)
    pubsub = redis.pubsub()

    await pubsub.subscribe("runtime:signal")
    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue

        data = msg["data"]
        import json
        parsed = json.loads(data)

        ## trigger.origin
        if parsed.get("type") == "origin:run":
            asyncio.create_task(main(redis_url))

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--redis", default="redis://localhost:6379")

    args = parser.parse_args()

    if args.run:
        asyncio.run(main(args.redis))
    else:
        asyncio.run(run_on_signal(args.redis))