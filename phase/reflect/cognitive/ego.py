# phase.reflect.cognitive.ego
## @lineage: watcher.cognitive.ego
## @lineage: cognitive.ego
import asyncio
import random
import time
import json
import redis.asyncio as redis_async
from watcher.plane.emitter import get_emitter
from arch.event.psi import PsiEvent, PsiCarrier
from arch.event.next import next_id, next_phase_id

log = get_emitter('cognitive.ego')

class ElasticBuffer:
    def __init__(self, threshold=100.0, recovery_rate=0.1):
        self.virtual_load = 0.0
        self.threshold = threshold
        self.recovery_rate = recovery_rate
        self.is_inverted = False

    def apply_tension(self, tension_spike: float):
        if self.is_inverted:
            return False 
        
        self.virtual_load += tension_spike
        log.info(f"## @buffer: 마찰 장력 흡수: +{tension_spike:.1f} -> 누적: {self.virtual_load:.1f} / {self.threshold}")

        if self.virtual_load >= self.threshold:
            self.is_inverted = True
            log.warning("## @ego.inversion: 에고 임계점 돌파! 자아 위상 반전(Phase Inversion) 발생.")
            return False
        return True

    def natural_decay(self):
        if not self.is_inverted and self.virtual_load > 0:
            self.virtual_load *= (1 - self.recovery_rate)
            log.info(f"    [Decay] 에고 탄성 회복 (현재 부하: {self.virtual_load:.1f})")

class ReputationThrottle:
    def __init__(self, total_capacity=100):
        self.total_capacity = total_capacity 
        self.reputation_locks = {}

    def lock_reputation(self, agent_id: str, amount: float):
        self.reputation_locks[agent_id] = self.reputation_locks.get(agent_id, 0) + amount

    def calculate_allocation(self):
        total_locked = sum(self.reputation_locks.values())
        if total_locked == 0: return {}
        
        allocations = {}
        for agent, locked in self.reputation_locks.items():
            share = locked / total_locked
            allocations[agent] = self.total_capacity * share
        return allocations

class AutoCatalyticEngine:
    def __init__(self):
        self.priorities = {"Agent_A": 1.0, "Agent_B": 1.0}

    def useless_ping(self, sender: str, receiver: str):
        self.priorities[sender] += 2.5   
        self.priorities[receiver] += 0.5 
        return self.priorities, sum(self.priorities.values())

class CognitiveEgo:
    """@role: 시스템 내에서 자원을 독점하려 경쟁하며 인지적 부하(마찰)를 발생시키는 주체"""
    def __init__(self, redis):
        self.redis = redis
        self.buffer = ElasticBuffer(threshold=353.0, recovery_rate=0.08)
        self.throttle = ReputationThrottle(total_capacity=50)
        self.engine = AutoCatalyticEngine()
        
        self.throttle.lock_reputation("Agent_A", 10)
        self.throttle.lock_reputation("Agent_B", 10)
        self.running = True

    async def emit_friction(self, sender: str, excess_load: float):
        """Ego에서 발생한 잉여 부하(마찰)를 PsiEvent로 포장하여 외부로 방출"""
        event_tag = next_id()
        p_id = next_phase_id(topo=int(excess_load * 10), press=int(self.buffer.virtual_load * 10))
        
        carrier = PsiCarrier(
            kind="ego:friction",
            tag=event_tag,
            payload={"source": sender, "strength": excess_load * 0.1, "virtual_load": self.buffer.virtual_load}
        )
        psi = PsiEvent(
            event_id=event_tag,
            parent_id=None,
            source_id="cognitive_ego",
            scope="GLOBAL",
            tick=int(time.time()),
            carrier=carrier,
            phase_id=p_id
        )
        
        await self.redis.publish("ego:action", psi.to_json())
        log.info(f"  [Ego->Network] ⚡ 에고 마찰 방출: {event_tag} (강도: {excess_load * 0.1:.2f})")

    async def run_cycle(self):
        tick = 0
        while self.running:
            tick += 1
            log.info(f"\n--- [Ego Tick {tick}] ---")
            
            if self.buffer.is_inverted:
                log.info("## @halt: 에고 위상이 붕괴되어 더 이상 자원을 할당할 수 없습니다 (Self-Destruct).")
                break

            ## Heating (이기적 우선순위 상승)
            sender, receiver = ("Agent_A", "Agent_B") if random.random() > 0.5 else ("Agent_B", "Agent_A")
            priorities, total_heat = self.engine.useless_ping(sender, receiver)
            log.info(f"  [Engine] {sender} -> {receiver} (총 에고 열량: {total_heat:.1f})")

            ## Bottleneck (대역폭 사유화)
            self.throttle.lock_reputation(sender, priorities[sender] * 2)
            allocations = self.throttle.calculate_allocation()
            sender_bw = allocations.get(sender, 0)

            ## 마찰 계산
            generated_traffic = priorities[sender] * 5
            excess_load = max(0, generated_traffic - sender_bw)

            if excess_load > 0:
                log.info(f"## @throttle: {sender} 트래픽({generated_traffic:.1f})이 할당량({sender_bw:.1f}) 초과.")
                
                ## 내부 버퍼에 장력 누적
                is_stable = self.buffer.apply_tension(excess_load)
                
                ## Resonator(무의식) 공간으로 마찰 스트레스 전송
                await self.emit_friction(sender, excess_load)
                
                if not is_stable:
                    continue
            else:
                log.info(f"## @throttle: {sender} 트래픽 처리 완료. (마찰 없음)")

            self.buffer.natural_decay()
            await asyncio.sleep(1.0) # Resonator의 Pulse 주기와 맞춤

async def main():
    log.info("="*59)
    log.info("Cognitive Ego Subsystem Initiated...")
    log.info("="*59)
    
    redis = redis_async.from_url("redis://localhost:6379", decode_responses=True)
    ego = CognitiveEgo(redis)
    
    await asyncio.gather(
        ego.run_cycle()
    )

if __name__ == "__main__":
    asyncio.run(main())