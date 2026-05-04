# bound.ext.feed
import asyncio
import json
import math
import random
import time
import redis.asyncio as redis_async

# 시스템의 글로벌 신경망(SSOT)
REDIS_URL = "redis://localhost:6379"

class ExtFeed:
    """
    @role: 장의 지표를 위상 공간의 중력(Phase)으로 번역하는 외부 에이전트
    @flow: field Data → Normalization(0~2π) → PsiEvent(ATTRACT_PHASE) → Redis Queue
    """
    def __init__(self, redis_url=REDIS_URL):
        self.redis_url = redis_url
        self.redis = None
        self.current_scalar = 60000.0 
        self.base_scalar = 60000.0

    async def connect(self):
        self.redis = redis_async.from_url(self.redis_url, decode_responses=True)
        print("[Oracle] Connected to Global Redis Matrix.")

    async def fetch_field_data(self) -> float:
        """현실의 랜덤워크(Random Walk) 변동성을 모방"""
        await asyncio.sleep(0.1) # I/O 모방
        volatility = random.uniform(-0.015, 0.015)
        self.current_scalar *= (1 + volatility)
        return self.current_scalar

    def normalize_to_phase(self, scalar: float) -> float:
        """
        @transduction: 선형적인 데이터를 순환하는 위상(Phase)으로 투영
        - ExchangeSensor는 math.sin(phase) 값을 통해 🟢(1.0) ~ 🔴(-1.0)을 판단
        - scalar가 높으면 phase를 π/2 (sin값 1.0)에 가깝게, 낮으면 3π/2 (sin값 -1.0)에 가깝게 매핑
        """
        ## 변화율 계산 (예: -5% ~ +5% 구간)
        momentum = (scalar - self.base_scalar) / self.base_scalar
        
        ## 모멘텀을 -1.0 ~ 1.0 사이로 클리핑
        normalized = max(-1.0, min(1.0, momentum * 20)) # 5% 변화 시 최대치 도달
        
        ## 위상각(Radian)으로 변환: -1.0 -> -π/2 (강한 매도), 1.0 -> π/2 (강한 매수)
        ## sin(phase)가 이 normalized 값을 그대로 반환하도록 역함수(asin) 적용
        phase = math.asin(normalized)
        
        ## 파이썬 math 모듈 체계에 맞게 0 ~ 2π로 보정
        if phase < 0:
            phase += 2 * math.pi
            
        return phase

    async def run_injection_loop(self, interval: float = 2.0):
        """@injection: 일정한 주기(Tick)마다 현실의 충격을 위상계로 주입"""
        await self.connect()
        print(">>> Oracle Initiating Reality Injection Pulse... <<<")
        
        tick = 1
        try:
            while True:
                # 1. 현실 데이터 스캔 및 번역
                raw_value = await self.fetch_field_data()
                phase_val = self.normalize_to_phase(raw_value)
                
                # 2. 오라클 펄스(PsiEvent) 조립
                # 이 이벤트는 시장의 '닻(Market Maker)'인 ATTRACTOR 노드들을 정밀 타격합니다.
                event_dict = {
                    "event_id": f"oracle-tick-{tick}",
                    "parent_id": None,
                    "source_id": "system.oracle",
                    "scope": "GLOBAL",
                    "tick": tick,
                    "phase_id": 0,  # 글로벌 큐를 타므로 phase_id는 0으로 둔탁하게 던짐
                    "carrier": {
                        "kind": "ATTRACT_PHASE", 
                        "tag": "MARKET_ORACLE",
                        "payload": {
                            # ExchangeSensor의 'ATTRACTOR'로 설정된 노드들의 ID 지정
                            "target_nodes": ["trader_0", "trader_10", "trader_20"], 
                            "phase": phase_val,
                            "raw_value": raw_value
                        }
                    },
                    "context": {"domain": "oracle", "source": "market_api"}
                }

                ## 글로벌 큐로 승격 (NodeRuntime이 이를 낚아챔)
                await self.redis.lpush("runtime:queue", json.dumps(event_dict))
                
                ## 시각적 로깅
                trend = "🟢 BULL" if raw_value > self.base_scalar else "🔴 BEAR"
                print(f"[Oracle Pulse] scalar: ${raw_value:,.2f} {trend} | Injected Phase: {phase_val:.3f} rad")

                tick += 1
                await asyncio.sleep(interval)
                
        except asyncio.CancelledError:
            print("[Oracle] Injection loop stopped.")
        except Exception as e:
            print(f"[Oracle] Critical Exception: {e}")

if __name__ == "__main__":
    oracle = ExtFeed()
    try:
        asyncio.run(oracle.run_injection_loop(interval=1.5))
    except KeyboardInterrupt:
        print("\n[Oracle] System Shutdown.")