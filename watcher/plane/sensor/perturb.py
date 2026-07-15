# watcher.plane.sensor.perturb
import asyncio
import json
import random
import time
import re
from typing import Optional, Any

import arch.topos.bound.tunnel as tunnel
from arch.contract.event.psi import PsiEvent, PsiCarrier
from phase.bind.resolver import resolve_channel, resolve_pattern
from watcher.plane.emitter import get_emitter

class SensorPerturb:
    """
    @role: Bound Elicitor / State Perturbator
    @desc:
    - node_id 기반이 아니라 prefix(domain) 기반 교란
    - xphi pattern을 통해 "현재 활성 경계"에만 작용
    """
    def __init__(self, mq_url: str = "redis://localhost:6379/0"):
        self.mq_url = mq_url
        self.mq: Optional[Any] = None
        self.log = get_emitter("nerve.perturb", phase="nerve")

        ## xphi subscribe pattern
        self.pattern = resolve_pattern()
        self._compiled = re.compile(self.pattern)

    async def connect(self):
        """@flow: Tunnel 어댑터를 통해 백엔드(Redis/Kafka)에 투명하게 연결합니다."""
        self.mq = await tunnel.from_url(self.mq_url)
        self.log.info(f"Perturbator online. pattern={self.pattern}")

    async def strike_domain_tension(self, namespace: str, intensity: float = 1.0):
        channel = resolve_channel(f"{namespace}:intensity")
        payload = {
            "ts": time.time(),
            "kind": "PERTURB",
            "namespace": namespace,
            "intensity": intensity,
        }

        self.log.warn(f"⚡ Domain strike → {namespace} (intensity={intensity})")
        await self.mq.publish(channel, event.to_json())

    async def inject_pattern_event(self):
        """
        @desc: xphi pattern에 매칭되는 domain으로 랜덤 perturb
        - "현재 닿을 수 있는 경계"만 자극
        """
        namespaces = ["psi", "delta", "execution", "xor", "loop", "theoria"]
        namespace = random.choice(namespaces)

        channel = resolve_channel(f"{namespace}:generated") if namespace == "delta" else resolve_channel(f"{namespace}:intensity")

        if not self._compiled.match(channel):
            # pattern 외 영역은 무시 (경계 외부)
            return

        event = PsiEvent(
            event_id=f"perturb-{int(time.time()*1000)}",
            parent_id=None,
            source_id="perturbator",
            scope="PHASE",
            tick=0,
            carrier=PsiCarrier(
                kind="PATTERN_INJECT",
                tag=namespace,
                payload=json.dumps({"noise": random.random()})
            ),
        )
        self.log.signal(f"Pattern inject → {channel}")
        await self.mq.publish(channel, event.to_json())

    async def induce_execution_variance(self, variance: float = 1.0):
        """execution domain 전체에 variance를 주입 → 특정 node가 아니라 실행 위상 자체를 교란"""
        channel = resolve_channel("execution:error_variance")
        payload = {
            "ts": time.time(),
            "variance": variance,
            "kind": "EXECUTION_DRIFT",
        }

        self.log.warn(f"🐢 Execution variance injected ({variance})")
        await self.mq.publish(channel, json.dumps(payload))

    async def observe_bound(self, duration: float = 5.0):
        """
        @flow: perturb 이후 xphi 노드들이 내뿜는 'echo' 채널을 관측
        """
        pubsub = self.mq.pubsub()
        # 자신이 던진 패턴이 아니라, xphi가 응답하는 echo 패턴을 구독
        # (Tunnel 어댑터의 UniversalPubSub가 psubscribe를 처리)
        await pubsub.subscribe("*:echo")

        self.log.info(f"Listening for echoes for {duration}s...")
        start = time.time()

        active = {}

        try:
            while time.time() - start < duration:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if not msg:
                    continue

                channel = msg["channel"]
                if isinstance(channel, bytes):
                    channel = channel.decode()

                ## channel format expected: "execution:echo", "psi:echo"
                prefix = channel.split(":")[0]
                active[prefix] = active.get(prefix, 0) + 1
        finally:
            # 안전한 자원 해제
            await pubsub.close()

        ## 가장 강하게 공명(Echo)한 prefix가 현재 시스템의 Main Boundary (활성 위상)
        if active:
            dominant = max(active.items(), key=lambda x: x[1])
            self.log.info(f"[bound] dominant={dominant[0]} freq={dominant[1]}")
            self.log.warn(f"Current Active Phase Space is: {dominant[0].upper()}")
        else:
            self.log.info("[bound] Void. No resonance detected.")

    async def run_cycle(self):
        """
        @flow: perturb → observe → bound
        """
        await self.inject_pattern_event()
        await asyncio.sleep(0.5)
        await self.observe_bound(duration=3.0)


if __name__ == "__main__":
    async def main():
        p = SensorPerturb()
        await p.connect()
        while True:
            await p.run_cycle()
            await asyncio.sleep(2.0)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n## Perturbator stopped")