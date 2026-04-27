# phi.spec.observer
"""
@flow: Φ → source → ψ(t) → bound → trace
"""
import asyncio
import random
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

class RateLimitExceeded(Exception):
    pass

@dataclass
class TimePoint:
    date: str
    value: float

class PhiInterpreter:
    def __init__(self):
        self.meanings = {
            "sigma": "(Σ) baseline stability carrier",
            "delta": "(Δ) structural transition vector",
            "omega": "(Ω) terminal attractor signal",
            "lambda": "(Λ) adaptive modulation field",
            "theta": "(Θ) rotational phase signal"
        }

    def get_signals(self) -> Dict[str, str]:
        return self.meanings

class AsyncSourceProvider:
    def __init__(self, max_concurrent: int = 3):
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def _mock_api_call(self, signal_id: str, steps: int) -> List[TimePoint]:
        await asyncio.sleep(random.uniform(0.2, 0.7))
        if random.random() < 0.20:
            raise RateLimitExceeded(f"429 RateLimit for {signal_id}")

        trajectory = []
        current = 100.0
        base_date = datetime.now() - timedelta(days=steps)
        volatility = 0.05 if signal_id in ["delta", "omega"] else 0.02
        for i in range(steps):
            date_str = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            current = current * (1 + random.uniform(-volatility, volatility))
            trajectory.append(TimePoint(date=date_str, value=round(current, 2)))
        return trajectory

    async def fetch_with_retry(self, signal_id: str, steps: int):
        for attempt in range(1, 4):
            async with self.semaphore:
                try:
                    return await self._mock_api_call(signal_id, steps)
                except RateLimitExceeded:
                    wait = 2 ** attempt
                    print(f"    [source] rate-limit → retry {wait}s")
                    await asyncio.sleep(wait)
        return None

class PsiTrajectory:
    def __init__(self, data: List[TimePoint]):
        self.data = data
        self.steps = len(data)
        self.values = [p.value for p in data]
        self.open_val = self.values[0]
        self.close_val = self.values[-1]
        self.change_pct = ((self.close_val - self.open_val) / self.open_val) * 100

    def describe(self):
        vmin = min(self.values)
        vmax = max(self.values)
        span = vmax - vmin
        return {
            "min": round(vmin, 2),
            "max": round(vmax, 2),
            "span": round(span, 2),
            "change_pct": round(self.change_pct, 2)
        }

class BoundDetector:

    @staticmethod
    def analyze(traj: PsiTrajectory):
        values = traj.values
        drops = []
        for i in range(1, len(values)):
            d = ((values[i] - values[i - 1]) / values[i - 1]) * 100
            drops.append(d)
        max_drop = min(drops) if drops else 0
        drawdown = ((min(values) - max(values)) / max(values)) * 100
        bound = max_drop <= -5 or drawdown <= -15
        return {
            "max_step_drop": round(max_drop, 2),
            "drawdown": round(drawdown, 2),
            "bound_alert": bound
        }

class TraceRuntime:
    def __init__(self, steps: int = 14):
        self.steps = steps
        self.interpreter = PhiInterpreter()
        self.source = AsyncSourceProvider()
        self.results = []

    async def _process_signal(self, signal_id: str, meaning: str):
        print(f"\nΦ phase :: {signal_id} {meaning}")
        print("  source → requesting signal stream")
        raw = await self.source.fetch_with_retry(signal_id, self.steps)
        if not raw:
            print("  source → failed")
            return

        traj = PsiTrajectory(raw)
        desc = traj.describe()
        print(
            f"  ψ(t) trajectory :: "
            f"min={desc['min']} max={desc['max']} span={desc['span']} "
            f"Δ={desc['change_pct']}%"
        )

        detector = BoundDetector.analyze(traj)
        status = "⚠ bound" if detector["bound_alert"] else "stable"
        print(
            f"  bound.scan :: "
            f"drop={detector['max_step_drop']}% "
            f"drawdown={detector['drawdown']}% "
            f"status={status}"
        )

        record = {
            "signal": signal_id,
            "meaning": meaning,
            "trajectory": desc,
            "bound": detector
        }

        self.results.append(record)
        print("  trace compiled")

    async def run(self):
        print("## trace: psi.phase")
        signals = self.interpreter.get_signals()
        tasks = [
            self._process_signal(sig, meaning)
            for sig, meaning in signals.items()
        ]

        await asyncio.gather(*tasks)
        print("\n## trace complete")
        return self.results

if __name__ == "__main__":
    runtime = TraceRuntime(steps=14)
    traces = asyncio.run(runtime.run())
    with open("simulated.trace.json", "w", encoding="utf-8") as f:
        json.dump(traces, f, indent=2)