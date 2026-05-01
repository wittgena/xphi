# sphere.receptor.lens.delta
"""
@flow: Field → [ ψ_asset(t), Φ ] → ψ_Φ(t) → ψ(t,w) → Bound(Spike, Co-Diff) → trace
"""
import FinanceDataReader as fdr
import pandas as pd
import datetime
import json
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict

@dataclass
class DeltaPoint:
    timestamp: datetime.datetime
    price: float

class DeltaTrajectory:
    """ψ(t): 단일 자산 또는 합성된 구조(섹터)의 연속 궤적"""
    def __init__(self, symbol: str, points: List[DeltaPoint]):
        self.symbol = symbol
        self.points = points

@dataclass
class WindowedReturn:
    """ψ(t,w): 특정 윈도우로 투영된 궤적 조각"""
    symbol: str
    start_time: datetime.datetime
    end_time: datetime.datetime
    return_value: float

@dataclass
class StructureIdentity:
    """Φ: 섹터/테마를 정의하는 위상 구조체"""
    name: str
    members: List[str]

class StructureAggregator:
    """개별 궤적들을 모아 하나의 구조적 궤적(ψ_Φ)으로 합성합니다."""
    @staticmethod
    def compose(name: str, trajectories: List[DeltaTrajectory]) -> DeltaTrajectory:
        if not trajectories:
            return DeltaTrajectory(name, [])
        
        ## 궤적들의 평균값(Center of Mass)을 구하여 새로운 궤적 생성
        series_list = [
            pd.Series({p.timestamp: p.price for p in traj.points}, name=traj.symbol)
            for traj in trajectories
        ]
        df = pd.concat(series_list, axis=1).dropna()
        avg_series = df.mean(axis=1)
        
        points = [DeltaPoint(timestamp=ts, price=float(val)) for ts, val in avg_series.items()]
        return DeltaTrajectory(name, points)

class MarketField:
    @staticmethod
    def load(symbol: str, start: str, end: str) -> DeltaTrajectory:
        df = fdr.DataReader(symbol, start, end)
        points = [DeltaPoint(pd.to_datetime(ts), float(price)) for ts, price in df["Close"].items()]
        return DeltaTrajectory(symbol, points)

class ReturnWindowStrategy:
    def __init__(self, window_days: int):
        self.window_days = window_days

    def generate(self, trajectory: DeltaTrajectory) -> List[WindowedReturn]:
        prices = pd.Series([p.price for p in trajectory.points], index=[p.timestamp for p in trajectory.points])
        pct = prices.pct_change(periods=self.window_days)
        
        windows = []
        for t, r in pct.dropna().items():
            start = t - pd.Timedelta(days=self.window_days)
            windows.append(WindowedReturn(trajectory.symbol, start, t, float(r)))
        return windows

@dataclass
class CoDiffEvent:
    """개별 종목이 자신이 속한 구조(섹터)로부터 이탈하는 현상 기록"""
    symbol: str
    structure: str
    end_time: str
    symbol_return: float
    structure_return: float
    co_diff: float

class CoDiffBoundStrategy:
    """구조(Φ)와 개별(ψ) 간의 상대적 괴리(Spike)를 탐지합니다."""
    def __init__(self, diff_threshold: float):
        self.diff_threshold = diff_threshold

    def scan(self, asset_window: WindowedReturn, structure_window: WindowedReturn) -> Optional[CoDiffEvent]:
        ## @guard: 시간대가 일치 확인
        if asset_window.end_time != structure_window.end_time:
            return None
            
        diff = asset_window.return_value - structure_window.return_value
        if abs(diff) >= self.diff_threshold:
            return CoDiffEvent(
                symbol=asset_window.symbol,
                structure=structure_window.symbol,
                end_time=str(asset_window.end_time.date()),
                symbol_return=round(asset_window.return_value, 4),
                structure_return=round(structure_window.return_value, 4),
                co_diff=round(diff, 4)
            )
        return None

class MarketBoundTracer:
    def __init__(self, structures: List[StructureIdentity], start: str, end: str):
        self.structures = structures
        self.start = start
        self.end = end
        self.window_strategy = ReturnWindowStrategy(window_days=7)
        self.codiff_strategy = CoDiffBoundStrategy(diff_threshold=0.1) ## 섹터 대비 10% 이상 괴리 시 탐지

    def run_scan(self):
        trace_artifact = []
        
        for structure in self.structures:
            print(f"\n[+] Processing Structure Φ: {structure.name}")
            
            ## stage.1: Field -> 개별 ψ_asset(t) 로드
            asset_trajectories = {}
            for symbol in structure.members:
                asset_trajectories[symbol] = MarketField.load(symbol, self.start, self.end)
                
            ## stage.2: 개별 ψ_asset(t) -> 구조 ψ_Φ(t) 로 합성
            struct_trajectory = StructureAggregator.compose(structure.name, list(asset_trajectories.values()))
            
            ## stage.3: 구조 궤적 투영 - ψ_Φ(t, w)
            struct_windows = {w.end_time: w for w in self.window_strategy.generate(struct_trajectory)}
            
            ## stage.4: 개별 궤적 투영 및 Boundary(Co-Diff) 스캔
            for symbol, traj in asset_trajectories.items():
                asset_windows = self.window_strategy.generate(traj)
                
                for aw in asset_windows:
                    sw = struct_windows.get(aw.end_time)
                    if sw:
                        ## 개별 종목이 자신이 속한 구조의 평균적 흐름에서 이탈했는가?
                        event = self.codiff_strategy.scan(aw, sw)
                        if event:
                            trace_artifact.append(asdict(event))
                            
        return trace_artifact

def main():
    ## Φ (Topological Structures) 정의
    structures = [
        StructureIdentity("Foundry", ["000660", "005930"]),
        StructureIdentity("REE_Core", ["383310", "272210", "294630"])
    ]

    scanner = MarketBoundTracer(
        structures=structures,
        start="2026-01-01",
        end=datetime.date.today().strftime("%Y-%m-%d")
    )

    trace = scanner.run_scan()
    
    artifact = {
        "system": "met.watcher.market.bound",
        "topos": "Field → [ψ_asset(t), Φ] → ψ_Φ(t,w) → CoDiff_Bound → trace",
        "trace_count": len(trace),
        "trace": trace
    }

    with open("market.trace.json", "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2, ensure_ascii=False)

    print(f"[✔] Co-Diff events detected: {len(trace)}")
    print(f"\n[anchor] market trace residue -> market.trace.json")

if __name__ == "__main__":
    main()