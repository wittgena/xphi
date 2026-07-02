# watcher.tracer.trajectory
import numpy as np
import datetime
from typing import List, Dict, Any, Callable, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod
from watcher.tracer.metrics import (
    trend_slope,
    acceleration,
    range_amplitude,
    path_length,
    mean_crossings,
    signal_energy,
    volatility,
    drawdown,
    skewness
)

@dataclass
class Point:
    timestamp: datetime.datetime
    value: float

@dataclass
class ContinuousTrajectory:
    """Ψ(t): continuous temporal trajectory (개별 노드의 연속 시계열)"""
    identity: str
    points: List[Point]

@dataclass
class WindowedTrajectory:
    """∂Φ: window-projected trajectory (특정 윈도우로 투영된 궤적 조각)"""
    identity: str
    start_time: datetime.datetime
    end_time: datetime.datetime
    points: List[Point]

@dataclass
class TopologicalStructure:
    """Φ: 섹터/테마/시스템 등 논리적으로 묶인 노드들의 위상 구조체"""
    name: str
    members: List[str]

class BaseBoundLensStrategy(ABC):
    """
    Lens의 기본 규격. 
    단일 궤적의 내재적 특성(Kinematic)을 보거나, 
    기준 궤적(reference_window)을 주입받아 관계적 특성(Topological)을 볼 수 있도록 확장됨.
    """
    @abstractmethod
    def scan(self, window: WindowedTrajectory, reference_window: Optional[WindowedTrajectory] = None) -> Dict[str, Any]:
        pass

class DefaultBoundLensStrategy(BaseBoundLensStrategy):
    """단일 노드의 절대적/통계적 폭주를 감지하는 운동학(Kinematic) 렌즈"""

    METRICS_REGISTRY: Dict[str, Callable[[np.ndarray], float]] = {
        "trend": trend_slope,
        "acceleration": acceleration,
        "range": range_amplitude,
        "path_length": path_length,
        "crossings": mean_crossings,
        "energy": signal_energy,
        "volatility": volatility,
        "drawdown": drawdown,
        "skewness": skewness
    }

    PRESETS = {
        "kinematic": ["trend", "acceleration", "range", "volatility"],
        "topology": ["path_length", "crossings", "energy", "volatility"],
        "tail_risk": ["drawdown", "skewness", "volatility", "range"]
    }

    def __init__(self, preset_name: str = "tail_risk"):
        if preset_name not in self.PRESETS:
            raise ValueError(f"Unknown preset: {preset_name}")
        self.preset_name = preset_name
        self.active_metrics = self.PRESETS[preset_name]

    def scan(self, window: WindowedTrajectory, reference_window: Optional[WindowedTrajectory] = None) -> Dict[str, Any]:
        values = np.array([p.value for p in window.points])
        if values.size < 3:
            return {
                "status": "insufficient_data",
                "preset": self.preset_name
            }

        computed = {}
        for name in self.active_metrics:
            func = self.METRICS_REGISTRY[name]
            computed[name] = func(values)

        return {
            "status": "valid",
            "preset": self.preset_name,
            "metrics": computed
        }

class CoDiffBoundLensStrategy(BaseBoundLensStrategy):
    """Φ(구조 전체)와 ψ(개별 노드) 간의 위상 괴리를 측정하는 관계형(Topological) 렌즈"""
    
    def __init__(self, diff_threshold: float = 0.1):
        self.diff_threshold = diff_threshold

    def scan(self, window: WindowedTrajectory, reference_window: Optional[WindowedTrajectory] = None) -> Dict[str, Any]:
        if not reference_window:
            return {"status": "missing_reference"}
            
        if not window.points or not reference_window.points:
            return {"status": "insufficient_data"}

        # 개별 궤적(Entity)의 Delta 계산
        e_start = window.points[0].value
        e_end = window.points[-1].value
        
        # 구조 전체(Structure)의 Delta 계산
        s_start = reference_window.points[0].value
        s_end = reference_window.points[-1].value

        if e_start == 0 or s_start == 0:
            return {"status": "invalid_baseline"}

        e_delta = (e_end - e_start) / e_start
        s_delta = (s_end - s_start) / s_start
        
        # Co-Diff: 구조의 흐름 대비 개별 노드의 이탈률
        co_diff = e_delta - s_delta
        is_ruptured = abs(co_diff) >= self.diff_threshold

        return {
            "status": "valid",
            "is_ruptured": is_ruptured,
            "metrics": {
                "entity_delta": round(e_delta, 4),
                "structure_delta": round(s_delta, 4),
                "co_diff": round(co_diff, 4)
            }
        }

class WindowStrategy(ABC):
    @abstractmethod
    def generate(self, trajectory: ContinuousTrajectory) -> List[WindowedTrajectory]:
        pass

class SlidingWindowStrategy(WindowStrategy):
    """지정된 시간 간격으로 연속된 윈도우 조각들을 생성"""
    def __init__(self, window_days: int, step_days: int):
        self.window_delta = datetime.timedelta(days=window_days)
        self.step_delta = datetime.timedelta(days=step_days)

    def generate(self, trajectory: ContinuousTrajectory) -> List[WindowedTrajectory]:
        points = trajectory.points
        if not points:
            return []

        start = points[0].timestamp
        end = points[-1].timestamp
        windows = []
        current = start
        
        while current + self.window_delta <= end:
            next_time = current + self.window_delta
            segment = [p for p in points if current <= p.timestamp < next_time]
            windows.append(
                WindowedTrajectory(
                    identity=trajectory.identity,
                    start_time=current,
                    end_time=next_time,
                    points=segment
                )
            )
            current += self.step_delta

        return windows

class WindowProjector:
    """Ψ → ∂Φ : 연속 궤적을 윈도우 전략에 따라 투영하는 프로젝터"""
    def __init__(self, strategy: WindowStrategy):
        self.strategy = strategy

    def project(self, trajectory: ContinuousTrajectory) -> List[WindowedTrajectory]:
        return self.strategy.generate(trajectory)