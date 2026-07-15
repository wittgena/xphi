# watcher.plane.meter
import time
import datetime
import logging
from collections import defaultdict, deque
from typing import Dict, Any, List, Optional
from watcher.tracer.trajectory import Point, WindowedTrajectory, DefaultBoundLensStrategy
from arch.topos.bound.sandbox.tunnel import TunnelFactory

log = logging.getLogger("watcher.meter")

class PressureMeter:
    """
    @role: L1 Data Collector
    @desc: Collects raw event timestamps and measures density based on a sliding window.
    """
    def __init__(self, window: float = 2.0):
        self.window = window
        self.history = defaultdict(deque)

    def record(self, key: str) -> float:
        """Records an event and immediately returns the current density."""
        now = time.time()
        q = self.history[key]
        
        # Remove old timestamps that fall outside the window (e.g., 2 seconds)
        while q and now - q[0] > self.window:
            q.popleft()
            
        q.append(now)
        return len(q) / self.window

    def get_history(self, key: str) -> deque:
        return self.history[key]


class MeterProjector:
    """
    @role: Architecture Bridge (Adapter)
    @desc: Projects a 1-dimensional timestamp queue (Deque) into a 
           multi-dimensional time-series trajectory (WindowedTrajectory) comprehensible to the Tracer layer.
    """
    def __init__(self, bins: int = 10):
        # Determines into how many micro-bins the window should be divided.
        # Example: 2.0 second window / 10 bins = time-series generation in 0.2 second intervals
        self.bins = bins

    def project(self, identity: str, timestamps: deque, window_size: float) -> WindowedTrajectory:
        now = time.time()
        start_time = now - window_size
        bin_size = window_size / self.bins
        
        # 1. Initialize micro-bins
        binned_counts = [0.0] * self.bins
        
        # 2. Allocate timestamps to bins to calculate micro-density
        for ts in timestamps:
            if ts < start_time: 
                continue
            
            bin_idx = int((ts - start_time) / bin_size)
            if 0 <= bin_idx < self.bins:
                binned_counts[bin_idx] += 1.0
                
        # 3. Convert to trajectory points
        points = []
        for i, count in enumerate(binned_counts):
            # Actual density of the corresponding micro-bin (events/second)
            micro_density = count / bin_size
            p_time = datetime.datetime.fromtimestamp(start_time + (i * bin_size))
            points.append(Point(timestamp=p_time, value=micro_density))
            
        return WindowedTrajectory(
            identity=identity,
            start_time=datetime.datetime.fromtimestamp(start_time),
            end_time=datetime.datetime.fromtimestamp(now),
            points=points
        )


class TelemetryEngine:
    """
    @role: Orchestrator
    @desc: Integrates the Meter (Collection) -> Projector (Transformation) -> Lens (Analysis) pipeline 
           to ultimately diagnose system turbulence and acceleration.
    """
    def __init__(self, window: float = 2.0, resolution_bins: int = 10):
        self.meter = PressureMeter(window=window)
        self.projector = MeterProjector(bins=resolution_bins)
        
        # Equip Kinematic Lens: Analyzes acceleration, trend, dispersion (volatility), etc.
        self.kinematic_lens = DefaultBoundLensStrategy(preset_name="kinematic")

    def analyze(self, key: str) -> Dict[str, Any]:
        """
        Records an event and returns the current density along with kinematic trajectory analysis results.
        """
        # 1. Record raw data and calculate simple density
        current_density = self.meter.record(key)
        
        # 2. Generate trajectory via Bridge
        raw_history = self.meter.get_history(key)
        trajectory = self.projector.project(key, raw_history, self.meter.window)
        
        # 3. Extract multi-dimensional metrics via Lens
        lens_result = self.kinematic_lens.scan(trajectory)
        metrics = lens_result.get("metrics", {})
        
        # 4. Threat assessment logic for defense and folding (Pre-emptive Backpressure)
        acceleration = metrics.get("acceleration", 0.0)
        volatility = metrics.get("volatility", 0.0)
        
        # [Assessment Logic Example] 
        # Even if density is still low, high acceleration and volatility indicate a bursting risk
        is_bursting = False
        if current_density > 3.0 and acceleration > 1.5:
            is_bursting = True
            log.warning(f"[Telemetry] Pre-emptive Turbulence detected on {key} (Acc: {acceleration:.2f})")
            
        return {
            "key": key,
            "density": round(current_density, 2),
            "is_bursting": is_bursting,
            "metrics": metrics,
            "status": lens_result.get("status")
        }

## Global singleton instance (injected and used in SurfacePlane, etc.)
default_telemetry = TelemetryEngine(window=2.0, resolution_bins=10)