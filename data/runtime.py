# data.runtime
import math
import time
from session.bound.plane import BoundPlane

class Field:
    """
    Oscillatory runtime measurement field.
    No structural decision logic.
    """
    def __init__(self):
        self.freq_a = (11 * math.pi) / 353
        self.freq_b = (4.0 * math.pi) / 353
        self.phase_a = self.freq_a * 14
        self.phase_b = 0.0
        self.threshold = math.cos(self.phase_a / 5)

    def evolve(self):
        self.phase_a += self.freq_a
        self.phase_b += self.freq_b

    def predict_future_load(self, steps_ahead):
        future_a = self.phase_a + (self.freq_a * steps_ahead)
        future_b = self.phase_b + (self.freq_b * steps_ahead)
        interference = math.sin(future_a) * math.cos(future_b)
        return abs(interference)

    def emit(self):
        interference = math.sin(self.phase_a) * math.cos(self.phase_b)
        if abs(interference) > self.threshold:
            return math.pi
        return 1.1


class PhaseRuntime:
    """
    Owns:
    - Time
    - Tick loop
    - Field evolution
    - Metric delivery
    """
    def __init__(self, model):
        self.model = model
        self.field = Field()
        self.field2 = Field()

    def run(self, ticks=400):
        BoundPlane.record(
            0,
            "BOOTSTRAP",
            "Integrated Phase System Initialized.",
            "SYS"
        )

        for tick in range(1, ticks + 1):
            future = self.field.predict_future_load(5)
            self.field.evolve()
            self.field2.evolve()

            load = self.field.emit() * 0.9 + self.field2.emit() * 0.1
            metrics = {
                "load": load,
                "future_load": future
            }
            self.model.step(tick, metrics)
            time.sleep(0.03)

class Bound:
    """
    Structural constraint surface.
    """
    def __init__(self, limit=0.5, nu=0.2, dt=1.0):
        self.limit = limit
        self.nu = nu
        self.dt = dt

class Watcher:
    """
    Structural coherence model.
    Owns:
    - Micro loop
    - Drift accumulation
    - Meso trigger
    """

    def __init__(self, bound):
        self.bound = bound
        self.load_coherence = 1.0
        self.drift_accumulator = 0.0
        self.version = 1

    ## @micro.dynamics
    def compute_advection(self, external_load_spike):
        return - (external_load_spike ** 2) * 0.1

    def compute_diffusion(self, tick):
        phase_mod = 1 + 0.05 * math.sin(tick * 0.1)
        return self.bound.nu * phase_mod * (1.0 - self.load_coherence**1.05)

    ## @step
    def step(self, tick, load, future_load):
        prediction_status = "SAFE"
        if future_load > 0.95:
            prediction_status = "!!!WARN!!!"
        elif future_load > 0.90:
            prediction_status = "CAUTION"

        contraction = self.compute_advection(load)
        relaxation = self.compute_diffusion(tick)

        predicted = self.load_coherence + self.bound.dt * (contraction + relaxation)
        delta = predicted - self.load_coherence

        tension_ratio = abs(delta) / self.bound.limit
        damping = min(1.0, tension_ratio)

        ## @micro.stabilize
        self.load_coherence += delta * (1 - damping)

        ## @drift.acc
        self.drift_accumulator = (
            0.9 * self.drift_accumulator + abs(delta)
        )

        ## @phase.classify
        if damping == 1.0:
            status = "SHOCK"
        elif abs(delta) < 0.002:
            status = "LOCK"
        elif delta < -0.01:
            status = "DRIFT"
        elif delta > 0.01:
            status = "RECOVERY"
        else:
            status = "STABLE"

        ## @meso.trigger
        if self.drift_accumulator > 0.15:
            BoundPlane.record(
                tick,
                "MESO_LOOP",
                f"Drift Saturated: {self.drift_accumulator:.3f}",
                "CRIT",
                prediction_status
            )
            self.re_synthesize(tick)
            self.drift_accumulator = 0.0
        else:
            log_msg = (
                f"Disp={self.load_coherence:.3f} | "
                f"Relax={relaxation:.3f} | "
                f"Damp={damping:.2f} | "
                f"DriftΣ={self.drift_accumulator:.3f} | "
                f"[{status}]"
            )

            BoundPlane.record(
                tick,
                "MICRO_LOOP",
                log_msg,
                "INFO",
                prediction_status
            )

    ## @meso.recofigure
    def re_synthesize(self, tick):
        self.version += 1
        BoundPlane.record(
            tick,
            "RE_SYNTH",
            f"Topology re-synthesis initiated. v{self.version}",
            "SYS",
            "REBOOT"
        )

        self.load_coherence = 1.0
        self.bound.nu *= 1.1

        BoundPlane.record(
            tick,
            "ANCHORING",
            f"New topology anchored. ν={self.bound.nu:.3f}",
            "SYS",
            "STABLE"
        )


class MesoModel:
    """
    Pure structural semantic model.
    """
    def __init__(self):
        self.bound = Bound()
        self.watcher = Watcher(self.bound)

    def step(self, tick, metrics):
        self.watcher.step(
            tick,
            metrics["load"],
            metrics["future_load"]
        )

if __name__ == "__main__":
    model = MesoModel()
    runtime = PhaseRuntime(model)
    runtime.run(400)