# surface.trace.delta
import time
import threading
import click
import redis
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class RedisTopos:
    """
    @role: ∂Φ boundary surface
    - stores current Φ
    - resonance Ψ / Ψ′ via pubsub
    """
    def __init__(self, host='localhost', port=6379, db=0):
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.state_key = "meta.self:state:current_phase"
        self.signal_channel = "meta.self:signals:phase_mutation"
        self.psi_channel = "meta.self:signals:psi"

    def get_current_phase(self) -> str:
        """Φ read (default = Φ0)"""
        return self.client.get(self.state_key) or "Φ0"

    def emit_psi(self, event_type: str, weight: int = 1):
        """
        @desc: Ψ emission
        @flow: Ψ → surface → (Prometheus / external Φ)
        """
        payload = {"event": event_type, "weight": weight, "ts": time.time()}
        print(f"Ψ emit → {payload}")
        self.client.publish(self.psi_channel, str(payload))


## SENSOR (Environment Observation)
class SourceCodeWatcher(FileSystemEventHandler):
    """
    @desc: filesystem mutation → semantic signal
    @flow: environment → Ψ
    """
    def __init__(self, surface: RedisTopos):
        self.surface = surface
        self.last_trigger = 0

    def on_modified(self, event):
        if time.time() - self.last_trigger < 2.0:
            return

        if event.src_path.endswith(".java") or event.src_path.endswith(".dsl"):
            print(f"\n✨ mutation detected → {event.src_path}")
            self.last_trigger = time.time()
            # Ψ emission
            self.surface.emit_psi("xphi_analysis_event", weight=1)


class FieldKernel:
    """
    @desc: Manages the autonomous closed-loop of the system.
    Evaluates Φ mutations and applies structural inversions (Ψ′).
    """
    def __init__(self, field: RedisTopos):
        self.field = field

    def apply_inversion(self, phase: str):
        """
        @desc: phase triggers structural mutation
        @flow: Φ -> Ψ′
        """
        if phase == "∂Φ":
            # expansion (ε ↑)
            self.field.emit_psi("xphi_new_event", weight=2)
        elif phase == "Φ4":
            # collapse (σ → 1)
            self.field.emit_psi("xphi_structure_event", weight=0)

    def watch_mutations(self):
        """
        @flow: Φ detection (Redis pubsub → Φ change)
        """
        pubsub = self.field.client.pubsub()
        pubsub.subscribe(self.field.signal_channel)

        for message in pubsub.listen():
            if message['type'] == 'message':
                new_phase = message['data']
                print(f"\n🌀 Φ mutation → {new_phase}")
                # Φ → Ψ′
                self.apply_inversion(new_phase)

    def watch_psi_feedback(self):
        """
        @desc: emitted signal re-enters loop
        @flow: Ψ′ → Ψ
        """
        pubsub = self.field.client.pubsub()
        pubsub.subscribe(self.field.psi_channel)

        for msg in pubsub.listen():
            if msg["type"] == "message":
                print(f"re-entry Ψ′ → {msg['data']}")

    def start_daemons(self):
        """Boots the background nervous system."""
        threading.Thread(target=self.watch_mutations, daemon=True).start()
        threading.Thread(target=self.watch_psi_feedback, daemon=True).start()


## INTERFACE (Morphogenesis & CLI)
class PhaseAwareGroup(click.Group):
    """
    @desc: CLI morphs by phase (no mutation here)
    @flow: Φ → surface projection
    """
    def list_commands(self, ctx):
        surface = RedisTopos()
        phase = surface.get_current_phase()

        commands = ['observe']

        if phase == "Φ0":
            commands.extend(['explore', 'build_all'])
        elif phase == "∂Φ":
            commands.extend(['resolve_conflict', 'purge_redundancy'])
        elif phase == "Φ4":
            commands.extend(['execute_singular'])

        return sorted(commands)


@click.command(cls=PhaseAwareGroup)
def cli():
    """Φ-aware runtime surface"""
    pass

@cli.command()
@click.option('--watch-dir', default='./src')
def observe(watch_dir):
    """
    @desc: boostrap loop
    @flow: Ψ → Φ → Ψ′ → Ψ
    """
    surface = RedisTopos()
    kernel = FieldKernel(surface)
    
    ## Start Kernel Daemons
    kernel.start_daemons()

    ## Start Environment Sensor
    event_handler = SourceCodeWatcher(surface)
    observer = Observer()
    observer.schedule(event_handler, path=watch_dir, recursive=True)
    observer.start()
    print(f"observing -> {watch_dir} (Φ={surface.get_current_phase()})")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nshutdown")

    observer.join()

@cli.command()
def explore():
    """Φ0 → expansion"""
    print("explore → ε ↑")

@cli.command()
def purge_redundancy():
    """∂Φ → reduce ρ"""
    print("purge → ρ ↓")

@cli.command()
def execute_singular():
    """Φ4 → collapse"""
    print("singular → σ = 1")

if __name__ == "__main__":
    cli()