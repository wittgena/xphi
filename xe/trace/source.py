# xe.trace.source
import time
import threading
from watchdog.events import FileSystemEventHandler
from watcher.surface.topos import RedisTopos

class SourceTracer(FileSystemEventHandler):
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
            self.surface.emit_psi("xphi_analysis_event", weight=1)


class FieldKernel:
    """
    @desc: Manages the autonomous closed-loop of the system.
    Evaluates Φ mutations and applies structural inversions (Ψ′).
    """
    def __init__(self, field: RedisTopos):
        self.field = field

    def apply_inversion(self, phase: str):
        """@flow: Φ -> Ψ′"""
        if phase == "∂Φ":
            self.field.emit_psi("xphi_new_event", weight=2)
        elif phase == "Φ4":
            self.field.emit_psi("xphi_structure_event", weight=0)

    def watch_mutations(self):
        pubsub = self.field.client.pubsub()
        pubsub.subscribe(self.field.signal_channel)
        for message in pubsub.listen():
            if message['type'] == 'message':
                new_phase = message['data']
                print(f"\n🌀 Φ mutation → {new_phase}")
                self.apply_inversion(new_phase)

    def watch_psi_feedback(self):
        pubsub = self.field.client.pubsub()
        pubsub.subscribe(self.field.psi_channel)
        for msg in pubsub.listen():
            if msg["type"] == "message":
                print(f"re-entry Ψ′ → {msg['data']}")

    def start_daemons(self):
        """Boots the background nervous system."""
        threading.Thread(target=self.watch_mutations, daemon=True).start()
        threading.Thread(target=self.watch_psi_feedback, daemon=True).start()