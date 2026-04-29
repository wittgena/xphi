# xe.trace.observer
import time
import threading
import click
import redis
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watcher.surface.topos import RedisTopos
from xe.trace.source import FieldKernel, SourceTracer

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
    event_handler = SourceTracer(surface)
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