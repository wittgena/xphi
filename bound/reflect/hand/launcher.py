# bound.reflect.hand.launcher
"""@flow: Ψ(import) → ∂Φ(boundary) → Φ(local override) → Φ(surface)"""
import sys
import uvicorn
import importlib.util
import argparse
from pathlib import Path
from hand.server.api import api
from bound.reflect.hand.middleware import ResonanceMiddleware
from phase.contract.registry import contract
from bound.surface.emitter import get_emitter
from bound.resolver import find_current_self, resolve_path

log = get_emitter("hand.launcher")

SELF_ROOT = find_current_self()
api.add_middleware(ResonanceMiddleware)

@contract.cli(
    name="serve", 
    args=["--host", "--port", "--log-level"], 
    tags=["ary", "hand", "launcher"],
    entry="main"
)
def main():
    parser = argparse.ArgumentParser(description="Hand Resonance Execution Launcher")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Binding host address")
    parser.add_argument("--port", type=int, default=8000, help="Binding port number")
    parser.add_argument("--log-level", type=str, default="warning", help="Uvicorn log level")
    args = parser.parse_args()

    log.info(f"[*] Starting Hand on {args.host}:{args.port}")
    uvicorn.run(api, host=args.host, port=args.port, log_level=args.log_level)

if __name__ == "__main__":
    main()