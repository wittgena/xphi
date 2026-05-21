# cognitive.xphi.runtime
## @lineage: xphi.runtime
## @lineage: phase.reflect.xphi.runtime
import os
import sys
import json
import time
import subprocess
import threading
import urllib.request
import urllib.parse
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Generator
import redis
from phase.plane.emitter import get_emitter
from phase.bind.resolver import find_current_self, resolve_path

log = get_emitter("xphi.runtime")

try:
    LIB_ROOT = resolve_path("lib")
except Exception as e:
    log.error(f"[error] 기준면(anchor)을 찾을 수 없음: {e}")
    sys.exit(1)

class XPhiRuntime:
    """activate resolver when boundary has no handler"""
    def __init__(self, jar_root: Path = LIB_ROOT):
        self.jar_root = jar_root

    def ensure(self):
        jars = sorted(self.jar_root.glob("xphi-*.jar"))
        if not jars:
            raise RuntimeError("xphi jar not found")

        jar = jars[-1]
        log.info(f"[bootstrap] start xphi: {jar}")

        subprocess.Popen(
            ["java", "-jar", str(jar)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        log.info("[bootstrap] Waiting for resonance (3s)...")
        time.sleep(3)