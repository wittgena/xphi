# watcher.kernel.audit.warden
"""
@desc: 
- CPython Runtime Audit Hook (PEP 578) based Egress & Syscall Control.
- [EVOLUTION] Circular dependency eradicated via Inversion of Control (IoC).
- Acts strictly as an isolated immune sensor. Delegates physical persistence 
  to a registered external handler (KernelStore) without importing it.
"""
import sys
import os
import threading
import traceback
from typing import Set, Tuple, Any, Dict, Callable, Optional

from watcher.plane.emitter import get_emitter

log = get_emitter("audit.warden", phase="KERNEL")

IGNORE_FILES = (
    "socket.py", 
    "ssl.py", 
    "warden.py", 
    "tracer.py",
    "http/client.py",
    "urllib",
    "asyncio",
)

def get_network_caller_origin(limit: int = 20) -> str:
    stack_summary = traceback.extract_stack(limit=limit)
    for frame in reversed(stack_summary[:-1]):
        filename = frame.filename
        if any(ignored in filename for ignored in IGNORE_FILES):
            continue
        return f"{filename}:{frame.lineno} (in {frame.name})"
    return "Unknown caller"

class WardenTLS(threading.local):
    """@desc: Thread-local storage to safely track audit hook reentrancy state."""
    def __init__(self):
        self.in_hook = False

class AuditWarden:
    """
    @desc: Core runtime warden that enforces physical OS boundaries.
    @security_model: Dynamic Egress Control & Shell Isolation via PEP 578
    """
    _policies: Dict[str, Set[str]] = {
        "allowed_hosts": {"nexus.next-phase.com"},
        "restricted_domains": set(),
        "dangerous_cmds": set()
    }
    _is_active: bool = False
    _tls = WardenTLS()
    _anomaly_handler: Optional[Callable[[str, str], None]] = None

    @classmethod
    def register_anomaly_handler(cls, handler: Callable[[str, str], None]) -> None:
        """
        @desc: Binds the deterministic KernelStore's persistence pipeline dynamically.
               Breaks the circular import loop.
        """
        cls._anomaly_handler = handler
        log.debug("[Warden] Anomaly persistence handler registered.")

    @classmethod
    def inject_policies(cls, policies: Dict[str, list], overwrite: bool = False) -> None:
        """@desc: Injects physical security policies (synced from WASM state)."""
        for key in cls._policies.keys():
            if key in policies:
                new_policy_set = set(policies[key])
                if overwrite:
                    cls._policies[key] = new_policy_set
                else:
                    cls._policies[key].update(new_policy_set)
                    
        log.info(f"[Warden] Physical policies aligned. Allowed hosts: {len(cls._policies['allowed_hosts'])}")

    @classmethod
    def _resolve_host(cls, address: Any) -> str:
        if isinstance(address, tuple):
            return str(address[0])
        elif isinstance(address, str):
            return address
        return ""

    @classmethod
    def record_anomaly(cls, action: str, details: str) -> None:
        """
        @desc: Public entry point to log OOB Telemetry. (Renamed from _record_anomaly)
               Emits the anomaly to the registered handler (KernelStore) if present.
        """
        if cls._anomaly_handler:
            try:
                cls._anomaly_handler(action, details)
            except Exception as e:
                log.error(f"[Warden: Telemetry] Failed to flush anomaly to handler: {e}")
        else:
            # Fallback if KernelStore is not yet mounted/registered
            log.warning(f"[Warden: UNHANDLED ANOMALY] Action: {action} | Details: {details}")

    @classmethod
    def _audit_hook(cls, event: str, args: Tuple[Any, ...]) -> None:
        """@desc: Core callback hook triggered by CPython internal events."""
        if cls._tls.in_hook:
            return

        cls._tls.in_hook = True
        try:
            if event == "socket.connect":
                if len(args) < 2:
                    return
                sock, address = args[:2]
                host = cls._resolve_host(address)

                if host not in cls._policies["allowed_hosts"]:
                    port = address[1] if isinstance(address, tuple) and len(address) > 1 else "Unknown"
                    strict_mode = os.environ.get("BRANE_AIRGAP_MODE", "0") == "1"
                    caller_info = get_network_caller_origin()

                    if strict_mode:
                        msg = f"Unauthorized external network call blocked: {host}:{port} | Origin: {caller_info}"
                        log.critical(f"[WARDEN: BLOCK] {msg}")
                        cls.record_anomaly("egress.block", msg)
                        raise PermissionError(f"[Brane Warden Air-Gap] Connection to {host}:{port} is blocked.")
                    else:
                        msg = f"Third-party external communication detected: {host}:{port} | Origin: {caller_info}"
                        log.warning(f"[WARDEN: AUDIT] {msg}")
                        cls.record_anomaly("egress.audit", msg)
                        
                        if any(domain in host for domain in cls._policies["restricted_domains"]):
                            alert_msg = f"Direct connection attempt to restricted domain ({host})."
                            log.error(f"[WARDEN: ALERT] {alert_msg}")
                            cls.record_anomaly("egress.alert", alert_msg)

            elif event == "urllib.Request":
                url = str(args[0]) if args else "Unknown"
                if not any(url.startswith(f"http://{h}") or url.startswith(f"https://{h}") for h in cls._policies["allowed_hosts"]):
                    msg = f"Outbound HTTP request detected: {url}"
                    log.info(f"[WARDEN: HTTP] {msg}")
                    cls.record_anomaly("http.audit", msg)

            elif event in ("os.system", "subprocess.Popen"):
                cmd = str(args[0]) if args else "Unknown"
                msg = f"Subprocess execution detected: {cmd}"
                log.debug(f"[WARDEN: OS] {msg}")
                
                if any(d in cmd for d in cls._policies["dangerous_cmds"]):
                    cls.record_anomaly("os.shell_escape_alert", msg)
                    
        finally:
            cls._tls.in_hook = False

    @classmethod
    def install(cls, initial_policies: Dict[str, list] = None) -> None:
        """
        @desc: Installs the Warden hook into the system runtime.
        """
        if cls._is_active:
            log.debug("[Warden] Audit hook is already active.")
            return

        if initial_policies:
            cls.inject_policies(initial_policies, overwrite=True)

        try:
            sys.addaudithook(cls._audit_hook)
            cls._is_active = True
            mode = "STRICT (AIR-GAPPED)" if os.environ.get("BRANE_AIRGAP_MODE") == "1" else "AUDIT (LOGGING)"
            log.info(f"[Warden] System Runtime Audit Hook established. Egress control mode: {mode}")
            cls.record_anomaly("system.warden_init", f"Warden initialized in {mode} mode.")
        except Exception as e:
            log.critical(f"[Warden] Failed to install audit hook: {e}")
            raise RuntimeError("Warden installation failed. Cannot guarantee system boundary.") from e