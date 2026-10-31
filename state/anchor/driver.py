# xphi.state.anchor.driver
"""
Pluggable storage driver interface for the AnchorStore.
Target Expansion Backends: 
- TiKV (Distributed RocksDB/Raft KV)
- FoundationDB (Ordered transactional KV)
- ScyllaDB (LSM-tree wide-column store)
"""
from __future__ import annotations

import os
import struct
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional, Tuple
from rocksdict import Rdict, Options, AccessType

_FDB_API_INITIALIZED = False


def _strinc(prefix: bytes) -> bytes:
    """
    Computes the exact lexicographical exclusive upper-bound key for prefix scans
    (Standard FoundationDB Subspace.range() algorithm).
    """
    stripped = prefix.rstrip(b"\xff")
    if not stripped:
        raise ValueError("Prefix must contain at least one byte not equal to 0xFF")
    return stripped[:-1] + bytes([stripped[-1] + 1])


class _GeneratorSeekIterator:
    """Bridges standard Python generators (scan_prefix) to C++ style cursor loops (seek/valid/next)."""
    def __init__(self, driver: "AbstractAnchorDriver"):
        self._driver = driver
        self._gen: Optional[Iterator[Tuple[bytes, bytes]]] = None
        self._current_key: Optional[bytes] = None
        self._current_val: Optional[bytes] = None
        self._is_valid: bool = False

    def seek(self, prefix: bytes) -> None:
        self._gen = self._driver.scan_prefix(prefix)
        self.next()

    def valid(self) -> bool:
        return self._is_valid

    def key(self) -> bytes:
        return self._current_key  # type: ignore

    def value(self) -> bytes:
        return self._current_val  # type: ignore

    def next(self) -> None:
        if self._gen is None:
            self._is_valid = False
            return
        try:
            self._current_key, self._current_val = next(self._gen)
            self._is_valid = True
        except StopIteration:
            self._is_valid = False
            self._current_key = None
            self._current_val = None


class AbstractAnchorDriver(ABC):
    """
    Base contract for append-only / ordered key-value storage backends.
    Subclasses only need to implement `get`, `put`, `scan_prefix`, and `close`.
    """
    is_sealer: bool = False
    path: str = "adb"

    @abstractmethod
    def get(self, key: bytes) -> Optional[bytes]:
        """Single-key lookup. Returns None if absent (single RTT for network KV)."""
        pass

    @abstractmethod
    def put(self, key: bytes, value: bytes) -> None:
        """Idempotent write for immutable CAS objects and mutable ref pointers."""
        pass

    @abstractmethod
    def scan_prefix(self, prefix: bytes) -> Iterator[Tuple[bytes, bytes]]:
        """Yields (key, value) byte pairs matching the given prefix."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Releases underlying storage handles or network sessions."""
        pass

    def try_catch_up_with_primary(self) -> None:
        """Syncs read-only view for shared local storage (no-op for networked KV stores)."""
        pass

    @staticmethod
    def split_namespace(key: bytes) -> Tuple[str, str]:
        """Splits b'namespace:id' into ('namespace', 'id') for partitioned KV stores."""
        decoded = key.decode("utf-8")
        if ":" in decoded:
            ns, ident = decoded.split(":", 1)
            return ns, ident
        return "default", decoded

    # --- Dict & Cursor compatibility mappings for AnchorStore and AnchorOracle ---
    def __contains__(self, key: bytes) -> bool:
        return self.get(key) is not None

    def __getitem__(self, key: bytes) -> bytes:
        val = self.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: bytes, value: bytes) -> None:
        self.put(key, value)

    def iter(self) -> Any:
        return _GeneratorSeekIterator(self)


class RocksAnchorDriver(AbstractAnchorDriver):
    """Default embedded RocksDB driver with native C-binding pass-through."""
    def __init__(self, target_path: str):
        self.path = str(target_path)
        opt = Options()
        opt.create_if_missing(True)
        try:
            self._db = Rdict(self.path, opt)
            self.is_sealer = True
        except Exception as e:
            error_msg = str(e).lower()
            if "lock" in error_msg or "temporarily unavailable" in error_msg:
                self.is_sealer = False
                self._db = Rdict(self.path, Options(), access_type=AccessType.read_only())
            else:
                raise

    def get(self, key: bytes) -> Optional[bytes]:
        return self._db.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self._db[key] = value

    def __contains__(self, key: bytes) -> bool:
        return key in self._db

    def __getitem__(self, key: bytes) -> bytes:
        return self._db[key]

    def iter(self) -> Any:
        return self._db.iter()

    def scan_prefix(self, prefix: bytes) -> Iterator[Tuple[bytes, bytes]]:
        iterator = self._db.iter()
        iterator.seek(prefix)
        while iterator.valid():
            k = iterator.key()
            if not k.startswith(prefix):
                break
            yield k, iterator.value()
            iterator.next()

    def try_catch_up_with_primary(self) -> None:
        if self.is_sealer or self._db is None:
            return
        if hasattr(self._db, "try_catch_up_with_primary"):
            self._db.try_catch_up_with_primary()
        else:
            self._db.close()
            self._db = Rdict(self.path, Options(), access_type=AccessType.read_only())

    def close(self) -> None:
        if getattr(self, "_db", None) is not None:
            self._db.close()

class FDBAnchorDriver(AbstractAnchorDriver):
    """
    FoundationDB driver implementing AbstractAnchorDriver.
    - Connects to native FoundationDB cluster when `foundationdb` package & daemon are active.
    - Gracefully falls back to an in-memory lexicographical FDB simulator (unless XPHI_FDB_STRICT=1)
    """
    def __init__(self, target_path: str, cluster_file: Optional[str] = None, timeout_ms: int = 2500):
        global _FDB_API_INITIALIZED
        self.path = str(target_path)
        subspace_name = os.path.basename(self.path.rstrip("/\\")) or "adb"
        self._prefix = f"xphi:{subspace_name}:".encode("utf-8")

        self.cluster_file = cluster_file or os.getenv("FDB_CLUSTER_FILE")
        self.timeout_ms = int(os.getenv("XPHI_FDB_TIMEOUT_MS", str(timeout_ms)))
        self.strict_mode = os.getenv("XPHI_FDB_STRICT", "0") == "1"

        self.is_sealer = True
        self.is_native_cluster = False
        self.last_versionstamp: bytes = b"\x00" * 10
        self._commit_seq: int = 0

        self._fdb: Any = None
        self._db: Any = None
        self._emulated_kv: Dict[bytes, bytes] = {}

        self._init_backend()

    def _init_backend(self) -> None:
        global _FDB_API_INITIALIZED
        try:
            import fdb
            if not _FDB_API_INITIALIZED:
                fdb.api_version(730)
                _FDB_API_INITIALIZED = True
            self._fdb = fdb

            db = fdb.open(self.cluster_file)
            # Probe cluster liveness with a bounded timeout so missing fdbserver never hangs forever
            tr = db.create_transaction()
            tr.options.set_timeout(self.timeout_ms)
            _ = tr[self._prefix + b"__health__"]
            self._db = db
            self.is_native_cluster = True
        except Exception as e:
            if self.strict_mode:
                raise RuntimeError(f"Native FoundationDB connection failed in strict mode: {e}") from e
            self._db = None
            self.is_native_cluster = False

    def _to_fdb_key(self, key: bytes) -> bytes:
        return self._prefix + key

    def _from_fdb_key(self, fdb_key: bytes) -> bytes:
        return fdb_key[len(self._prefix):]

    def get(self, key: bytes) -> Optional[bytes]:
        raw_k = self._to_fdb_key(key)
        if self.is_native_cluster and self._db is not None:
            tr = self._db.create_transaction()
            tr.options.set_timeout(self.timeout_ms)
            val = tr[raw_k]
            return bytes(val) if val.present() else None
        return self._emulated_kv.get(raw_k)

    def put(self, key: bytes, value: bytes) -> None:
        raw_k = self._to_fdb_key(key)
        raw_v = bytes(value)
        if self.is_native_cluster and self._db is not None:
            tr = self._db.create_transaction()
            tr.options.set_timeout(self.timeout_ms)
            tr.set(raw_k, raw_v)
            vs_fut = tr.get_versionstamp()
            tr.commit().wait()
            self.last_versionstamp = bytes(vs_fut.wait())[:10].ljust(10, b"\x00")
        else:
            self._emulated_kv[raw_k] = raw_v
            self._commit_seq += 1
            self.last_versionstamp = struct.pack(">QH", self._commit_seq, 0)

    def scan_prefix(self, prefix: bytes) -> Iterator[Tuple[bytes, bytes]]:
        """Streams lexicographically ordered (key, value) pairs matching `prefix` using _strinc bounds."""
        start_key = self._to_fdb_key(prefix)
        end_key = _strinc(start_key)

        if self.is_native_cluster and self._db is not None:
            tr = self._db.create_transaction()
            tr.options.set_timeout(self.timeout_ms)
            for kv in tr.get_range(start_key, end_key):
                yield self._from_fdb_key(bytes(kv.key)), bytes(kv.value)
        else:
            for k in sorted(self._emulated_kv.keys()):
                if k >= end_key:
                    break
                if k >= start_key:
                    yield self._from_fdb_key(k), self._emulated_kv[k]

    def clear_subspace(self) -> None:
        """Clears all keys under this driver's subspace prefix (used during E2E teardown)."""
        start_key = self._prefix
        end_key = _strinc(start_key)
        if self.is_native_cluster and self._db is not None:
            tr = self._db.create_transaction()
            tr.options.set_timeout(self.timeout_ms)
            tr.clear_range(start_key, end_key)
            tr.commit().wait()
        else:
            self._emulated_kv.clear()

    def close(self) -> None:
        self._db = None
        self._emulated_kv.clear()


class AnchorDriverFactory:
    """Instantiates the configured storage driver based on environment settings."""
    @staticmethod
    def mount(target_path: str) -> AbstractAnchorDriver:
        backend = os.getenv("XPHI_ANCHOR_DRIVER", "rocksdb").lower()
        if backend == "rocksdb":
            return RocksAnchorDriver(target_path)
        elif backend in ("fdb", "foundationdb"):
            return FDBAnchorDriver(target_path)
        raise ValueError(f"Unsupported anchor driver: {backend}")