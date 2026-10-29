# xphi.state.anchor.driver
"""
Pluggable storage driver interface for the AnchorStore.
Target Expansion Backends: 
- TiKV (Distributed RocksDB/Raft KV)
- FoundationDB (Ordered transactional KV)
- ScyllaDB/Cassandra (LSM-tree wide-column store)
"""
import os
from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional, Tuple
from rocksdict import Rdict, Options, AccessType


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


class AnchorDriverFactory:
    """Instantiates the configured storage driver based on environment settings."""
    @staticmethod
    def mount(target_path: str) -> AbstractAnchorDriver:
        backend = os.getenv("XPHI_ANCHOR_DRIVER", "rocksdb").lower()
        if backend == "rocksdb":
            return RocksAnchorDriver(target_path)
        raise ValueError(f"Unsupported anchor driver: {backend}")