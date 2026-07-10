"""
kvstore.store
=============
A minimal log-structured key-value store with on-disk persistence, an
in-memory hash index, and compaction. The design follows the same core
idea as Bitcask / the LSM "write-ahead log + memtable" pattern used by
real systems (Riak, early LevelDB, Kafka's log segments):

  * Every write (set or delete) is *appended* to a log file on disk —
    never overwritten in place. Sequential appends are cheap even on
    spinning disks and make crash recovery simple (a partial write can
    only ever be a truncated *tail* record, never a corrupted middle).
  * An in-memory hash index maps each key to the byte offset/length of
    its most recent value in the log. Reads are a single seek + read —
    O(1) regardless of log size, at the cost of keeping every key
    (not value) resident in memory.
  * Because old values are never removed in place, the log grows
    forever unless compacted. `compact()` rewrites the log keeping only
    the current value for each live key, in insertion order, dropping
    tombstoned (deleted) keys and every superseded value.

This is intentionally NOT a full LSM-tree (no SSTables, no leveled
merges, no bloom filters) — it's the simplest version of the idea that
is still genuinely correct and crash-safe, which is what makes it a
useful thing to have implemented by hand rather than just read about.
"""
import os
import struct
import threading

# Record header: 1-byte flag | 4-byte key length | 4-byte value length
# big-endian, fixed width, so records self-describe how many bytes to
# read next without needing delimiters.
_HEADER = struct.Struct(">BII")
_SET_FLAG = 0
_DELETE_FLAG = 1


class CorruptRecordError(Exception):
    """Raised when the log format is invalid in a way that isn't a
    simple crash-truncated tail (which is handled silently)."""


class KVStore:
    """Append-only log-structured key-value store.

    Parameters
    ----------
    path: str
        Path to the log file on disk. Created if it does not exist.
        Reopening an existing path replays the log to rebuild the
        in-memory index, so data survives process restarts.
    """

    def __init__(self, path):
        self.path = path
        self._index = {}  # key(bytes) -> (value_offset: int, value_len: int)
        self._lock = threading.RLock()
        self._file = None
        self._open()
        self._rebuild_index()

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    def _open(self):
        if not os.path.exists(self.path):
            open(self.path, "ab").close()
        self._file = open(self.path, "r+b")

    def close(self):
        with self._lock:
            if self._file is not None:
                self._file.close()
                self._file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def __repr__(self):
        return f"KVStore(path={self.path!r}, keys={len(self)})"

    # ------------------------------------------------------------------ #
    # index rebuild / crash recovery
    # ------------------------------------------------------------------ #
    def _rebuild_index(self):
        """Replay the log from byte 0, rebuilding the in-memory index.

        A record is only trusted once its header AND full key AND full
        value have been read in full. Anything shorter (the process was
        killed mid-`write()`) is treated as a truncated tail: replay
        stops there and the file itself is truncated to the last known-
        good offset, so the next append starts from clean state instead
        of leaving corrupt bytes in the middle of future reads.
        """
        self._file.seek(0)
        index = {}
        good_offset = 0
        while True:
            header_bytes = self._file.read(_HEADER.size)
            if len(header_bytes) == 0:
                break
            if len(header_bytes) < _HEADER.size:
                break  # truncated tail
            flag, klen, vlen = _HEADER.unpack(header_bytes)
            key = self._file.read(klen)
            if len(key) < klen:
                break  # truncated tail

            if flag == _DELETE_FLAG:
                index.pop(key, None)
                good_offset = self._file.tell()
                continue

            if flag != _SET_FLAG:
                raise CorruptRecordError(
                    f"unknown record flag {flag!r} at offset {good_offset}"
                )

            value_offset = self._file.tell()
            value = self._file.read(vlen)
            if len(value) < vlen:
                break  # truncated tail
            index[key] = (value_offset, vlen)
            good_offset = self._file.tell()

        # Drop any trailing partial record left by a crash mid-write.
        self._file.seek(good_offset)
        self._file.truncate(good_offset)
        self._index = index

    # ------------------------------------------------------------------ #
    # core API
    # ------------------------------------------------------------------ #
    def set(self, key, value):
        """Store `value` under `key`, overwriting any prior value."""
        key_b = _to_bytes(key, "key")
        val_b = _to_bytes(value, "value")
        with self._lock:
            record = _HEADER.pack(_SET_FLAG, len(key_b), len(val_b)) + key_b + val_b
            self._file.seek(0, os.SEEK_END)
            self._file.write(record)
            self._file.flush()
            os.fsync(self._file.fileno())
            value_offset = self._file.tell() - len(val_b)
            self._index[key_b] = (value_offset, len(val_b))

    def get(self, key, default=None):
        """Return the value for `key`, or `default` if not present."""
        key_b = _to_bytes(key, "key")
        with self._lock:
            entry = self._index.get(key_b)
            if entry is None:
                return default
            offset, vlen = entry
            self._file.seek(offset)
            return self._file.read(vlen)

    def delete(self, key):
        """Delete `key`. Returns True if it existed, False otherwise."""
        key_b = _to_bytes(key, "key")
        with self._lock:
            if key_b not in self._index:
                return False
            record = _HEADER.pack(_DELETE_FLAG, len(key_b), 0) + key_b
            self._file.seek(0, os.SEEK_END)
            self._file.write(record)
            self._file.flush()
            os.fsync(self._file.fileno())
            del self._index[key_b]
            return True

    def __contains__(self, key):
        return _to_bytes(key, "key") in self._index

    def __len__(self):
        return len(self._index)

    def keys(self):
        """Return all live keys as str (decoded utf-8)."""
        return [k.decode("utf-8", errors="replace") for k in self._index.keys()]

    def items(self):
        """Yield (key, value) for every live key. Value is bytes."""
        for key_b in list(self._index.keys()):
            yield key_b.decode("utf-8", errors="replace"), self.get(key_b)

    # ------------------------------------------------------------------ #
    # compaction
    # ------------------------------------------------------------------ #
    def compact(self):
        """Rewrite the log keeping only the current value for each live
        key (dropping tombstones and superseded values).

        Returns (old_size_bytes, new_size_bytes).
        """
        with self._lock:
            old_size = os.path.getsize(self.path)
            tmp_path = self.path + ".compact.tmp"
            new_index = {}
            with open(tmp_path, "wb") as tmp:
                for key_b, (offset, vlen) in self._index.items():
                    self._file.seek(offset)
                    value = self._file.read(vlen)
                    record = _HEADER.pack(_SET_FLAG, len(key_b), len(value)) + key_b + value
                    tmp.write(record)
                    new_value_offset = tmp.tell() - len(value)
                    new_index[key_b] = (new_value_offset, len(value))
                tmp.flush()
                os.fsync(tmp.fileno())

            self._file.close()
            os.replace(tmp_path, self.path)
            self._file = open(self.path, "r+b")
            self._index = new_index
            new_size = os.path.getsize(self.path)
            return old_size, new_size

    # ------------------------------------------------------------------ #
    # observability
    # ------------------------------------------------------------------ #
    def stats(self):
        """Return a dict describing the current health of the log, to help
        decide whether `compact()` is worth running right now.

        Keys
        ----
        live_keys : int
            Number of keys currently live in the index.
        log_size_bytes : int
            Current size of the on-disk log file.
        live_bytes : int
            Bytes the log would occupy if compacted right now (header +
            key + value for each live entry, using the current on-disk
            record layout).
        dead_bytes : int
            log_size_bytes - live_bytes -- space reclaimable by
            compaction: old/superseded values, delete tombstones, and
            their headers/keys.
        dead_ratio : float
            dead_bytes / log_size_bytes, in [0.0, 1.0]. 0.0 on an empty
            log. A high ratio (e.g. > 0.5) means most of the file on
            disk is dead weight and compact() would shrink it a lot;
            a ratio near 0 means compaction would reclaim little and
            isn't worth the rewrite cost yet.
        """
        with self._lock:
            log_size_bytes = os.path.getsize(self.path)
            live_bytes = sum(
                _HEADER.size + len(key_b) + vlen
                for key_b, (_, vlen) in self._index.items()
            )
            # live_bytes is derived from the in-memory index and can't
            # exceed the file it was built from, but guard against a
            # negative dead_bytes from any future accounting drift.
            dead_bytes = max(0, log_size_bytes - live_bytes)
            dead_ratio = (dead_bytes / log_size_bytes) if log_size_bytes else 0.0
            return {
                "live_keys": len(self._index),
                "log_size_bytes": log_size_bytes,
                "live_bytes": live_bytes,
                "dead_bytes": dead_bytes,
                "dead_ratio": dead_ratio,
            }


def _to_bytes(value, label):
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(f"{label} must be str or bytes, got {type(value).__name__}")
