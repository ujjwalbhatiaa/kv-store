# kv-store

A minimal **log-structured key-value store** with on-disk persistence, an
in-memory hash index, crash recovery, and compaction — implemented from
scratch in pure Python (stdlib only, no database engine underneath).

This is the same core idea behind real systems like [Bitcask](https://riak.com/assets/bitcask-intro.pdf)
(Riak's storage engine) and the write-ahead-log + memtable pattern used by
LevelDB/RocksDB/Kafka log segments — built small enough to read end to end
in one sitting, but with the same correctness properties: durable writes,
O(1) reads, and safe recovery from a crash mid-write.

## Why this design

| Design choice | Why |
|---|---|
| **Append-only log** — every write is appended, never edited in place | Sequential appends are the cheapest possible disk operation, and a writer that only ever appends can never corrupt data that was already durably written — a crash can only ever leave a **truncated tail**, never a corrupted middle. |
| **In-memory hash index** (`key -> (offset, length)`) | Every `get()` becomes one dict lookup + one `seek()` + one `read()` — O(1) regardless of how large the log has grown. The cost: every *key* (not value) must fit in memory. |
| **fsync on every write** | Without `fsync()`, a `write()` can sit in the OS page cache and vanish on a power loss/OS crash even though the file *looks* written. Calling `fsync()` after every `set`/`delete` makes each operation durable at the cost of being disk-latency-bound (see benchmarks below) — a real, deliberate durability-vs-throughput tradeoff, not an oversight. |
| **Tombstone records for delete** (not just removing from the index) | If deletes only touched the in-memory index, a deleted key would **reappear** after a restart (the replay would still see the old `set` record and have no idea it was later deleted). Writing an explicit delete record to the log is what makes deletes durable. |
| **Compaction as a separate, explicit step** | The log never shrinks on its own — every overwrite or delete just appends more bytes. `compact()` rewrites the log keeping only the current value per live key, which is *when* space is reclaimed. Real LSM systems make the same choice (compaction is a background job, not an inline cost of every write). |

## Crash recovery, in detail

On open, `KVStore` replays the entire log from byte 0 to rebuild the index.
Each record is only trusted once its **header, full key, and full value**
have all been read successfully. If any of those three reads comes back
short — because the process was killed mid-`write()` — replay stops at the
last fully-valid record, and **the file itself is truncated** to that
offset. This means:

- Data written before a crash is never lost.
- A half-written record from the crash is discarded, not left as silent
  corruption in the middle of the file.
- The store is immediately writable again after recovery (verified by
  `test_truncated_tail_record_is_recovered_from`, which hand-crafts a
  truncated record on disk, reopens the store, and confirms both correct
  recovery *and* that further writes still work).

## Usage

```python
from kvstore.store import KVStore

with KVStore("mydata.db") as store:
    store.set("name", "ujjwal")
    store.set("role", "AI intern candidate")
    store.get("name")          # b"ujjwal"
    "name" in store            # True
    store.delete("name")
    store.get("name")          # None
    store.compact()            # reclaim space from old/deleted records

# reopening replays the log — all surviving data comes back automatically
store2 = KVStore("mydata.db")
store2.get("role")             # b"AI intern candidate"
```

### CLI

```bash
python cli.py mydata.db set name ujjwal
python cli.py mydata.db get name
python cli.py mydata.db delete name
python cli.py mydata.db keys
python cli.py mydata.db compact
python cli.py mydata.db repl        # interactive shell
```

## On-disk record format

```
+---------+------------+------------+-----+-------+
| flag(1) | key_len(4) | val_len(4) | key | value |
+---------+------------+------------+-----+-------+
```
`flag` is `0` for a set, `1` for a delete (tombstone — no value bytes
follow). Lengths are big-endian unsigned 32-bit integers (`struct` format
`">BII"`), so every record is fully self-describing: you always know
exactly how many bytes to read next without needing delimiters or escaping.

## Benchmarks

Measured on this sandbox's CPU, single process, **with `fsync()` on every
write** (the durable path — see [`benchmarks/RESULTS.md`](benchmarks/RESULTS.md),
regenerate with `python benchmarks/bench.py`):

| Workload | Throughput |
|---|---|
| Sequential write, 20,000 unique keys | **1,543 writes/sec** |
| Random read, 20,000 gets | **784,239 reads/sec** |
| Overwrite-heavy, 20,000 writes to 50 keys | **1,749 writes/sec** |
| Compaction of the overwrite-heavy log | 1,004,890 → 2,540 bytes (**99.7% reduction**) in 1.19ms |

The ~500x gap between read and write throughput is the expected signature
of this design: reads never touch the disk more than once and never wait
on `fsync`, while every write pays a full disk-durability round-trip. It's
also a direct, measured illustration of *why* real systems batch writes or
relax fsync policy (e.g. Kafka's `acks`/flush-interval settings, Redis's
AOF `everysec` mode) when they need higher write throughput than "durable
on every single call" allows.

## Testing

```bash
python -m pytest tests/ -v
```

**19/19 tests pass**, covering: basic get/set/delete/overwrite, missing-key
behavior, type validation, `keys()`/`items()`/`len()`, persistence across
process restart (including that overwrite history replays to the *last*
write, not the first), delete-tombstone durability across restart, crash
recovery from a hand-crafted truncated tail record, compaction correctness
(file shrinks, keys retain correct values, deleted keys stay gone) both
immediately and after a subsequent reopen, and rejection of an unrecognized
record flag as a genuine corruption case (distinct from a normal crash-
truncated tail).

## Known limitations (by design, for scope)

- **Single process, no concurrent-process locking.** The in-process
  `threading.RLock` makes it thread-safe within one process, but two
  separate processes opening the same file would corrupt each other's
  writes. A real system would need a file lock or a single-writer server
  process in front of the log.
- **Full key index must fit in memory.** This is the standard Bitcask
  tradeoff — values live on disk, but every key has a permanent in-memory
  footprint.
- **No leveled compaction / SSTables.** `compact()` is a single full-log
  rewrite, not the multi-level background merge strategy of a real
  LSM-tree (RocksDB/LevelDB). That's a substantially bigger system; this
  project implements the simplest version of the idea that is still fully
  correct and crash-safe.

## Structure

```
kv-store/
├── kvstore/
│   ├── __init__.py
│   └── store.py           # KVStore: set/get/delete/compact + crash recovery
├── tests/
│   └── test_store.py      # 19 tests
├── benchmarks/
│   ├── bench.py
│   └── RESULTS.md          # regenerated by bench.py
├── cli.py                  # command-line + REPL interface
├── README.md
└── LICENSE
```

*Built 2026-07-09 as part of an ongoing portfolio-building project.*
