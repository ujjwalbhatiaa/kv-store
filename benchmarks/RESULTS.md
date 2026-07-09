# kvstore benchmark results

Measured on this sandbox's CPU (single process, fsync'd writes — i.e. the
*durable* write path, not a best-case buffered-write number). Regenerate
with `python benchmarks/bench.py`.

## Sequential write (20,000 unique keys)
- 12.960s total, **1,543 writes/sec**
- Final log size: 1,037,780 bytes

## Random read (20,000 gets, keys shuffled)
- 0.026s total, **784,239 reads/sec**
- Every read is a single index lookup + one `seek()` + one `read()` —
  O(1) regardless of log size, since the index holds the exact byte
  offset and length of each value.

## Overwrite-heavy write (20,000 writes, 50 unique keys)
- 11.432s total, **1,749 writes/sec**
- Log grows to 1,004,890 bytes despite only
  50 live keys, because every overwrite appends a full
  new record instead of mutating in place.

## Compaction
- 1,004,890 -> 2,540 bytes
  (**99.7% reduction**) in 1.19ms
- Confirms the core tradeoff of a log-structured store: writes are cheap
  (pure append) and reads are O(1), but disk usage is reclaimed lazily via
  compaction rather than immediately on delete/overwrite.
