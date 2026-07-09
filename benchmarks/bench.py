#!/usr/bin/env python3
"""
Simple throughput benchmark for kvstore.KVStore.

Measures:
  1. Sequential write throughput (unique keys)
  2. Random read throughput (against the keys just written)
  3. Overwrite-heavy write throughput (same small key set, many writes)
  4. Compaction time/size-reduction for the overwrite-heavy log

Run:  python benchmarks/bench.py
Writes a fresh report to benchmarks/RESULTS.md with the numbers measured
on the machine that ran it (no hand-typed numbers).
"""
import os
import random
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvstore.store import KVStore


def bench_sequential_write(n=20_000):
    path = tempfile.mktemp(prefix="kvbench_seq_", suffix=".db")
    store = KVStore(path)
    start = time.perf_counter()
    for i in range(n):
        store.set(f"key-{i}", f"value-number-{i}-padding-xxxxxxxx")
    elapsed = time.perf_counter() - start
    size = os.path.getsize(path)
    store.close()
    os.remove(path)
    return {
        "n": n,
        "elapsed_s": elapsed,
        "ops_per_sec": n / elapsed,
        "final_size_bytes": size,
    }


def bench_random_read(n=20_000):
    path = tempfile.mktemp(prefix="kvbench_read_", suffix=".db")
    store = KVStore(path)
    for i in range(n):
        store.set(f"key-{i}", f"value-number-{i}-padding-xxxxxxxx")

    keys = [f"key-{i}" for i in range(n)]
    random.shuffle(keys)

    start = time.perf_counter()
    for k in keys:
        store.get(k)
    elapsed = time.perf_counter() - start
    store.close()
    os.remove(path)
    return {
        "n": n,
        "elapsed_s": elapsed,
        "ops_per_sec": n / elapsed,
    }


def bench_overwrite_heavy(n_writes=20_000, n_unique_keys=50):
    path = tempfile.mktemp(prefix="kvbench_overwrite_", suffix=".db")
    store = KVStore(path)
    start = time.perf_counter()
    for i in range(n_writes):
        key = f"hot-{i % n_unique_keys}"
        store.set(key, f"value-{i}-padding-xxxxxxxxxxxxxxxx")
    elapsed = time.perf_counter() - start
    size_before_compaction = os.path.getsize(path)

    compact_start = time.perf_counter()
    old_size, new_size = store.compact()
    compact_elapsed = time.perf_counter() - compact_start

    store.close()
    os.remove(path)
    return {
        "n_writes": n_writes,
        "n_unique_keys": n_unique_keys,
        "elapsed_s": elapsed,
        "ops_per_sec": n_writes / elapsed,
        "size_before_compaction_bytes": size_before_compaction,
        "size_after_compaction_bytes": new_size,
        "reduction_pct": 100 * (1 - new_size / size_before_compaction),
        "compact_elapsed_s": compact_elapsed,
    }


def main():
    print("Running kvstore benchmarks (fsync'd writes, single process)...\n")

    seq = bench_sequential_write()
    print(f"Sequential write: {seq['n']:,} unique keys in {seq['elapsed_s']:.3f}s "
          f"= {seq['ops_per_sec']:,.0f} ops/sec")

    read = bench_random_read()
    print(f"Random read:      {read['n']:,} gets in {read['elapsed_s']:.3f}s "
          f"= {read['ops_per_sec']:,.0f} ops/sec")

    over = bench_overwrite_heavy()
    print(f"Overwrite-heavy:  {over['n_writes']:,} writes to {over['n_unique_keys']} "
          f"keys in {over['elapsed_s']:.3f}s = {over['ops_per_sec']:,.0f} ops/sec")
    print(f"Compaction:       {over['size_before_compaction_bytes']:,} -> "
          f"{over['size_after_compaction_bytes']:,} bytes "
          f"({over['reduction_pct']:.1f}% reduction) in {over['compact_elapsed_s']*1000:.2f}ms")

    report = f"""# kvstore benchmark results

Measured on this sandbox's CPU (single process, fsync'd writes — i.e. the
*durable* write path, not a best-case buffered-write number). Regenerate
with `python benchmarks/bench.py`.

## Sequential write ({seq['n']:,} unique keys)
- {seq['elapsed_s']:.3f}s total, **{seq['ops_per_sec']:,.0f} writes/sec**
- Final log size: {seq['final_size_bytes']:,} bytes

## Random read ({read['n']:,} gets, keys shuffled)
- {read['elapsed_s']:.3f}s total, **{read['ops_per_sec']:,.0f} reads/sec**
- Every read is a single index lookup + one `seek()` + one `read()` —
  O(1) regardless of log size, since the index holds the exact byte
  offset and length of each value.

## Overwrite-heavy write ({over['n_writes']:,} writes, {over['n_unique_keys']} unique keys)
- {over['elapsed_s']:.3f}s total, **{over['ops_per_sec']:,.0f} writes/sec**
- Log grows to {over['size_before_compaction_bytes']:,} bytes despite only
  {over['n_unique_keys']} live keys, because every overwrite appends a full
  new record instead of mutating in place.

## Compaction
- {over['size_before_compaction_bytes']:,} -> {over['size_after_compaction_bytes']:,} bytes
  (**{over['reduction_pct']:.1f}% reduction**) in {over['compact_elapsed_s']*1000:.2f}ms
- Confirms the core tradeoff of a log-structured store: writes are cheap
  (pure append) and reads are O(1), but disk usage is reclaimed lazily via
  compaction rather than immediately on delete/overwrite.
"""
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "RESULTS.md")
    with open(out_path, "w") as f:
        f.write(report)
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
