#!/usr/bin/env python3
"""
Command-line interface for kvstore.

Usage:
    python cli.py <db_file> set <key> <value>
    python cli.py <db_file> get <key>
    python cli.py <db_file> delete <key>
    python cli.py <db_file> keys
    python cli.py <db_file> compact
    python cli.py <db_file> stats
    python cli.py <db_file> repl        # interactive shell
"""
import sys

from kvstore.store import KVStore


def _print_get(store, key):
    val = store.get(key)
    if val is None:
        print(f"(nil)")
    else:
        print(val.decode("utf-8", errors="replace"))

def _print_stats(store):
    stats = store.stats()
    print(f"live_keys:      {stats['live_keys']}")
    print(f"log_size_bytes: {stats['log_size_bytes']}")
    print(f"live_bytes:     {stats['live_bytes']}")
    print(f"dead_bytes:     {stats['dead_bytes']}")
    print(f"dead_ratio:     {stats['dead_ratio']:.1%}")
    if stats["dead_ratio"] > 0.5:
        print("-> more than half the log is dead weight; consider running 'compact'")


def repl(store):
    print(f"kvstore REPL — {store.path}  ({len(store)} keys). Commands: set/get/delete/keys/stats/compact/exit")
    while True:
        try:
            line = input("kv> ").strip()
        except EOFError:
            break
        if not line:
            continue
        parts = line.split(maxsplit=2)
        cmd = parts[0].lower()
        try:
            if cmd in ("exit", "quit"):
                break
            elif cmd == "set" and len(parts) == 3:
                store.set(parts[1], parts[2])
                print("OK")
            elif cmd == "get" and len(parts) == 2:
                _print_get(store, parts[1])
            elif cmd == "delete" and len(parts) == 2:
                print("OK" if store.delete(parts[1]) else "(not found)")
            elif cmd == "keys":
                for k in store.keys():
                    print(k)
            elif cmd == "compact":
                old, new = store.compact()
                print(f"compacted: {old} -> {new} bytes ({old - new} bytes reclaimed)")
            elif cmd == "stats":
                _print_stats(store)
            else:
                print("usage: set <k> <v> | get <k> | delete <k> | keys | stats | compact | exit")
        except Exception as e:
            print(f"error: {e}")


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 1

    db_path, cmd = argv[1], argv[2].lower()
    store = KVStore(db_path)
    try:
        if cmd == "repl":
            repl(store)
        elif cmd == "set":
            if len(argv) != 5:
                print("usage: set <key> <value>")
                return 1
            store.set(argv[3], argv[4])
            print("OK")
        elif cmd == "get":
            if len(argv) != 4:
                print("usage: get <key>")
                return 1
            _print_get(store, argv[3])
        elif cmd == "delete":
            if len(argv) != 4:
                print("usage: delete <key>")
                return 1
            print("OK" if store.delete(argv[3]) else "(not found)")
        elif cmd == "keys":
            for k in store.keys():
                print(k)
        elif cmd == "compact":
            old, new = store.compact()
            print(f"compacted: {old} -> {new} bytes ({old - new} bytes reclaimed)")
        elif cmd == "stats":
            _print_stats(store)
        else:
            print(__doc__)
            return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
