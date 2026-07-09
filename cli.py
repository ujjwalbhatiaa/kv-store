#!/usr/bin/env python3
"""
Command-line interface for kvstore.

Usage:
    python cli.py <db_file> set <key> <value>
    python cli.py <db_file> get <key>
    python cli.py <db_file> delete <key>
    python cli.py <db_file> keys
    python cli.py <db_file> compact
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


def repl(store):
    print(f"kvstore REPL — {store.path}  ({len(store)} keys). Commands: set/get/delete/keys/compact/exit")
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
            else:
                print("usage: set <k> <v> | get <k> | delete <k> | keys | compact | exit")
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
        else:
            print(__doc__)
            return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
