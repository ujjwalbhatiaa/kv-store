"""
Unit + integration tests for kvstore.store.KVStore.

Run with:  python -m pytest tests/ -v
or (no pytest available): python tests/test_store.py
"""
import os
import struct
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvstore.store import KVStore, CorruptRecordError, _HEADER


class TempDBMixin:
    def make_path(self):
        fd, path = tempfile.mkstemp(prefix="kvstore_test_", suffix=".db")
        os.close(fd)
        os.remove(path)  # KVStore creates it fresh
        self._paths_to_clean.append(path)
        return path

    def setUp(self):
        self._paths_to_clean = []

    def tearDown(self):
        for p in self._paths_to_clean:
            for candidate in (p, p + ".compact.tmp"):
                if os.path.exists(candidate):
                    os.remove(candidate)


class TestBasicOps(TempDBMixin, unittest.TestCase):
    def test_set_and_get(self):
        store = KVStore(self.make_path())
        store.set("name", "ujjwal")
        self.assertEqual(store.get("name"), b"ujjwal")
        store.close()

    def test_get_missing_key_returns_none(self):
        store = KVStore(self.make_path())
        self.assertIsNone(store.get("nope"))
        self.assertEqual(store.get("nope", default="fallback"), "fallback")
        store.close()

    def test_overwrite_returns_latest_value(self):
        store = KVStore(self.make_path())
        store.set("k", "v1")
        store.set("k", "v2")
        store.set("k", "v3")
        self.assertEqual(store.get("k"), b"v3")
        self.assertEqual(len(store), 1)  # index has one entry, not three
        store.close()

    def test_delete(self):
        store = KVStore(self.make_path())
        store.set("k", "v")
        self.assertTrue("k" in store)
        deleted = store.delete("k")
        self.assertTrue(deleted)
        self.assertIsNone(store.get("k"))
        self.assertFalse("k" in store)
        store.close()

    def test_delete_missing_key_returns_false(self):
        store = KVStore(self.make_path())
        self.assertFalse(store.delete("nope"))
        store.close()

    def test_bytes_values_supported(self):
        store = KVStore(self.make_path())
        store.set(b"binkey", b"\x00\x01\xff\xfe")
        self.assertEqual(store.get(b"binkey"), b"\x00\x01\xff\xfe")
        store.close()

    def test_rejects_non_str_bytes(self):
        store = KVStore(self.make_path())
        with self.assertRaises(TypeError):
            store.set(123, "value")
        with self.assertRaises(TypeError):
            store.set("key", 123)
        store.close()

    def test_keys_and_len(self):
        store = KVStore(self.make_path())
        store.set("a", "1")
        store.set("b", "2")
        store.set("c", "3")
        store.delete("b")
        self.assertEqual(sorted(store.keys()), ["a", "c"])
        self.assertEqual(len(store), 2)
        store.close()

    def test_items(self):
        store = KVStore(self.make_path())
        store.set("a", "1")
        store.set("b", "2")
        result = dict(store.items())
        self.assertEqual(result, {"a": b"1", "b": b"2"})
        store.close()


class TestPersistence(TempDBMixin, unittest.TestCase):
    def test_data_survives_reopen(self):
        path = self.make_path()
        store = KVStore(path)
        store.set("persisted", "yes")
        store.set("counter", "42")
        store.close()

        store2 = KVStore(path)
        self.assertEqual(store2.get("persisted"), b"yes")
        self.assertEqual(store2.get("counter"), b"42")
        self.assertEqual(len(store2), 2)
        store2.close()

    def test_delete_survives_reopen(self):
        path = self.make_path()
        store = KVStore(path)
        store.set("a", "1")
        store.set("b", "2")
        store.delete("a")
        store.close()

        store2 = KVStore(path)
        self.assertIsNone(store2.get("a"))
        self.assertEqual(store2.get("b"), b"2")
        self.assertEqual(len(store2), 1)
        store2.close()

    def test_overwrite_history_replayed_correctly_on_reopen(self):
        path = self.make_path()
        store = KVStore(path)
        for i in range(5):
            store.set("k", f"v{i}")
        store.close()

        store2 = KVStore(path)
        self.assertEqual(store2.get("k"), b"v4")  # only the last write should win
        store2.close()

    def test_truncated_tail_record_is_recovered_from(self):
        """Simulate a crash mid-write: append a well-formed record, then a
        second record whose header claims more value bytes than were
        actually flushed to disk. Reopening must discard the corrupt tail
        and keep everything written before it, and remain writable after."""
        path = self.make_path()
        store = KVStore(path)
        store.set("safe", "value")
        store.close()

        # Hand-craft a truncated tail record directly on disk: a valid
        # header claiming a 100-byte value, but only 3 bytes actually follow.
        with open(path, "ab") as f:
            key = b"broken"
            header = _HEADER.pack(0, len(key), 100)  # lies about value length
            f.write(header + key + b"xyz")

        store2 = KVStore(path)
        self.assertEqual(store2.get("safe"), b"value")
        self.assertIsNone(store2.get("broken"))  # never fully committed
        # store must still be writable after recovering from a truncated tail
        store2.set("after_recovery", "ok")
        self.assertEqual(store2.get("after_recovery"), b"ok")
        store2.close()

        # and that write should itself survive another reopen
        store3 = KVStore(path)
        self.assertEqual(store3.get("after_recovery"), b"ok")
        store3.close()

    def test_context_manager_closes_file(self):
        path = self.make_path()
        with KVStore(path) as store:
            store.set("k", "v")
        # file handle should be closed; reopening should still work
        store2 = KVStore(path)
        self.assertEqual(store2.get("k"), b"v")
        store2.close()


class TestCompaction(TempDBMixin, unittest.TestCase):
    def test_compact_shrinks_file_with_many_overwrites(self):
        path = self.make_path()
        store = KVStore(path)
        for i in range(200):
            store.set("hot_key", f"value_number_{i}")  # same key, 200 writes
        size_before = os.path.getsize(path)
        old_size, new_size = store.compact()
        size_after = os.path.getsize(path)

        self.assertEqual(old_size, size_before)
        self.assertEqual(new_size, size_after)
        self.assertLess(new_size, old_size)
        self.assertEqual(store.get("hot_key"), b"value_number_199")
        store.close()

    def test_compact_drops_deleted_keys(self):
        path = self.make_path()
        store = KVStore(path)
        store.set("keep", "1")
        store.set("drop", "2")
        store.delete("drop")
        store.compact()

        self.assertEqual(sorted(store.keys()), ["keep"])
        self.assertIsNone(store.get("drop"))
        store.close()

    def test_compact_preserves_correctness_across_reopen(self):
        path = self.make_path()
        store = KVStore(path)
        for i in range(50):
            store.set(f"key{i % 5}", f"v{i}")  # 5 keys, overwritten repeatedly
        store.compact()
        store.close()

        store2 = KVStore(path)
        for i in range(5):
            # last write for key{i} was when the loop index was the largest
            # value < 50 with (index % 5 == i)
            expected_i = max(j for j in range(50) if j % 5 == i)
            self.assertEqual(store2.get(f"key{i}"), f"v{expected_i}".encode())
        store2.close()

    def test_compact_on_empty_store(self):
        store = KVStore(self.make_path())
        old, new = store.compact()
        self.assertEqual(len(store), 0)
        store.close()


class TestCorruptRecord(TempDBMixin, unittest.TestCase):
    def test_unknown_flag_raises(self):
        path = self.make_path()
        # write a record with an invalid flag byte (2) directly
        with open(path, "wb") as f:
            key = b"k"
            val = b"v"
            f.write(_HEADER.pack(2, len(key), len(val)) + key + val)

        with self.assertRaises(CorruptRecordError):
            KVStore(path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
