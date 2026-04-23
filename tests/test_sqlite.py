"""Tests for :mod:`shelvez.sqlite`._Database`.

This module is the foundational storage layer (zstd-compressed blobs in a
SQLite ``Dict`` table) used by both :mod:`shelvez.shelve` and
:mod:`shelvez.sqlcache`. The tests below exercise the flag semantics,
MutableMapping protocol, persistence of the trained zstd dictionary across
reopens, and ``optimize_database``.
"""

from __future__ import annotations

import pickle
from pathlib import Path

import pytest

from shelvez import sqlite as shelvez_sqlite


def _dumps(obj) -> bytes:
    return pickle.dumps(obj, protocol=5)


def _open(path: str, flag: str = "c"):
    """Open ``_Database`` the way ``shelvez.open`` does.

    The raw class requires ``autocommit=True`` in order for writes to actually
    land (and so that ``VACUUM`` in :meth:`optimize_database` is not run
    inside an implicit transaction)."""
    return shelvez_sqlite.open(path, flag=flag, sqlite3_kargs={"autocommit": True})


class TestFlags:
    def test_flag_r_requires_existing_file(self, tmp_path: Path):
        missing = tmp_path / "missing.db"
        with pytest.raises(shelvez_sqlite.error):
            _open(str(missing), flag="r")

    def test_flag_c_creates_file(self, db_path: str):
        db = _open(db_path, flag="c")
        try:
            assert Path(db_path).exists()
        finally:
            db.close()

    def test_flag_n_truncates(self, db_path: str):
        db = _open(db_path, flag="c")
        db["keep"] = _dumps("value")
        db.close()

        db2 = _open(db_path, flag="n")
        try:
            assert "keep" not in db2
            assert len(db2) == 0
        finally:
            db2.close()

    def test_flag_r_is_read_only(self, db_path: str):
        db = _open(db_path, flag="c")
        db["k"] = _dumps("v")
        db.close()

        ro = _open(db_path, flag="r")
        try:
            assert ro["k"] == _dumps("v")
            with pytest.raises(shelvez_sqlite.error):
                ro["k2"] = _dumps("v2")
        finally:
            ro.close()

    def test_invalid_flag(self, db_path: str):
        with pytest.raises(ValueError):
            _open(db_path, flag="x")

    def test_flag_w_requires_existing_file(self, tmp_path: Path):
        missing = tmp_path / "missing.db"
        with pytest.raises(shelvez_sqlite.error):
            _open(str(missing), flag="w")

    def test_flag_w_opens_existing_read_write(self, db_path: str):
        with _open(db_path, flag="c") as db:
            db["k"] = _dumps("initial")

        with _open(db_path, flag="w") as db:
            assert db["k"] == _dumps("initial")
            db["k2"] = _dumps("added")
            del db["k"]

        with _open(db_path, flag="r") as db:
            assert "k" not in db
            assert db["k2"] == _dumps("added")

    def test_flag_w_does_not_truncate(self, db_path: str):
        """Unlike ``n``, ``w`` must not clobber existing data."""
        with _open(db_path, flag="c") as db:
            for i in range(3):
                db[f"k{i}"] = _dumps(i)

        with _open(db_path, flag="w") as db:
            assert len(db) == 3


class TestMapping:
    def test_roundtrip(self, db_path: str):
        db = _open(db_path, flag="c")
        try:
            payload = _dumps({"a": 1, "b": [1, 2, 3]})
            db["k"] = payload
            assert db["k"] == payload
        finally:
            db.close()

    def test_missing_key_raises(self, db_path: str):
        db = _open(db_path, flag="c")
        try:
            with pytest.raises(KeyError):
                _ = db["nope"]
        finally:
            db.close()

    def test_delete(self, db_path: str):
        db = _open(db_path, flag="c")
        try:
            db["k"] = _dumps(1)
            del db["k"]
            assert "k" not in db
            with pytest.raises(KeyError):
                del db["k"]
        finally:
            db.close()

    def test_len_iter_contains(self, db_path: str):
        db = _open(db_path, flag="c")
        try:
            for i in range(5):
                db[f"k{i}"] = _dumps(i)
            assert len(db) == 5
            assert set(iter(db)) == {f"k{i}" for i in range(5)}
            assert "k3" in db
            assert "missing" not in db
        finally:
            db.close()

    def test_context_manager_closes(self, db_path: str):
        with _open(db_path, flag="c") as db:
            db["k"] = _dumps("v")
        with pytest.raises(shelvez_sqlite.error):
            _ = db["k"]


class TestCompression:
    def test_values_are_compressed(self, db_path: str):
        """Raw stored bytes should differ from the input payload, proving zstd
        compression is actually happening."""
        import sqlite3

        db = _open(db_path, flag="c")
        try:
            payload = _dumps("x" * 2048)
            db["big"] = payload
        finally:
            db.close()

        cx = sqlite3.connect(db_path)
        try:
            raw = cx.execute("SELECT value FROM Dict WHERE key = ?", ("big",)).fetchone()[0]
        finally:
            cx.close()

        assert raw != payload
        assert len(raw) < len(payload)


class TestOptimizeDatabase:
    def test_optimize_trains_dict_and_keeps_values(self, db_path: str):
        db = _open(db_path, flag="c")
        try:
            originals = {f"k{i}": _dumps({"user": "alice", "n": i}) for i in range(200)}
            for k, v in originals.items():
                db[k] = v

            db.optimize_database()

            for k, v in originals.items():
                assert db[k] == v
        finally:
            db.close()

    def test_trained_dict_persists_across_reopen(self, db_path: str):
        db = _open(db_path, flag="c")
        try:
            originals = {f"k{i}": _dumps({"user": "alice", "n": i}) for i in range(200)}
            for k, v in originals.items():
                db[k] = v
            db.optimize_database()
        finally:
            db.close()

        # Reopen: _load_zstd_dict must pick up the persisted dict so that
        # decompressing the previously-written blobs still works.
        db2 = _open(db_path, flag="r")
        try:
            for k, v in originals.items():
                assert db2[k] == v
        finally:
            db2.close()


class TestClose:
    def test_operations_after_close_raise(self, db_path: str):
        db = _open(db_path, flag="c")
        db["k"] = _dumps("v")
        db.close()

        with pytest.raises(shelvez_sqlite.error):
            _ = db["k"]

    def test_double_close_is_safe(self, db_path: str):
        db = _open(db_path, flag="c")
        db.close()
        db.close()
