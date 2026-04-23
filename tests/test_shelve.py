"""Functional tests for :mod:`shelvez.shelve`.

Benchmarks live in ``tests/benchmarks/`` and are excluded from the default
``pytest`` run.
"""

from __future__ import annotations

import pytest

import shelvez


def test_basic_pickle_roundtrip(db_path: str):
    with shelvez.open(db_path, flag="c") as db:
        db["k1"] = "value1"
        db["k2"] = 123
        db["k3"] = {"nested": [1, 2, 3]}

        assert db["k1"] == "value1"
        assert db["k2"] == 123
        assert db["k3"] == {"nested": [1, 2, 3]}


def test_pydantic_serializer(db_path: str):
    from pydantic import BaseModel

    class MyModel(BaseModel):
        key: str
        key2: str = "default"

    serializer = shelvez.serialer.PydanticSerializer(MyModel)
    with shelvez.open(db_path, flag="c", serializer=serializer) as db:
        db["a"] = MyModel(key="v1")
        db["b"] = MyModel(key="v2", key2="explicit")
        assert db["a"] == MyModel(key="v1")
        assert db["b"] == MyModel(key="v2", key2="explicit")


def test_persistence_across_reopen(db_path: str):
    with shelvez.open(db_path, flag="c") as db:
        db["persisted"] = {"n": 42}

    with shelvez.open(db_path, flag="r") as db:
        assert db["persisted"] == {"n": 42}


def test_len_iter_contains_delete(db_path: str):
    with shelvez.open(db_path, flag="c") as db:
        db["a"] = 1
        db["b"] = 2
        db["c"] = 3

        assert len(db) == 3
        assert set(iter(db)) == {"a", "b", "c"}
        assert "b" in db

        del db["b"]
        assert "b" not in db
        assert len(db) == 2


def test_get_default(db_path: str):
    with shelvez.open(db_path, flag="c") as db:
        db["exists"] = "yes"
        assert db.get("exists") == "yes"
        assert db.get("missing") is None
        assert db.get("missing", "fallback") == "fallback"


def test_writeback_buffers_mutations(db_path: str):
    """With ``writeback=True`` in-place mutations of mutable values must be
    flushed back on :meth:`sync` / :meth:`close`."""
    with shelvez.open(db_path, flag="c", writeback=True) as db:
        db["list"] = [1, 2, 3]
        db["list"].append(4)  # relies on writeback cache
        db.sync()

    with shelvez.open(db_path, flag="r") as db:
        assert db["list"] == [1, 2, 3, 4]


def test_writeback_populates_cache_on_first_read(db_path: str):
    """Regression for :file:`shelvez/shelve.py`'s ``__getitem__`` writeback
    branch: when a key is read from disk for the first time under
    ``writeback=True``, the deserialized value must be stashed into the
    in-memory cache so that subsequent in-place mutations are captured on
    :meth:`sync`.

    The existing ``test_writeback_buffers_mutations`` only exercises the
    cache-hit-after-set path; this one exercises the cache-miss-then-fill
    path, which is the actual reason ``writeback`` exists."""
    # Seed without writeback so the cache starts empty on reopen.
    with shelvez.open(db_path, flag="c") as db:
        db["list"] = [1, 2, 3]

    with shelvez.open(db_path, flag="c", writeback=True) as db:
        assert db.cache == {}
        value = db["list"]
        assert value == [1, 2, 3]
        assert db.cache == {"list": [1, 2, 3]}
        value.append(4)

    with shelvez.open(db_path, flag="r") as db:
        assert db["list"] == [1, 2, 3, 4]


def test_no_writeback_drops_inplace_mutations(db_path: str):
    """Sanity check of the complementary case: without writeback, in-place
    mutations on cached values are *not* persisted."""
    with shelvez.open(db_path, flag="c", writeback=False) as db:
        db["list"] = [1, 2, 3]
        db["list"].append(4)

    with shelvez.open(db_path, flag="r") as db:
        assert db["list"] == [1, 2, 3]


def test_access_after_close_raises(db_path: str):
    db = shelvez.open(db_path, flag="c")
    db["k"] = "v"
    db.close()

    with pytest.raises(ValueError):
        _ = db["k"]


def test_clear(db_path: str):
    with shelvez.open(db_path, flag="c") as db:
        db["a"] = 1
        db["b"] = 2
        db.clear()
        assert len(db) == 0
        assert "a" not in db
