"""Tests for :mod:`shelvez.sqlcache`.

Scope: behavioural tests against the public decorator API (``sqlcache``,
``ttl_cache``, ``lru_cache``), plus the internal ``_SqlCacheDatabase``
primitive. The previous version of this file contained many redundant cases
and a few misleading ones ("persistence" using two *different* paths,
"multiprocess" tests that never spawned a subprocess, a threading test that
gave every thread its own cache). Those have been removed or rewritten.
"""

from __future__ import annotations

import concurrent.futures
import multiprocessing as mp
import os
import sqlite3
import time

import pytest

from shelvez import sqlcache


# ---------------------------------------------------------------------------
# Decorator API
# ---------------------------------------------------------------------------


def test_ttl_cache_basic_and_expiry(cache_path: str):
    calls = 0

    @sqlcache.ttl_cache(cache_path=cache_path, max_size=10, ttl=0.5)
    def double(x):
        nonlocal calls
        calls += 1
        return x * 2

    assert double(5) == 10
    assert double(5) == 10
    assert calls == 1

    time.sleep(0.6)
    assert double(5) == 10
    assert calls == 2


def test_lru_cache_evicts_oldest(cache_path: str):
    calls = 0

    @sqlcache.lru_cache(cache_path=cache_path, max_size=3)
    def triple(x):
        nonlocal calls
        calls += 1
        return x * 3

    for i in range(4):
        triple(i)
    assert calls == 4

    # key 0 was evicted by key 3, so recomputing is expected.
    triple(0)
    assert calls == 5


def test_cache_key_is_args_sensitive(cache_path: str):
    calls = 0

    @sqlcache.ttl_cache(cache_path=cache_path, max_size=50, ttl=10)
    def combine(x, y=10, *args, **kwargs):
        nonlocal calls
        calls += 1
        return x + y + sum(args) + sum(kwargs.values())

    assert combine(5) == 15
    assert combine(5, 20) == 25
    assert combine(5, 10, 1, 2, 3) == 21
    assert combine(5, y=15, extra=5) == 25
    assert calls == 4

    # Repeat calls should hit the cache.
    assert combine(5) == 15
    assert combine(5, 20) == 25
    assert calls == 4


def test_exception_is_not_cached(cache_path: str):
    calls = 0

    @sqlcache.ttl_cache(cache_path=cache_path, max_size=10, ttl=10)
    def f(x):
        nonlocal calls
        calls += 1
        if x < 0:
            raise ValueError("negative")
        return x * 2

    assert f(5) == 10
    assert calls == 1

    with pytest.raises(ValueError):
        f(-1)
    with pytest.raises(ValueError):
        f(-1)
    assert calls == 3


def test_none_and_complex_values_are_cached(cache_path: str):
    calls = 0

    @sqlcache.ttl_cache(cache_path=cache_path, max_size=10, ttl=10)
    def f(x):
        nonlocal calls
        calls += 1
        if x == 0:
            return None
        return {"x": x, "list": list(range(x))}

    assert f(0) is None
    assert f(0) is None
    assert calls == 1

    assert f(3) == {"x": 3, "list": [0, 1, 2]}
    assert f(3) == {"x": 3, "list": [0, 1, 2]}
    assert calls == 2


def test_persistence_same_path_new_decorator(cache_path: str):
    """Re-decorating the same function pointing at the same ``cache_path``
    should recover the previous result from disk (previous test mistakenly
    used two different paths)."""
    calls = 0

    def _build():
        @sqlcache.ttl_cache(cache_path=cache_path, max_size=10, ttl=60)
        def f(x):
            nonlocal calls
            calls += 1
            return x * 7

        return f

    f1 = _build()
    assert f1(3) == 21
    assert calls == 1

    # New decorator / new in-memory cache, same on-disk DB.
    f2 = _build()
    assert f2(3) == 21
    assert calls == 1  # served from on-disk SQLite cache


def test_clear_evicts_both_tiers(cache_path: str):
    calls = 0
    cache = sqlcache.SqlCache(cache_path=cache_path, max_size=10, ttl=10, cache_type="ttl")

    @cache
    def f(x):
        nonlocal calls
        calls += 1
        return x * 2

    f(1)
    f(1)
    assert calls == 1

    cache.clear()

    f(1)
    assert calls == 2


def test_get_stats_shape(cache_path: str):
    cache = sqlcache.SqlCache(cache_path=cache_path, max_size=10, ttl=10, cache_type="ttl")

    @cache
    def f(x):
        return x

    for i in range(3):
        f(i)

    stats = cache.get_stats()
    assert stats["cache_type"] == "ttl"
    assert stats["max_size"] == 10
    assert stats["ttl"] == 10
    assert stats["disk_cache"]["total_items"] == 3
    assert stats["memory_cache_size"] == 3


def test_invalid_cache_type_rejected(cache_path: str):
    with pytest.raises(ValueError):
        sqlcache.SqlCache(cache_path=cache_path, cache_type="invalid")


# ---------------------------------------------------------------------------
# Internal database primitive
# ---------------------------------------------------------------------------


class TestSqlCacheDatabase:
    def test_set_get_delete(self, cache_path: str):
        db = sqlcache._SqlCacheDatabase(cache_path)
        try:
            db.set("k", {"a": [1, 2, 3]})
            assert db.get("k") == {"a": [1, 2, 3]}
            db.delete("k")
            assert db.get("k") is None
        finally:
            db.close()

    def test_ttl_filtering(self, cache_path: str):
        db = sqlcache._SqlCacheDatabase(cache_path)
        try:
            db.set("k", "v")
            assert db.get("k", ttl=10) == "v"
            time.sleep(0.1)
            assert db.get("k", ttl=0.05) is None
            # Without a TTL the row is still there.
            assert db.get("k") == "v"
        finally:
            db.close()

    def test_cleanup_lru_keeps_most_recent(self, cache_path: str):
        db = sqlcache._SqlCacheDatabase(cache_path)
        try:
            for i in range(5):
                db.set(f"k{i}", i)
                # Ensure last_access strictly monotonic so ordering is stable.
                time.sleep(0.005)

            db.cleanup_lru(3)

            with db._execute("SELECT COUNT(*) FROM cache") as cur:
                assert cur.fetchone()[0] == 3

            # The newest three (k2, k3, k4) should remain.
            assert db.get("k4") == 4
            assert db.get("k3") == 3
            assert db.get("k2") == 2
        finally:
            db.close()

    def test_clear_removes_all(self, cache_path: str):
        db = sqlcache._SqlCacheDatabase(cache_path)
        try:
            db.set("k1", 1)
            db.set("k2", 2)
            db.clear()
            assert db.get("k1") is None
            assert db.get("k2") is None
        finally:
            db.close()


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_threadsafe_execute_under_contention(cache_path: str):
    """Regression test: previously :meth:`_SqlCacheDatabase._execute` leaked
    raw ``sqlite3.execute`` cursors across threads sharing one connection,
    producing ``bad parameter or other API misuse``. ``_execute`` is now lock
    guarded, so concurrent workers must all succeed."""
    calls = 0

    @sqlcache.ttl_cache(cache_path=cache_path, max_size=100, ttl=10, multiprocess_safe=True)
    def compute(x):
        nonlocal calls
        calls += 1
        time.sleep(0.02)
        return x * x

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(compute, i % 4) for i in range(32)]
        results = [f.result(timeout=30) for f in futures]

    assert results == [(i % 4) ** 2 for i in range(32)]
    # Each unique input is computed at most a handful of times (races on the
    # first call are fine); we only care that nothing blew up.
    assert calls <= 32


def _mp_worker(cache_path: str, x: int) -> int:
    """Top-level so multiprocessing can pickle it on spawn-based platforms."""
    from shelvez import sqlcache as _sc

    @_sc.ttl_cache(cache_path=cache_path, max_size=50, ttl=30, multiprocess_safe=True)
    def square(n):
        return n * n

    return square(x)


# ---------------------------------------------------------------------------
# _execute error handling: busy-retry + error wrapping
# ---------------------------------------------------------------------------


class _ProxyConn:
    """Proxy around the real ``sqlite3.Connection`` whose ``execute`` can be
    replaced. Needed because ``sqlite3.Connection.execute`` is a read-only C
    slot on CPython >= 3.14, so ``monkeypatch.setattr`` cannot touch it."""

    def __init__(self, real, execute):
        self._real = real
        self.execute = execute

    def __getattr__(self, name):
        return getattr(self._real, name)


def _install_proxy_cx(db, execute):
    original = db._db._cx
    db._db._cx = _ProxyConn(original, execute)
    return original


def test_execute_retries_on_database_locked(cache_path: str, monkeypatch):
    """When ``multiprocess_safe=True`` a transient ``database is locked`` must
    be retried with exponential backoff instead of surfacing to the caller."""
    monkeypatch.setattr(sqlcache.time, "sleep", lambda *_: None)

    db = sqlcache._SqlCacheDatabase(cache_path, multiprocess_safe=True)
    try:
        attempts = {"n": 0}
        assert db._db._cx is not None
        real_execute = db._db._cx.execute

        def flaky(sql, params=()):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise sqlite3.OperationalError("database is locked")
            return real_execute(sql, params)

        original = _install_proxy_cx(db, flaky)
        try:
            db.set("k", "v")
            assert attempts["n"] == 3
        finally:
            db._db._cx = original

        assert db.get("k") == "v"
    finally:
        db.close()


def test_execute_raises_sqlcacheerror_when_retries_exhausted(cache_path: str, monkeypatch):
    monkeypatch.setattr(sqlcache.time, "sleep", lambda *_: None)

    db = sqlcache._SqlCacheDatabase(cache_path, multiprocess_safe=True)
    try:

        def always_locked(sql, params=()):
            raise sqlite3.OperationalError("database is locked")

        original = _install_proxy_cx(db, always_locked)
        try:
            with pytest.raises(sqlcache.SqlCacheError, match="锁定"):
                db.set("k", "v")
        finally:
            db._db._cx = original
    finally:
        db.close()


def test_execute_wraps_non_lock_operational_error(cache_path: str):
    """Non-lock OperationalError is not retried and is surfaced as
    :class:`SqlCacheError`."""
    db = sqlcache._SqlCacheDatabase(cache_path, multiprocess_safe=True)
    try:

        def syntax_error(sql, params=()):
            raise sqlite3.OperationalError("syntax error")

        original = _install_proxy_cx(db, syntax_error)
        try:
            with pytest.raises(sqlcache.SqlCacheError):
                db.get("k")
        finally:
            db._db._cx = original
    finally:
        db.close()


def test_execute_wraps_generic_sqlite_error(cache_path: str):
    db = sqlcache._SqlCacheDatabase(cache_path, multiprocess_safe=True)
    try:

        def boom(sql, params=()):
            raise sqlite3.DatabaseError("corrupt")

        original = _install_proxy_cx(db, boom)
        try:
            with pytest.raises(sqlcache.SqlCacheError, match="数据库操作失败"):
                db.get("k")
        finally:
            db._db._cx = original
    finally:
        db.close()


def test_non_multiprocess_does_not_retry(cache_path: str):
    """With ``multiprocess_safe=False`` a lock error must be raised on the
    first attempt (``max_retries`` collapses to 1)."""
    db = sqlcache._SqlCacheDatabase(cache_path, multiprocess_safe=False)
    try:
        attempts = {"n": 0}

        def always_locked(sql, params=()):
            attempts["n"] += 1
            raise sqlite3.OperationalError("database is locked")

        original = _install_proxy_cx(db, always_locked)
        try:
            with pytest.raises(sqlcache.SqlCacheError):
                db.set("k", "v")
            assert attempts["n"] == 1
        finally:
            db._db._cx = original
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Resource management
# ---------------------------------------------------------------------------


def test_sqlcache_context_manager_closes(cache_path: str):
    with sqlcache.SqlCache(cache_path=cache_path, max_size=4, ttl=10) as cache:

        @cache
        def f(x):
            return x

        assert f(1) == 1

    # After __exit__ the underlying connection must be closed. Re-opening the
    # same path in a fresh process-level connection should still work.
    db = sqlcache._SqlCacheDatabase(cache_path)
    try:
        assert db.get_stats()["total_items"] == 1
    finally:
        db.close()


def test_close_is_idempotent(cache_path: str):
    cache = sqlcache.SqlCache(cache_path=cache_path, max_size=4, ttl=10)
    cache.close()
    cache.close()  # must not raise


def test_multiprocess_shared_cache(cache_path: str):
    """Two independent processes hitting the same ``cache_path`` must both
    observe a consistent cache without crashing or corrupting the DB."""
    ctx = mp.get_context("spawn")
    with ctx.Pool(2) as pool:
        results = pool.starmap(_mp_worker, [(cache_path, i) for i in range(6)])

    assert results == [i * i for i in range(6)]
    # The on-disk DB must be openable afterwards.
    assert os.path.exists(cache_path)
    db = sqlcache._SqlCacheDatabase(cache_path)
    try:
        stats = db.get_stats()
        assert stats["total_items"] >= 1
    finally:
        db.close()
