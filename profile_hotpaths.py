"""Focused profiler for shelvez hot paths.

Runs three workloads and prints a cProfile pstats summary for each so we
can see where time actually goes before optimising:

1. ``shelve_rw``: 10k random write+read roundtrips (mirrors the benchmark).
2. ``sqlcache_miss``: 2k fresh keys through the decorator (memory cache
   empty, compute + serialize + zstd + sqlite insert).
3. ``sqlcache_hit_disk``: 2k keys already on disk, but the in-memory
   cache is pre-cleared so every call exercises the SQLite ``get`` path
   (this is the path that currently also does an ``UPDATE access_count``
   write on every read).

Run with:

    uv run python profile_hotpaths.py
"""

from __future__ import annotations

import cProfile
import io
import pstats
import random
import tempfile
import time
from pathlib import Path

import shelvez
from shelvez import sqlcache


def _make_payload(n: int = 10_000) -> dict[str, dict]:
    rng = random.Random(0xC0FFEE)
    return {str(rng.randint(1000, 9_999_999)): {"value": str(rng.randint(1_000_000, 9_999_999))} for _ in range(n)}


def _profile(label: str, fn, *, top: int = 25) -> float:
    pr = cProfile.Profile()
    t0 = time.perf_counter()
    pr.enable()
    try:
        fn()
    finally:
        pr.disable()
    elapsed = time.perf_counter() - t0

    buf = io.StringIO()
    stats = pstats.Stats(pr, stream=buf).strip_dirs().sort_stats("cumulative")
    stats.print_stats(top)
    print(f"\n=== {label}  (wall {elapsed * 1000:.1f} ms) ===")
    print(buf.getvalue())

    buf = io.StringIO()
    pstats.Stats(pr, stream=buf).strip_dirs().sort_stats("tottime").print_stats(top)
    print(f"--- {label}  [tottime] ---")
    print(buf.getvalue())
    return elapsed


def workload_shelve_rw(db_path: str) -> None:
    data = _make_payload(10_000)
    db = shelvez.open(db_path, flag="c")
    try:
        db.dict.begin()
        for k, v in data.items():
            db[k] = v
        db.dict.commit()
        for k, v in data.items():
            assert db[k] == v
    finally:
        db.close()


def workload_sqlcache_miss(cache_path: str) -> None:
    @sqlcache.lru_cache(cache_path=cache_path, max_size=10_000, multiprocess_safe=False)
    def compute(x: int) -> dict:
        return {"x": x, "s": str(x) * 4}

    for i in range(2_000):
        compute(i)


def workload_sqlcache_hit_disk(cache_path: str) -> None:
    cache = sqlcache.SqlCache(
        cache_path=cache_path,
        max_size=10_000,
        cache_type="lru",
        multiprocess_safe=False,
    )

    @cache
    def compute(x: int) -> dict:
        return {"x": x, "s": str(x) * 4}

    for i in range(2_000):
        compute(i)
    cache._memory_cache.clear()

    for i in range(2_000):
        compute(i)
    cache.close()


def _best_of(label: str, fn, trials: int = 5) -> float:
    """Warm once, then return the best wall time of ``trials`` runs.

    Wall time is noisier than cProfile's accumulated stats; best-of-N
    gives us a sharper signal when comparing before/after.
    """
    fn()  # warm
    best = float("inf")
    for _ in range(trials):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    print(f"{label}: best-of-{trials} {best * 1000:.2f} ms")
    return best


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        _profile("shelve_rw (10k writes+reads)", lambda: workload_shelve_rw(str(tmp_path / "shelve.db")))
        _profile("sqlcache_miss (2k compute+store)", lambda: workload_sqlcache_miss(str(tmp_path / "miss.db")))
        _profile("sqlcache_hit_disk (2k disk hits)", lambda: workload_sqlcache_hit_disk(str(tmp_path / "hit.db")))

        print("\n=== best-of-5 wall time (excludes import + DB open overhead) ===")
        _best_of("shelve_rw         ", lambda: workload_shelve_rw(str(tmp_path / "shelve_bo.db")))
        _best_of("sqlcache_miss     ", lambda: workload_sqlcache_miss(str(tmp_path / "miss_bo.db")))
        _best_of("sqlcache_hit_disk ", lambda: workload_sqlcache_hit_disk(str(tmp_path / "hit_bo.db")))


if __name__ == "__main__":
    main()
