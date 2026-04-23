"""Performance benchmarks for :mod:`shelvez`.

These are excluded from the default ``pytest`` run via the ``benchmark``
marker (see ``pyproject.toml``). Run them explicitly with::

    pytest tests/benchmarks -m benchmark
"""

from __future__ import annotations

import random
import shelve as stdlib_shelve
from dbm.sqlite3 import open as dbm_sqlite_open
from pathlib import Path

import pytest

import shelvez


pytestmark = pytest.mark.benchmark


def _make_payload(n: int = 10_000) -> dict[str, dict]:
    return {str(random.randint(1000, 9999)): {"value": str(random.randint(1_000_000, 9_999_999))} for _ in range(n)}


def _report_size(path: str, label: str) -> None:
    size_kb = Path(path).stat().st_size / 1024
    print(f"{label} database size: {size_kb:.2f} kB")


def test_shelvez_pickle_speed(db_path: str, benchmark):
    data = _make_payload()
    db = shelvez.open(db_path, flag="c")

    def run():
        for k, v in data.items():
            db[k] = v
            assert db[k] == v

    benchmark(run)
    db.dict.optimize_database()
    db.close()
    _report_size(db_path, "shelvez pickle")


def test_shelvez_json_speed(db_path: str, benchmark):
    data = _make_payload()
    db = shelvez.open(db_path, flag="c", serializer=shelvez.serialer.JsonSerializer())

    def run():
        for k, v in data.items():
            db[k] = v
            assert db[k] == v

    benchmark(run)
    db.dict.optimize_database()
    db.close()
    _report_size(db_path, "shelvez json")


def test_shelvez_pydantic_speed(db_path: str, benchmark):
    from pydantic import BaseModel

    class Item(BaseModel):
        value: str

    data = {str(random.randint(1000, 9999)): Item(value=str(random.randint(1_000_000, 9_999_999))) for _ in range(10_000)}
    db = shelvez.open(
        db_path,
        flag="c",
        serializer=shelvez.serialer.PydanticSerializer(model=Item),
    )

    def run():
        for k, v in data.items():
            db[k] = v
            assert db[k] == v

    benchmark(run)
    db.dict.optimize_database()
    db.close()
    _report_size(db_path, "shelvez pydantic")


def test_stdlib_shelve_speed(db_path: str, benchmark):
    data = _make_payload()

    # stdlib shelve needs the file to exist first.
    dbm_sqlite_open(db_path, flag="c").close()
    db = stdlib_shelve.open(db_path, flag="c")

    def run():
        for k, v in data.items():
            db[k] = v
            assert db[k] == v

    benchmark(run)
    db.close()
    _report_size(db_path, "stdlib shelve")
