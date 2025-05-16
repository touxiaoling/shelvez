import os
import sqlite3
from pathlib import Path
from contextlib import suppress, closing
from collections.abc import MutableMapping

from .zstd import ZstdCompressor

BUILD_TABLE = """
  CREATE TABLE IF NOT EXISTS Dict (
    key TEXT UNIQUE NOT NULL PRIMARY KEY,
    value BLOB NOT NULL
  )
"""
GET_SIZE = "SELECT COUNT (key) FROM Dict"
LOOKUP_KEY = "SELECT value FROM Dict WHERE key = CAST(? AS TEXT)"
STORE_KV = "REPLACE INTO Dict (key, value) VALUES (CAST(? AS TEXT), CAST(? AS BLOB))"
DELETE_KEY = "DELETE FROM Dict WHERE key = CAST(? AS TEXT)"
ITER_KEYS = "SELECT key FROM Dict"
BUILD_ZSTD_TABLE = """
  CREATE TABLE IF NOT EXISTS Zstd (
    key TEXT UNIQUE NOT NULL,
    value BLOB NOT NULL
  )
"""
LOOKUP_ZSTD = "SELECT value FROM Zstd WHERE key = ?"
STORE_ZSTD = "INSERT OR REPLACE INTO Zstd (key, value) VALUES (?, ?)"


class error(OSError):
    pass


_ERR_CLOSED = "DBM object has already been closed"
_ERR_REINIT = "DBM object does not support reinitialization"


def _normalize_uri(path):
    path = Path(path)
    uri = path.absolute().as_uri()
    while "//" in uri:
        uri = uri.replace("//", "/")
    return uri


class _Database(MutableMapping):
    def __init__(self, path, /, *, flag, mode, sqlite3_kargs={}):
        if hasattr(self, "_cx"):
            raise error(_ERR_REINIT)

        path = os.fsdecode(path)
        match flag:
            case "r":
                flag = "ro"
            case "w":
                flag = "rw"
            case "c":
                flag = "rwc"
                Path(path).touch(mode=mode, exist_ok=True)
            case "n":
                flag = "rwc"
                Path(path).unlink(missing_ok=True)
                Path(path).touch(mode=mode)
            case _:
                raise ValueError(f"Flag must be one of 'r', 'w', 'c', or 'n', not {flag!r}")

        # We use the URI format when opening the database.
        uri = _normalize_uri(path)
        uri = f"{uri}?mode={flag}"

        try:
            self._cx = sqlite3.connect(uri, uri=True, **sqlite3_kargs)
        except sqlite3.Error as exc:
            raise error(str(exc))

        # This is an optimization only; it's ok if it fails.
        with suppress(sqlite3.OperationalError):
            self._cx.execute("PRAGMA journal_mode = wal")
            self._cx.execute("PRAGMA synchronous = normal")
            self._cx.execute("PRAGMA busy_timeout = 5000")
            # self._cx.execute("PRAGMA cache_size = -20000")
            # self._cx.execute("PRAGMA temp_store = MEMORY")
            # self._cx.execute("PRAGMA mmap_size = 2147483648")
            # self._cx.execute("PRAGMA page_size = 8192")

        if flag == "rwc":
            self._execute(BUILD_TABLE)
        self._execute(BUILD_ZSTD_TABLE)

        zstd_dict = self._load_zstd_dict()
        self.compressor = ZstdCompressor(zstd_dict=zstd_dict)

    def _execute(self, *args, **kwargs):
        if not self._cx:
            raise error(_ERR_CLOSED)
        try:
            return closing(self._cx.execute(*args, **kwargs))
        except sqlite3.Error as exc:
            raise error(str(exc))

    def __len__(self):
        with self._execute(GET_SIZE) as cu:
            row = cu.fetchone()
        return row[0]

    def __getitem__(self, key):
        with self._execute(LOOKUP_KEY, (key,)) as cu:
            row = cu.fetchone()
        if not row:
            raise KeyError(key)
        value = self.compressor.decompress(row[0])
        return value

    def __setitem__(self, key, value):
        value = self.compressor.compress(value)
        self._execute(STORE_KV, (key, value))

    def __delitem__(self, key):
        with self._execute(DELETE_KEY, (key,)) as cu:
            if not cu.rowcount:
                raise KeyError(key)

    def __iter__(self):
        try:
            with self._execute(ITER_KEYS) as cu:
                for row in cu:
                    yield row[0]
        except sqlite3.Error as exc:
            raise error(str(exc))

    def _load_zstd_dict(self):
        with self._execute(LOOKUP_ZSTD, ("dict",)) as cu:
            row = cu.fetchone()
        if row:
            return row[0]

    def _save_zstd_dict(self, zstd_dict):
        self._execute(STORE_ZSTD, ("dict", zstd_dict))

    def optimize_database(self):
        samples = [value for value in self.values()]
        zstd_dict = ZstdCompressor.optimize_dict(samples)

        rows = [(k, v) for k, v in self.items()]
        self.compressor = ZstdCompressor(zstd_dict=zstd_dict)
        [self.__setitem__(k, v) for k, v in rows]
        self._save_zstd_dict(zstd_dict)
        self._execute("VACUUM")

    def close(self):
        if self._cx:
            self._cx.close()
            self._cx = None

    def keys(self):
        return list(super().keys())

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def open(filename, /, flag="r", mode=0o666, sqlite3_kargs={}):
    """Open a dbm.sqlite3 database and return the dbm object.

    The 'filename' parameter is the name of the database file.

    The optional 'flag' parameter can be one of ...:
        'r' (default): open an existing database for read only access
        'w': open an existing database for read/write access
        'c': create a database if it does not exist; open for read/write access
        'n': always create a new, empty database; open for read/write access

    The optional 'mode' parameter is the Unix file access mode of the database;
    only used when creating a new database. Default: 0o666.
    """
    return _Database(filename, flag=flag, mode=mode, sqlite3_kargs=sqlite3_kargs)
