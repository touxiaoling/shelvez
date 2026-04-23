import os
import sqlite3
from pathlib import Path
from contextlib import suppress, closing, contextmanager
from collections.abc import MutableMapping

from .zstd import ZstdCompressor

BUILD_TABLE = """
  CREATE TABLE IF NOT EXISTS Dict (
    key TEXT UNIQUE NOT NULL PRIMARY KEY,
    value BLOB NOT NULL
  )
"""
GET_SIZE = "SELECT COUNT(key) FROM Dict"
LOOKUP_KEY = "SELECT value FROM Dict WHERE key = ?"
EXISTS_KEY = "SELECT 1 FROM Dict WHERE key = ? LIMIT 1"
STORE_KV = "REPLACE INTO Dict (key, value) VALUES (?, ?)"
DELETE_KEY = "DELETE FROM Dict WHERE key = ?"
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
    _cx: sqlite3.Connection | None

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

        self._in_tx = False

        # Throughput PRAGMAs; all strictly advisory, so wrap in suppress.
        with suppress(sqlite3.OperationalError):
            self._cx.execute("PRAGMA journal_mode = wal")
            self._cx.execute("PRAGMA synchronous = normal")
            self._cx.execute("PRAGMA busy_timeout = 5000")
            self._cx.execute("PRAGMA cache_size = -20000")
            self._cx.execute("PRAGMA temp_store = MEMORY")
            self._cx.execute("PRAGMA mmap_size = 268435456")

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
        return self.compressor.decompress(row[0])

    def __contains__(self, key):
        with self._execute(EXISTS_KEY, (key,)) as cu:
            return cu.fetchone() is not None

    def __setitem__(self, key, value):
        value = self.compressor.compress(value)
        with self._execute(STORE_KV, (key, value)):
            pass

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
        with self._execute(STORE_ZSTD, ("dict", zstd_dict)):
            pass

    # ------------------------------------------------------------------
    # Explicit transactions
    # ------------------------------------------------------------------
    def begin(self):
        """Open an explicit SQLite transaction.

        Calling this while a transaction is already open is a no-op, which
        makes it safe to nest ``with db:`` inside another ``with db:`` or
        inside :meth:`transaction`.
        """
        if not self._cx:
            raise error(_ERR_CLOSED)
        if self._in_tx:
            return
        try:
            self._cx.execute("BEGIN")
        except sqlite3.Error as exc:
            raise error(str(exc))
        self._in_tx = True

    def commit(self):
        # NOTE: with ``autocommit=True`` (the mode we open connections in),
        # ``Connection.commit()`` is documented as a no-op. We therefore
        # drive the transaction with explicit SQL statements.
        if not self._cx or not self._in_tx:
            return
        try:
            self._cx.execute("COMMIT")
        finally:
            self._in_tx = False

    def rollback(self):
        if not self._cx or not self._in_tx:
            return
        try:
            self._cx.execute("ROLLBACK")
        finally:
            self._in_tx = False

    @contextmanager
    def transaction(self):
        """Batch several writes into a single SQLite transaction.

        Every ``__setitem__`` / ``__delitem__`` inside the block goes into
        the same transaction instead of each getting its own fsync. On
        exception the transaction is rolled back.
        """
        self.begin()
        try:
            yield self
        except BaseException:
            self.rollback()
            raise
        else:
            self.commit()

    def _executemany(self, sql, seq_of_params):
        if not self._cx:
            raise error(_ERR_CLOSED)
        try:
            return closing(self._cx.executemany(sql, seq_of_params))
        except sqlite3.Error as exc:
            raise error(str(exc))

    def optimize_database(self):
        # Read once, keep (key, decompressed_value) in memory so we don't
        # iterate the table twice.
        pairs = list(self.items())
        samples = [v for _, v in pairs]

        zstd_dict = ZstdCompressor.optimize_dict(samples)
        self.compressor = ZstdCompressor(zstd_dict=zstd_dict)

        compressed = [(k, self.compressor.compress(v)) for k, v in pairs]

        with self.transaction():
            with self._executemany(STORE_KV, compressed):
                pass
            with self._execute(STORE_ZSTD, ("dict", zstd_dict)):
                pass

        # VACUUM cannot run inside an active transaction.
        with self._execute("VACUUM"):
            pass

    def close(self):
        if self._cx:
            if self._in_tx:
                with suppress(sqlite3.Error):
                    self._cx.execute("ROLLBACK")
                self._in_tx = False
            self._cx.close()
            self._cx = None

    def keys(self):
        with self._execute(ITER_KEYS) as cu:
            return [row[0] for row in cu.fetchall()]

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, *args):
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
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
