import collections.abc
from typing import cast

from .serializer import BaseSerializer, PickleSerializer
from . import sqlite

__all__ = ["Shelf", "open"]


class _ClosedDict(collections.abc.MutableMapping):
    "Marker for a closed dict.  Access attempts raise a ValueError."

    def closed(self, *args):
        raise ValueError("invalid operation on closed shelf")

    __iter__ = __len__ = __getitem__ = __setitem__ = __delitem__ = keys = closed

    def __repr__(self):
        return "<Closed Dictionary>"


class Shelf(collections.abc.MutableMapping):
    """Persistent dict-like shelf backed by a single SQLite file."""

    def __init__(
        self,
        filename,
        flag: str = "c",
        writeback: bool = False,
        serializer: BaseSerializer | None = None,
    ):
        sqlite3_kargs = {"autocommit": True, "check_same_thread": False}
        # 运行时 self.dict 在 close() 后会被替换为 _ClosedDict() 哨兵，
        # 对外正常操作都走 _Database；此处按 _Database 声明，close 里用
        # cast 做类型兜底，避免在每个调用点做无意义的类型收窄。
        self.dict: sqlite._Database = sqlite.open(filename, flag, sqlite3_kargs=sqlite3_kargs)
        self.writeback = writeback
        self.cache: dict[str, object] = {}
        self.serializer: BaseSerializer = serializer or PickleSerializer()
        self._closed = False

    def __iter__(self):
        return iter(self.dict.keys())

    def __len__(self):
        return len(self.dict)

    def __contains__(self, key: object) -> bool:
        return key in self.dict

    def __getitem__(self, key: str):
        try:
            return self.cache[key]
        except KeyError:
            pass
        value = self.serializer.unserialize(self.dict[key])
        if self.writeback:
            self.cache[key] = value
        return value

    def __setitem__(self, key: str, value):
        if self.writeback:
            self.cache[key] = value
        self.dict[key] = self.serializer.serialize(value)

    def __delitem__(self, key: str):
        del self.dict[key]
        self.cache.pop(key, None)

    def clear(self):
        """Remove all items from the shelf."""
        # see https://github.com/python/cpython/issues/107089
        self.cache.clear()
        self.dict.clear()

    def __enter__(self):
        # Opens an explicit SQLite transaction so all writes inside the
        # ``with`` block are batched into a single commit on success (or
        # rolled back on exception). This is a big speed win for bulk
        # ``db[k] = v`` loops; previously every item was its own fsync-ing
        # write under ``autocommit=True``.
        self.dict.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                # Flush writeback cache so its entries participate in the
                # pending transaction before we commit.
                self.sync()
                self.dict.commit()
            else:
                # Drop pending writeback so ``close()`` doesn't replay those
                # writes after we just rolled the transaction back.
                self.cache = {}
                self.dict.rollback()
        finally:
            self.close()

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self.sync()
            self.dict.close()
        finally:
            # 用 _ClosedDict 作为哨兵；之后再对 shelf 做任何操作都会在
            # _ClosedDict 上 raise ValueError。
            self.dict = cast(sqlite._Database, _ClosedDict())

    def __del__(self):
        # __init__ 失败时可能没设置 writeback，避免 close() 再次出错。
        # see http://bugs.python.org/issue1339007
        if not hasattr(self, "writeback"):
            return
        self.close()

    def sync(self):
        if self.writeback and self.cache:
            self.writeback = False
            for key, entry in self.cache.items():
                self[key] = entry
            self.writeback = True
            self.cache = {}


def open(
    filename,
    flag: str = "c",
    writeback: bool = False,
    serializer: BaseSerializer | None = None,
) -> Shelf:
    """Open a persistent dictionary backed by SQLite + zstd.

    Args:
        filename: Path to the SQLite database file.
        flag: 'r' (read-only), 'w' (read/write existing), 'c' (create if
            missing, default), or 'n' (always create a new, empty db).
        writeback: Cache every read value so in-place mutations are
            preserved and written back on ``sync()`` / ``close()``.
        serializer: Value serializer. Defaults to :class:`PickleSerializer`.
    """
    return Shelf(filename, flag, writeback, serializer=serializer)
