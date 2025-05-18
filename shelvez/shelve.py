import collections.abc
from functools import partial
import json

from . import serialer
from . import sqlite

__all__ = ["Shelf", "DbfilenameShelf", "open"]


class _ClosedDict(collections.abc.MutableMapping):
    "Marker for a closed dict.  Access attempts raise a ValueError."

    def closed(self, *args):
        raise ValueError("invalid operation on closed shelf")

    __iter__ = __len__ = __getitem__ = __setitem__ = __delitem__ = keys = closed

    def __repr__(self):
        return "<Closed Dictionary>"


class Shelf(collections.abc.MutableMapping):
    """Base class for shelf implementations.

    This is initialized with a dictionary-like object.
    See the module's __doc__ string for an overview of the interface.
    """

    def __init__(self, dict: sqlite._Database, writeback=False, keyencoding="utf-8", serializer=None):
        self.dict = dict
        self.writeback = writeback
        self.cache = {}
        self.keyencoding = keyencoding
        if serializer is None:
            self.serializer = serialer.PickleSerializer()
        else:
            self.serializer: serialer.BaseSerializer = serializer

    def __iter__(self):
        for k in self.dict.keys():
            yield k

    def __len__(self):
        return len(self.dict)

    def __contains__(self, key: str):
        return key in self.dict

    def get(self, key: str, default=None):
        if key in self.dict:
            return self[key]
        return default

    def __getitem__(self, key: str):
        try:
            value = self.cache[key]
        except KeyError:
            f = self.dict[key]
            value = self.serializer.unserialize(f)
            if self.writeback:
                self.cache[key] = value
        return value

    def __setitem__(self, key: str, value: dict):
        if self.writeback:
            self.cache[key] = value
        self.dict[key] = self.serializer.serialize(value)

    def __delitem__(self, key: str):
        del self.dict[key]
        try:
            del self.cache[key]
        except KeyError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        if self.dict is None:
            return
        try:
            self.sync()
            try:
                self.dict.close()
            except AttributeError:
                pass
        finally:
            # Catch errors that may happen when close is called from __del__
            # because CPython is in interpreter shutdown.
            try:
                self.dict = _ClosedDict()
            except:
                self.dict = None

    def __del__(self):
        if not hasattr(self, "writeback"):
            # __init__ didn't succeed, so don't bother closing
            # see http://bugs.python.org/issue1339007 for details
            return
        self.close()

    def sync(self):
        if self.writeback and self.cache:
            self.writeback = False
            for key, entry in self.cache.items():
                self[key] = entry
            self.writeback = True
            self.cache = {}
        if hasattr(self.dict, "sync"):
            self.dict.sync()


class DbfilenameShelf(Shelf):
    """Shelf implementation using the "dbm" generic dbm interface.

    This is initialized with the filename for the dbm database.
    See the module's __doc__ string for an overview of the interface.
    """

    def __init__(self, filename, flag="c", writeback=False, serializer=None):
        sqlite3_kargs = dict(autocommit=True, check_same_thread=False)
        Shelf.__init__(
            self, dict=sqlite.open(filename, flag, sqlite3_kargs=sqlite3_kargs), writeback=writeback, serializer=serializer
        )

    def clear(self):
        """Remove all items from the shelf."""
        # Call through to the clear method on dbm-backed shelves.
        # see https://github.com/python/cpython/issues/107089
        self.cache.clear()
        self.dict.clear()


def open(filename, flag="c", writeback=False, serializer=None):
    """Open a persistent dictionary for reading and writing.

    The filename parameter is the base filename for the underlying
    database.  As a side-effect, an extension may be added to the
    filename and more than one file may be created.  The optional flag
    parameter has the same interpretation as the flag parameter of
    dbm.open(). The optional protocol parameter specifies the
    version of the pickle protocol.

    See the module's __doc__ string for an overview of the interface.
    """

    return DbfilenameShelf(filename, flag, writeback, serializer=serializer)
