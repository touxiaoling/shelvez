import os
import sqlite3
import time
import hashlib
import pickle
import random
import threading
import weakref
from pathlib import Path
from contextlib import suppress, closing, contextmanager
from functools import wraps
from typing import Callable, Any, Optional, Union
from collections import OrderedDict

from .sqlite import _Database
from .serializer import BaseSerializer, PickleSerializer


class SqlCacheError(Exception):
    """SqlCache相关错误"""

    pass


class _LRUCache:
    """最小化的 LRU 实现，替代 ``cachetools.LRUCache``。

    只提供热路径用到的 ``in`` / ``[]`` / ``clear`` / ``len`` 语义；底层直接
    复用 ``OrderedDict`` 的 C 级实现，和 ``cachetools`` 性能基本一致，但免去
    一个三方依赖。非线程安全——上层调用点本来也没有为内存缓存加锁。
    """

    __slots__ = ("maxsize", "_data")

    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self._data: OrderedDict = OrderedDict()

    def __contains__(self, key) -> bool:
        return key in self._data

    def __getitem__(self, key):
        self._data.move_to_end(key)
        return self._data[key]

    def __setitem__(self, key, value) -> None:
        if key in self._data:
            self._data.move_to_end(key)
        self._data[key] = value
        if len(self._data) > self.maxsize:
            self._data.popitem(last=False)

    def __len__(self) -> int:
        return len(self._data)

    def clear(self) -> None:
        self._data.clear()


class _TTLCache(_LRUCache):
    """带绝对过期时间的 LRU，替代 ``cachetools.TTLCache``。

    存储 ``(value, expire_at)`` 元组，``__contains__`` / ``__getitem__`` 做
    惰性过期（过期即删并视为 miss），超过 ``maxsize`` 时按 LRU 淘汰最旧项。
    采用 ``time.monotonic`` 避免系统时钟回拨影响过期判断。
    """

    __slots__ = ("ttl",)

    def __init__(self, maxsize: int, ttl: float):
        super().__init__(maxsize)
        self.ttl = ttl

    def __contains__(self, key) -> bool:
        item = self._data.get(key)
        if item is None:
            return False
        if item[1] < time.monotonic():
            del self._data[key]
            return False
        return True

    def __getitem__(self, key):
        value, expire_at = self._data[key]
        if expire_at < time.monotonic():
            del self._data[key]
            raise KeyError(key)
        self._data.move_to_end(key)
        return value

    def __setitem__(self, key, value) -> None:
        super().__setitem__(key, (value, time.monotonic() + self.ttl))


# 能直接用 ``repr`` 得到稳定、可哈希键的原生类型集合。把常见的标量类型
# 都列出来，避免 ``_generate_key`` 掉到 ``pickle.dumps`` 慢路径。
# ``bool`` 是 ``int`` 的子类，``type(True) is bool`` 所以独立列出不会误判。
_FAST_KEY_TYPES: tuple[type, ...] = (int, str, bytes, float, bool, type(None))

# 采样率：命中时以此概率更新 ``last_access``。调高更贴近精确 LRU，调低
# 更省写；1/8 是一个工程折衷，对 ``cleanup_lru`` 的近似误差已经很小。
_ACCESS_UPDATE_SAMPLE_RATE = 0.125


class _SqlCacheDatabase:
    """SQLite缓存数据库管理类"""

    # ``key`` is a raw 16-byte BLAKE2b digest (not hex): halves the stored
    # size and skips the TEXT→BLOB comparison path in SQLite. Old caches
    # created with a TEXT column still work — SQLite stores BLOB values
    # regardless of declared affinity — but rows inserted before the
    # hex-digest → raw-digest switch will no longer be reachable (the
    # on-disk cache acts as if those entries were evicted, which is the
    # correct failure mode for a cache).
    BUILD_TABLE = """
        CREATE TABLE IF NOT EXISTS cache (
            key BLOB UNIQUE NOT NULL PRIMARY KEY,
            value BLOB NOT NULL,
            created_at REAL NOT NULL,
            access_count INTEGER DEFAULT 0,
            last_access REAL NOT NULL
        )
    """

    CREATE_INDEX = """
        CREATE INDEX IF NOT EXISTS idx_created_at ON cache(created_at)
    """

    CREATE_ACCESS_INDEX = """
        CREATE INDEX IF NOT EXISTS idx_last_access ON cache(last_access)
    """

    def __init__(self, cache_path: str, multiprocess_safe: bool = True):
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.multiprocess_safe = multiprocess_safe

        # 使用 autocommit=True：否则 Python sqlite3 的隐式事务会把每次
        # ``set``/``delete`` 写入关在一个从未提交的事务里，导致：
        #   1) 同进程里第二个连接 ``get`` 看不到数据（读的是已提交快照）；
        #   2) 第二个连接再写入时撞上第一个连接的写锁，触发 "database is locked"
        #      并让 busy_timeout 最多各阻塞 30s，单个用例耗时爆炸到分钟级。
        sqlite3_kargs = {"autocommit": True}
        if multiprocess_safe:
            sqlite3_kargs["check_same_thread"] = False

        # 使用现有的_Database类来管理SQLite连接
        self._db = _Database(str(self.cache_path), flag="c", mode=0o666, sqlite3_kargs=sqlite3_kargs)

        # 多线程共享同一 sqlite3 连接时必须外部加锁，否则游标生命周期
        # 会相互踩踏，引发 "bad parameter or other API misuse"。
        self._lock = threading.RLock()

        # 为多进程环境优化PRAGMA设置
        if multiprocess_safe:
            self._optimize_for_multiprocess()

        with self._execute(self.BUILD_TABLE):
            pass
        with self._execute(self.CREATE_INDEX):
            pass
        with self._execute(self.CREATE_ACCESS_INDEX):
            pass

        # 使用现有的序列化器
        self.serializer = PickleSerializer()

        # 用 weakref.finalize 兜底，确保对象被 GC 时一定会关闭底层 sqlite3
        # 连接；对热路径零开销（仅在对象被回收时触发一次）。
        self._finalizer = weakref.finalize(self, self._finalize_db, self._db)

    @staticmethod
    def _finalize_db(db) -> None:
        """Finalizer：不能引用 self，否则会阻止 GC。"""
        with suppress(Exception):
            db.close()

    def _optimize_for_multiprocess(self):
        """为多进程环境优化SQLite设置"""
        cx = self._db._cx
        if cx is None:
            return
        try:
            # 为多进程环境优化的PRAGMA设置
            cx.execute("PRAGMA journal_mode = wal")  # WAL模式支持并发读取
            cx.execute("PRAGMA synchronous = normal")  # 平衡性能和安全性
            cx.execute("PRAGMA busy_timeout = 30000")  # 30秒超时
            cx.execute("PRAGMA cache_size = -20000")  # 20MB缓存
            cx.execute("PRAGMA temp_store = MEMORY")  # 临时表存储在内存
            cx.execute("PRAGMA mmap_size = 268435456")  # 256MB内存映射
            cx.execute("PRAGMA page_size = 4096")  # 4KB页面大小
        except sqlite3.OperationalError:
            # PRAGMA设置失败不影响功能，忽略错误
            pass

    @contextmanager
    def _execute(self, sql: str, params: tuple = ()):
        """冷路径用的通用 SQL 执行器。

        热点的 ``set`` / ``get`` / ``delete`` / ``cleanup_*`` 改走下面两个
        直接方法（``_exec_no_result`` / ``_exec_fetchone``）——它们不通过
        ``@contextmanager`` generator，省掉每次调用分配 ``GeneratorContext
        Manager`` 和两次 ``next()`` 的开销，实测每次 cache op 可省 ~1 µs。
        保留这个方法是因为外部测试 / 不在热路径上的 ``get_stats`` 仍以
        ``with db._execute(...) as cur:`` 的方式使用它。
        """
        cursor = self._exec_with_retry(sql, params)
        try:
            yield cursor
        finally:
            cursor.close()

    def _exec_with_retry(self, sql: str, params: tuple):
        """核心：拿锁 → ``cx.execute`` → 按需重试，返回未关闭的 cursor。

        多进程安全模式下遇到 ``database is locked`` 做指数退避重试，其它
        SQLite 错误统一转成 :class:`SqlCacheError`。调用方负责 ``close``。
        """
        max_retries = 5 if self.multiprocess_safe else 1

        with self._lock:
            cx = self._db._cx
            if cx is None:
                raise SqlCacheError("数据库连接已关闭")
            attempt = 0
            while True:
                try:
                    return cx.execute(sql, params)
                except sqlite3.OperationalError as exc:
                    attempt += 1
                    if self.multiprocess_safe and "database is locked" in str(exc).lower() and attempt < max_retries:
                        time.sleep(0.1 * (2 ** (attempt - 1)) + random.uniform(0, 0.1))
                        continue
                    raise SqlCacheError(f"数据库被锁定，可能是多进程同时访问导致的。请稍后重试: {exc}")
                except sqlite3.Error as exc:
                    raise SqlCacheError(f"数据库操作失败: {exc}")
                except Exception as exc:
                    raise SqlCacheError(f"数据库操作失败: {exc}")

    def _exec_no_result(self, sql: str, params: tuple = ()) -> None:
        """执行一条 SQL 并立刻关闭游标，不读取结果。用于写路径。"""
        cu = self._exec_with_retry(sql, params)
        cu.close()

    def _exec_fetchone(self, sql: str, params: tuple = ()):
        """执行一条 SQL 并返回 ``cursor.fetchone()``；完成后关闭游标。"""
        cu = self._exec_with_retry(sql, params)
        try:
            return cu.fetchone()
        finally:
            cu.close()

    def _generate_key(self, func_name: str, args: tuple, kwargs: dict) -> bytes:
        """生成缓存键。

        返回 16 字节 BLAKE2b 摘要（原始 bytes，不做 hex 编码）：

        - 相比旧版 ``md5(pickle.dumps(...)).hexdigest()``：
          1) 跳过 ``pickle.dumps`` 的解释器字节码构造——对 ``(int|str|
             bytes|float|bool|None)`` 常见参数走 ``repr`` 快路径，几乎所有
             实际调用点都会命中；
          2) 用 BLAKE2b 取代 MD5，在 CPython 上等速或略快，且输出截断到
             16 字节，足够做进程内缓存键且显著减小 SQLite 索引体积；
          3) 直接返回 ``bytes``，省掉 ``hexdigest`` 的 Unicode 构造。
        """
        if all(type(a) in _FAST_KEY_TYPES for a in args) and all(
            type(k) is str and type(v) in _FAST_KEY_TYPES for k, v in kwargs.items()
        ):
            if kwargs:
                data = f"{func_name}\x00{args!r}\x00{sorted(kwargs.items())!r}".encode()
            else:
                data = f"{func_name}\x00{args!r}".encode()
        else:
            data = pickle.dumps((func_name, args, tuple(sorted(kwargs.items()))), protocol=5)
        return hashlib.blake2b(data, digest_size=16).digest()

    def get(self, key: bytes, ttl: Optional[float] = None) -> Optional[Any]:
        """获取缓存值。

        以前每次命中都会同步执行一次 ``UPDATE access_count + last_access``，
        在 WAL 模式下这是实打实的磁盘写，让热缓存的读路径多花一次 SQLite
        ``execute``（profile 显示读 2k 次要跑 10k 次 ``execute``）。现在改
        成概率性采样更新——LRU 只关心相对顺序，被跳过的 ~90% 命中不会
        让最终淘汰顺序出现显著偏差，但读路径的 execute 次数直接减半。
        """
        current_time = time.time()

        if ttl:
            row = self._exec_fetchone(
                "SELECT value FROM cache WHERE key = ? AND created_at > ?",
                (key, current_time - ttl),
            )
        else:
            row = self._exec_fetchone("SELECT value FROM cache WHERE key = ?", (key,))
        if row is None:
            return None

        # 采样更新：~1/8 概率真正写回 ``last_access``。Redis 的近似 LRU 也
        # 用类似的采样思路；对上层 ``cleanup_lru`` 的行为几乎无影响。
        if random.random() < _ACCESS_UPDATE_SAMPLE_RATE:
            with suppress(SqlCacheError):
                self._exec_no_result(
                    "UPDATE cache SET access_count = access_count + 1, last_access = ? WHERE key = ?",
                    (current_time, key),
                )

        compressed_value = row[0]
        decompressed_value = self._db.compressor.decompress(compressed_value)
        return self.serializer.unserialize(decompressed_value)

    def set(self, key: bytes, value: Any):
        """设置缓存值"""
        current_time = time.time()
        serialized_value = self.serializer.serialize(value)
        compressed_value = self._db.compressor.compress(serialized_value)

        self._exec_no_result(
            "INSERT OR REPLACE INTO cache (key, value, created_at, access_count, last_access) VALUES (?, ?, ?, ?, ?)",
            (key, compressed_value, current_time, 1, current_time),
        )

    def delete(self, key: bytes):
        """删除缓存项"""
        self._exec_no_result("DELETE FROM cache WHERE key = ?", (key,))

    def clear(self):
        """清空所有缓存"""
        self._exec_no_result("DELETE FROM cache")

    def cleanup_expired(self, ttl: float):
        """清理过期缓存"""
        current_time = time.time()
        self._exec_no_result("DELETE FROM cache WHERE created_at < ?", (current_time - ttl,))

    def cleanup_lru(self, max_size: int):
        """清理LRU缓存，保留最近访问的max_size个"""
        row = self._exec_fetchone("SELECT COUNT(*) FROM cache")
        count = row[0] if row else 0

        if count > max_size:
            excess = count - max_size
            self._exec_no_result(
                "DELETE FROM cache WHERE key IN (SELECT key FROM cache ORDER BY last_access ASC LIMIT ?)",
                (excess,),
            )

    def get_stats(self) -> dict:
        """获取缓存统计信息"""
        with self._execute("SELECT COUNT(*), AVG(access_count), MAX(last_access) FROM cache") as cursor:
            row = cursor.fetchone()
            return {"total_items": row[0] or 0, "avg_access_count": row[1] or 0, "last_access": row[2] or 0}

    def close(self):
        """关闭数据库连接（幂等）。"""
        # 先撤销 finalizer，避免 GC 时再跑一次
        finalizer = getattr(self, "_finalizer", None)
        if finalizer is not None and finalizer.alive:
            finalizer.detach()
        if hasattr(self, "_db"):
            with suppress(Exception):
                self._db.close()


class SqlCache:
    """SQLite缓存装饰器类"""

    def __init__(
        self,
        cache_path: str = "cache.db",
        max_size: int = 1000,
        ttl: Optional[float] = None,
        cache_type: str = "lru",
        multiprocess_safe: bool = True,
    ):
        """
        初始化SQLite缓存

        Args:
            cache_path: 缓存数据库文件路径
            max_size: 最大缓存数量
            ttl: 缓存时间（秒），None表示不过期
            cache_type: 缓存类型，"ttl"或"lru"
            multiprocess_safe: 是否启用多进程安全模式
        """
        self.cache_path = cache_path
        self.max_size = max_size
        self.ttl = ttl
        self.cache_type = cache_type.lower()
        self.multiprocess_safe = multiprocess_safe

        if self.cache_type not in ["ttl", "lru"]:
            raise ValueError("cache_type必须是'ttl'或'lru'")

        # 创建数据库实例
        self._db = _SqlCacheDatabase(cache_path, multiprocess_safe=multiprocess_safe)

        # 创建内存缓存用于快速访问
        if self.cache_type == "ttl":
            self._memory_cache = _TTLCache(maxsize=max_size, ttl=ttl or 3600)
        else:
            self._memory_cache = _LRUCache(maxsize=max_size)

        # GC 兜底：即使用户从不调用 close()，连接也会在 SqlCache 被回收时
        # 随之关闭，避免 sqlite3.Connection 触发 ResourceWarning。
        self._finalizer = weakref.finalize(self, self._finalize_cache, self._db)

    @staticmethod
    def _finalize_cache(db) -> None:
        with suppress(Exception):
            db.close()

    def __call__(self, func: Callable) -> Callable:
        """装饰器实现"""

        # 在装饰时确定函数名，避免每次调用都 ``getattr + repr(func)``——
        # ``repr`` 作为 ``getattr`` 的默认值会被急切求值，profile 显示这
        # 一条路径每次调用多花 ~0.5 µs，纯浪费。
        func_name = getattr(func, "__name__", None) or repr(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = self._db._generate_key(func_name, args, kwargs)

            # 先尝试从内存缓存获取
            if cache_key in self._memory_cache:
                return self._memory_cache[cache_key]

            # 从SQLite缓存获取
            cached_value = self._db.get(cache_key, self.ttl)
            if cached_value is not None:
                # 更新内存缓存
                self._memory_cache[cache_key] = cached_value
                return cached_value

            # 执行函数并缓存结果
            result = func(*args, **kwargs)

            # 存储到SQLite缓存
            self._db.set(cache_key, result)

            # 更新内存缓存
            self._memory_cache[cache_key] = result

            # 执行清理策略
            self._cleanup()

            return result

        return wrapper

    def _cleanup(self):
        """执行缓存清理"""
        if self.cache_type == "ttl" and self.ttl:
            # TTL缓存：清理过期项
            self._db.cleanup_expired(self.ttl)
        else:
            # LRU缓存：清理超出最大数量的项
            self._db.cleanup_lru(self.max_size)

    def clear(self):
        """清空所有缓存"""
        self._db.clear()
        self._memory_cache.clear()

    def get_stats(self) -> dict:
        """获取缓存统计信息"""
        db_stats = self._db.get_stats()
        return {
            "disk_cache": db_stats,
            "memory_cache_size": len(self._memory_cache),
            "cache_type": self.cache_type,
            "max_size": self.max_size,
            "ttl": self.ttl,
        }

    def close(self):
        """关闭缓存（幂等）。"""
        finalizer = getattr(self, "_finalizer", None)
        if finalizer is not None and finalizer.alive:
            finalizer.detach()
        if hasattr(self, "_db"):
            self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


# 便捷函数
def sqlcache(
    cache_path: str = "cache.db",
    max_size: int = 1000,
    ttl: Optional[float] = None,
    cache_type: str = "lru",
    multiprocess_safe: bool = True,
):
    """
    SQLite缓存装饰器

    Args:
        cache_path: 缓存数据库文件路径
        max_size: 最大缓存数量
        ttl: 缓存时间（秒），None表示不过期
        cache_type: 缓存类型，"ttl"或"lru"
        multiprocess_safe: 是否启用多进程安全模式

    Returns:
        装饰器函数
    """
    cache = SqlCache(
        cache_path=cache_path, max_size=max_size, ttl=ttl, cache_type=cache_type, multiprocess_safe=multiprocess_safe
    )
    return cache


# 预定义的装饰器
def ttl_cache(cache_path: str = "cache.db", max_size: int = 1000, ttl: float = 3600, multiprocess_safe: bool = True):
    """TTL缓存装饰器"""
    return sqlcache(cache_path=cache_path, max_size=max_size, ttl=ttl, cache_type="ttl", multiprocess_safe=multiprocess_safe)


def lru_cache(cache_path: str = "cache.db", max_size: int = 1000, multiprocess_safe: bool = True):
    """LRU缓存装饰器"""
    return sqlcache(cache_path=cache_path, max_size=max_size, cache_type="lru", multiprocess_safe=multiprocess_safe)
