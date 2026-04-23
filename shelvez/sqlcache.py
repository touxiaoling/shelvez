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

from cachetools import TTLCache, LRUCache
from .sqlite import _Database
from .serialer import BaseSerializer, PickleSerializer


class SqlCacheError(Exception):
    """SqlCache相关错误"""

    pass


class _SqlCacheDatabase:
    """SQLite缓存数据库管理类"""

    BUILD_TABLE = """
        CREATE TABLE IF NOT EXISTS cache (
            key TEXT UNIQUE NOT NULL PRIMARY KEY,
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
        try:
            # 为多进程环境优化的PRAGMA设置
            self._db._cx.execute("PRAGMA journal_mode = wal")  # WAL模式支持并发读取
            self._db._cx.execute("PRAGMA synchronous = normal")  # 平衡性能和安全性
            self._db._cx.execute("PRAGMA busy_timeout = 30000")  # 30秒超时
            self._db._cx.execute("PRAGMA cache_size = -20000")  # 20MB缓存
            self._db._cx.execute("PRAGMA temp_store = MEMORY")  # 临时表存储在内存
            self._db._cx.execute("PRAGMA mmap_size = 268435456")  # 256MB内存映射
            self._db._cx.execute("PRAGMA page_size = 4096")  # 4KB页面大小
        except sqlite3.OperationalError:
            # PRAGMA设置失败不影响功能，忽略错误
            pass

    @contextmanager
    def _execute(self, sql: str, params: tuple = ()):
        """执行 SQL 并返回游标的上下文管理器。

        - 使用可重入锁包裹，保证多线程下同一 sqlite3 连接的游标串行化。
        - 锁在 ``with`` 块内一直持有，避免用户正在 ``fetchone`` 时其他线程介入。
        - 在多进程安全模式下，遇到 ``database is locked`` 时做指数退避重试。
        """
        max_retries = 5 if self.multiprocess_safe else 1
        base_delay = 0.1

        with self._lock:
            for attempt in range(max_retries):
                try:
                    cursor = self._db._cx.execute(sql, params)
                    break
                except sqlite3.OperationalError as exc:
                    if self.multiprocess_safe and "database is locked" in str(exc).lower() and attempt < max_retries - 1:
                        delay = base_delay * (2**attempt) + random.uniform(0, 0.1)
                        time.sleep(delay)
                        continue
                    raise SqlCacheError(f"数据库被锁定，可能是多进程同时访问导致的。请稍后重试: {exc}")
                except sqlite3.Error as exc:
                    raise SqlCacheError(f"数据库操作失败: {exc}")
                except Exception as exc:
                    raise SqlCacheError(f"数据库操作失败: {exc}")

            try:
                yield cursor
            finally:
                cursor.close()

    def _generate_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """生成缓存键"""
        # 将参数序列化并生成哈希
        key_data = (func_name, args, tuple(sorted(kwargs.items())))
        key_bytes = pickle.dumps(key_data)
        return hashlib.md5(key_bytes).hexdigest()

    def get(self, key: str, ttl: Optional[float] = None) -> Optional[Any]:
        """获取缓存值"""
        current_time = time.time()

        # 构建查询条件
        if ttl:
            query = "SELECT value, created_at FROM cache WHERE key = ? AND created_at > ?"
            params = (key, current_time - ttl)
        else:
            query = "SELECT value, created_at FROM cache WHERE key = ?"
            params = (key,)

        with self._execute(query, params) as cursor:
            row = cursor.fetchone()
        if row is None:
            return None

        with self._execute(
            "UPDATE cache SET access_count = access_count + 1, last_access = ? WHERE key = ?",
            (current_time, key),
        ):
            pass

        compressed_value = row[0]
        decompressed_value = self._db.compressor.decompress(compressed_value)
        return self.serializer.unserialize(decompressed_value)

    def set(self, key: str, value: Any):
        """设置缓存值"""
        current_time = time.time()
        serialized_value = self.serializer.serialize(value)
        compressed_value = self._db.compressor.compress(serialized_value)

        with self._execute(
            "INSERT OR REPLACE INTO cache (key, value, created_at, access_count, last_access) VALUES (?, ?, ?, ?, ?)",
            (key, compressed_value, current_time, 1, current_time),
        ):
            pass

    def delete(self, key: str):
        """删除缓存项"""
        with self._execute("DELETE FROM cache WHERE key = ?", (key,)):
            pass

    def clear(self):
        """清空所有缓存"""
        with self._execute("DELETE FROM cache"):
            pass

    def cleanup_expired(self, ttl: float):
        """清理过期缓存"""
        current_time = time.time()
        with self._execute("DELETE FROM cache WHERE created_at < ?", (current_time - ttl,)):
            pass

    def cleanup_lru(self, max_size: int):
        """清理LRU缓存，保留最近访问的max_size个"""
        with self._execute("SELECT COUNT(*) FROM cache") as cursor:
            count = cursor.fetchone()[0]

        if count > max_size:
            excess = count - max_size
            with self._execute(
                "DELETE FROM cache WHERE key IN (SELECT key FROM cache ORDER BY last_access ASC LIMIT ?)",
                (excess,),
            ):
                pass

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
            self._memory_cache = TTLCache(maxsize=max_size, ttl=ttl or 3600)
        else:
            self._memory_cache = LRUCache(maxsize=max_size)

        # GC 兜底：即使用户从不调用 close()，连接也会在 SqlCache 被回收时
        # 随之关闭，避免 sqlite3.Connection 触发 ResourceWarning。
        self._finalizer = weakref.finalize(self, self._finalize_cache, self._db)

    @staticmethod
    def _finalize_cache(db) -> None:
        with suppress(Exception):
            db.close()

    def __call__(self, func: Callable) -> Callable:
        """装饰器实现"""

        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            cache_key = self._db._generate_key(func.__name__, args, kwargs)

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
