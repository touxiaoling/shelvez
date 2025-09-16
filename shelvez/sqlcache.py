import os
import sqlite3
import time
import hashlib
import pickle
from pathlib import Path
from contextlib import suppress, closing
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

    def __init__(self, cache_path: str):
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)

        # 使用现有的_Database类来管理SQLite连接
        self._db = _Database(str(self.cache_path), flag="c", mode=0o666)
        self._execute(self.BUILD_TABLE)
        self._execute(self.CREATE_INDEX)
        self._execute(self.CREATE_ACCESS_INDEX)

        # 使用现有的序列化器
        self.serializer = PickleSerializer()

    def _execute(self, sql: str, params: tuple = ()):
        """执行SQL语句"""
        try:
            return self._db._execute(sql, params)
        except Exception as e:
            raise SqlCacheError(f"数据库操作失败: {e}")

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
            if row:
                # 更新访问信息
                self._execute(
                    "UPDATE cache SET access_count = access_count + 1, last_access = ? WHERE key = ?", (current_time, key)
                )
                # 解压缩并反序列化
                compressed_value = row[0]
                decompressed_value = self._db.compressor.decompress(compressed_value)
                return self.serializer.unserialize(decompressed_value)
        return None

    def set(self, key: str, value: Any):
        """设置缓存值"""
        current_time = time.time()
        # 序列化并压缩
        serialized_value = self.serializer.serialize(value)
        compressed_value = self._db.compressor.compress(serialized_value)

        self._execute(
            "INSERT OR REPLACE INTO cache (key, value, created_at, access_count, last_access) VALUES (?, ?, ?, ?, ?)",
            (key, compressed_value, current_time, 1, current_time),
        )

    def delete(self, key: str):
        """删除缓存项"""
        self._execute("DELETE FROM cache WHERE key = ?", (key,))

    def clear(self):
        """清空所有缓存"""
        self._execute("DELETE FROM cache")

    def cleanup_expired(self, ttl: float):
        """清理过期缓存"""
        current_time = time.time()
        self._execute("DELETE FROM cache WHERE created_at < ?", (current_time - ttl,))

    def cleanup_lru(self, max_size: int):
        """清理LRU缓存，保留最近访问的max_size个"""
        # 获取当前缓存数量
        with self._execute("SELECT COUNT(*) FROM cache") as cursor:
            count = cursor.fetchone()[0]

        if count > max_size:
            # 删除最久未访问的项
            excess = count - max_size
            self._execute("DELETE FROM cache WHERE key IN (SELECT key FROM cache ORDER BY last_access ASC LIMIT ?)", (excess,))

    def get_stats(self) -> dict:
        """获取缓存统计信息"""
        with self._execute("SELECT COUNT(*), AVG(access_count), MAX(last_access) FROM cache") as cursor:
            row = cursor.fetchone()
            return {"total_items": row[0] or 0, "avg_access_count": row[1] or 0, "last_access": row[2] or 0}

    def close(self):
        """关闭数据库连接"""
        if hasattr(self, "_db"):
            self._db.close()


class SqlCache:
    """SQLite缓存装饰器类"""

    def __init__(
        self, cache_path: str = "cache.db", max_size: int = 1000, ttl: Optional[float] = None, cache_type: str = "lru"
    ):
        """
        初始化SQLite缓存

        Args:
            cache_path: 缓存数据库文件路径
            max_size: 最大缓存数量
            ttl: 缓存时间（秒），None表示不过期
            cache_type: 缓存类型，"ttl"或"lru"
        """
        self.cache_path = cache_path
        self.max_size = max_size
        self.ttl = ttl
        self.cache_type = cache_type.lower()

        if self.cache_type not in ["ttl", "lru"]:
            raise ValueError("cache_type必须是'ttl'或'lru'")

        # 创建数据库实例
        self._db = _SqlCacheDatabase(cache_path)

        # 创建内存缓存用于快速访问
        if self.cache_type == "ttl":
            self._memory_cache = TTLCache(maxsize=max_size, ttl=ttl or 3600)
        else:
            self._memory_cache = LRUCache(maxsize=max_size)

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
        """关闭缓存"""
        self._db.close()


# 便捷函数
def sqlcache(cache_path: str = "cache.db", max_size: int = 1000, ttl: Optional[float] = None, cache_type: str = "lru"):
    """
    SQLite缓存装饰器

    Args:
        cache_path: 缓存数据库文件路径
        max_size: 最大缓存数量
        ttl: 缓存时间（秒），None表示不过期
        cache_type: 缓存类型，"ttl"或"lru"

    Returns:
        装饰器函数
    """
    cache = SqlCache(cache_path=cache_path, max_size=max_size, ttl=ttl, cache_type=cache_type)
    return cache


# 预定义的装饰器
def ttl_cache(cache_path: str = "cache.db", max_size: int = 1000, ttl: float = 3600):
    """TTL缓存装饰器"""
    return sqlcache(cache_path=cache_path, max_size=max_size, ttl=ttl, cache_type="ttl")


def lru_cache(cache_path: str = "cache.db", max_size: int = 1000):
    """LRU缓存装饰器"""
    return sqlcache(cache_path=cache_path, max_size=max_size, cache_type="lru")
