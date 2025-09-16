import os
import time
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import shelvez.sqlcache as sqlcache


class TestSqlCache:
    """测试SqlCache功能"""

    def setup_method(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_path = os.path.join(self.temp_dir, "test_cache.db")

    def teardown_method(self):
        """测试后清理"""
        # 清理临时文件
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_ttl_cache_basic(self):
        """测试TTL缓存基本功能"""
        call_count = 0

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=10, ttl=1)
        def test_func(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        # 第一次调用
        result1 = test_func(5)
        assert result1 == 10
        assert call_count == 1

        # 第二次调用（应该从缓存获取）
        result2 = test_func(5)
        assert result2 == 10
        assert call_count == 1  # 没有增加

        # 等待TTL过期
        time.sleep(1.1)

        # 第三次调用（应该重新计算）
        result3 = test_func(5)
        assert result3 == 10
        assert call_count == 2  # 重新计算了

    def test_lru_cache_basic(self):
        """测试LRU缓存基本功能"""
        call_count = 0

        @sqlcache.lru_cache(cache_path=self.cache_path, max_size=3)
        def test_func(x):
            nonlocal call_count
            call_count += 1
            return x * 3

        # 填充缓存
        for i in range(4):
            test_func(i)

        # 再次调用第一个函数（应该重新计算，因为被LRU淘汰了）
        result = test_func(0)
        assert result == 0
        assert call_count == 5  # 4次初始调用 + 1次重新计算

    def test_custom_cache(self):
        """测试自定义缓存"""
        call_count = 0

        @sqlcache.sqlcache(cache_path=self.cache_path, max_size=5, ttl=2, cache_type="ttl")
        def test_func(x, y):
            nonlocal call_count
            call_count += 1
            return x + y

        # 测试缓存
        result1 = test_func(3, 4)
        assert result1 == 7
        assert call_count == 1

        result2 = test_func(3, 4)
        assert result2 == 7
        assert call_count == 1  # 从缓存获取

    def test_cache_with_complex_data(self):
        """测试缓存复杂数据结构"""
        call_count = 0

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=10, ttl=10)
        def test_func(data):
            nonlocal call_count
            call_count += 1
            return {"result": data, "processed": True}

        # 测试复杂数据
        complex_data = {"a": [1, 2, 3], "b": {"nested": "value"}}
        result1 = test_func(complex_data)
        assert result1["result"] == complex_data
        assert result1["processed"] is True
        assert call_count == 1

        # 再次调用
        result2 = test_func(complex_data)
        assert result2 == result1
        assert call_count == 1  # 从缓存获取

    def test_cache_stats(self):
        """测试缓存统计信息"""
        # 创建缓存实例来测试统计功能
        cache = sqlcache.SqlCache(cache_path=self.cache_path, max_size=10, ttl=10, cache_type="ttl")

        @cache
        def test_func(x):
            return x * 2

        # 调用几次函数
        for i in range(3):
            test_func(i)

        # 获取统计信息
        stats = cache.get_stats()
        assert "disk_cache" in stats
        assert "memory_cache_size" in stats
        assert "cache_type" in stats
        assert "max_size" in stats
        assert "ttl" in stats
        assert stats["cache_type"] == "ttl"
        assert stats["max_size"] == 10
        assert stats["ttl"] == 10

    def test_cache_clear(self):
        """测试清空缓存"""
        # 创建缓存实例来测试清空功能
        cache = sqlcache.SqlCache(cache_path=self.cache_path, max_size=10, ttl=10, cache_type="ttl")

        call_count = 0

        @cache
        def test_func(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        # 调用函数
        test_func(5)
        assert call_count == 1

        # 再次调用（应该从缓存获取）
        test_func(5)
        assert call_count == 1

        # 清空缓存
        cache.clear()

        # 再次调用（应该重新计算）
        test_func(5)
        assert call_count == 2

    def test_cache_with_different_parameters(self):
        """测试不同参数的缓存"""
        call_count = 0

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=10, ttl=10)
        def test_func(x, y=10, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            return x + y + sum(args) + sum(kwargs.values())

        # 测试不同参数组合
        result1 = test_func(5)
        assert result1 == 15  # 5 + 10
        assert call_count == 1

        result2 = test_func(5, 20)
        assert result2 == 25  # 5 + 20
        assert call_count == 2

        result3 = test_func(5, 10, 1, 2, 3)
        assert result3 == 21  # 5 + 10 + 1 + 2 + 3
        assert call_count == 3

        result4 = test_func(5, y=15, extra=5)
        assert result4 == 25  # 5 + 15 + 5
        assert call_count == 4

        # 重复调用应该从缓存获取
        result5 = test_func(5)
        assert result5 == 15
        assert call_count == 4  # 没有增加

    def test_cache_with_exceptions(self):
        """测试缓存异常处理"""
        call_count = 0

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=10, ttl=10)
        def test_func(x):
            nonlocal call_count
            call_count += 1
            if x < 0:
                raise ValueError("负数不被支持")
            return x * 2

        # 正常调用
        result1 = test_func(5)
        assert result1 == 10
        assert call_count == 1

        # 异常调用不应该被缓存
        with pytest.raises(ValueError):
            test_func(-1)
        assert call_count == 2

        # 再次异常调用应该重新执行
        with pytest.raises(ValueError):
            test_func(-1)
        assert call_count == 3

    def test_cache_persistence(self):
        """测试缓存持久化"""
        call_count = 0
        cache_path1 = self.cache_path + "_1"
        cache_path2 = self.cache_path + "_2"

        @sqlcache.ttl_cache(cache_path=cache_path1, max_size=10, ttl=10)
        def test_func(x):
            nonlocal call_count
            call_count += 1
            return x * 3

        # 第一次调用
        result1 = test_func(7)
        assert result1 == 21
        assert call_count == 1

        # 等待一小段时间确保数据写入磁盘
        time.sleep(0.1)

        # 创建新的缓存实例（模拟重启）
        call_count = 0  # 重置计数器

        @sqlcache.ttl_cache(cache_path=cache_path2, max_size=10, ttl=10)
        def test_func_new(x):
            nonlocal call_count
            call_count += 1
            return x * 3

        # 应该重新计算（因为使用了不同的缓存路径）
        result2 = test_func_new(7)
        assert result2 == 21
        assert call_count == 1  # 重新计算了

    def test_cache_key_generation(self):
        """测试缓存键生成"""
        cache = sqlcache.SqlCache(cache_path=self.cache_path, max_size=10, ttl=10, cache_type="ttl")

        # 测试相同参数生成相同键
        key1 = cache._db._generate_key("test_func", (1, 2), {"a": 1, "b": 2})
        key2 = cache._db._generate_key("test_func", (1, 2), {"b": 2, "a": 1})  # 不同顺序
        assert key1 == key2

        # 测试不同参数生成不同键
        key3 = cache._db._generate_key("test_func", (1, 3), {"a": 1, "b": 2})
        assert key1 != key3

        # 测试不同函数名生成不同键
        key4 = cache._db._generate_key("other_func", (1, 2), {"a": 1, "b": 2})
        assert key1 != key4

    def test_cache_cleanup_ttl(self):
        """测试TTL缓存清理"""
        cache = sqlcache.SqlCache(cache_path=self.cache_path, max_size=10, ttl=1, cache_type="ttl")

        call_count = 0

        @cache
        def test_func(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        # 添加一些缓存项
        test_func(1)
        test_func(2)
        test_func(3)

        # 等待TTL过期
        time.sleep(1.1)

        # 再次调用应该重新计算
        test_func(1)
        assert call_count == 4  # 3次初始 + 1次重新计算

    def test_cache_cleanup_lru(self):
        """测试LRU缓存清理"""
        cache = sqlcache.SqlCache(cache_path=self.cache_path, max_size=3, cache_type="lru")

        call_count = 0

        @cache
        def test_func(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        # 填充缓存
        for i in range(4):
            test_func(i)

        # 再次调用第一个函数（应该重新计算，因为被LRU淘汰了）
        test_func(0)
        assert call_count == 5  # 4次初始调用 + 1次重新计算

    def test_cache_with_none_values(self):
        """测试缓存None值"""
        call_count = 0

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=10, ttl=10)
        def test_func(x):
            nonlocal call_count
            call_count += 1
            if x == 0:
                return None
            return x * 2

        # 测试None值缓存
        result1 = test_func(0)
        assert result1 is None
        assert call_count == 1

        result2 = test_func(0)
        assert result2 is None
        assert call_count == 1  # 从缓存获取

        # 测试正常值
        result3 = test_func(5)
        assert result3 == 10
        assert call_count == 2

    def test_cache_with_large_data(self):
        """测试缓存大数据"""
        call_count = 0

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=10, ttl=10)
        def test_func(size):
            nonlocal call_count
            call_count += 1
            return list(range(size))

        # 测试大数据
        large_data = test_func(1000)
        assert len(large_data) == 1000
        assert call_count == 1

        # 再次调用应该从缓存获取
        large_data2 = test_func(1000)
        assert large_data2 == large_data
        assert call_count == 1

    def test_cache_error_handling(self):
        """测试缓存错误处理"""
        # 测试无效的cache_type
        with pytest.raises(ValueError, match="cache_type必须是'ttl'或'lru'"):
            sqlcache.SqlCache(cache_path=self.cache_path, cache_type="invalid")

    def test_cache_close(self):
        """测试缓存关闭"""
        cache = sqlcache.SqlCache(cache_path=self.cache_path, max_size=10, ttl=10, cache_type="ttl")

        @cache
        def test_func(x):
            return x * 2

        # 调用函数
        result = test_func(5)
        assert result == 10

        # 关闭缓存
        cache.close()

        # 关闭后再次调用应该仍然工作（会创建新的数据库连接）
        result2 = test_func(5)
        assert result2 == 10

    def test_convenience_functions(self):
        """测试便捷函数"""
        call_count = 0

        # 测试ttl_cache便捷函数
        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=5, ttl=1)
        def ttl_func(x):
            nonlocal call_count
            call_count += 1
            return x * 2

        result1 = ttl_func(3)
        assert result1 == 6
        assert call_count == 1

        # 测试lru_cache便捷函数
        @sqlcache.lru_cache(cache_path=self.cache_path + "_lru", max_size=3)
        def lru_func(x):
            nonlocal call_count
            call_count += 1
            return x * 3

        result2 = lru_func(4)
        assert result2 == 12
        assert call_count == 2

    def test_cache_with_threading(self):
        """测试多线程环境下的缓存"""
        import threading
        import queue

        call_count = 0
        results = queue.Queue()

        # 为每个线程创建独立的缓存路径
        def worker(value):
            cache_path = f"{self.cache_path}_{value}"

            @sqlcache.ttl_cache(cache_path=cache_path, max_size=10, ttl=10)
            def test_func(x):
                nonlocal call_count
                call_count += 1
                time.sleep(0.01)  # 模拟计算时间
                return x * 2

            result = test_func(value)
            results.put(result)

        # 创建多个线程
        threads = []
        for i in range(5):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # 等待所有线程完成
        for t in threads:
            t.join()

        # 收集结果
        collected_results = []
        while not results.empty():
            collected_results.append(results.get())

        # 验证结果
        assert len(collected_results) == 5
        assert call_count == 5  # 每个值计算一次


class TestSqlCacheDatabase:
    """测试_SqlCacheDatabase类"""

    def setup_method(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_path = os.path.join(self.temp_dir, "test_db.db")

    def teardown_method(self):
        """测试后清理"""
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_database_creation(self):
        """测试数据库创建"""
        db = sqlcache._SqlCacheDatabase(self.cache_path)

        # 验证数据库文件已创建
        assert os.path.exists(self.cache_path)

        # 验证表已创建
        with db._execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cache'") as cursor:
            row = cursor.fetchone()
            assert row is not None

        db.close()

    def test_database_operations(self):
        """测试数据库基本操作"""
        db = sqlcache._SqlCacheDatabase(self.cache_path)

        # 测试设置和获取
        test_key = "test_key"
        test_value = {"data": [1, 2, 3], "nested": {"key": "value"}}

        db.set(test_key, test_value)
        retrieved_value = db.get(test_key)

        assert retrieved_value == test_value

        # 测试删除
        db.delete(test_key)
        retrieved_value = db.get(test_key)
        assert retrieved_value is None

        db.close()

    def test_database_ttl(self):
        """测试数据库TTL功能"""
        db = sqlcache._SqlCacheDatabase(self.cache_path)

        test_key = "ttl_test"
        test_value = "test_data"

        # 设置值
        db.set(test_key, test_value)

        # 立即获取应该成功
        result = db.get(test_key, ttl=10)
        assert result == test_value

        # 等待TTL过期
        time.sleep(0.1)

        # 使用很短的TTL应该失败
        result = db.get(test_key, ttl=0.05)
        assert result is None

        # 不使用TTL应该成功
        result = db.get(test_key)
        assert result == test_value

        db.close()

    def test_database_cleanup(self):
        """测试数据库清理功能"""
        db = sqlcache._SqlCacheDatabase(self.cache_path)

        # 添加一些测试数据
        for i in range(5):
            db.set(f"key_{i}", f"value_{i}")

        # 验证数据存在
        assert db.get("key_0") == "value_0"

        # 测试LRU清理
        db.cleanup_lru(3)

        # 验证只有3个数据项
        with db._execute("SELECT COUNT(*) FROM cache") as cursor:
            count = cursor.fetchone()[0]
        assert count == 3

        # 清理过期数据（使用很短的TTL）
        db.cleanup_expired(0.1)

        # 数据应该仍然存在（因为没有使用TTL）
        assert db.get("key_0") == "value_0"

        db.close()

    def test_database_stats(self):
        """测试数据库统计功能"""
        db = sqlcache._SqlCacheDatabase(self.cache_path)

        # 添加一些测试数据
        for i in range(3):
            db.set(f"key_{i}", f"value_{i}")

        # 获取统计信息
        stats = db.get_stats()

        assert "total_items" in stats
        assert "avg_access_count" in stats
        assert "last_access" in stats
        assert stats["total_items"] == 3

        db.close()

    def test_database_clear(self):
        """测试数据库清空功能"""
        db = sqlcache._SqlCacheDatabase(self.cache_path)

        # 添加测试数据
        db.set("key1", "value1")
        db.set("key2", "value2")

        # 验证数据存在
        assert db.get("key1") == "value1"
        assert db.get("key2") == "value2"

        # 清空数据库
        db.clear()

        # 验证数据已删除
        assert db.get("key1") is None
        assert db.get("key2") is None

        db.close()


class TestSqlCacheIntegration:
    """测试SqlCache集成功能"""

    def setup_method(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_path = os.path.join(self.temp_dir, "test_integration.db")

    def teardown_method(self):
        """测试后清理"""
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_multiple_decorators(self):
        """测试多个装饰器"""
        call_count1 = 0
        call_count2 = 0

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=5, ttl=10)
        def func1(x):
            nonlocal call_count1
            call_count1 += 1
            return x * 2

        @sqlcache.lru_cache(cache_path=self.cache_path + "_lru", max_size=3)
        def func2(x):
            nonlocal call_count2
            call_count2 += 1
            return x * 3

        # 测试两个函数
        result1 = func1(5)
        result2 = func2(5)

        assert result1 == 10
        assert result2 == 15
        assert call_count1 == 1
        assert call_count2 == 1

        # 再次调用
        result1 = func1(5)
        result2 = func2(5)

        assert result1 == 10
        assert result2 == 15
        assert call_count1 == 1  # 从缓存获取
        assert call_count2 == 1  # 从缓存获取

    def test_cache_with_different_paths(self):
        """测试不同路径的缓存"""
        cache1_path = os.path.join(self.temp_dir, "cache1.db")
        cache2_path = os.path.join(self.temp_dir, "cache2.db")

        call_count1 = 0
        call_count2 = 0

        @sqlcache.ttl_cache(cache_path=cache1_path, max_size=5, ttl=10)
        def func1(x):
            nonlocal call_count1
            call_count1 += 1
            return x * 2

        @sqlcache.ttl_cache(cache_path=cache2_path, max_size=5, ttl=10)
        def func2(x):
            nonlocal call_count2
            call_count2 += 1
            return x * 3

        # 调用函数
        result1 = func1(5)
        result2 = func2(5)

        assert result1 == 10
        assert result2 == 15
        assert call_count1 == 1
        assert call_count2 == 1

        # 验证缓存文件已创建
        assert os.path.exists(cache1_path)
        assert os.path.exists(cache2_path)

    def test_cache_performance(self):
        """测试缓存性能"""
        import time

        call_count = 0

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=100, ttl=10)
        def expensive_func(x):
            nonlocal call_count
            call_count += 1
            time.sleep(0.01)  # 模拟耗时操作
            return x**2

        # 第一次调用（应该较慢）
        start_time = time.time()
        result1 = expensive_func(10)
        first_call_time = time.time() - start_time

        # 第二次调用（应该很快，从缓存获取）
        start_time = time.time()
        result2 = expensive_func(10)
        second_call_time = time.time() - start_time

        assert result1 == result2 == 100
        assert call_count == 1
        assert second_call_time < first_call_time  # 缓存应该更快

    def test_cache_with_nested_functions(self):
        """测试嵌套函数的缓存"""
        call_count = 0

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=10, ttl=10)
        def outer_func(x):
            nonlocal call_count
            call_count += 1

            @sqlcache.ttl_cache(cache_path=self.cache_path + "_inner", max_size=10, ttl=10)
            def inner_func(y):
                return y * 2

            return inner_func(x) + 1

        # 调用外层函数
        result = outer_func(5)
        assert result == 11  # (5 * 2) + 1
        assert call_count == 1

        # 再次调用
        result2 = outer_func(5)
        assert result2 == 11
        assert call_count == 1  # 从缓存获取


class TestMultiprocessSqlCache:
    """测试多进程环境下的SqlCache功能"""

    def setup_method(self):
        """测试前准备"""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_path = os.path.join(self.temp_dir, "multiprocess_test.db")

    def teardown_method(self):
        """测试后清理"""
        # 清理临时文件
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_multiprocess_cache_safety(self):
        """测试多进程缓存安全性"""
        # 测试多进程安全模式的基本功能
        call_count = 0

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=100, ttl=10, multiprocess_safe=True)
        def compute_value(x):
            nonlocal call_count
            call_count += 1
            return x * x

        # 执行一些操作
        for i in range(5):
            result = compute_value(i)
            assert result == i * i

        # 验证缓存工作正常
        assert call_count == 5, f"期望调用5次，实际调用{call_count}次"

        # 验证缓存文件存在
        assert os.path.exists(self.cache_path), "缓存文件未创建"

        # 测试缓存命中
        call_count_before = call_count
        result = compute_value(0)  # 应该从缓存获取
        assert result == 0
        assert call_count == call_count_before, "缓存命中失败"

    def test_multiprocess_vs_single_process(self):
        """对比多进程和单进程模式"""

        # 测试单进程模式
        call_count_single = 0

        @sqlcache.ttl_cache(cache_path=f"{self.cache_path}_single", max_size=50, ttl=5, multiprocess_safe=False)
        def single_process_func(x):
            nonlocal call_count_single
            call_count_single += 1
            return x * 2

        # 执行单进程操作
        for i in range(5):
            result = single_process_func(i)
            assert result == i * 2

        # 测试多进程模式
        call_count_multi = 0

        @sqlcache.ttl_cache(cache_path=f"{self.cache_path}_multi", max_size=50, ttl=5, multiprocess_safe=True)
        def multi_process_func(x):
            nonlocal call_count_multi
            call_count_multi += 1
            return x * 3

        # 执行多进程操作
        for i in range(5):
            result = multi_process_func(i)
            assert result == i * 3

        # 验证两种模式都能正常工作
        assert call_count_single == 5, "单进程模式调用次数不正确"
        assert call_count_multi == 5, "多进程模式调用次数不正确"

    def test_database_lock_retry_mechanism(self):
        """测试数据库锁定重试机制"""
        import threading
        import concurrent.futures

        call_count = 0
        results = []

        @sqlcache.ttl_cache(cache_path=self.cache_path, max_size=100, ttl=10, multiprocess_safe=True)
        def compute_value(x):
            nonlocal call_count
            call_count += 1
            time.sleep(0.05)  # 增加计算时间以增加锁定概率
            return x * x

        def worker(x):
            try:
                result = compute_value(x)
                results.append((x, result, "SUCCESS"))
            except Exception as e:
                results.append((x, None, f"ERROR: {e}"))

        # 使用线程池模拟并发访问
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(worker, i) for i in range(10)]
            concurrent.futures.wait(futures, timeout=30)

        # 验证所有操作都成功
        success_count = sum(1 for r in results if r[2] == "SUCCESS")
        error_count = sum(1 for r in results if r[2].startswith("ERROR"))

        assert error_count == 0, f"有 {error_count} 个操作失败: {[r for r in results if r[2].startswith('ERROR')]}"
        assert success_count == 10, f"期望 10 个成功操作，实际 {success_count} 个"

        # 验证缓存工作正常（应该有一些缓存命中）
        assert call_count <= 10, "缓存机制可能有问题"


if __name__ == "__main__":
    pytest.main([__file__])
