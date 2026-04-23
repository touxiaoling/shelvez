# Introduction
[![PyPI](https://img.shields.io/pypi/v/shelvez.svg?color=blue)](https://pypi.org/project/shelvez/#history)

This package functions similarly to Python's built-in `shelve` but offers additional features such as:

- **Zstandard (zstd) compression** for efficient storage  ✅ DONE
- **SQLite-backed transactions** — batch writes with `with shelve.open(...) as db:`; the underlying `db.dict` also exposes `begin` / `commit` / `transaction()` for advanced use  ✅ DONE
- **Multiple serialization formats support**: JSON, Pickle, and Pydantic models  ✅ DONE
- **SQLite-based function caching** with TTL and LRU strategies  ✅ DONE

---

## 📊 Benchmark
> Benchmark functions are defined in `tests/benchmarks/test_shelve_bench.py`.

### 🔥 Performance Comparison

| Test Case                     | Min (ms) | Max (ms) | Mean (ms) | StdDev (ms) | Median (ms) | OPS (ops/sec) | Rounds |
|-------------------------------|----------|----------|-----------|-------------|-------------|---------------|--------|
| `shelvez_pickle_speed`        | 147.78   | 154.81   | 149.30    | 2.29        | 148.49      | 6.70          | 8      |
| `shelvez_pydantic_speed`      | 151.57   | 155.64   | 153.13    | 1.53        | 152.90      | 6.53          | 7      |
| `shelvez_json_speed`         | 154.66   | 157.41   | 155.99    | 1.00        | 156.28      | 6.41          | 7      |
| `stdlib_shelve_speed`        | 264.41   | 277.42   | 268.82    | 5.14        | 266.58      | 3.72          | 5      |

> OPS = Operations Per Second (calculated as 1 / Mean). Timings are from one environment and will differ on other CPUs / interpreters.

---

### 🗂️ Database Size Comparison

> Same workload as the speed benchmarks: **10,000** random key–value pairs (`_make_payload` in `tests/benchmarks/test_shelve_bench.py`). For `shelvez`, sizes are taken **after** `db.dict.optimize_database()` (custom zstd dictionary + recompression). Reproduce the printed sizes with `pytest tests/benchmarks -m benchmark -s`.

| Backend                 | File Size |
|-------------------------|-----------|
| `shelve`                | 380.00 kB |
| `shelvez` (Pickle)      | 312.00 kB |
| `shelvez` (JSON)        | 312.00 kB |
| `shelvez` (Pydantic)    | 308.00 kB |

> The kB values above were checked with `pytest tests/benchmarks -m benchmark -s` (stdout from `_report_size`); minor drift is normal because the benchmark uses random keys/values and trains a zstd dictionary. For this workload, `shelvez` produces smaller files and faster writes than stdlib `shelve` in the same test harness.

---

## Installation

```bash
pip install shelvez
```

---
## Base Usage

```python
import shelvez as shelve

db = shelve.open("any_db_path/your_db.db")
db["key"] = "value"
print(db["key"])
db.close()
```
---
## Serialization (default is Pickle)

The default serialization method uses Pickle, with the Pickle data further compressed by zstd. For specific data types, you can also choose other serialization methods to achieve better version compatibility and reduce storage size.

---
### with JSON-serializable dicts
```python
import shelvez as shelve

# Use Json serializer
serializer = shelve.serializer.JsonSerializer()
db = shelve.open("any_db_path/your_db.db", serializer=serializer)

db["key"] = {"key":"value"}
```
---
### with Pydantic model
```python
from pydantic import BaseModel
import shelvez as shelve

class MyModel(BaseModel):
    value: str

# use Pydantic serializer
serializer = shelve.serializer.PydanticSerializer(MyModel)
db = shelve.open("any_db_path/your_db.db", serializer=serializer)

db["key"] = MyModel(value="value")
```
---
### with Self Custom Serialization
To implement your own serialization method, you need to create a subclass of `serializer.BaseSerializer` and override the following two methods:
1. `serialize(self, obj) -> bytes`: This method takes a Python object (obj) and returns its serialized form as bytes. Implement this method with your custom serialization logic.
2. `unserialize(self, data: bytes)`: This method takes the serialized bytes (data) and returns the original Python object by deserializing it.

Here is a template example:
```python
from shelvez import serializer

class CustomSerializer(serializer.BaseSerializer):
    def serialize(self, obj) -> bytes:
        # Implement custom serialization logic here
        # Convert `obj` to bytes
        pass

    def unserialize(self, data: bytes):
        # Implement custom deserialization logic here
        # Convert bytes back to original object
        pass
```
---
## Using zstd Compression Dictionary

After accumulating a sufficient amount of data, you can generate a custom zstd compression dictionary for your database by calling `db.dict.optimize_database()`. This function will also recompress the existing data using the newly created dictionary.
When stored data shares similar structures or formats, a personalized zstd dictionary can greatly enhance compression efficiency, particularly for relatively small datasets.
Typically, generating the dictionary after storing a few thousand samples yields good results.

⚠️ Warning: During the optimization process, do not perform any other read or write operations on the database to prevent data corruption or inconsistent states.

```python
import shelvez as shelve

db = shelve.open("any_db_path/your_db.db")
db["key"] = "value"
# ... store more data as needed

# Generate and apply a custom zstd compression dictionary
db.dict.optimize_database()

db.close()
```

---

## Function Caching with SqlCache

Shelvez now includes a powerful SQLite-based caching system that allows you to cache function results to SQLite with compression. This is particularly useful for expensive computations that you want to persist across application restarts.

### Basic Usage

```python
import shelvez.sqlcache as sqlcache
import time

# TTL Cache - cache results for 1 hour
@sqlcache.ttl_cache(cache_path="cache.db", max_size=1000, ttl=3600)
def expensive_function(x):
    time.sleep(1)  # Simulate expensive computation
    return x * x

# First call - computes and caches
result = expensive_function(5)  # Takes 1 second
print(result)  # 25

# Second call - retrieves from cache
result = expensive_function(5)  # Instant!
print(result)  # 25
```

### LRU Cache

```python
# LRU Cache - keeps only the 100 most recently used results
@sqlcache.lru_cache(cache_path="cache.db", max_size=100)
def fibonacci(n):
    if n <= 1:
        return n
    return fibonacci(n-1) + fibonacci(n-2)

# Results are cached and persist across application restarts
result = fibonacci(30)  # Computed once, cached forever
```

### Custom Cache Configuration

```python
# Custom cache with specific settings
@sqlcache.sqlcache(
    cache_path="my_cache.db",
    max_size=500,
    ttl=1800,  # 30 minutes
    cache_type="ttl"  # or "lru"
)
def my_function(x, y):
    return x + y
```

### Cache Management

```python
# Create a cache instance for advanced management
cache = sqlcache.SqlCache(
    cache_path="advanced_cache.db",
    max_size=1000,
    ttl=3600,
    cache_type="ttl"
)

@cache
def my_function(x):
    return x * 2

# Get cache statistics
stats = cache.get_stats()
print(f"Cache items: {stats['disk_cache']['total_items']}")
print(f"Memory cache size: {stats['memory_cache_size']}")

# Clear all cached data
cache.clear()

# Close the cache
cache.close()
```

### Features

- **Dual-layer caching**: Memory cache for speed + disk cache for persistence
- **Compression**: All cached data is compressed using zstd for efficient storage
- **Thread-safe**: Safe to use in multi-threaded applications
- **Automatic cleanup**: TTL caches automatically expire old entries, LRU caches remove least recently used items
- **Flexible serialization**: Supports any Python object that can be pickled
- **Statistics**: Get detailed information about cache usage and performance

---
