# Introduction
[![PyPI](https://img.shields.io/pypi/v/shelvez.svg?color=blue)](https://pypi.org/project/shelvez/#history)

This package functions similarly to Python’s built-in `shelve` but offers additional features such as:

- **Zstandard (zstd) compression** for efficient storage  ✅ DONE
- **SQLite-backed transactions** to ensure data integrity  ⚠️ TODO
- **Multiple serialization formats support**: JSON, Pickle, and Pydantic models  ✅ DONE

---

## 📊 Benchmark
> Benchmark functions are defined in `tests/test_shelve.py`.

### 🔥 Performance Comparison

| Test Case                 | Min (ms) | Max (ms) | Mean (ms) | StdDev (ms) | Median (ms) | OPS (ops/sec) | Rounds |
|--------------------------|----------|----------|-----------|-------------|-------------|----------------|--------|
| `shelve_speed`           | 443.62   | 459.37   | 450.19    | 5.96        | 450.10      | 2.22           | 5      |
| `shelvez_pickle_speed`   | 237.18   | 243.08   | 240.01    | 2.54        | 239.53      | 4.17           | 5      |
| `shelvez_pydantic_speed` | 245.33   | 318.90   | 263.59    | 31.14       | 252.38      | 3.79           | 5      |
| `shelvez_json_speed`     | 246.83   | 250.72   | 249.37    | 1.57        | 249.44      | 4.01           | 5      |

> OPS = Operations Per Second (calculated as 1 / Mean)

---

### 🗂️ Database Size Comparison

> File sizes are measured after writing the same number key-value data using each backend.

| Backend                 | File Size |
|-------------------------|-----------|
| `shelve`                | 380.00 kB |
| `shelvez` (Pickle)      | 312.00 kB |
| `shelvez` (JSON)        | 312.00 kB |
| `shelvez` (Pydantic)    | 308.00 kB |

> Smaller database files and faster write performance make `shelvez` a more efficient alternative to the standard `shelve` module.

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
To implement your own serialization method, you need to create a subclass of serializer.BaseSerializer and override the following two methods:
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
