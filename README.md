# Introduction
[![PyPI](https://img.shields.io/pypi/v/shelvez.svg?color=blue)](https://pypi.org/project/shelvez/#history)

This package functions similarly to Python’s built-in `shelve` but offers additional features such as:

- **Zstandard (zstd) compression** for efficient storage  ✅ DONE  
- **SQLite-backed transactions** to ensure data integrity  ⚠️ TODO  
- **Multiple serialization formats support**: JSON, Pickle, and Pydantic models  ✅ DONE  

---

## Features

- Transparent compression using zstd  
- Atomic transactions with SQLite  
- Flexible serialization: choose between JSON, Pickle, or Pydantic serialization  
- Safe concurrent read/write operations in multithreaded environments  
- Easy-to-use API similar to Python's built-in `shelve`  

---

## Installation

```bash
pip install shelvez
```

---
## Base Usage

```python
import shelve_sqlite_zstd as shelve

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
import shelve_sqlite_zstd as shelve

# Use Json serializer
serializer = shelve.serializer.JsonSerializer()
db = shelve.open("any_db_path/your_db.db", serializer=serializer)

db["key"] = {"key":"value"}
```
---
### with Pydantic model
```python
from pydantic import BaseModel
import shelve_sqlite_zstd as shelve

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
from shelve_sqlite_zstd import serializer

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
import shelve_sqlite_zstd as shelve

db = shelve.open("any_db_path/your_db.db")
db["key"] = "value"
# ... store more data as needed

# Generate and apply a custom zstd compression dictionary
db.dict.optimize_database()

db.close()
```
---
