import pytest
import pytest_benchmark
import tempfile
from pathlib import Path


@pytest.fixture
def temp_db_path():
    with tempfile.NamedTemporaryFile(suffix=".db") as temp_file:
        yield temp_file.name


def test_shelve_default(temp_db_path):
    import shelvez as shelve

    # Create a test database
    example_db = {
        "key1": "value1",
        "key2": "value2",
        "key3": 123,
    }
    db_path = temp_db_path
    print(db_path)
    db = shelve.open(db_path, flag="c")

    for key, value in example_db.items():
        db[key] = value
        assert db[key] == value


def test_shelve_pydintic(temp_db_path):
    import shelvez as shelve
    from pydantic import BaseModel

    class MyModel(BaseModel):
        key: str
        key2: str = "default"
        key3: int = 0

    # Create a test database
    example_db = {
        "key1": MyModel(key="value1"),
        "key2": MyModel(key="value2", key2="value2"),
        "key3": MyModel(key="value3"),
    }
    db_path = temp_db_path
    print(db_path)
    serializer = shelve.serialer.PydanticSerializer(MyModel)
    db = shelve.open(db_path, flag="c", serializer=serializer)

    for key, value in example_db.items():
        db[key] = value
        assert db[key] == value


def test_shelvez_speed(temp_db_path, benchmark):
    import random
    import shelvez
    from pathlib import Path

    example_db = {str(random.randint(1000, 9999)): {"value": str(random.randint(1000000, 9999999))} for i in range(10000)}

    db = shelvez.open(temp_db_path, flag="c")

    def benchmark_shelvez():
        for key, value in example_db.items():
            db[key] = value
            assert db[key] == value

    benchmark(benchmark_shelvez)
    db.dict.optimize_database()
    db.close()
    db_size = Path(temp_db_path).stat().st_size / 1024
    print(f"shelvez pickle Database size: {db_size:.2f} kB")


def test_shelvez_json_speed(temp_db_path, benchmark):
    import random
    import shelvez

    example_db = {str(random.randint(1000, 9999)): {"value": str(random.randint(1000000, 9999999))} for i in range(10000)}

    db = shelvez.open(temp_db_path, flag="c", serializer=shelvez.serialer.JsonSerializer())

    def benchmark_shelvez_json():
        for key, value in example_db.items():
            db[key] = value
            assert db[key] == value

    benchmark(benchmark_shelvez_json)
    db.dict.optimize_database()
    db.close()
    db_size = Path(temp_db_path).stat().st_size / 1024
    print(f"shelvez json Database size: {db_size:.2f} kB")


def test_shelvez_pydintic_speed(temp_db_path, benchmark):
    import random
    import shelvez
    from pydantic import BaseModel

    class MyModel(BaseModel):
        value: str

    example_db = {
        str(random.randint(1000, 9999)): MyModel.model_validate({"value": str(random.randint(1000000, 9999999))})
        for i in range(10000)
    }

    db = shelvez.open(temp_db_path, flag="c", serializer=shelvez.serialer.PydanticSerializer(model=MyModel))

    def benchmark_shelvez_json():
        for key, value in example_db.items():
            db[key] = value
            assert db[key] == value

    benchmark(benchmark_shelvez_json)
    db.dict.optimize_database()
    db.close()
    db_size = Path(temp_db_path).stat().st_size / 1024
    print(f"shelvez pydintic Database size: {db_size:.2f} kB")


def test_shelve_speed(temp_db_path, benchmark):
    import random
    import shelve
    from dbm.sqlite3 import open

    example_db = {str(random.randint(1000, 9999)): {"value": str(random.randint(1000000, 9999999))} for i in range(10000)}

    db = open(temp_db_path, flag="c")
    db.close()
    db = shelve.open(temp_db_path, flag="c")

    def benchmark_shelve():
        for key, value in example_db.items():
            db[key] = value
            assert db[key] == value

    benchmark(benchmark_shelve)

    db.close()
    db_size = Path(temp_db_path).stat().st_size / 1024
    print(f"shelve Database size: {db_size:.2f} kB")
