import pytest
import tempfile


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
