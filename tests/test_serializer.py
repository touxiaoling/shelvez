from shelvez import serializer


def any_serializer_test(serializer: serializer.BaseSerializer, data, echo=False):
    serialized_data = serializer.serialize(data)
    if echo:
        print(f"Serialized data: {serialized_data}")
    assert isinstance(serialized_data, bytes)
    unserialized_data = serializer.unserialize(serialized_data)
    assert unserialized_data == data


def test_json_serialization():
    json_data_list = [
        {"key": "value", "key2": "value2"},
    ]
    ser = serializer.JsonSerializer()
    for data in json_data_list:
        any_serializer_test(ser, data)


def test_pickle_serialization():
    pickle_data_list = [
        {"key": "value", "key2": "value2"},
        {"key": 1, "key2": 2},
        {"key": [1, 2, 3], "key2": [4, 5, 6]},
        {"key": (1, 2, 3), "key2": (4, 5, 6)},
        {"key": {1: "a", 2: "b"}, "key2": {3: "c", 4: "d"}},
        1123,
        "hello world",
    ]

    ser = serializer.PickleSerializer()
    for data in pickle_data_list:
        any_serializer_test(ser, data)


def test_pydantic_serialization():
    from pydantic import BaseModel

    class MyModel(BaseModel):
        key: str
        key2: str = "default"
        key3: int = 0

    data_list = [
        MyModel(key="value"),
        MyModel(key="value2", key2="value2"),
        MyModel(key="value3"),
    ]
    for data in data_list:
        ser = serializer.PydanticSerializer(MyModel)
        any_serializer_test(ser, data=data, echo=True)
