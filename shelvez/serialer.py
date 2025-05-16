from abc import abstractmethod
from functools import partial

import pydantic

import json

import pickle


class BaseSerializer:
    @abstractmethod
    def serialize(self, obj) -> bytes:
        raise NotImplementedError("Subclasses should implement this!")

    @abstractmethod
    def unserialize(self, obj: bytes):
        raise NotImplementedError("Subclasses should implement this!")


class JsonSerializer(BaseSerializer):
    def serialize(self, obj: dict):
        jsons = json.dumps(obj, indent=None, ensure_ascii=False, separators=(",", ":"))
        return jsons.encode("utf-8")

    def unserialize(self, obj: bytes):
        return json.loads(obj)


class PickleSerializer(BaseSerializer):
    def __init__(self, protocol=None):
        if protocol is None:
            protocol = 5
        self.protocol = protocol

    def serialize(self, obj: dict):
        return pickle.dumps(obj, protocol=self.protocol)

    def unserialize(self, obj: bytes):
        return pickle.loads(obj)


class PydanticSerializer(BaseSerializer):
    def __init__(self, model: pydantic.BaseModel):
        self.model = model

    def serialize(self, obj: pydantic.BaseModel):
        return obj.model_dump_json(exclude_unset=True, indent=None).encode("utf-8")

    def unserialize(self, obj: bytes):
        return self.model.model_validate_json(obj)
