from abc import abstractmethod
from typing import TYPE_CHECKING

import json
import pickle

if TYPE_CHECKING:
    import pydantic


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
    """Pydantic 是可选依赖。

    只有真正构造 ``PydanticSerializer`` 的用户才会触发 ``import pydantic``；
    ``import shelvez`` 本身不引入 pydantic，这样不用 Pydantic 的用户可以完全
    不安装它。缺失时给出明确的 ``ImportError`` 并附安装命令。
    """

    def __init__(self, model: "type[pydantic.BaseModel]"):
        try:
            import pydantic  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "PydanticSerializer requires the optional 'pydantic' package. "
                "Install it with:  pip install pydantic  (or `pip install shelvez[pydantic]`)."
            ) from exc
        self.model = model

    def serialize(self, obj: "pydantic.BaseModel"):
        return obj.model_dump_json(exclude_unset=True, indent=None).encode("utf-8")

    def unserialize(self, obj: bytes):
        return self.model.model_validate_json(obj)
