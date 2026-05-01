# bridge.xor.intent.signature
import ast
import importlib
import inspect
import re
import sys
import types
import typing
from copy import deepcopy
from typing import Any, Iterator
from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo
from dspy.signatures.field import InputField, OutputField
from dspy.signatures.signature import SignatureMeta

class IntentSignature(BaseModel, metaclass=SignatureMeta):
    @classmethod
    def with_instructions(cls, instructions: str) -> type["Signature"]:
        return IntentSignature(cls.fields, instructions)

    @classmethod
    def with_updated_fields(cls, name: str, type_: type | None = None, **kwargs: dict[str, Any]) -> type["Signature"]:
        fields_copy = deepcopy(cls.fields)
        fields_copy[name].json_schema_extra = {
            **fields_copy[name].json_schema_extra,
            **kwargs,
        }
        if type_ is not None:
            fields_copy[name].annotation = type_
        return IntentSignature(fields_copy, cls.instructions)

    @classmethod
    def prepend(cls, name, field, type_=None) -> type["Signature"]:
        return cls.insert(0, name, field, type_)

    @classmethod
    def append(cls, name, field, type_=None) -> type["Signature"]:
        return cls.insert(-1, name, field, type_)

    @classmethod
    def delete(cls, name) -> type["Signature"]:
        fields = dict(cls.fields)
        fields.pop(name, None)
        return IntentSignature(fields, cls.instructions)

    @classmethod
    def insert(cls, index: int, name: str, field, type_: type | None = None) -> type["Signature"]:
        if type_ is None:
            type_ = field.annotation
        if type_ is None:
            type_ = str

        input_fields = list(cls.input_fields.items())
        output_fields = list(cls.output_fields.items())
        lst = input_fields if field.json_schema_extra["__dspy_field_type"] == "input" else output_fields
        if index < 0:
            index += len(lst) + 1
        if index < 0 or index > len(lst):
            raise ValueError(
                f"Invalid index to insert: {index}, index must be in the range of [{len(lst) - 1}, {len(lst)}] for "
                f"{field.json_schema_extra['__dspy_field_type']} fields, but received: {index}.",
            )
        lst.insert(index, (name, (type_, field)))

        new_fields = dict(input_fields + output_fields)
        return IntentSignature(new_fields, cls.instructions)

    @classmethod
    def equals(cls, other) -> bool:
        """Compare the JSON schema of two Signature classes."""
        if not isinstance(other, type) or not issubclass(other, BaseModel):
            return False
        if cls.instructions != other.instructions:
            return False
        for name in cls.fields.keys() | other.fields.keys():
            if name not in other.fields or name not in cls.fields:
                return False
            if cls.fields[name].json_schema_extra != other.fields[name].json_schema_extra:
                return False
        return True

    @classmethod
    def dump_state(cls):
        state = {"instructions": cls.instructions, "fields": []}
        for field in cls.fields:
            state["fields"].append(
                {
                    "prefix": cls.fields[field].json_schema_extra["prefix"],
                    "description": cls.fields[field].json_schema_extra["desc"],
                }
            )
        return state

    @classmethod
    def load_state(cls, state):
        signature_copy = IntentSignature(deepcopy(cls.fields), cls.instructions)
        signature_copy.instructions = state["instructions"]
        for field, saved_field in zip(signature_copy.fields.values(), state["fields"], strict=False):
            field.json_schema_extra["prefix"] = saved_field["prefix"]
            field.json_schema_extra["desc"] = saved_field["description"]
        return signature_copy