from typing import Any

from pydantic import BaseModel, field_validator, model_validator
from typing import ClassVar, Tuple


class BaseSchema(BaseModel):
    """BaseModel that coerces empty values into None and provides hex→dec support."""

    # subclasses can override this with a tuple of field names
    _num_fields: ClassVar[Tuple[str, ...]] = ()
    _address_fields: ClassVar[Tuple[str, ...]] = ()

    @model_validator(mode="before")
    @classmethod
    def empty_to_none(cls, data: Any):
        if isinstance(data, dict):
            return {k: (None if v in ("", [], {}, ()) else v) for k, v in data.items()}
        return data

    @field_validator("*", mode="before")
    @classmethod
    def normalize_address(cls, val, info):
        if info.field_name not in cls._address_fields:
            return val

        return val.lower() if val else None

    @field_validator("*", mode="before")
    @classmethod
    def hex_to_dec(cls, val, info):
        """Convert hex strings to integers if field is in _num_fields."""
        if info.field_name not in cls._num_fields:
            return val
        if val is None or isinstance(val, int):
            return val
        try:
            return int(val, 16)
        except Exception as e:
            raise ValueError(
                f"Class: {cls.__name__} - "
                f"Field: {info.field_name} - "
                f"Value: {val} - Error: {e}"
            )
