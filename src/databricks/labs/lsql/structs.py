import dataclasses
import datetime
import enum
import types
from dataclasses import dataclass
from typing import ClassVar, Protocol, get_args, get_type_hints


class StructInferError(TypeError):
    pass


class SqlType(Protocol):
    """Represents a Spark SQL type."""

    def as_sql(self) -> str: ...


@dataclass
class NullableType(SqlType):
    """Represents a nullable type."""

    inner_type: SqlType

    def as_sql(self) -> str:
        return self.inner_type.as_sql()


@dataclass
class ArrayType(SqlType):
    """Represents an array type."""

    element_type: SqlType

    def as_sql(self) -> str:
        return f"ARRAY<{self.element_type.as_sql()}>"


@dataclass
class MapType(SqlType):
    """Represents a map type."""

    key_type: SqlType
    value_type: SqlType

    def as_sql(self) -> str:
        return f"MAP<{self.key_type.as_sql()},{self.value_type.as_sql()}>"


@dataclass
class PrimitiveType(SqlType):
    """Represents a primitive type."""

    name: str

    def as_sql(self) -> str:
        return self.name


@dataclass
class StructField:
    """Represents a field in a struct type."""

    name: str
    type: SqlType

    @property
    def nullable(self) -> bool:
        return isinstance(self.type, NullableType)

    def as_sql(self) -> str:
        return f"{self.name}:{self.type.as_sql()}"


@dataclass
class StructType(SqlType):
    """Represents a struct type."""

    fields: list[StructField]

    def as_sql(self) -> str:
        """Returns a DDL representation of the struct type."""
        fields = ",".join(f.as_sql() for f in self.fields)
        return f"STRUCT<{fields}>"

    def as_schema(self) -> str:
        """Returns a schema representation of the struct type."""
        fields = []
        for field in self.fields:
            not_null = "" if field.nullable else " NOT NULL"
            fields.append(f"{field.name} {field.type.as_sql()}{not_null}")
        return ", ".join(fields)


class StructInference:
    """Infers Spark SQL types from Python types."""

    _PRIMITIVES: ClassVar[dict[type, str]] = {
        str: "STRING",
        int: "LONG",
        bool: "BOOLEAN",
        float: "FLOAT",
        datetime.date: "DATE",
        datetime.datetime: "TIMESTAMP",
    }

    def as_ddl(self, type_ref: type) -> str:
        """Returns a DDL representation of the type."""
        v = self._infer(type_ref, [])
        return v.as_sql()

    def as_schema(self, type_ref: type) -> str:
        """Returns a schema representation of the type."""
        v = self._infer(type_ref, [])
        if hasattr(v, "as_schema"):
            return v.as_schema()
        raise StructInferError(f"Cannot generate schema for {type_ref}")

    def _infer(self, type_ref: type, path: list[str]) -> SqlType:
        """Infers the SQL type from the Python type. Raises StructInferError if the type is not supported."""
        if dataclasses.is_dataclass(type_ref):
            return self._infer_struct(type_ref, path)
        if isinstance(type_ref, enum.EnumMeta):
            return self._infer_primitive(str, path)
        if type_ref in self._PRIMITIVES:
            return self._infer_primitive(type_ref, path)
        if type_ref is list:
            raise StructInferError("Cannot determine element type of list. Rewrite as: list[XXX]")
        if type_ref is set:
            raise StructInferError("Cannot determine element type of set. Rewrite as: set[XXX]")
        if type_ref is dict:
            raise StructInferError("Cannot determine key and value types of dict. Rewrite as: dict[XXX, YYY]")
        return self._infer_generic(type_ref, path)

    def _infer_primitive(self, type_ref: type, path: list[str]) -> PrimitiveType:
        """Infers the primitive SQL type from the Python type. Raises StructInferError if the type is not supported."""
        if type_ref in self._PRIMITIVES:
            return PrimitiveType(self._PRIMITIVES[type_ref])
        raise StructInferError(f'{".".join(path)}: unknown: {type_ref}')

    def _infer_generic(self, type_ref: type, path: list[str]) -> SqlType:
        """Infers the SQL type from the generic Python type. Uses internal APIs to handle generic types."""
        # pylint: disable-next=import-outside-toplevel
        from typing import (  # type: ignore[attr-defined]
            _GenericAlias,
            _UnionGenericAlias,
        )

        if isinstance(type_ref, (types.UnionType, _UnionGenericAlias)):  # type: ignore[attr-defined]
            return self._infer_nullable(type_ref, path)
        if isinstance(type_ref, (types.GenericAlias, _GenericAlias)):  # type: ignore[attr-defined]
            if type_ref.__origin__ in (dict, list) or isinstance(type_ref, types.GenericAlias):
                return self._infer_container(type_ref, path)
        prefix = ".".join(path)
        if prefix:
            prefix = f"{prefix}: "
        raise StructInferError(f"{prefix}unsupported type: {type_ref.__name__}")

    def _infer_nullable(self, type_ref: type, path: list[str]) -> SqlType:
        """Infers nullability from Optional[x] or `x | None` types."""
        type_args = get_args(type_ref)
        if len(type_args) > 2:
            raise StructInferError(f'{".".join(path)}: union: too many variants: {type_args}')
        first_type = self._infer(type_args[0], [*path, "(first)"])
        if type_args[1] is not type(None):
            msg = f'{".".join(path)}.(second): not a NoneType: {type_args[1]}'
            raise StructInferError(msg)
        return NullableType(first_type)

    def _infer_container(self, type_ref: type, path: list[str]) -> SqlType:
        """Infers the SQL type from the generic container Python type."""
        type_args = get_args(type_ref)
        if not type_args:
            raise StructInferError(f"Missing type arguments: {type_args} in {type_ref}")
        if len(type_args) == 2:
            key_type = self._infer(type_args[0], [*path, "key"])
            value_type = self._infer(type_args[1], [*path, "value"])
            return MapType(key_type, value_type)
        # here we make a simple assumption that not two type arguments means a list
        element_type = self._infer(type_args[0], path)
        return ArrayType(element_type)

    def _infer_struct(self, type_ref: type, path: list[str]) -> StructType:
        """Infers the struct type from the Python dataclass type."""
        fields = []
        for field, hint in get_type_hints(type_ref).items():
            origin = getattr(hint, "__origin__", None)
            if origin is ClassVar:
                continue
            field_type = self._infer(hint, [*path, field])
            fields.append(StructField(field, field_type))
        return StructType(fields)
