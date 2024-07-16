import abc
import dataclasses
import enum
import types
from collections.abc import Callable
from typing import Any, get_args, get_type_hints


def snake_to_camel(snake_name: str) -> str:
    """
    Converts a snake case string to camel case.

    Args:
        snake_name (str): The snake case string to convert.

    Returns:
        str: The converted camel case string.
    """
    words = snake_name.split("_")
    return words[0] + "".join(word.capitalize() for word in words[1:])


def is_assignable(
    type_ref: type, raw: Any, path: list[str], name_transform: Callable[[str], str]
) -> tuple[bool, str | None]:
    """
    Checks if a value can be assigned to a specific type.

    Args:
        type_ref (type):
            The type to check against.
        raw (Any):
            The value to check.
        path (list[str]):
            The path to the value.
        name_transform (Callable[[str], str]):
            A function to transform the name.

    Returns:
        tuple[bool, str | None]:
            A tuple containing a boolean indicating if the value can be assigned and a string
            explaining why if it cannot.
    """
    # Check if the type is a dataclass
    if dataclasses.is_dataclass(type_ref):
        return _is_assignable_from_dataclass(type_ref, raw, path, name_transform)
    # Check if the type is a generic alias
    if isinstance(type_ref, types.GenericAlias):
        return _is_assignable_from_generic_alias(type_ref, raw, path, name_transform)
    # Check if the type is a type | type, a special case of Optional[type]
    if isinstance(type_ref, types.UnionType):
        return _is_assignable_from_union(type_ref, raw, path, name_transform)
    # Check if the type is an abstract base class
    if isinstance(type_ref, abc.ABCMeta):
        # until we generate method that returns subtypes
        return True, None
    # Check if the type is an enum
    if isinstance(type_ref, enum.EnumMeta):
        if raw in type_ref._value2member_map_:
            return True, None
        return False, _explain_why(type_ref, raw, path)
    # Check if the type is None
    if type_ref == types.NoneType:
        if raw is None:
            return True, None
        return False, None
    # Check if the type is a basic type
    if type_ref in (int, bool, float, str):
        if isinstance(raw, type_ref):
            return True, None
        return False, _explain_why(type_ref, raw, path)
    return False, f'{".".join(path)}: unknown: {raw}'


# Function to explain why a value cannot be assigned to a type
def _explain_why(type_ref: type, raw: Any, path: list[str]) -> str:
    if raw is None:
        raw = "value is missing"
    return f'{".".join(path)}: not a {type_ref.__name__}: {raw}'


# Function to check if a value can be assigned to a generic alias
def _is_assignable_from_generic_alias(type_ref, raw, path, name_transform):
    if not isinstance(raw, list):
        return False, _explain_why(list, raw, path)
    type_args = get_args(type_ref)
    if not type_args:
        raise TypeError(f"Missing type arguments: {type_args}")
    item_ref = type_args[0]
    for i, v in enumerate(raw):
        valid, why_not = is_assignable(item_ref, v, [*path, f"{i}"], name_transform)
        if not valid:
            return False, why_not
    return True, None


# Function to check if a value can be assigned to a union
def _is_assignable_from_union(type_ref, raw, path, name_transform):
    combo = []
    for variant in get_args(type_ref):
        valid, why_not = is_assignable(variant, raw, [], name_transform)
        if valid:
            return True, None
        if why_not:
            combo.append(why_not)
    return False, f'{".".join(path)}: union: {" or ".join(combo)}'


# Function to check if a value can be assigned to a dataclass
def _is_assignable_from_dataclass(type_ref, raw, path, name_transform):
    if not isinstance(raw, dict):
        return False, _explain_why(dict, raw, path)
    for field, hint in get_type_hints(type_ref).items():
        field = name_transform(field)
        valid, why_not = is_assignable(hint, raw.get(field), [*path, field], name_transform)
        if not valid:
            return False, why_not
    return True, None
