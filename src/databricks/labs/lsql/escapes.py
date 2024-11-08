def escape_name(name: str) -> str:
    """Escapes the name to make it SQL safe."""
    return f"`{name.strip('`').replace('`', '``')}`"


def escape_full_name(full_name: str) -> str:
    """
    Escapes the full name components to make them SQL safe.

    Args:
        full_name (str): The dot-separated name of a catalog object.

    Returns:
         str: The path with all parts escaped in backticks.
    """
    if not full_name:
        return full_name
    parts = full_name.split(".", maxsplit=2)
    return ".".join(escape_name(part) for part in parts)
