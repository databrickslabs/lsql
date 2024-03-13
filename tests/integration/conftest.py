import json
import logging
import os
import pathlib
import string
import sys
from typing import Callable, MutableMapping

import pytest
from databricks.labs.blueprint.logger import install_logger
from databricks.sdk import WorkspaceClient
from pytest import fixture

from databricks.labs.lsql.__about__ import __version__

install_logger()
logging.getLogger("databricks").setLevel("DEBUG")


def _is_in_debug() -> bool:
    return os.path.basename(sys.argv[0]) in {"_jb_pytest_runner.py", "testlauncher.py"}


@fixture  # type: ignore[no-redef]
def debug_env_name():
    return "ucws"


@fixture
def debug_env(monkeypatch, debug_env_name) -> MutableMapping[str, str]:
    if not _is_in_debug():
        return os.environ
    conf_file = pathlib.Path.home() / ".databricks/debug-env.json"
    if not conf_file.exists():
        return os.environ
    with conf_file.open("r") as f:
        conf = json.load(f)
        if debug_env_name not in conf:
            sys.stderr.write(
                f"""{debug_env_name} not found in ~/.databricks/debug-env.json

            this usually means that you have to add the following fixture to
            conftest.py file in the relevant directory:

            @fixture
            def debug_env_name():
                return 'ENV_NAME' # where ENV_NAME is one of: {", ".join(conf.keys())}
            """
            )
            msg = f"{debug_env_name} not found in ~/.databricks/debug-env.json"
            raise KeyError(msg)
        for k, v in conf[debug_env_name].items():
            monkeypatch.setenv(k, v)
    return os.environ


@fixture
def make_random():
    import random

    def inner(k=16) -> str:
        """Generate a random string of fixed length"""
        # get a random meaningful word from the system dictionary if it exists
        system_wordlist = pathlib.Path("/usr/share/dict/words")
        if system_wordlist.exists():
            with system_wordlist.open("r", encoding=sys.getdefaultencoding()) as f:
                at_lest_len = [_.lower() for _ in f.read().splitlines() if len(_) > k]
                first = random.choice(at_lest_len)
                second = random.choice(at_lest_len)
                return f"{first}_{second}"
        charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return "".join(random.choices(charset, k=int(k)))

    return inner


@fixture
def product_info():
    return "lsql", __version__


@fixture
def ws(product_info, debug_env) -> WorkspaceClient:
    # Use variables from Unified Auth
    # See https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html
    product_name, product_version = product_info
    return WorkspaceClient(host=debug_env["DATABRICKS_HOST"], product=product_name, product_version=product_version)


@pytest.fixture
def env_or_skip(debug_env) -> Callable[[str], str]:
    skip = pytest.skip
    if _is_in_debug():
        skip = pytest.fail  # type: ignore[assignment]

    def inner(var: str) -> str:
        if var not in debug_env:
            skip(f"Environment variable {var} is missing")
        return debug_env[var]

    return inner
