"""
pytest configuration for srpo
"""
# This is here for compatibility with pdbpp; it should be harmless.
import collections
collections.Callable = collections.abc.Callable
from contextlib import suppress

import pytest
import rpyc
from srpo import get_registry, set_registry_path, terminate_all


@pytest.fixture(autouse=True, scope="session")
def switch_resgistry_path(tmp_path_factory):
    """Switch the registry path as to not squish other processes."""
    path = tmp_path_factory.getbasetemp() / "srpo_test_registry.sqlite"
    # remove any registry from previous runs
    if path.exists():
        path.unlink()
    # set the registry and yield
    with set_registry_path(path):
        yield
    # terminate all running servers
    terminate_all(path)
    # remove any registry
    if path.exists():
        with suppress(PermissionError):
            path.unlink()


@pytest.fixture(scope="session", autouse=True)
def configure_rpyc_for_testing():
    """Setup rpyc defaults to be a bit more friendly for debugging/testsing."""
    rpyc.core.protocol.DEFAULT_CONFIG["sync_request_timeout"] = 300


@pytest.fixture(scope="session")
def registry_path():
    """Get the current registry path"""
    return get_registry().filename
