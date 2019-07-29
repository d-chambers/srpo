"""
pytest configuration for srpo
"""

from pathlib import Path

import pytest

from srpo.core import set_registry_path, terminate_all


@pytest.fixture(autouse=True, scope="session")
def switch_resgistry_path(tmp_path_factory):
    """ Switch the registry path as to not squish other processes. """
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
        path.unlink()
