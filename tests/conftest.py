"""
pytest configuration for srpo
"""

from pathlib import Path

import pytest

from srpo.core import set_registry_path


@pytest.fixture(autouse=True, scope="session")
def switch_resgistry_path():
    """ Switch the registry path as to not squish other processes. """
    path = Path(__file__).parent / "srpo_test_registry.sqlite"
    # remove any registry from previous runs
    if path.exists():
        path.unlink()
    # set the registry and yield
    with set_registry_path(path):
        yield
    # remove any registry
    if path.exists():
        path.unlink()
