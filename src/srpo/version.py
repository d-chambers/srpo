"""
Module for handling pytaska's version
"""

from __future__ import annotations

from contextlib import suppress
from functools import cache
from importlib.metadata import PackageNotFoundError, version


@cache
def get_version_str():
    """Get the version string associated with pytaska."""
    version_str = "0.0.0"
    # try to get version from installed metadata
    with suppress(PackageNotFoundError):
        version_str = version("pytaska")
    return version_str


@cache
def get_last_version_str():
    """Determine the last (non git commit) version."""
    version_str = get_version_str()
    last_version = ".".join(version_str.split(".")[:3])
    return last_version
