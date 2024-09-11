"""
SRPOS CLI
"""

from __future__ import annotations

import rich
import typer

import srpo
from srpo import terminate, terminate_all

app = typer.Typer()


@app.command()
def ls(registry_path=None):
    """
    List all the srp processes current registered.
    """
    registry = dict(srpo.get_registry(registry_path=registry_path))
    rich.print("SRPO registered objects:")
    rich.print(registry)


@app.command()
def kill(name: str | None = None, all: bool = False, registry_path: str | None = None):
    """
    Kill a single srpo project by name or kill all of them.

    Parameters
    ----------
    name
        The registered name of the srpo object.
    all
        If True kill all srpo processes.
    """
    if all:
        terminate_all(registry_path)
    else:
        terminate(name, registry_path=registry_path)


if __name__ == "__main__":
    app()
