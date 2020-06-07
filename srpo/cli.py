"""
SRPOS CLI
"""
from pprint import pprint
from typing import Optional

import psutil
import typer

import srpo
from srpo.core import terminate, terminate_all

app = typer.Typer()


@app.command()
def ls(registry_path=None):
    """
    List all the srp processes current registered.
    """
    registry = dict(srpo.get_registry(registry_path=registry_path))
    print("SRPO registered objects:")
    pprint(registry)


@app.command()
def kill(
    name: Optional[str] = None, all: bool = False, registry_path: Optional[str] = None
):
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
