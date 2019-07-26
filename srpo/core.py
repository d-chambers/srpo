"""
Core module of srpo.
"""
import multiprocessing
import os
import time
from pathlib import Path
from typing import Any, Optional

import psutil
import rpyc
from rpyc.utils.server import ThreadPoolServer
from sqlitedict import SqliteDict

from srpo.utils import (
    _create_srpo_service,
    get_registry,
    SrpoProxy,
    get_current_registry_path,
)

# enable pickling in rpyc, 'cause living on the edge is the only way to live
rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


def transcend(
    obj: Any,
    name: str,
    server_threads=1,
    port=0,
    remote=True,
    registry_path: Optional[str] = None,
) -> SrpoProxy:
    """
    Transcend an object to its own process.

    If the name is already in use simple return.

    Parameters
    ----------
    obj
        Any python object.
    name
        A string identifier so other processes can find the object.
    """
    # name is already in use; just bail out and return proxy to it
    registry_path = registry_path or get_current_registry_path()
    server_registry = get_registry(registry_path)
    if name in server_registry:
        return get_proxy(name)

    # attributes the object must not posses
    service = _create_srpo_service(obj, name, registry_path=registry_path)

    def _remote():
        """ Code to execute on forked process. """

        kwargs = dict(
            hostname="localhost",
            nbThreads=server_threads,
            protocol_config=dict(allow_public_attrs=True),
            port=port,
        )

        server = ThreadPoolServer(service, **kwargs)
        sql_kwargs = dict(
            filename=server_registry.filename,
            tablename=server_registry.tablename,
            flag="c",
        )
        registery = SqliteDict(**sql_kwargs)
        registery[name] = (server.host, server.port, os.getpid())
        registery.commit()
        service._server = server
        server.start()

    if remote:
        multiprocessing.Process(target=_remote).start()
    else:
        _remote()

    # give the server a bit of time to start before releasing control
    time.sleep(0.1)
    # the name should be in the remote server now
    assert name in server_registry
    return get_proxy(name)


def terminate(name: str) -> None:
    """
    Terminate a processes containing a transcended object.

    Parameters
    ----------
    name
        The name of the transcended object
    """
    server_registry = get_registry()
    if name not in server_registry:
        return
    # get process id and kill process
    pid = server_registry[name][-1]
    psutil.Process(pid).terminate()
    # remove name from registry
    server_registry.pop(name)


def terminate_all(registry_path: Optional[Path]):
    """ Terminate all processes in a registry. """
    registry = get_registry(registry_path)
    for key in registry:
        proxy = SrpoProxy(key)
        proxy.shutdown()
    Path(registry_path).unlink()


def get_proxy(name: str) -> SrpoProxy:
    """
    Get a proxy for a transcendent object.

    Parameters
    ----------
    name
        The name of the transcended object.
    """

    server_registry = get_registry()
    if name not in server_registry:
        msg = f"could not find server associated with {name}"
        raise ConnectionError(msg)
    host, port, _ = server_registry[name]
    # try to connect, register this end of proxy, return proxy
    try:
        connection = rpyc.connect(host, port)
    except Exception:
        msg = f"could not connect to server associated with {name}"
        raise ConnectionError(msg)

    return SrpoProxy(name, connection)
