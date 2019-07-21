"""
Core module of srpo.
"""
import multiprocessing
import os
import time
from typing import Any

import psutil
import rpyc
from rpyc.utils.server import ThreadPoolServer
from sqlitedict import SqliteDict

from srpo.utils import _create_srpo_service, get_registry

# enable pickling in rpyc, 'cause living on the edge is the only way to live
rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True


def transcend(obj: Any, name: str, server_threads=1, port=0, remote=True) -> None:
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
    # name is already in use; just bail out
    server_registry = get_registry("server")
    proxy_registry = get_registry("proxy")
    if name in server_registry:
        return

    # attributes the object must not posses
    service = _create_srpo_service(obj, proxy_registry=proxy_registry)

    def _remote():
        """ Code to execute on forked process. """

        kwargs = dict(
            hostname='localhost',
            nbThreads=server_threads,
            protocol_config=dict(allow_public_attrs=True),
            port=port,
        )

        server = ThreadPoolServer(service, **kwargs)

        sql_kwargs = dict(filename=server_registry.filename,
                          tablename=server_registry.tablename,
                          flag='c')
        registery = SqliteDict(**sql_kwargs)
        registery[name] = (server.host, server.port, os.getpid())
        registery.commit()
        server.start()

    if remote:
        multiprocessing.Process(target=_remote).start()
    else:
        _remote()

    # give the server a bit of time to start before releasing control
    time.sleep(0.2)
    # the name should be in the remote server now
    assert name in server_registry


def terminate(name: str) -> None:
    """
    Terminate a processes  containing a transcended object.

    Parameters
    ----------
    name
        The name of the transcended object
    """
    server_registry = get_registry('server')
    if name not in server_registry:
        return
    # get process id and kill process
    pid = server_registry[name][-1]
    psutil.Process(pid).terminate()
    # remove name from registry
    server_registry.pop(name)


def get_proxy(name: str):
    """
    Get a proxy for a transcendent object.

    Parameters
    ----------
    name
        The name of the transcended object.

    Returns
    -------

    """
    server_registry = get_registry('server')
    assert name in server_registry
    host, port, _ = server_registry[name]
    # try to connect, return proxy
    return rpyc.connect(host, port).root
