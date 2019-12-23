"""
Core module of srpo.
"""
import multiprocessing
import os
import pickle
import time
from contextlib import suppress
from functools import wraps
from pathlib import Path
from typing import Any, Optional, Union

import psutil
import rpyc
from rpyc import Service
from rpyc.utils.classic import obtain
from rpyc.utils.server import ThreadPoolServer
from sqlitedict import SqliteDict

# enable pickling in rpyc, 'cause living on the edge is the only way to live
rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True

# State for where the simple registry is found
_REGISTRY_STATE = dict(
    default=Path().home() / ".srpo_registry.sqlite",
    current=Path().home() / ".srpo_registry.sqlite",
)


# --- Service and proxy wrapper


class PassThrough:
    """ Class to pass through simple python interactions to self._obj """

    obj = None

    # any attributes should be just passed to obj
    def __getattr__(self, item):
        return getattr(self.obj, item)

    # set get attrs
    def __getitem__(self, item):
        return self.obj[item]

    def __setitem__(self, item, value):
        self.obj[item] = value

    def __iter__(self):
        return iter(self.obj)

    def __len__(self):
        return len(self.obj)

    def __str__(self):
        return str(self.obj)


class SrpoProxy(PassThrough):
    def __init__(self, connection, name):
        """
        Get a proxy for a transcendent object.

        Parameters
        ----------
        connection
            The rpyc connection object to the service.
        """
        self._connection = connection
        self._name = name
        self.obj = self._connection.root
        self._proxy_id = (id(self), psutil.Process().pid)
        self.obj.register_proxy(self._proxy_id)

    def __getattr__(self, item):
        value = getattr(self.obj, item)
        # try to pickle and de-pickle return object to get rid of netrefs.
        try:
            return obtain(value)
        except Exception:  # cant pickle this whatever it is
            return value

    def __del__(self):
        with suppress(Exception):
            self.obj.deregister_proxy(self._proxy_id)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        with suppress(Exception):
            self.obj.close(self._proxy_id)


def _create_srpo_service(object, server_name, registry_path=None):
    """ Create a rpyc service from object. """

    class ProxyService(Service, PassThrough):
        _proxies = set()
        _server = None
        obj = object
        name = server_name
        _registry_path = registry_path

        def on_connect(self, conn):
            # register the proxy
            pass

        def on_disconnect(self, conn):
            # deregister the proxy
            # shutdown the server if no proxies are using it
            pass

        def register_proxy(self, proxy_id):
            self.__dict__["_proxy_id"] = proxy_id
            self._proxies.add(proxy_id)

        def deregister_proxy(self, proxy_id):
            """ Remove a proxy from the registry. """
            with suppress(TypeError, KeyError):
                self._proxies.remove(proxy_id)
            # if the registry is empty pop the name out of the registry
            if not self._proxies:
                get_registry(self._registry_path).pop(self.name, None)

        def close(self, proxy_id=None):
            """ Close down the server if one is attached. """
            if self._server:
                with suppress(RuntimeError):
                    self._server.close()
                # Deregister proxy
                self.deregister_proxy(proxy_id)
                get_registry(self._registry_path).pop(self.name, None)

    return ProxyService


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

    def _remote():
        """ Code to execute on forked process. """

        service = _create_srpo_service(obj, name, registry_path=registry_path)

        kwargs = dict(
            hostname="localhost",
            nbThreads=server_threads,
            protocol_config=dict(allow_public_attrs=True),
            port=port,
        )

        server = ThreadPoolServer(service(), **kwargs)
        sql_kwargs = dict(
            filename=server_registry.filename,
            tablename=server_registry.tablename,
            flag="c",
        )
        # register new server
        registery = SqliteDict(**sql_kwargs)
        registery[name] = (server.host, server.port, os.getpid())
        registery.commit()
        #
        service._server = server
        server.start()

    if remote:
        multiprocessing.Process(target=_remote).start()
    else:
        # if True:
        # breakpoint()
        _remote()

    # give the server a bit of time to start before releasing control
    time.sleep(0.2)  # TODO:
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
        try:
            proxy = get_proxy(key)
        except Exception:
            continue
        proxy.shutdown()
    Path(registry_path).unlink()


def _wrap_methods_with_obtain(old_func):
    """ """

    @wraps(old_func)
    def _wrap(*args, **kwargs):
        return obtain(old_func(*args, **kwargs))

    return _wrap


def _pickle_results(func):
    @wraps(func)
    def _wrap(*args, **kwargs):
        return pickle.dumps(func(*args, **kwargs))

    return _wrap


def get_proxy(name: str) -> SrpoProxy:
    """
    Get a proxy for a transcendent object.

    Parameters
    ----------
    name
        The name of the transcended object.
    """
    # if another proxy was passed we just need to peel the name off this one.
    if isinstance(name, SrpoProxy):
        name = name._name

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

    return SrpoProxy(connection, name=name)


def get_registry(registry_path: Optional[Union[str, Path]] = None) -> SqliteDict:
    """
    Get the sqlite backed registry (key value pair).

    Parameters
    ----------
    registry_path
        Either "server" or "proxy"

    Returns
    -------

    """
    path = registry_path or get_current_registry_path()
    kwargs = dict(autocommit=True, tablename="server")
    return SqliteDict(path, **kwargs)


def get_current_registry_path():
    """ Return the current registry path """
    return _REGISTRY_STATE["current"]


def set_registry_path(new_path):
    """
    Set the registry path to not squish other processes instances of srpo.

    This can either be used as a single call or context manager.

    Parameters
    ----------
    new_path
        The new path to use.
    """

    class _RegistryManager:
        def __init__(self):
            _REGISTRY_STATE["current"] = new_path

        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_val, exc_tb):
            _REGISTRY_STATE["current"] = _REGISTRY_STATE["default"]

    return _RegistryManager()
