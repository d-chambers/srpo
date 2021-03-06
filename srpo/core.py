"""
Core module of srpo.
"""
import multiprocessing
import os
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Optional, Union

import psutil
import rpyc
from rpyc import Service
from rpyc.utils.classic import obtain
from rpyc.utils.server import ThreadPoolServer
from sqlitedict import SqliteDict

from srpo.exceptions import SrpoConnectionError

# enable pickling in rpyc, 'cause living on the edge is the only way to live
rpyc.core.protocol.DEFAULT_CONFIG["allow_pickle"] = True
rpyc.core.protocol.DEFAULT_CONFIG["allow_all_attrs"] = True
rpyc.core.protocol.DEFAULT_CONFIG["allow_public_attrs"] = True
rpyc.core.protocol.DEFAULT_CONFIG["propagate_KeyboardInterrupt_locally"] = True

# State for where the simple registry is found
_REGISTRY_STATE = dict(
    default=Path().home() / ".srpo_registry.sqlite",
    current=Path().home() / ".srpo_registry.sqlite",
)


# --- Service and proxy wrapper


def _maybe_unwrap_value(value, cls):
    """ If the object is the same type as self, return it, else try to
    to unwrap it. """

    if cls is not None and isinstance(value, cls):
        return value
    # else try to pickle and de-pickle return object to get rid of netref.
    try:
        return obtain(value)
    except Exception:  # cant pickle this whatever it is, just return
        return value


def _unpack_input_outputs(self, name, doc):
    """Method to generate methods which simply pass arguments to proxy obj """

    def _func(self, *args, **kwargs):
        args = _maybe_unwrap_value(args, None)
        kwargs = _maybe_unwrap_value(kwargs, None)
        value = getattr(self.obj, name)(*args, **kwargs)
        return _maybe_unwrap_value(value, type(self))

    setattr(_func, "__doc__", doc)
    return _func


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
    """
    A poxy object for accessing rpyc service.
    """

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
        # give instances all methods of object
        for name, doc in self.obj.methods.items():
            wrap = _unpack_input_outputs(self, name, doc)
            setattr(self, name, wrap.__get__(self, type(self)))

    def __getattr__(self, item):
        return _maybe_unwrap_value(getattr(self.obj, item), type(self))

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
    obj_dir = {x: getattr(object, x) for x in dir(object) if not x.startswith("_")}

    class ProxyService(Service, PassThrough):
        _proxies = set()
        _server = None
        obj = object
        name = server_name
        _registry_path = registry_path
        # get a dict of method name / docstring
        methods = {
            x: i.__doc__
            for x, i in obj_dir.items()
            if hasattr(i, "__doc__") and callable(i)
        }
        attrs = set(obj_dir) - set(methods)

        def __init__(self):
            # wrap all methods with packers/unpackers
            for name, doc in self.methods.items():
                wrap = _unpack_input_outputs(self, name, doc)
                setattr(self, name, wrap.__get__(self, type(self)))
            super(ProxyService, self).__init__()

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

        @property
        def public_methods(self):
            """ Return a tuple of object attributes. """

            return tuple(x for x in dir(self.obj) if not x.startswith("_"))

    return ProxyService


def transcend(
    obj: Any,
    name: str,
    server_threads=1,
    port=0,
    remote: bool = True,
    registry_path: Optional[str] = None,
    daemon=True,
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
    server_threads
        The number of threads to allow for the server pool. If one is used
        everything is executed synchronously.
    port
        The port to bind to.
    remote
        If True run the server on a remote process. Typically only set to
        False for debugging.
    registry_path
        The path to the simple sqlitedict used to register IPs and ports.
    daemon
        If True start the transcended server in a daemon process. Only has an
        effect when remote == True.
    """
    # Get the registry path. This does need to be here to preserve any changes
    # in path for when a new process starts.
    registry_path = registry_path or get_current_registry_path()
    server_registry = get_registry(registry_path)
    # If the object has already been transcended just return it
    if name in server_registry:
        try:
            return get_proxy(name)
        # If it fails remove it and start over
        except SrpoConnectionError:
            terminate(name)
            time.sleep(0.2)

    def _remote():
        """ Code to execute on forked process. """
        service = _create_srpo_service(obj, name, registry_path=registry_path)
        # set new process group

        protocol = dict(allow_all_attrs=True)

        kwargs = dict(
            hostname="localhost",
            nbThreads=server_threads,
            protocol_config=protocol,
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
        # get a new new view of registry, make sure name is there
        assert name in SqliteDict(**sql_kwargs)
        service._server = server
        server.start()

    if remote:  # launch other process to run server
        proc = multiprocessing.Process(target=_remote, daemon=daemon)
        # this is a dirty hack to let the process live after script exists
        proc.__del__ = lambda: None
        proc.join = lambda *args, **kwargs: None
        # start process
        proc.start()
        # give the server a bit of time to start before releasing control
        for _ in range(100):
            if name in server_registry:
                return get_proxy(name)
            time.sleep(0.1)
    else:
        _remote()

    # the name should be in the remote server now
    server_registry = get_registry(registry_path)
    assert name in server_registry
    return get_proxy(name)


def terminate(name: str, registry_path: Optional[Path] = None) -> None:
    """
    Terminate a processes containing a transcended object.

    Parameters
    ----------
    name
        The name of the transcended object.
    registry_path
        The path to the simple sqlitedict used to register IPs and ports.
    """
    server_registry = get_registry(registry_path)
    registry_path = registry_path or server_registry.filename
    if name not in server_registry:
        return
    # be nice and tell the process to shutdown
    with suppress(Exception):
        get_proxy(name).shutdown()
        time.sleep(0.01)
    # get process id and kill process
    pid = server_registry[name][-1]
    with suppress((psutil.NoSuchProcess, psutil.AccessDenied)):
        psutil.Process(pid).terminate()
    # remove name from registry and unlink if empty
    server_registry.pop(name, None)
    if not server_registry:
        Path(registry_path).unlink()


def terminate_all(registry_path: Optional[Path] = None):
    """ Terminate all processes in a registry. """
    registry = dict(get_registry(registry_path))
    for key in registry:
        terminate(key, registry_path=registry_path)


def get_proxy(name: str, registry_path: Optional[str] = None) -> SrpoProxy:
    """
    Get a proxy for a transcendent object.

    Parameters
    ----------
    name
        The name of the transcended object.
    registry_path
        The path to the simple sqlitedict used to register IPs and ports.
    """
    # if another proxy was passed we just need to peel the name off this one.
    if isinstance(name, SrpoProxy):
        name = name._name

    server_registry = get_registry(registry_path)
    if name not in server_registry:
        msg = f"could not find server associated with {name}"
        raise SrpoConnectionError(msg)
    host, port, _ = server_registry[name]
    # try to connect, register this end of proxy, return proxy
    try:
        connection = rpyc.connect(host, port)
    except Exception as e:
        msg = f"could not connect to server associated with {name}"
        raise SrpoConnectionError(msg + f"\n server traceback: \n{e}")

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
