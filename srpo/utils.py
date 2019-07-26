"""
Utilities for srpo.
"""
from contextlib import suppress
from pathlib import Path
from typing import Optional, Union

import psutil
from rpyc.core.service import Service
from sqlitedict import SqliteDict

STATE = dict(
    default=Path().home() / ".spro_registry.sqlite",
    current=Path().home() / ".spro_registry.sqlite",
)


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
    return STATE["current"]


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
            STATE["current"] = new_path

        def __enter__(self):
            pass

        def __exit__(self, exc_type, exc_val, exc_tb):
            STATE["current"] = STATE["default"]

    return _RegistryManager()


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
        return str(self)


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


class SrpoProxy(PassThrough):
    def __init__(self, name, connection):
        """
        Get a proxy for a transcendent object.

        Parameters
        ----------
        name
            The name of the transcended object.
        """
        self._connection = connection
        self.obj = self._connection.root
        self._proxy_id = (id(self), psutil.Process().pid)
        self.obj.register_proxy(self._proxy_id)

    def __getattr__(self, item):
        return getattr(self.obj, item)

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
