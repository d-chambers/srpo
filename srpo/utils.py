"""
Utilities for srpo.
"""
import sys
from functools import wraps
from pathlib import Path

from rpyc.core.service import ClassicService, Service
from sqlitedict import SqliteDict

STATE = dict(
    default=Path().home() / ".spro_registry.sqlite",
    current=Path().home() / ".spro_registry.sqlite",
)


def get_registry(registry_type="server"):
    """
    Get
    Parameters
    ----------
    registry_type
        Either "server" or "proxy"

    Returns
    -------

    """
    kwargs = dict(autocommit=True, tablename=registry_type)
    return SqliteDict(STATE["current"], **kwargs)


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


class PassThroughDescriptor:
    """ A descriptor to simply pass through get, set, and del to an object. """

    def __init__(self, attr, obj):
        self._obj = obj
        self._attr = attr

    def __get__(self, instance, owner):
        return self._obj

    def __set__(self, instance, value):
        setattr(self._obj, self._attr, value)

    def __delattr__(self, item):
        delattr(self._obj, self._attr)


def _wrap_entity(obj, name):
    """ Wrap a callable or attribute. """
    # first get the value of the name and decided what to do with it
    value = getattr(obj, name)

    if callable(value):

        @wraps(value)
        def out(self, *args, **kwargs):
            return value(*args, **kwargs)

    else:
        out = PassThroughDescriptor(obj, name)

    return out


def _create_srpo_service(obj, proxy_registry):
    """ Create a rpyc service from object. """

    #
    # sacred_attrs = {"on_connect", "on_disconnect"}
    # dir_set = set(dir(obj))
    # assert (
    #     not sacred_attrs & dir_set
    # ), f"object must not have attributes named: {sacred_attrs}"
    #
    # # add connection and disconnection logic
    # def on_connect(self, conn):
    #     # register the proxy
    #     import remote_pdb;
    #     remote_pdb.set_trace('127.0.0.1', 8886)
    #     pass
    #
    # def on_disconnect(self, conn):
    #     # deregister the proxy
    #     # shutdown the server if no proxies are using it
    #     pass
    #
    # # wrap all attr/methods
    # attr_dict = {i: _wrap_entity(obj, i) for i in dir_set if not i.startswith('_')}
    # attr_dict["on_connect"] = on_connect
    # attr_dict["on_disconnect"] = on_disconnect
    #
    # def bob():
    #     print('bob')
    #
    # # create server, register service and start
    #
    # di = {'get': _wrap_entity(obj, 'get')}
    # cls = type("MyService", (ClassicService,), di) #attr_dict)

    # return cls

    class ProxyService(Service):
        _proxies = set()
        _proxy_id = None

        def on_connect(self, conn):
            # register the proxy
            pass

        def on_disconnect(self, conn):
            # deregister the proxy
            # shutdown the server if no proxies are using it
            pass

        # any attributes should be just passed to obj
        def __getattr__(self, item):
            return getattr(obj, item)

        # set get attrs
        def __getitem__(self, item):
            return obj[item]

        def __setitem__(self, item, value):
            obj[item] = value

        def __iter__(self):
            return iter(obj)

        def __len__(self):
            return len(obj)

        def __str__(self):
            return str(self)

        def register_proxy(self, proxy_id):
            self._proxies.add(proxy_id)

        def deregister_proxy(self, proxy_id):
            try:
                self._proxies.remove(proxy_id)
            except TypeError:
                pass
            # if no registered proxies shutdown server
            if not len(self._proxies):
                sys.exit(0)

        def __del__(self):
            if self._proxy_id:
                self.deregister_proxy(self._proxy_id)

    return ProxyService
