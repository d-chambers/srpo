"""
Utilities for srpo.
"""
from functools import wraps
from pathlib import Path

from rpyc.core.service import Service
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

    sacred_attrs = {"on_connect", "on_disconnect"}
    dir_set = set(dir(obj))
    assert (
        not sacred_attrs & dir_set
    ), f"object must not have attributes named: {sacred_attrs}"

    # add connection and disconnection logic
    def on_connect(self, conn):
        # register the proxy
        import remote_pdb;
        remote_pdb.set_trace('127.0.0.1', 8886)
        pass

    def on_disconnect(self, conn):
        # deregister the proxy
        # shutdown the server if no proxies are using it
        pass

    # wrap all attr/methods
    attr_dict = {i: _wrap_entity(obj, i) for i in dir_set}
    attr_dict["on_connect"] = on_connect
    attr_dict["on_disconnect"] = on_disconnect

    # create server, register service and start
    # return type("MyService", (Service,), attr_dict)

    class SimpleService(Service):
        def on_connect(self, conn):
            # register the proxy
            pass

        def on_disconnect(self, conn):
            # deregister the proxy
            # shutdown the server if no proxies are using it
            pass

        def bobit_all(self):
            return {'hey', 'ellie'}

        art = 'Nope'

    return SimpleService
