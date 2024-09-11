"""
Simple, no frills remote objects in python.
"""
# This is here for compatibility with pdbpp; it should be harmless.
import collections
collections.Callable = collections.abc.Callable


from .core import set_registry_path, terminate_all, get_registry, transcend, terminate, get_proxy
