# -*- coding: utf-8 -*-

"""
test_srpo
----------------------------------

Tests for `srpo` module.
"""

import pytest

from srpo import get_proxy, transcend, terminate
from srpo.utils import get_registry


@pytest.fixture(scope="class")
def transcended_dict():
    """ Transcend a simple dictionary, return proxy, then cleanup. """
    obj = {"simple": True}
    name = "simple_dict"
    breakpoint()
    transcend(obj, name, remote=True)
    yield get_proxy(name)
    terminate(name)


class TestSimpleDict:
    """ Test the basic attributes of Transcended dictionary. """

    def test_can_add_value(self, transcended_dict):
        """ Ensure we can add a value to the transcended dict. """
        breakpoint()
        transcended_dict["bob"] = 2
        proxy = get_proxy(transcended_dict)
        assert "bob" in proxy
        assert proxy["bob"] == 2
