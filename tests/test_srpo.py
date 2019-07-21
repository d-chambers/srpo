# -*- coding: utf-8 -*-

"""
test_srpo
----------------------------------

Tests for `srpo` module.
"""

import pytest

from srpo import get_proxy, transcend, terminate


@pytest.fixture(scope="class")
def transcended_dict():
    """ Transcend a simple dictionary, return proxy, then cleanup. """
    obj = {"simple": True}
    name = "simple_dict"
    transcend(obj, name, remote=True)
    yield get_proxy(name)
    terminate(name)


class TestSimpleDict:
    """ Test the basic attributes of Transcended dictionary. """

    def test_inited_values(self, transcended_dict):
        """ Ensure the value is still in the dict before it transcend. """
        assert 'simple' in transcended_dict
        assert transcended_dict['simple'] is True

    def test_can_add_value(self, transcended_dict):
        """ Ensure we can add a value to the transcended dict. """
        transcended_dict["bob"] = 2
        proxy = get_proxy("simple_dict")
        assert "bob" in proxy
        assert proxy["bob"] == 2

    def test_can_get_dict(self, transcended_dict):
        """ Ensure a dict can be made out of the transcended_dict """
        some_dict = dict(transcended_dict)
        assert isinstance(some_dict, dict)
