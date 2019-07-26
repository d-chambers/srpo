"""
Tests for utilities and misc. functionality
"""
import multiprocessing

import pytest
from sqlitedict import SqliteDict


class TestMultiprocessingSqlLite:
    """ tests for multiprocessing use with sqlitedict """

    @pytest.fixture
    def sqldict(self, tmp_path):
        path = tmp_path / "temp.sqlite"
        path.parent.mkdir(exist_ok=True, parents=True)
        yield SqliteDict(path)
        path.unlink()

    @pytest.fixture
    def sqldict_added_value(self, sqldict):
        """ add a  value using multiprocessing, return. """
        path = str(sqldict.filename)

        def _func():
            with SqliteDict(path) as mydict:
                mydict["bob"] = 2
                mydict.commit()

        proc = multiprocessing.Process(target=_func)
        proc.start()
        proc.join()

        yield sqldict

        proc.kill()

    def test_value_added(self, sqldict_added_value):
        """ ensure the value was added to the dict. """
        assert "bob" in sqldict_added_value
