# -*- coding: utf-8 -*-

"""
test_srpo
----------------------------------

Tests for `srpo` module.
"""
import threading
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from types import SimpleNamespace
from pathlib import Path

import pytest
import psutil

from srpo import get_proxy, transcend, terminate
from srpo.core import get_registry


@pytest.fixture(scope="class")
def transcended_dict():
    """ Transcend a simple dictionary, return proxy, then cleanup. """
    obj = {"simple": True}
    name = "simple_dict"
    yield transcend(obj, name, remote=True)
    terminate(name)


class TestSimpleDict:
    """ Test the basic attributes of Transcended dictionary. """

    def test_inited_values(self, transcended_dict):
        """ Ensure the value is still in the dict before it transcend. """
        assert "simple" in transcended_dict
        assert transcended_dict["simple"] is True

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


class TestSetAttr:
    """ Class for setting and fetching remote attrs """

    @pytest.fixture(scope="class")
    def transcended_simple_namespace(self):
        ns = SimpleNamespace(bob=2)
        with transcend(ns, "bob") as tns:
            yield tns

    def test_get_attr(self, transcended_simple_namespace):
        """ ensure we can get the expected attrs. """
        assert transcended_simple_namespace.bob == 2

    def test_set_existing_attr(self, transcended_simple_namespace):
        """ Ensure we can set attrs. """

        transcended_simple_namespace.bob = 3
        assert transcended_simple_namespace.bob == 3

    def test_set_new_attr(self, transcended_simple_namespace):
        """ Ensure we can set a new attribute. """
        transcended_simple_namespace.new_attr = True
        assert hasattr(transcended_simple_namespace, "new_attr")
        assert transcended_simple_namespace.new_attr is True


class TestOneProcessOneThread:
    """ Test that transcended objects run on one thread / process. """

    worker_count = 8

    @pytest.fixture(scope="class")
    def thread_proc_counter(self, tmpdir_factory):
        """ Return a transcended thread/process counter """

        class ThreadProcCounter:
            """ Sample class to log current thread/proc. """

            def __init__(self):
                self.path = Path(tmpdir_factory.mktemp("base"))

            def get_log_name(self):
                proc = psutil.Process().pid
                thread = threading.get_ident()
                return self.path / f"{proc}_{thread}.txt"

            def log(self):
                """ write an empty file with the process id and thread id. """
                path = self.get_log_name()
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("w") as fi:
                    fi.write("something.txt")

            def remove_all_logs(self):
                """ remove all the logs. """
                for p in self.path.parent.rglob("*.txt"):
                    p.unlink()

            @property
            def log_count(self):
                """ return the count of files created. """
                path = self.get_log_name().parent
                paths = list(path.rglob("*.txt"))
                return len(paths)

        proxy = transcend(ThreadProcCounter(), "threadproccount")
        yield proxy
        proxy.close()

    @pytest.fixture(scope="class")
    def process_pool(self):
        """ return a thread pool """
        with ProcessPoolExecutor(self.worker_count) as executor:
            yield executor

    @pytest.fixture(scope="class")
    def thread_pool(self):
        """ return a thread pool """
        with ThreadPoolExecutor(self.worker_count) as executor:
            yield executor

    def run_on_pool(self, pool, thread_proc_counter):
        """ Run on a pool. """
        proxy = get_proxy(thread_proc_counter)
        out = []
        for num in range(self.worker_count):
            out.append(pool.submit(proxy.log))
        list(as_completed(out))

    def test_file_created(self, thread_proc_counter):
        """ Test that the file is created and has thread and proc in name. """
        thread_proc_counter.log()
        assert thread_proc_counter.get_log_name().exists()
        assert thread_proc_counter.log_count == 1
        thread_proc_counter.remove_all_logs()

    def test_threads(self, thread_proc_counter, thread_pool):
        """ running the log on multiple threads only creates one file. """
        self.run_on_pool(thread_pool, thread_proc_counter)
        assert thread_proc_counter.log_count == 1

    def test_process(self, thread_proc_counter, process_pool):
        """ running the log on multiple threads only creates one file. """
        self.run_on_pool(process_pool, thread_proc_counter)
        assert thread_proc_counter.log_count == 1


class TestServerShutdown:
    """ Test that the server shutdowns when the last proxy dies. """

    def test_create_and_shutdown_server(self):
        """ Ensure when the last proxy terminates the server shuts down """
        # create an object and transcend it
        name = "simple_obj_from_test"
        obj = {}
        proxy = transcend(obj, name)
        # delete the proxy and make sure the server shuts down
        proxy.close()
        assert name not in get_registry()

        with pytest.raises(Exception):  # Note: Must use Exception here
            get_proxy(name)


class TestSTDout:
    """ Tests that stdout/error gets forwarded to proxy end. """

    @pytest.fixture
    def stdouting(self):
        """ Define a class for stdout test """

        class STDOuter:
            def print(self, msg):
                print(msg)

        return STDOuter()

    def test_stdout_captured(self, stdouting, capsys):
        """ Ensure stdout gets captured. """
        some_str = "a string to capture"
        stdouting.print(some_str)
        captured = capsys.readouterr()
        assert some_str in str(captured)
