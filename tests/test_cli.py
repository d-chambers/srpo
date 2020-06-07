"""
Tests for the cli
"""
from subprocess import run

import srpo


class TestList:
    def test_object_appears_in_registry(self, registry_path):
        """Ensure the object appears in the registry."""
        _ = srpo.transcend("bob", name="transcended_bob")
        cmd = f"srpo ls --registry-path {registry_path}"
        res = run(cmd, shell=True, capture_output=True)
        output_str = res.stdout.decode("utf8").split("\n")[1]
        assert "transcended_bob" in output_str


class TestKill:
    def test_kill_by_name(self, registry_path):
        """Ensure srpo objects can be killed by name."""
        name = "transcended_bill"
        _ = srpo.transcend("Bill", name)
        assert name in dict(srpo.get_registry())
        # now issue kill command, ensure bill goes away
        cmd = f"srpo kill --name {name} --registry-path {registry_path}"
        run(cmd, shell=True)
        assert name not in srpo.get_registry()
