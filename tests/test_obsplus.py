"""
Tess for using srpo with obsplus' WaveBanke
"""
from pathlib import Path

import pytest

pytest.importorskip("obsplus")

import obspy
import obsplus
import pandas as pd


from srpo import transcend


@pytest.fixture(scope="class")
def transcended_bank(tmpdir_factory):
    """ Transcend a bank server. """
    tmpdir = Path(tmpdir_factory.mktemp("bob"))
    ds = obsplus.load_dataset("bingham").copy_to(tmpdir)
    bank = obsplus.WaveBank(ds.waveform_path)
    proxy = transcend(bank, "wavebank")
    yield proxy
    proxy.close()


class TestBankBasics:
    """ Ensure the basics of the bank still work. """

    def test_update(self, transcended_bank):
        """ Delete the index and ensure the update doesn't error. """
        Path(transcended_bank.index_path).unlink()
        transcended_bank.update_index()

    def test_get_index(self, transcended_bank):
        """ Ensure we can read the index."""
        df = transcended_bank.read_index()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_waveforms(self, transcended_bank):
        """ Ensure waveforms are returned. """
        st = transcended_bank.get_waveforms()
        assert isinstance(st, obspy.Stream)
        assert len(st)
