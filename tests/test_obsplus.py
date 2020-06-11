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
from obsplus.interfaces import WaveformClient


@pytest.fixture(scope="class")
def transcended_bank(tmpdir_factory):
    """ Transcend a bank server. """
    tmpdir = Path(tmpdir_factory.mktemp("bob"))
    ds = obsplus.load_dataset("bingham_test").copy_to(tmpdir)
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

    def test_get_waveforms_empty(self, transcended_bank):
        """ Ensure waveforms are returned. """
        st = transcended_bank.get_waveforms()
        assert isinstance(st, obspy.Stream)
        assert len(st)

    def test_get_waveforms_with_kwargs(self, transcended_bank):
        """Ensure values can be passed to the functions."""
        t1 = obspy.UTCDateTime("2013-04-11 03:42:51.995")
        st = transcended_bank.get_waveforms(starttime=t1, station="OSS")
        assert isinstance(st, obspy.Stream)
        assert len(st)

    def test_get_waveforms_bulk(self, transcended_bank):
        """Ensure bulk parameters don't choke bank. """
        t1 = obspy.UTCDateTime("2013-04-11 03:42:51.995")
        t2 = obspy.UTCDateTime("2013-04-11 03:44:01.985")
        bulk = [("UU", "OSS", "01", "ENZ", t1, t2)]

        out = transcended_bank.get_waveforms_bulk(bulk)
        assert isinstance(out, obspy.Stream)
        assert len(out) == 1

    def test_str_rep(self, transcended_bank):
        """ Ensure the transcended bank works. """
        out = str(transcended_bank)
        assert isinstance(out, str)

    def test_isinstance_waveform_client(self, transcended_bank):
        """ The duck typing should still work. """
        assert isinstance(transcended_bank, WaveformClient)
