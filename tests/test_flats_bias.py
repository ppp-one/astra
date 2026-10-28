from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pytest
from astropy.io import fits

from astra.observatory import Observatory
from astra.utils import ephemeris


class FakePairedDevices(dict):
    def __init__(self, flats_config):
        super().__init__(Telescope="T1", Camera="Cam1")
        self.camera = MagicMock()
        self.flats_config = flats_config

    def get_device_config(self, device_type):
        return {"flats": self.flats_config}


@pytest.fixture(autouse=True)
def sun_in_flat_position(monkeypatch):
    sun = SimpleNamespace(alt=SimpleNamespace(degree=-5.0))
    monkeypatch.setattr(ephemeris, "is_sun_rising", lambda location: (False, True, sun))


def _run_flats(master_bias, flats_config=None):
    """Run a flats sequence whose first exposure time is out of range."""
    obs = MagicMock()
    order = []
    obs.take_master_dark.side_effect = lambda *a, **k: (
        order.append("bias") or master_bias
    )
    obs.setup_observatory.side_effect = lambda *a, **k: order.append("setup")
    obs.flats_exptime.return_value = 0.5
    action = SimpleNamespace(
        device_name="Cam1",
        start_time=None,
        end_time=None,
        action_value={"filter": ["V"], "n": [3]},
    )
    flats_config = flats_config or {
        "target_adu": 30000,
        "lower_exptime_limit": 1,
        "upper_exptime_limit": 60,
    }

    Observatory.flats_sequence(obs, action, FakePairedDevices(flats_config))

    return obs, order


def test_flats_use_the_median_of_the_master_bias(tmp_path):
    master_bias = tmp_path / "Cam1_master_bias_0.000.fits"
    fits.PrimaryHDU(np.full((4, 6), 312.0, dtype=np.float32)).writeto(master_bias)

    obs, order = _run_flats(master_bias)

    assert order == ["bias", "setup"]
    assert obs.take_master_dark.call_args.args[3] == 0
    assert obs.flats_exptime.call_args.args[4] == 312.0
    obs.logger.warning.assert_not_called()


def test_flats_without_master_bias_use_a_bias_level_of_zero():
    obs, _ = _run_flats(None)

    assert obs.flats_exptime.call_args.args[4] == 0.0
    obs.logger.warning.assert_called_once()


def test_old_bias_offset_is_ignored_with_a_warning(tmp_path):
    master_bias = tmp_path / "Cam1_master_bias_0.000.fits"
    fits.PrimaryHDU(np.full((4, 6), 312.0, dtype=np.float32)).writeto(master_bias)
    flats_config = {
        "target_adu": 30000,
        "bias_offset": 300,
        "lower_exptime_limit": 1,
        "upper_exptime_limit": 60,
    }

    obs, _ = _run_flats(master_bias, flats_config)

    assert obs.flats_exptime.call_args.args[4] == 312.0
    assert "bias_offset" in obs.logger.warning.call_args.args[0]
