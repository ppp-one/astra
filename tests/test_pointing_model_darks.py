from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pytest
from astropy.coordinates import EarthLocation
from astropy.io import fits

from astra.action_configs import PointingModelActionConfig
from astra.observatory import Observatory


class FakePairedDevices(dict):
    def __init__(self, filter_wheel=True, camera_config=None):
        super().__init__(Telescope="T1", Camera="Cam1")
        if filter_wheel:
            self["FilterWheel"] = "FW1"
            self.filter_wheel = SimpleNamespace(get=lambda name: 2)
        self.camera_config = camera_config or {}

    def get_device_config(self, device_type):
        return self.camera_config


def _fake_observatory(tmp_path, fail_at=None):
    """An observatory whose darks have the values 10, 20, 30, ... ADU."""
    obs = MagicMock()
    obs.exposures = []

    def perform_exposure(camera, exptime, maxadu, action, **kwargs):
        i = len(obs.exposures)
        obs.exposures.append(kwargs)
        if i == fail_at:
            return False, None
        path = tmp_path / "pointing_model" / f"Cam1_dark_{exptime:.3f}_{i}.fits"
        path.parent.mkdir(parents=True, exist_ok=True)
        data = np.full((4, 6), 10 * (i + 1), dtype=np.uint16)
        fits.PrimaryHDU(data, header=fits.Header({"EXPTIME": exptime})).writeto(path)
        return True, path

    obs.perform_exposure = perform_exposure
    return obs


def _take(obs, paired_devices, n=3, dark_filter=None, exptime=3.0):
    return Observatory.take_master_dark(
        obs,
        SimpleNamespace(device_name="Cam1"),
        paired_devices,
        SimpleNamespace(device_name="Cam1"),
        exptime=exptime,
        maxadu=65535,
        n=n,
        dark_filter=dark_filter,
    )


def test_master_dark_is_the_median_of_the_darks(tmp_path):
    obs = _fake_observatory(tmp_path)

    master = _take(obs, FakePairedDevices(), n=3)

    assert master == tmp_path / "pointing_model" / "Cam1_master_dark_3.000.fits"
    with fits.open(master) as hdul:
        np.testing.assert_array_equal(hdul[0].data, np.full((4, 6), 20.0))
        assert hdul[0].header["NCOMBINE"] == 3
    assert [e["use_light"] for e in obs.exposures] == [False] * 3
    obs._configure_filter.assert_not_called()
    obs.execute_and_monitor_device_task.assert_not_called()


def test_dark_filter_is_used_and_the_wheel_goes_back(tmp_path):
    obs = _fake_observatory(tmp_path)

    master = _take(obs, FakePairedDevices(), dark_filter="Dark")

    assert master is not None
    obs._configure_filter.assert_called_once()
    assert obs._configure_filter.call_args.args[1] == {"filter": "Dark"}
    restore = obs.execute_and_monitor_device_task.call_args
    assert restore.args[:4] == ("FilterWheel", "Position", 2, "Position")


def test_dark_filter_comes_from_the_camera_config(tmp_path):
    obs = _fake_observatory(tmp_path)
    paired_devices = FakePairedDevices(camera_config={"dark_filter": "Blank"})

    assert _take(obs, paired_devices) is not None
    assert obs._configure_filter.call_args.args[1] == {"filter": "Blank"}

    obs._configure_filter.reset_mock()
    assert _take(obs, paired_devices, dark_filter="Dark") is not None
    assert obs._configure_filter.call_args.args[1] == {"filter": "Dark"}


def test_master_of_zero_second_darks_is_a_master_bias(tmp_path):
    obs = _fake_observatory(tmp_path)

    master = _take(obs, FakePairedDevices(), exptime=0)

    assert master == tmp_path / "pointing_model" / "Cam1_master_bias_0.000.fits"


def test_dark_filter_without_filter_wheel_takes_no_darks(tmp_path):
    obs = _fake_observatory(tmp_path)

    assert _take(obs, FakePairedDevices(filter_wheel=False), dark_filter="Dark") is None

    assert obs.exposures == []
    obs.logger.warning.assert_called_once()


def test_failed_dark_gives_no_master_and_the_wheel_goes_back(tmp_path):
    obs = _fake_observatory(tmp_path, fail_at=1)

    assert _take(obs, FakePairedDevices(), dark_filter="Dark") is None

    assert len(obs.exposures) == 2
    obs.logger.warning.assert_called_once()
    obs.execute_and_monitor_device_task.assert_called_once()


def test_pointing_model_subtracts_its_own_darks(tmp_path):
    """The darks come before the dome opens, and each plate solve uses them."""
    obs = MagicMock()
    order = []
    master = tmp_path / "master.fits"
    obs.take_master_dark.side_effect = lambda *a, **k: order.append("darks") or master
    obs.open_observatory.side_effect = lambda *a: order.append("open")
    obs.check_conditions.side_effect = [True, False]
    obs.perform_exposure.return_value = (True, tmp_path / "light.fits")
    obs.pointing_correction.return_value = (True, None)
    obs.get_image_handler.return_value = SimpleNamespace(
        header={},
        get_observatory_location=lambda: EarthLocation.from_geodetic(
            -70.4, -24.6, 2400
        ),
    )
    action = SimpleNamespace(
        device_name="Cam1",
        start_time=datetime(2026, 10, 22, tzinfo=UTC),
        action_value={
            "n": 1,
            "exptime": 3.0,
            "dark_subtraction": True,
            "n_darks": 4,
            "dark_filter": "Dark",
        },
        summary_string=lambda verbose=False: "pointing model",
    )
    paired_devices = FakePairedDevices()

    Observatory.pointing_model_sequence(obs, action, paired_devices)

    assert order == ["darks", "open"]
    kwargs = obs.take_master_dark.call_args.kwargs
    assert (kwargs["n"], kwargs["dark_filter"]) == (4, "Dark")
    assert obs.pointing_correction.call_args.kwargs["dark_frame"] == master


def test_config_defaults_and_validation():
    config = PointingModelActionConfig()
    assert (config.dark_subtraction, config.n_darks, config.dark_filter) == (
        False,
        5,
        None,
    )
    PointingModelActionConfig(dark_subtraction=True, dark_filter="Dark")

    with pytest.raises(ValueError, match="dark_subtraction"):
        PointingModelActionConfig(dark_filter="Dark")
    with pytest.raises(ValueError, match="n_darks"):
        PointingModelActionConfig(dark_subtraction=True, n_darks=0)
