from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from astra.observatory import Observatory


class FakePairedDevices(dict):
    def __init__(self, filter_wheel=True, dark_filter="Dark"):
        super().__init__(Camera="Cam1")
        if filter_wheel:
            self["FilterWheel"] = "FW1"
            self.filter_wheel = SimpleNamespace(get=lambda name: 2)
        self.camera_config = {} if dark_filter is None else {"dark_filter": dark_filter}

    def get_device_config(self, device_type):
        return self.camera_config


def _run(paired_devices, action_filter=None):
    obs = MagicMock()
    order = []
    obs._configure_filter.side_effect = lambda pd, av, i: order.append(av["filter"])
    obs.image_sequence.side_effect = lambda *a: order.append("sequence")
    obs._restore_filter_position.side_effect = lambda pd, pos, after: order.append(pos)
    action = SimpleNamespace(
        device_name="Cam1", action_value={"exptime": [0], "n": [3]}
    )
    if action_filter is not None:
        action.action_value["filter"] = action_filter

    Observatory.calibration_sequence(obs, action, paired_devices)
    return obs, order


def test_dark_filter_is_used_and_the_wheel_goes_back():
    _, order = _run(FakePairedDevices())

    assert order == ["Dark", "sequence", 2]


def test_wheel_goes_back_after_an_error():
    obs = MagicMock()
    obs.image_sequence.side_effect = RuntimeError("camera error")
    action = SimpleNamespace(device_name="Cam1", action_value={})

    with pytest.raises(RuntimeError):
        Observatory.calibration_sequence(obs, action, FakePairedDevices())

    assert obs._restore_filter_position.call_args.args[1] == 2


def test_filter_of_the_action_wins():
    obs, order = _run(FakePairedDevices(), action_filter="V")

    assert order == ["sequence"]
    obs._restore_filter_position.assert_not_called()


def test_no_dark_filter_changes_nothing():
    obs, order = _run(FakePairedDevices(dark_filter=None))

    assert order == ["sequence"]
    obs.logger.warning.assert_not_called()


def test_dark_filter_without_filter_wheel_takes_frames_with_a_warning():
    obs, order = _run(FakePairedDevices(filter_wheel=False))

    assert order == ["sequence"]
    obs.logger.warning.assert_called_once()
