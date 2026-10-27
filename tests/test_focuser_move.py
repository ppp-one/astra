from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import astra.autofocus
from astra.autofocus import AstraAutofocusDeviceManager, AstraFocuser
from astra.observatory import Observatory


class FakeFocuser:
    """Starts moving only after `delay` polls, as drivers that report IsMoving late."""

    def __init__(
        self, position=7000, delay=3, duration=5, stuck=False, error=0, jitter=0
    ):
        self.position = position
        self.delay, self.duration, self.stuck = delay, duration, stuck
        self.error, self.jitter = error, jitter
        self.target = None
        self.polls = 0

    def get(self, name, **kwargs):
        if name == "Absolute":
            return True
        if name == "MaxStep":
            return 100_000
        if name == "Move":
            self.target, self.polls = kwargs["Position"], 0
            return None
        if name == "IsMoving":
            self.polls += 1
            moving = self.delay < self.polls <= self.delay + self.duration
            if (
                self.target is not None
                and not self.stuck
                and self.polls > self.delay + self.duration
            ):
                self.position = self.target + self.error
            return moving and not self.stuck
        if name == "Position":
            return self.position + (self.jitter if self.polls % 2 else 0)
        raise AssertionError(name)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(astra.autofocus.time, "sleep", lambda _: None)


def make_focuser(device, focus_tolerance=0):
    observatory = MagicMock()
    observatory.check_conditions.return_value = True
    focuser = AstraFocuser(
        observatory, device, settle_time=0, focus_tolerance=focus_tolerance
    )
    return focuser, observatory


def test_waits_for_a_move_that_starts_late():
    device = FakeFocuser()
    focuser, observatory = make_focuser(device)
    focuser.move_focuser_to_position(8000)
    assert device.position == 8000
    observatory.logger.warning.assert_not_called()


def test_warns_if_the_focuser_does_not_move():
    device = FakeFocuser(stuck=True)
    focuser, observatory = make_focuser(device)
    focuser.move_focuser_to_position(8000, start_timeout=0.01)
    assert device.position == 7000
    messages = [call.args[0] for call in observatory.logger.warning.call_args_list]
    assert any("did not start moving" in message for message in messages)
    assert any("not at the requested 8000" in message for message in messages)


def test_move_to_the_current_position_does_not_wait():
    device = FakeFocuser()
    focuser, observatory = make_focuser(device)
    focuser.move_focuser_to_position(7000, start_timeout=0.01)
    observatory.logger.warning.assert_not_called()


def test_warns_if_the_focuser_ends_outside_the_tolerance():
    device = FakeFocuser(error=3)
    focuser, observatory = make_focuser(device, focus_tolerance=2)
    focuser.move_focuser_to_position(8000)
    assert device.position == 8003
    messages = [call.args[0] for call in observatory.logger.warning.call_args_list]
    assert any("not at the requested 8000" in message for message in messages)


def test_does_not_warn_if_the_focuser_ends_within_the_tolerance():
    device = FakeFocuser(error=-2)
    focuser, observatory = make_focuser(device, focus_tolerance=2)
    focuser.move_focuser_to_position(8000)
    assert device.position == 7998
    observatory.logger.warning.assert_not_called()


def test_position_jitter_within_the_tolerance_is_not_a_start():
    device = FakeFocuser(jitter=1)
    focuser, observatory = make_focuser(device, focus_tolerance=2)
    focuser.move_focuser_to_position(8000)
    assert device.position == 8000
    observatory.logger.warning.assert_not_called()


def test_small_move_within_the_tolerance_does_not_wait():
    device = FakeFocuser(stuck=True)
    focuser, observatory = make_focuser(device, focus_tolerance=2)
    focuser.move_focuser_to_position(7002, start_timeout=0.01)
    observatory.logger.warning.assert_not_called()


@pytest.mark.parametrize("settle_factor, expected", [(0.0, 0.0), (1.0, 4.0)])
def test_waits_settle_factor_exposures_after_a_move(
    monkeypatch, settle_factor, expected
):
    events = []
    monkeypatch.setattr(
        astra.autofocus.time, "sleep", lambda seconds: events.append(seconds)
    )
    focuser, observatory = make_focuser(FakeFocuser())
    camera = MagicMock()
    camera.perform_exposure.side_effect = lambda texp: events.append("exposure")
    manager = AstraAutofocusDeviceManager(
        observatory,
        action_value={},
        action=MagicMock(),
        astra_camera=camera,
        astra_focuser=focuser,
        settle_factor=settle_factor,
    )
    manager.perform_exposure_at(focus_position=8000, texp=4.0)
    assert focuser.get_current_position() == 8000
    assert events[-2:] == [expected, "exposure"]


class FakePairedDevices:
    camera_name = "camera"

    def __init__(self, configs):
        self.configs = configs

    def __contains__(self, device_type):
        return device_type in self.configs

    def get_device_config(self, device_type):
        return self.configs[device_type]


def test_telescope_settle_factor_warns_once_per_camera():
    observatory = SimpleNamespace(logger=MagicMock(), _settle_factor_warned=set())
    paired_devices = FakePairedDevices(
        {"Camera": {}, "Telescope": {"settle_factor": 1.0}}
    )
    for _ in range(3):
        assert Observatory.get_settle_factor(observatory, paired_devices) == 1.0
    observatory.logger.warning.assert_called_once()
