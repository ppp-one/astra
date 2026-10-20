"""Tests for the dome shutter guard and AsTelOS link check of the SPECULOOS example."""

import importlib.util
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from astra.observatory import Observatory

SPECULOOS_PATH = (
    Path(__file__).parent.parent
    / "example_observatories"
    / "speculoos"
    / "speculoos.py"
)
_spec = importlib.util.spec_from_file_location("speculoos_example", SPECULOOS_PATH)
speculoos_example = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(speculoos_example)
SPECULOOS = speculoos_example.SPECULOOS

CLEAN = "DRIVES|0::,SYSTEM|0::,AUXILIARY|0::,UNKNOWN|0::"


def make_device(replies: dict):
    """Fake AlpacaDevice. Each reply is a value or a list of values in order."""
    device = MagicMock()
    queues = {k: list(v) if isinstance(v, list) else v for k, v in replies.items()}

    def get(method, **kwargs):
        key = kwargs.get("Command", method)
        value = queues[key]
        if isinstance(value, list):
            return value.pop(0) if len(value) > 1 else value[0]
        return value

    device.get.side_effect = get
    return device


@pytest.fixture
def obs():
    """SPECULOOS instance without Observatory.__init__, with short guard times."""
    o = object.__new__(SPECULOOS)
    o._shutter_command_lock = threading.Lock()
    o._last_shutter_command = {}
    o._last_astelos_link_check = float("-inf")
    o.SHUTTER_COMMAND_MIN_GAP = 0.3
    o.READY_STATE_SETTLE_TIME = 0.1
    o.READY_STATE_POLL_INTERVAL = 0.01
    o.SHUTTER_GUARD_TIMEOUT = 1.0
    o.OPENING_WAIT_TIMEOUT = 0.3
    o.logger = MagicMock(error_free=True)
    o.safety_monitor = MagicMock(weather_safe=True)
    o.watchdog_running = True
    o.device_manager = MagicMock()
    o.dome = make_device({"ShutterStatus": 1})
    o.telescope = make_device(
        {"TELESCOPE.READY_STATE": "0.0", "TELESCOPE.STATUS.LIST": CLEAN}
    )
    o.device_manager.devices = {
        "Dome": {"dome": o.dome},
        "Telescope": {"tel": o.telescope},
    }
    return o


def set_telescope(obs, replies):
    obs.telescope = make_device(replies)
    obs.device_manager.devices["Telescope"]["tel"] = obs.telescope


def test_no_wait_when_shutter_already_in_desired_state(obs):
    assert obs._wait_before_shutter_command("dome", "CloseShutter", 1) is True
    obs.telescope.get.assert_not_called()
    assert "dome" not in obs._last_shutter_command


def test_waits_while_power_change_in_progress(obs):
    set_telescope(obs, {"TELESCOPE.READY_STATE": ["0.0", "0.2", "0.6", "1.0"]})

    start = time.monotonic()
    assert obs._wait_before_shutter_command("dome", "OpenShutter", 0) is True

    assert time.monotonic() - start >= obs.READY_STATE_SETTLE_TIME
    ready_reads = [
        c for c in obs.telescope.get.call_args_list if c.kwargs.get("Command")
    ]
    assert len(ready_reads) >= 4
    assert "dome" in obs._last_shutter_command


def test_waits_minimum_gap_after_last_shutter_command(obs):
    obs._last_shutter_command["dome"] = time.monotonic()

    start = time.monotonic()
    assert obs._wait_before_shutter_command("dome", "OpenShutter", 0) is True

    assert time.monotonic() - start >= obs.SHUTTER_COMMAND_MIN_GAP


def test_open_refused_when_driver_lost_astelos(obs):
    set_telescope(obs, {"TELESCOPE.READY_STATE": "Client not Connected"})

    assert obs._wait_before_shutter_command("dome", "OpenShutter", 0) is False
    obs.logger.report_device_issue.assert_called_once()
    assert "dome" not in obs._last_shutter_command


def test_close_sent_when_driver_lost_astelos(obs):
    obs.dome = make_device({"ShutterStatus": 0})
    obs.device_manager.devices["Dome"]["dome"] = obs.dome
    set_telescope(obs, {"TELESCOPE.READY_STATE": "Client not Connected"})

    assert obs._wait_before_shutter_command("dome", "CloseShutter", 1) is True
    obs.logger.warning.assert_called_once()
    obs.logger.report_device_issue.assert_not_called()


def test_open_wait_stops_when_weather_unsafe(obs):
    set_telescope(obs, {"TELESCOPE.READY_STATE": "0.5"})
    obs.safety_monitor.weather_safe = False

    assert obs._wait_before_shutter_command("dome", "OpenShutter", 0) is False
    obs.logger.report_device_issue.assert_not_called()


def test_close_not_weather_sensitive_waits_for_power_change(obs):
    obs.dome = make_device({"ShutterStatus": 0})
    obs.device_manager.devices["Dome"]["dome"] = obs.dome
    set_telescope(obs, {"TELESCOPE.READY_STATE": ["0.2", "0.9", "1.0"]})
    obs.safety_monitor.weather_safe = False

    assert (
        obs._wait_before_shutter_command(
            "dome", "CloseShutter", 1, weather_sensitive=False
        )
        is True
    )


def use_dome(obs, replies):
    obs.dome = make_device(replies)
    obs.device_manager.devices["Dome"]["dome"] = obs.dome


def shutter_reads(obs):
    return [c for c in obs.dome.get.call_args_list if c.args[0] == "ShutterStatus"]


def test_close_waits_for_opening_to_end(obs):
    # First read is the "already closed?" check, then opening twice, then open.
    use_dome(obs, {"ShutterStatus": [2, 2, 2, 0]})
    set_telescope(obs, {"TELESCOPE.READY_STATE": "1.0"})

    assert obs._wait_before_shutter_command("dome", "CloseShutter", 1) is True

    assert len(shutter_reads(obs)) == 4
    obs.logger.warning.assert_not_called()
    assert "dome" in obs._last_shutter_command


def test_close_sent_after_opening_wait_timeout(obs):
    use_dome(obs, {"ShutterStatus": 2})
    set_telescope(obs, {"TELESCOPE.READY_STATE": "1.0"})

    start = time.monotonic()
    assert obs._wait_before_shutter_command("dome", "CloseShutter", 1) is True

    assert time.monotonic() - start >= obs.OPENING_WAIT_TIMEOUT
    assert "still opening" in obs.logger.warning.call_args.args[0]


def test_close_opening_wait_stops_on_error(obs):
    use_dome(obs, {"ShutterStatus": 2})
    obs.logger.error_free = False

    assert obs._wait_before_shutter_command("dome", "CloseShutter", 1) is False
    assert "dome" not in obs._last_shutter_command


def test_close_opening_wait_survives_read_error(obs):
    obs.dome = MagicMock()
    obs.dome.get.side_effect = [2, RuntimeError("no reply")]
    obs.device_manager.devices["Dome"]["dome"] = obs.dome
    set_telescope(obs, {"TELESCOPE.READY_STATE": "1.0"})

    assert obs._wait_before_shutter_command("dome", "CloseShutter", 1) is True


def test_open_does_not_wait_for_opening(obs):
    use_dome(obs, {"ShutterStatus": 2})

    assert obs._wait_before_shutter_command("dome", "OpenShutter", 0) is True
    obs.logger.warning.assert_not_called()


def test_open_refused_when_power_change_does_not_end(obs):
    set_telescope(obs, {"TELESCOPE.READY_STATE": "0.5"})
    obs.SHUTTER_GUARD_TIMEOUT = 0.2

    assert obs._wait_before_shutter_command("dome", "OpenShutter", 0) is False
    obs.logger.report_device_issue.assert_called_once()


@patch.object(Observatory, "execute_and_monitor_device_task")
def test_guard_only_for_shutter_commands(base_task, obs):
    with patch.object(
        SPECULOOS, "_wait_before_shutter_command", return_value=False
    ) as guard:
        obs.execute_and_monitor_device_task(
            "Dome", "ShutterStatus", 0, "OpenShutter", device_name="dome"
        )
        guard.assert_called_once()
        base_task.assert_not_called()

        obs.execute_and_monitor_device_task(
            "Telescope", "AtPark", True, "Park", device_name="tel"
        )
        guard.assert_called_once()
        base_task.assert_called_once()


@patch.object(Observatory, "execute_and_monitor_device_task")
def test_shutter_command_runs_after_guard(base_task, obs):
    with patch.object(SPECULOOS, "_wait_before_shutter_command", return_value=True):
        obs.execute_and_monitor_device_task(
            "Dome",
            "ShutterStatus",
            1,
            "CloseShutter",
            device_name="dome",
            weather_sensitive=False,
        )
    base_task.assert_called_once()
    assert base_task.call_args.kwargs["weather_sensitive"] is False


@patch.object(Observatory, "_watchdog_step")
def test_link_check_reports_lost_connection(base_step, obs):
    set_telescope(obs, {"TELESCOPE.STATUS.LIST": "Client not Connected"})

    obs._watchdog_step()

    obs.logger.report_device_issue.assert_called_once()
    assert (
        "no working connection to AsTelOS"
        in (obs.logger.report_device_issue.call_args.kwargs["message"])
    )


@patch.object(Observatory, "_watchdog_step")
def test_link_check_quiet_when_connected_and_throttled(base_step, obs):
    obs._watchdog_step()
    obs._watchdog_step()

    obs.logger.report_device_issue.assert_not_called()
    status_reads = [
        c
        for c in obs.telescope.get.call_args_list
        if c.kwargs.get("Command") == "TELESCOPE.STATUS.LIST"
    ]
    assert len(status_reads) == 1


@patch.object(Observatory, "_watchdog_step")
def test_link_check_skipped_when_errors_present(base_step, obs):
    obs.logger.error_free = False

    obs._watchdog_step()

    obs.telescope.get.assert_not_called()
