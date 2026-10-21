"""Tests for the dome shutter guard and AsTelOS link check of the SPECULOOS example."""

import importlib.util
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

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
    """SPECULOOS instance without Observatory.__init__, with short wait times."""
    o = object.__new__(SPECULOOS)
    o._shutter_command_lock = threading.Lock()
    o._last_astelos_link_check = float("-inf")
    o._link_check_thread = None
    o._sequence_lock = threading.Lock()
    o._sequences_running = 0
    o.SHUTTER_MOVE_TIMEOUT = 0.3
    o.SHUTTER_POLL_INTERVAL = 0.01
    o.SHUTTER_SETTLE_AFTER_MOVE = 0.05
    o.ASTELOS_LINK_RECHECK_DELAY = 0.01
    o.logger = MagicMock(error_free=True)
    o.safety_monitor = MagicMock(weather_safe=True)
    o.watchdog_running = True
    o.device_manager = MagicMock()
    o.dome = make_device({"ShutterStatus": 1})
    o.telescope = make_device({"TELESCOPE.STATUS.LIST": CLEAN})
    o.device_manager.devices = {
        "Dome": {"dome": o.dome},
        "Telescope": {"tel": o.telescope},
    }
    return o


def set_telescope(obs, replies):
    obs.telescope = make_device(replies)
    obs.device_manager.devices["Telescope"]["tel"] = obs.telescope


def use_dome(obs, replies):
    obs.dome = make_device(replies)
    obs.device_manager.devices["Dome"]["dome"] = obs.dome


def shutter_reads(obs):
    return [c for c in obs.dome.get.call_args_list if c.args[0] == "ShutterStatus"]


def test_no_wait_when_shutter_not_moving(obs):
    start = time.monotonic()
    assert obs._wait_before_shutter_command("dome", "OpenShutter") is True

    assert time.monotonic() - start < obs.SHUTTER_SETTLE_AFTER_MOVE
    assert len(shutter_reads(obs)) == 1
    obs.telescope.get.assert_not_called()


def test_close_waits_while_opening_then_settles(obs):
    # The 2026-10-18 pattern: a close while the open still powers up, which
    # the driver reports as "opening" (2).
    use_dome(obs, {"ShutterStatus": [2, 2, 2, 0]})

    start = time.monotonic()
    assert obs._wait_before_shutter_command("dome", "CloseShutter") is True

    assert len(shutter_reads(obs)) == 4
    assert time.monotonic() - start >= obs.SHUTTER_SETTLE_AFTER_MOVE
    assert "finish opening" in obs.logger.info.call_args.args[0]


def test_open_waits_while_closing(obs):
    use_dome(obs, {"ShutterStatus": [3, 3, 1]})

    assert obs._wait_before_shutter_command("dome", "OpenShutter") is True

    assert len(shutter_reads(obs)) == 3
    assert "finish closing" in obs.logger.info.call_args.args[0]


def test_close_sent_after_timeout(obs):
    use_dome(obs, {"ShutterStatus": 2})

    start = time.monotonic()
    assert obs._wait_before_shutter_command("dome", "CloseShutter") is True

    assert obs.SHUTTER_MOVE_TIMEOUT <= time.monotonic() - start < 1.0
    assert "Sending CloseShutter anyway" in obs.logger.warning.call_args.args[0]
    obs.logger.report_device_issue.assert_not_called()


def test_open_refused_after_timeout(obs):
    use_dome(obs, {"ShutterStatus": 3})

    assert obs._wait_before_shutter_command("dome", "OpenShutter") is False
    obs.logger.report_device_issue.assert_called_once()


def test_open_wait_stops_when_weather_unsafe(obs):
    use_dome(obs, {"ShutterStatus": 3})
    obs.safety_monitor.weather_safe = False

    assert obs._wait_before_shutter_command("dome", "OpenShutter") is False
    obs.logger.report_device_issue.assert_not_called()


def test_close_not_weather_sensitive_keeps_waiting(obs):
    use_dome(obs, {"ShutterStatus": [2, 2, 0]})
    obs.safety_monitor.weather_safe = False

    assert (
        obs._wait_before_shutter_command(
            "dome", "CloseShutter", weather_sensitive=False
        )
        is True
    )


@pytest.mark.parametrize("error_sensitive, expected", [(True, False), (False, True)])
def test_wait_and_errors(obs, error_sensitive, expected):
    use_dome(obs, {"ShutterStatus": [2, 2, 0]})
    obs.logger.error_free = False

    assert (
        obs._wait_before_shutter_command(
            "dome", "CloseShutter", error_sensitive=error_sensitive
        )
        is expected
    )


def test_read_error_lets_command_through(obs):
    obs.dome = MagicMock()
    obs.dome.get.side_effect = [2, RuntimeError("no reply")]
    obs.device_manager.devices["Dome"]["dome"] = obs.dome

    assert obs._wait_before_shutter_command("dome", "CloseShutter") is True


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


def run_watchdog_step(obs):
    """Run one watchdog step and wait for the link check thread, if any."""
    obs._watchdog_step()
    if obs._link_check_thread is not None:
        obs._link_check_thread.join(timeout=2)


def status_reads(obs):
    return [
        c
        for c in obs.telescope.get.call_args_list
        if c.kwargs.get("Command") == "TELESCOPE.STATUS.LIST"
    ]


@patch.object(Observatory, "_watchdog_step")
def test_link_check_reports_lost_connection(base_step, obs):
    set_telescope(obs, {"TELESCOPE.STATUS.LIST": "Client not Connected"})

    run_watchdog_step(obs)

    obs.logger.report_device_issue.assert_called_once()
    message = obs.logger.report_device_issue.call_args.kwargs["message"]
    assert "no working connection to AsTelOS" in message
    assert "reconnect devices" in message
    assert len(status_reads(obs)) == 2  # read twice before reporting


@patch.object(Observatory, "_watchdog_step")
def test_link_check_ignores_one_bad_reply(base_step, obs):
    set_telescope(obs, {"TELESCOPE.STATUS.LIST": ["Client not Connected", CLEAN]})

    run_watchdog_step(obs)

    obs.logger.report_device_issue.assert_not_called()


@patch.object(Observatory, "_watchdog_step")
def test_link_check_failed_call_only_warns(base_step, obs):
    obs.telescope.get.side_effect = RuntimeError("timeout")

    run_watchdog_step(obs)

    obs.logger.report_device_issue.assert_not_called()
    obs.logger.warning.assert_called()


@patch.object(Observatory, "_watchdog_step")
def test_link_check_quiet_when_connected_and_throttled(base_step, obs):
    run_watchdog_step(obs)
    run_watchdog_step(obs)

    obs.logger.report_device_issue.assert_not_called()
    assert len(status_reads(obs)) == 1


@patch.object(Observatory, "_watchdog_step")
def test_link_check_runs_in_its_own_thread(base_step, obs):
    release = threading.Event()
    obs.telescope.get.side_effect = lambda *a, **k: release.wait(2) and CLEAN

    start = time.monotonic()
    obs._watchdog_step()  # must not wait for the blocked driver call
    assert time.monotonic() - start < 0.5
    assert obs._link_check_thread.is_alive()

    release.set()
    obs._link_check_thread.join(timeout=2)


@patch.object(Observatory, "_watchdog_step")
def test_link_check_skipped_when_errors_present(base_step, obs):
    obs.logger.error_free = False

    run_watchdog_step(obs)

    obs.telescope.get.assert_not_called()


@patch.object(Observatory, "_watchdog_step")
def test_link_check_skipped_during_sequence_or_shutter_command(base_step, obs):
    obs._sequences_running = 1
    run_watchdog_step(obs)
    obs._sequences_running = 0

    with obs._shutter_command_lock:
        run_watchdog_step(obs)

    obs.telescope.get.assert_not_called()


def test_shutter_lock_held_while_command_runs(obs):
    held = []

    def base(*args, **kwargs):
        held.append(obs._shutter_command_lock.locked())

    with (
        patch.object(Observatory, "execute_and_monitor_device_task", side_effect=base),
        patch.object(SPECULOOS, "_wait_before_shutter_command", return_value=True),
    ):
        obs.execute_and_monitor_device_task(
            "Dome", "ShutterStatus", 1, "CloseShutter", device_name="dome"
        )
        obs.execute_and_monitor_device_task(
            "Telescope", "AtPark", True, "Park", device_name="tel"
        )

    assert held == [True, False]


def test_sequence_resumes_polls_after_an_exception(obs):
    with pytest.raises(RuntimeError), obs._sequence():
        assert obs._sequences_running == 1
        raise RuntimeError("boom")

    obs.device_manager.resume_polls.assert_called_once()
    assert obs._sequences_running == 0


@pytest.mark.parametrize(
    "close, reported", [(True, False), (False, True)], ids=["close", "open"]
)
def test_lost_link_during_ack_check(obs, close, reported):
    """In the close path a lost link only warns, so the close still runs."""
    set_telescope(obs, {"TELESCOPE.STATUS.LIST": "Client not Connected"})

    with patch.object(
        SPECULOOS, "config", new_callable=PropertyMock, return_value={"Telescope": []}
    ):
        obs.speculoos_check_and_ack_error(close=close)

    assert obs.logger.report_device_issue.called is reported
    assert obs.logger.warning.called is (not reported)
