"""Tests that keep the web server responsive while a schedule runs.

Covers:
    - `execute_and_monitor_device_task` with `schedule_sensitive` stops
      waiting when the schedule stops. Other tasks, such as the close
      sequence, do not stop. A schedule action cools the camera with
      `schedule_sensitive`.
    - `/api/close` closes only after the schedule thread has ended, also when
      a robotic switch request is still waiting for it, so no action can move
      the devices during the close.
    - The handlers that block are plain `def`, so FastAPI runs them in a
      worker thread, not in the event loop.
"""

import inspect
import threading
import time
import types
from threading import Event
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import astra.observatory as observatory_module
from astra import main
from astra.observatory import Observatory
from astra.scheduler import ScheduleManager
from astra.thread_manager import ThreadManager


def _monitor_observatory(running: bool):
    """Build a stand-in with what execute_and_monitor_device_task uses."""
    camera = MagicMock()
    camera.get.return_value = 20.0  # never reaches the set temperature
    obs = SimpleNamespace(
        logger=MagicMock(error_free=True),
        weather_safe=True,
        schedule_manager=SimpleNamespace(running=running),
        device_manager=SimpleNamespace(device_task_monitor_queue={"cam0": {}}),
        devices={"Camera": {"cam0": camera}},
    )
    return obs, camera


def _set_temperature(obs, timeout: float, schedule_sensitive: bool) -> None:
    Observatory.execute_and_monitor_device_task(
        obs,
        "Camera",
        "CCDTemperature",
        -10,
        "SetCCDTemperature",
        device_name="cam0",
        run_command_type="set",
        abs_tol=1,
        timeout=timeout,
        weather_sensitive=False,
        schedule_sensitive=schedule_sensitive,
    )


class TestScheduleSensitiveTask:
    def test_schedule_stop_ends_wait(self):
        obs, camera = _monitor_observatory(running=True)

        def stop():
            obs.schedule_manager.running = False

        threading.Timer(1, stop).start()
        start = time.monotonic()
        _set_temperature(obs, timeout=60, schedule_sensitive=True)

        assert time.monotonic() - start < 10
        camera.set.assert_called_once_with("SetCCDTemperature", -10)
        obs.logger.report_device_issue.assert_not_called()

    def test_other_tasks_ignore_schedule_stop(self):
        # For example the close sequence, which runs after a schedule stop
        obs, camera = _monitor_observatory(running=False)

        _set_temperature(obs, timeout=1, schedule_sensitive=False)

        camera.set.assert_called_once_with("SetCCDTemperature", -10)
        # It waited for the full timeout
        obs.logger.report_device_issue.assert_called_once()

    def test_schedule_action_cools_with_schedule_stop(self, monkeypatch):
        paired_devices = MagicMock()
        paired_devices.get_device_config.return_value = {"temperature": -10}
        monkeypatch.setattr(
            observatory_module,
            "PairedDevices",
            SimpleNamespace(from_observatory=lambda **kwargs: paired_devices),
        )
        obs = SimpleNamespace(
            devices={"Camera": {"cam0": MagicMock()}},
            config={"Camera": []},
            logger=MagicMock(error_free=True),
            cool_camera=MagicMock(),
            save_set_temperature=MagicMock(),
            check_conditions=lambda action: True,
            schedule_manager=SimpleNamespace(running=True),
            watchdog_running=True,
            weather_safe=True,
        )
        action = MagicMock(action_type="cool_camera", device_name="cam0")

        Observatory.run_action(obs, action)

        calls = obs.cool_camera.call_args_list
        assert len(calls) == 2
        for call in calls:
            assert call.kwargs["schedule_sensitive"] is True


@pytest.fixture
def running_schedule(tmp_path, monkeypatch):
    """A running schedule whose thread waits until the test releases it."""
    release = Event()
    events = []
    # Ends the thread even if the test fails, so a wait fails the test, not hangs it
    timer = threading.Timer(10, release.set)
    timer.start()

    def schedule_thread():
        release.wait()
        events.append("schedule ended")

    thread_manager = ThreadManager()
    thread_manager.start_thread(target=schedule_thread, thread_id="schedule")
    manager = ScheduleManager(tmp_path / "schedule.jsonl", None, MagicMock())
    manager.running = True
    obs = SimpleNamespace(
        logger=MagicMock(),
        schedule_manager=manager,
        thread_manager=thread_manager,
        close_observatory=lambda: events.append("close"),
        robotic_switch=True,
    )
    obs.toggle_robotic_switch = types.MethodType(Observatory.toggle_robotic_switch, obs)
    monkeypatch.setattr(main, "OBSERVATORY", obs)

    yield obs, release, events

    release.set()
    timer.cancel()


def _post_in_thread(path: str) -> threading.Thread:
    # Not used as a context manager, so the lifespan does not load devices.
    thread = threading.Thread(target=TestClient(main.app).post, args=(path,))
    thread.start()
    return thread


class TestCloseOrder:
    def test_close_waits_for_schedule_thread(self, running_schedule):
        _, release, events = running_schedule

        close = _post_in_thread("/api/close")
        time.sleep(0.5)
        assert events == []

        release.set()
        close.join(5)
        assert events == ["schedule ended", "close"]

    def test_close_waits_for_robotic_switch_off(self, running_schedule):
        obs, release, events = running_schedule

        switch_off = _post_in_thread("/api/roboticswitch")
        deadline = time.monotonic() + 5
        while obs.schedule_manager.running and time.monotonic() < deadline:
            time.sleep(0.05)
        # The schedule has stopped, but its thread still runs
        assert obs.schedule_manager.running is False

        close = _post_in_thread("/api/close")
        time.sleep(0.5)
        assert events == []

        release.set()
        switch_off.join(5)
        close.join(5)
        assert events == ["schedule ended", "close"]


@pytest.mark.parametrize(
    "handler",
    [
        main.roboticswitch,
        main.start_schedule,
        main.stop_schedule,
        main.schedule,
        main.validate_schedule,
        main.edit_schedule,
        main.polling,
        main.guiding_data,
        main.log,
    ],
)
def test_blocking_handlers_run_in_worker_thread(handler):
    assert not inspect.iscoroutinefunction(handler)
