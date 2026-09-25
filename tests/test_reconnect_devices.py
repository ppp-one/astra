"""Tests for reconnecting all devices without an Astra restart.

Covers:
    - `Observatory.reconnect_devices` stops the watchdog before it touches the
      devices, cancels while device tasks run, waits for the old devices'
      queued messages, clears the error state, and replaces the devices.
    - A cancelled reconnect leaves the watchdog running as before, and does
      not log an error (an error would make the watchdog close the observatory).
    - `Observatory.stop_watchdog` also stops a watchdog thread that has just
      started and sets its own flag.
    - `/api/reconnect_devices` refuses while a schedule, a reconnect or a task
      that uses the devices runs, and starts the reconnect in a background
      thread. Starting the watchdog is refused while the reconnect runs.
    - With the Alpaca simulators, the reconnect gives new, live device
      processes that poll again.
"""

import time
from datetime import UTC, datetime
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import astra.observatory as observatory_module
from astra.observatory import Observatory
from astra.thread_manager import ThreadManager


def _fake_observatory(
    monkeypatch,
    watchdog_stops=True,
    watchdog_alive=True,
    busy=(),
    flushed=True,
    load=None,
    connect=None,
):
    """Build a stand-in for Observatory that records the reconnect steps."""
    calls = []
    obs = SimpleNamespace()

    obs.logger = MagicMock()
    obs.logger.error_free = True
    obs.logger.error_source = [{"device_type": "Camera", "device_name": "cam0"}]

    def stop_watchdog(timeout):
        calls.append("stop_watchdog")
        obs.watchdog_running = False
        return watchdog_stops

    def flush(timeout):
        calls.append("flush")
        return flushed

    def load_devices():
        calls.append(("load_devices", obs.logger.error_free))
        if load is not None:
            load(obs)

    def connect_all_devices():
        calls.append("connect_all_devices")
        if connect is not None:
            connect(obs)

    obs.watchdog_running = watchdog_alive
    obs.stop_watchdog = stop_watchdog
    obs.start_watchdog = MagicMock()
    obs.device_tasks_running = lambda: list(busy)
    obs.connect_all_devices = connect_all_devices
    obs.thread_manager = SimpleNamespace(
        is_thread_running=lambda thread_id: thread_id == "watchdog" and watchdog_alive
    )
    obs.schedule_manager = SimpleNamespace(running=False)
    obs.queue_manager = SimpleNamespace(flush=flush)
    obs.device_manager = SimpleNamespace(
        stop_all_devices=lambda: calls.append("stop_all_devices"),
        load_devices=load_devices,
    )
    obs.robotic_switch = True
    obs.guider_manager = "old guider manager"
    obs._observatory_locations = {"T1": "location"}
    obs._equatorial_systems = {"T1": "system"}
    obs._rate_offset_support = {"T1": True}

    def from_observatory(observatory):
        calls.append("guider_manager")
        return "new guider manager"

    monkeypatch.setattr(
        observatory_module,
        "GuiderManager",
        SimpleNamespace(from_observatory=from_observatory),
    )

    return obs, calls


class TestReconnectDevices:
    def test_replaces_devices_in_order(self, monkeypatch):
        obs, calls = _fake_observatory(monkeypatch)
        obs.logger.error_free = False

        assert Observatory.reconnect_devices(obs) is True

        # The old messages are processed and the error state is clear before
        # the new devices load, so only new errors stop the new watchdog.
        assert calls == [
            "stop_watchdog",
            "stop_all_devices",
            "flush",
            ("load_devices", True),
            "connect_all_devices",
            "guider_manager",
        ]
        assert obs.logger.error_source == []
        assert obs.robotic_switch is False
        assert obs.guider_manager == "new guider manager"
        assert obs._observatory_locations == {}
        assert obs._equatorial_systems == {}
        assert obs._rate_offset_support == {}

    def test_watchdog_that_does_not_stop_keeps_running(self, monkeypatch):
        obs, calls = _fake_observatory(monkeypatch, watchdog_stops=False)

        assert Observatory.reconnect_devices(obs) is False

        assert calls == ["stop_watchdog"]
        assert obs.watchdog_running is True
        assert obs.logger.error_free is True
        obs.logger.error.assert_not_called()
        obs.logger.warning.assert_called_once()

    def test_cancelled_while_device_tasks_run(self, monkeypatch):
        obs, calls = _fake_observatory(monkeypatch, busy=["guider", "object"])

        assert Observatory.reconnect_devices(obs) is False

        assert calls == ["stop_watchdog"]
        obs.start_watchdog.assert_called_once()
        obs.logger.error.assert_not_called()
        assert "guider, object" in obs.logger.warning.call_args.args[0]

    def test_cancel_does_not_restart_a_watchdog_that_was_stopped(self, monkeypatch):
        obs, calls = _fake_observatory(
            monkeypatch, watchdog_alive=False, busy=["guider"]
        )

        assert Observatory.reconnect_devices(obs) is False

        obs.start_watchdog.assert_not_called()

    def test_cancel_does_not_restart_a_watchdog_that_handles_errors(self, monkeypatch):
        obs, calls = _fake_observatory(monkeypatch, busy=["guider"])
        obs.logger.error_free = False

        assert Observatory.reconnect_devices(obs) is False

        obs.start_watchdog.assert_not_called()

    def test_continues_when_flush_times_out(self, monkeypatch):
        obs, calls = _fake_observatory(monkeypatch, flushed=False)

        assert Observatory.reconnect_devices(obs) is True

        assert "connect_all_devices" in calls
        obs.logger.warning.assert_called_once()

    def test_returns_false_when_load_fails(self, monkeypatch):
        def load(obs):
            raise ValueError("bad config file")

        obs, calls = _fake_observatory(monkeypatch, load=load)

        assert Observatory.reconnect_devices(obs) is False

        assert "connect_all_devices" not in calls
        obs.logger.error.assert_called_once()

    def test_returns_false_when_connect_fails(self, monkeypatch):
        def connect(obs):
            raise Exception("Some devices failed to connect.")

        obs, calls = _fake_observatory(monkeypatch, connect=connect)

        assert Observatory.reconnect_devices(obs) is False

        assert calls[-1] == "connect_all_devices"
        obs.logger.error.assert_called_once()

    def test_returns_false_when_a_device_reports_an_error(self, monkeypatch):
        def connect(obs):
            obs.logger.error_free = False

        obs, calls = _fake_observatory(monkeypatch, connect=connect)

        assert Observatory.reconnect_devices(obs) is False

        assert calls[-1] == "guider_manager"
        obs.logger.warning.assert_called_once()


class TestStopWatchdog:
    def test_stops_a_watchdog_that_sets_its_flag_after_the_stop(self):
        obs = SimpleNamespace(watchdog_running=False, thread_manager=ThreadManager())
        go = Event()

        def watchdog():
            # Like Observatory.watchdog: the thread sets the flag when it starts.
            go.wait()
            obs.watchdog_running = True
            while obs.watchdog_running:
                time.sleep(0.05)

        obs.thread_manager.start_thread(target=watchdog, thread_id="watchdog")
        Thread(target=lambda: (time.sleep(0.2), go.set())).start()

        assert Observatory.stop_watchdog(obs, timeout=5) is True
        assert obs.watchdog_running is False

    def test_returns_false_on_timeout(self):
        obs = SimpleNamespace(watchdog_running=True, thread_manager=ThreadManager())
        release = Event()
        obs.thread_manager.start_thread(target=release.wait, thread_id="watchdog")

        assert Observatory.stop_watchdog(obs, timeout=0.3) is False

        release.set()


class TestDeviceTasksRunning:
    def test_lists_live_threads_that_can_use_devices(self):
        obs = SimpleNamespace(thread_manager=ThreadManager())
        release = Event()
        for thread_id, thread_type in [
            ("queue", "queue"),
            ("watchdog", "watchdog"),
            ("backup", "Backup"),
            ("complete_headers", "Headers"),
            ("reconnect_devices", "Reconnect"),
            ("guider", "guider"),
            (3, "object"),
        ]:
            obs.thread_manager.start_thread(
                target=release.wait, thread_id=thread_id, thread_type=thread_type
            )
        obs.thread_manager.start_thread(target=lambda: None, thread_type="flats")

        try:
            assert Observatory.device_tasks_running(obs) == ["guider", "object"]
        finally:
            release.set()


class TestReconnectEndpoint:
    @pytest.fixture
    def observatory(self, monkeypatch):
        import astra.main as main

        running = set()
        obs = SimpleNamespace(
            logger=MagicMock(),
            schedule_manager=SimpleNamespace(running=False),
            thread_manager=MagicMock(),
            running=running,
            busy=[],
            start_watchdog=MagicMock(),
        )
        obs.thread_manager.is_thread_running.side_effect = (
            lambda thread_id: thread_id in running
        )
        obs.device_tasks_running = lambda: list(obs.busy)
        monkeypatch.setattr(main, "OBSERVATORY", obs)
        return obs

    @pytest.fixture
    def client(self):
        from astra.main import app

        # Not used as a context manager, so the lifespan does not load devices.
        return TestClient(app)

    def test_starts_reconnect_thread(self, observatory, client):
        from astra.main import reconnect_devices_task

        response = client.post("/api/reconnect_devices").json()

        assert response["status"] == "success"
        observatory.thread_manager.start_thread.assert_called_once()
        kwargs = observatory.thread_manager.start_thread.call_args.kwargs
        assert kwargs["target"] is reconnect_devices_task
        assert kwargs["thread_id"] == "reconnect_devices"

    def test_refused_while_schedule_flag_is_set(self, observatory, client):
        observatory.schedule_manager.running = True

        response = client.post("/api/reconnect_devices").json()

        assert response["status"] == "error"
        assert "schedule" in response["message"]
        observatory.thread_manager.start_thread.assert_not_called()

    @pytest.mark.parametrize(
        ("thread_id", "word"),
        [
            ("schedule", "schedule"),
            ("reconnect_devices", "already in progress"),
        ],
    )
    def test_refused_while_thread_runs(self, observatory, client, thread_id, word):
        observatory.running.add(thread_id)

        response = client.post("/api/reconnect_devices").json()

        assert response["status"] == "error"
        assert word in response["message"]
        observatory.thread_manager.start_thread.assert_not_called()

    def test_refused_while_device_tasks_run(self, observatory, client):
        observatory.busy = ["guider", "object"]

        response = client.post("/api/reconnect_devices").json()

        assert response["status"] == "error"
        assert "guider, object" in response["message"]
        observatory.thread_manager.start_thread.assert_not_called()

    def test_start_watchdog_refused_during_reconnect(self, observatory, client):
        observatory.running.add("reconnect_devices")

        response = client.post("/api/startwatchdog").json()

        assert response["status"] == "error"
        observatory.start_watchdog.assert_not_called()

    def test_start_watchdog_still_works(self, observatory, client):
        observatory.logger.error_free = False

        response = client.post("/api/startwatchdog").json()

        assert response["status"] == "success"
        assert observatory.logger.error_free is True
        observatory.start_watchdog.assert_called_once()


class TestReconnectDevicesTask:
    def _observatory(self, monkeypatch, replace_devices):
        import astra.main as main

        obs = SimpleNamespace(devices={"FilterWheel": {}}, logger=MagicMock())

        def reconnect_devices():
            if replace_devices:
                obs.devices = {"FilterWheel": {}}
            return replace_devices

        obs.reconnect_devices = reconnect_devices
        monkeypatch.setattr(main, "OBSERVATORY", obs)
        update = MagicMock()
        monkeypatch.setattr(main, "update_filter_wheels", update)
        return main, update

    def test_reads_filter_names_of_new_devices(self, monkeypatch):
        main, update = self._observatory(monkeypatch, replace_devices=True)

        main.reconnect_devices_task()

        update.assert_called_once()

    def test_cancelled_reconnect_does_not_call_old_devices(self, monkeypatch):
        main, update = self._observatory(monkeypatch, replace_devices=False)

        main.reconnect_devices_task()

        update.assert_not_called()


class TestUpdateFilterWheels:
    def test_failed_read_keeps_previous_names(self, monkeypatch):
        import astra.main as main

        def names(_):
            raise OSError("handle is closed")

        obs = SimpleNamespace(
            devices={
                "FilterWheel": {
                    "fw1": SimpleNamespace(get=lambda _: ["R", "G"]),
                    "fw2": SimpleNamespace(get=names),
                }
            },
            logger=MagicMock(),
        )
        monkeypatch.setattr(main, "FWS", {"fw1": ["old"], "fw2": ["old2"]})

        with pytest.raises(OSError):
            main.update_filter_wheels(obs)

        assert main.FWS == {"fw1": ["old"], "fw2": ["old2"]}

    def test_reads_all_names(self, monkeypatch):
        import astra.main as main

        obs = SimpleNamespace(
            devices={"FilterWheel": {"fw1": SimpleNamespace(get=lambda _: ["R"])}},
            logger=MagicMock(),
        )
        monkeypatch.setattr(main, "FWS", {})

        main.update_filter_wheels(obs)

        assert main.FWS == {"fw1": ["R"]}


@pytest.mark.slow
def test_reconnect_with_simulators(server_url, observatory):
    old_devices = [
        d for by_name in observatory.devices.values() for d in by_name.values()
    ]
    started = datetime.now(UTC)

    assert observatory.reconnect_devices() is True

    new_devices = [
        d for by_name in observatory.devices.values() for d in by_name.values()
    ]
    assert len(new_devices) == len(old_devices)
    assert not {id(d) for d in old_devices} & {id(d) for d in new_devices}
    assert all(not d.is_alive() for d in old_devices)
    assert all(d.is_alive() for d in new_devices)

    # The watchdog thread sets its flag when it starts.
    deadline = time.time() + 10
    while not observatory.watchdog_running and time.time() < deadline:
        time.sleep(0.2)
    assert observatory.watchdog_running

    # The new processes poll again.
    camera = next(iter(observatory.devices["Camera"].values()))
    fresh = False
    deadline = time.time() + 15
    while not fresh and time.time() < deadline:
        latest = camera.poll_latest() or {}
        fresh = any(
            v.get("datetime") is not None and v["datetime"] > started
            for v in latest.values()
        )
        time.sleep(0.5)
    assert fresh
