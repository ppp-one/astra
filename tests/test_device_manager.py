from dataclasses import dataclass
from typing import Iterable, List

import pytest

from astra.device_manager import DeviceManager
from astra.logger import ObservatoryLogger


class FakeAlpacaDevice:
    """Minimal fake replacement for AlpacaDevice used in tests."""

    def __init__(
        self,
        ip,
        device_type,
        device_number,
        device_name,
        queue,
        debug,
        connectable=True,
    ):
        self.ip = ip
        self.device_type = device_type
        self.device_number = device_number
        self.device_name = device_name
        self.queue = queue
        self.debug = debug
        self.connectable = connectable
        self.polls = {}
        self.started = False
        self.stopped = False
        self._alive = True

    def start(self):
        self.started = True

    def set(self, name, value):
        setattr(self, name, value)

    def start_poll(self, command, delay):
        self.polls[command] = delay

    def pause_polls(self):
        self.polls_paused = True

    def resume_polls(self):
        self.polls_paused = False

    def stop(self):
        self.stopped = True

    def is_alive(self):
        return self._alive

    def force_poll(self, command):
        self.last_forced = command


class DummyQueueManager:
    def __init__(self):
        class Q:
            def put(self, *a, **k):
                pass

        self.queue = Q()


class DummyThreadManager:
    pass


@dataclass
class Row:
    device_type: str
    device_command: str
    fixed: bool = False


class FitsConfig:
    def __init__(self, rows: Iterable[Row]):
        self._rows: List[Row] = list(rows)

    def iterrows(self):
        for r in self._rows:
            yield (
                None,
                {
                    "device_type": r.device_type,
                    "device_command": r.device_command,
                    "fixed": r.fixed,
                },
            )


@pytest.fixture
def device_manager(monkeypatch, observatory_config):
    """Fixture that populates the real observatory_config, patches AlpacaDevice,
    and returns an initialized DeviceManager (not yet connected)."""
    # patch AlpacaDevice used in DeviceManager
    import astra.device_manager as dm

    monkeypatch.setattr(dm, "AlpacaDevice", FakeAlpacaDevice)

    # populate config
    observatory_config.clear()
    observatory_config.update(
        {
            "Camera": [
                {
                    "ip": "127.0.0.1",
                    "device_number": 0,
                    "device_name": "cam0",
                    "polling_interval": 2,
                }
            ],
            "ObservingConditions": [
                {
                    "ip": "127.0.0.2",
                    "device_number": 0,
                    "device_name": "oc0",
                    "polling_interval": 3,
                }
            ],
            "SafetyMonitor": [
                {"ip": "127.0.0.3", "device_number": 0, "device_name": "s0"}
            ],
        }
    )

    logger = ObservatoryLogger("test")
    qm = DummyQueueManager()
    tm = DummyThreadManager()
    dmgr = DeviceManager(observatory_config, logger, qm, tm)
    return dmgr


class TestDeviceManager:
    def test_load_and_list_names(self, device_manager):
        device_manager.load_devices()

        names = device_manager.list_device_names("Camera")
        assert names == ["cam0"]
        names_oc = device_manager.list_device_names("ObservingConditions")
        assert names_oc == ["oc0"]

    def test_connect_all_and_polling(self, device_manager):
        fits = FitsConfig(
            [
                Row("Camera", "Temperature", False),
                Row("ObservingConditions", "Humidity", False),
                Row("SafetyMonitor", "IsSafe", False),
            ]
        )

        device_manager.load_devices()
        device_manager.connect_all(fits)

        cam = device_manager.devices["Camera"]["cam0"]
        assert cam.started
        assert cam.polls.get("Temperature") == 2

        oc = device_manager.devices["ObservingConditions"]["oc0"]
        assert oc.polls.get("Humidity") == 3

        s = device_manager.devices["SafetyMonitor"]["s0"]
        assert s.polls.get("IsSafe") == 1

    def test_pause_resume_stop_and_health(self, device_manager):
        device_manager.load_devices()

        device_manager.pause_polls()
        for t in device_manager.devices:
            for d in device_manager.devices[t].values():
                assert getattr(d, "polls_paused", True) is True

        device_manager.resume_polls()
        for t in device_manager.devices:
            for d in device_manager.devices[t].values():
                assert getattr(d, "polls_paused", False) is False

        assert device_manager.check_devices_alive() is True
        device_manager.devices["Camera"]["cam0"]._alive = False
        assert device_manager.check_devices_alive() is False

        device_manager.stop_all_devices()
        for t in device_manager.devices:
            for d in device_manager.devices[t].values():
                assert d.stopped is True

    def test_force_poll_observing_conditions(self, device_manager):
        device_manager.load_devices()

        fits = FitsConfig([Row("ObservingConditions", "Humidity", False)])
        device_manager.force_poll_observing_conditions(fits)
        oc = device_manager.devices["ObservingConditions"]["oc0"]
        assert getattr(oc, "last_forced", None) == "Humidity"
