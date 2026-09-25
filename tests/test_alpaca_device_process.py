import pytest

from astra.alpaca_device_process import AlpacaDevice


class DummyQueue:
    def put(self, *args, **kwargs):
        pass


class DummyDevice:
    def __init__(self):
        self.called = False

    def AbortExposure(self):
        self.called = True
        return "ok"


def test_get_executes_callable_without_kwargs():
    device = AlpacaDevice(
        ip="127.0.0.1",
        device_type="Camera",
        device_number=0,
        device_name="cam0",
        queue=DummyQueue(),
        debug=False,
    )
    device.device = DummyDevice()

    result = device.get__("AbortExposure", pipe=False)

    assert result["status"] == "success"
    assert result["data"] == "ok"
    assert device.device.called is True


@pytest.fixture
def start_device():
    """Start real device processes; kill any that are still alive at teardown.

    A live child process blocks pytest at exit, so a failed test must not
    leave one behind. No Alpaca server is needed: run() only creates the client.
    """
    devices = []

    def _start(name: str) -> AlpacaDevice:
        device = AlpacaDevice(
            ip="127.0.0.1",
            device_type="Camera",
            device_number=0,
            device_name=name,
            queue=DummyQueue(),
            debug=False,
        )
        device.start()
        devices.append(device)
        return device

    yield _start

    for device in devices:
        if device.is_alive():
            device.kill()
            device.join(5)


def test_stop_ends_process(start_device):
    device = start_device("cam_stop")

    device.stop(timeout=5)

    assert not device.is_alive()
    assert device.exitcode == 0
    assert device.front_pipe.closed
    assert device.back_pipe.closed


def test_stop_kills_process_when_lock_is_held(start_device):
    device = start_device("cam_hung")

    # A hung get() holds the lock; stop() must not wait for it forever.
    device.lock.acquire()
    try:
        device.stop(timeout=0.5)
    finally:
        device.lock.release()

    assert not device.is_alive()
    # The lock holder may still read front_pipe, so only back_pipe is closed.
    assert device.back_pipe.closed
    assert not device.front_pipe.closed


def test_poll_latest_after_stop_returns_cached_values(start_device):
    device = start_device("cam_cache")
    cached = {"CCDTemperature": {"value": -10.0, "datetime": None}}
    device._cached_poll_latest = cached

    device.stop(timeout=5)

    assert device.poll_latest() == cached


def test_poll_latest_after_stop_without_cache_returns_none(start_device):
    device = start_device("cam_nocache")

    device.stop(timeout=5)

    assert device.poll_latest() is None
