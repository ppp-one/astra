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
