import pytest

from astra.paired_devices import PairedDevices


class DummyDevice:
    def __init__(self, name):
        self.name = name


def make_dummy_devices(paired_device_names):
    # Create a dummy devices dict for all device types
    devices = {}
    for dtype, device_name in paired_device_names.items():
        if dtype == "Camera":
            devices.setdefault("Camera", {})[device_name] = DummyDevice(device_name)
        elif dtype == "Dome":
            devices.setdefault("Dome", {})[device_name] = DummyDevice(device_name)
        elif dtype == "Focuser":
            devices.setdefault("Focuser", {})[device_name] = DummyDevice(device_name)
        elif dtype == "Telescope":
            devices.setdefault("Telescope", {})[device_name] = DummyDevice(device_name)
            devices.setdefault("Guider", {})[device_name] = DummyDevice(
                f"Guider_{device_name}"
            )
        elif dtype == "FilterWheel":
            devices.setdefault("FilterWheel", {})[device_name] = DummyDevice(
                device_name
            )
        elif dtype == "Rotator":
            devices.setdefault("Rotator", {})[device_name] = DummyDevice(device_name)
    return devices


@pytest.fixture
def dummy_observatory_config():
    # Minimal config for testing
    return {
        "Camera": [
            {
                "device_name": "TestCam",
                "paired_devices": {
                    "Camera": "TestCam",
                    "Dome": "TestDome",
                    "Focuser": "TestFocuser",
                    "Telescope": "TestScope",
                    "FilterWheel": "TestFW",
                    "Rotator": "TestRotator",
                },
            }
        ],
        "Dome": [{"device_name": "TestDome", "other": 1}],
        "Focuser": [{"device_name": "TestFocuser", "other": 2}],
        "Telescope": [{"device_name": "TestScope", "other": 3}],
        "FilterWheel": [{"device_name": "TestFW", "other": 4}],
        "Rotator": [{"device_name": "TestRotator", "other": 5}],
    }


@pytest.fixture
def paired_device_names(dummy_observatory_config):
    return dummy_observatory_config["Camera"][0]["paired_devices"].copy()


@pytest.fixture
def dummy_devices(paired_device_names):
    return make_dummy_devices(paired_device_names)


@pytest.fixture
def paired_devices(dummy_devices, dummy_observatory_config, paired_device_names):
    return PairedDevices(
        paired_device_names=paired_device_names,
        devices=dummy_devices,
        observatory_config=dummy_observatory_config,
        camera_name="TestCam",
    )


def test_camera_name_property(paired_devices):
    assert paired_devices.camera_name == "TestCam"


def test_get_device_config(paired_devices):
    config = paired_devices.get_device_config("Dome")
    assert config["device_name"] == "TestDome"


def test_get_device(paired_devices):
    device = paired_devices.get_device("Camera")
    assert device.name == "TestCam"
    assert paired_devices.get_device("Dome").name == "TestDome"


def test_camera_property(paired_devices):
    assert paired_devices.camera.name == "TestCam"


def test_dome_property(paired_devices):
    assert paired_devices.dome.name == "TestDome"


def test_focuser_property(paired_devices):
    assert paired_devices.focuser.name == "TestFocuser"


def test_telescope_property(paired_devices):
    assert paired_devices.telescope.name == "TestScope"


def test_guider_property(paired_devices):
    assert paired_devices.guider.name == "Guider_TestScope"


def test_filter_wheel_property(paired_devices):
    assert paired_devices.filter_wheel.name == "TestFW"


def test_rotator_property(paired_devices):
    assert paired_devices.rotator.name == "TestRotator"


def test_repr(paired_devices):
    r = repr(paired_devices)
    assert r.startswith("PairedDevices(")


def test_missing_device_raises():
    pd = PairedDevices(
        {"Camera": "TestCam"}, devices={"Camera": {"TestCam": DummyDevice("TestCam")}}  # type: ignore
    )
    with pytest.raises(ValueError):
        _ = pd.dome
    with pytest.raises(KeyError):
        _ = pd["Dome"]


def test_from_camera_name(dummy_devices, dummy_observatory_config):
    pd = PairedDevices.from_camera_name(
        camera_name="TestCam",
        devices=dummy_devices,
        observatory_config=dummy_observatory_config,
    )
    assert pd["Camera"] == "TestCam"
    assert pd["Dome"] == "TestDome"


def test_from_observatory(
    dummy_devices, dummy_observatory_config, paired_device_names
):
    class DummyObservatory:
        devices = dummy_devices
        config = dummy_observatory_config

    obs = DummyObservatory()
    pd = PairedDevices.from_observatory(obs, camera_name="TestCam")
    assert pd["Camera"] == "TestCam"
    pd2 = PairedDevices.from_observatory(obs, paired_device_names=paired_device_names)
    assert pd2["Dome"] == "TestDome"
