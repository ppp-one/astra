"""Tests for the readout_mode and gain fields of imaging actions."""

from unittest.mock import MagicMock

import pytest

from astra.action_configs import (
    AutofocusConfig,
    CalibrateGuidingActionConfig,
    CalibrationActionConfig,
    FlatsActionConfig,
    ObjectActionConfig,
    PointingModelActionConfig,
    action_value_schema,
)
from astra.observatory import Observatory


def _object(**kwargs):
    return ObjectActionConfig(object="Test", exptime=10.0, **kwargs)


IMAGING_CONFIGS = [
    lambda **kw: _object(**kw),
    lambda **kw: CalibrationActionConfig(exptime=[0.0], n=[1], **kw),
    lambda **kw: FlatsActionConfig(filter=["V"], n=[1], **kw),
    lambda **kw: CalibrateGuidingActionConfig(**kw),
    lambda **kw: PointingModelActionConfig(**kw),
    lambda **kw: AutofocusConfig(**kw),
]


class MockCamera:
    """Camera in Gains list mode, with two readout modes."""

    def __init__(self, gains=("Low", "Medium", "High")):
        self.properties = {
            "BinX": 1,
            "BinY": 1,
            "CameraXSize": 2048,
            "CameraYSize": 2048,
            "StartX": 0,
            "StartY": 0,
            "NumX": 2048,
            "NumY": 2048,
            "ReadoutModes": ["Normal", "Fast"],
            "ReadoutMode": 0,
            "Gain": 0,
        }
        if gains is not None:
            self.properties["Gains"] = list(gains)
        self.set_calls = []

    def get(self, prop):
        if prop not in self.properties:
            raise RuntimeError(f"{prop} not implemented")
        return self.properties[prop]

    def set(self, prop, value):
        self.set_calls.append((prop, value))
        self.properties[prop] = value


@pytest.fixture
def observatory():
    obs = MagicMock(spec=Observatory)
    obs.logger = MagicMock()
    obs._configure_camera = Observatory._configure_camera.__get__(obs)
    return obs


def _paired(camera):
    paired = MagicMock()
    paired.__contains__ = lambda self, key: key == "Camera"
    paired.__getitem__ = lambda self, key: "TestCamera"
    paired.camera = camera
    return paired


class TestConfig:
    @pytest.mark.parametrize("make", IMAGING_CONFIGS)
    def test_defaults_leave_camera_unchanged(self, make):
        config = make()
        assert config.readout_mode is None
        assert config.gain is None

    @pytest.mark.parametrize("make", IMAGING_CONFIGS)
    def test_accepts_names_and_numbers(self, make):
        config = make(readout_mode="Fast", gain=100)
        assert config.readout_mode == "Fast"
        assert config.gain == 100

    @pytest.mark.parametrize("field", ["readout_mode", "gain"])
    @pytest.mark.parametrize("value", [True, 1.5, ["Fast"]])
    def test_rejects_wrong_types(self, field, value):
        with pytest.raises(TypeError, match=field):
            _object(**{field: value})

    def test_accepts_negative_gain(self):
        # In ASCOM gains-value mode, GainMin can be below zero.
        assert _object(gain=-5).gain == -5

    def test_from_dict_reads_fields(self):
        config = ObjectActionConfig.from_dict(
            {"object": "Test", "exptime": 1.0, "readout_mode": 1, "gain": "High"}
        )
        assert config.readout_mode == 1
        assert config.gain == "High"

    def test_schema_describes_fields(self):
        entries = {e["name"]: e for e in action_value_schema(ObjectActionConfig)}
        assert entries["readout_mode"]["default"] is None
        assert "ReadoutModes" in entries["readout_mode"]["description"]
        assert "Gains" in entries["gain"]["description"]
        assert entries["readout_mode"]["example"] == "Fast"
        assert entries["gain"]["example"] == "High"


class TestConfigureCamera:
    def test_sets_readout_mode_by_name(self, observatory):
        camera = MockCamera()
        observatory._configure_camera(_paired(camera), _object(readout_mode="Fast"))
        assert camera.properties["ReadoutMode"] == 1

    def test_name_match_is_exact(self, observatory):
        camera = MockCamera()
        with pytest.raises(ValueError, match="'fast' is not in"):
            observatory._configure_camera(_paired(camera), _object(readout_mode="fast"))

    def test_sets_readout_mode_by_index(self, observatory):
        camera = MockCamera()
        observatory._configure_camera(_paired(camera), _object(readout_mode=1))
        assert camera.properties["ReadoutMode"] == 1

    def test_sets_gain_by_name(self, observatory):
        camera = MockCamera()
        observatory._configure_camera(_paired(camera), _object(gain="High"))
        assert camera.properties["Gain"] == 2

    def test_sets_gain_value_without_gains_list(self, observatory):
        camera = MockCamera(gains=None)
        observatory._configure_camera(_paired(camera), _object(gain=139))
        assert camera.properties["Gain"] == 139

    def test_unknown_name_raises(self, observatory):
        camera = MockCamera()
        with pytest.raises(
            ValueError, match="'Turbo' is not in the camera's ReadoutModes"
        ):
            observatory._configure_camera(
                _paired(camera), _object(readout_mode="Turbo")
            )
        assert camera.set_calls == []

    def test_gain_name_in_gains_value_mode_raises(self, observatory):
        # In gains-value mode, Gains raises NotImplementedException.
        camera = MockCamera(gains=None)
        with pytest.raises(RuntimeError, match="Gains not implemented"):
            observatory._configure_camera(_paired(camera), _object(gain="High"))
        assert camera.set_calls == []

    def test_readout_mode_is_set_before_gain(self, observatory):
        camera = MockCamera()
        observatory._configure_camera(
            _paired(camera), _object(readout_mode="Fast", gain="High")
        )
        assert camera.set_calls[:2] == [("ReadoutMode", 1), ("Gain", 2)]

    def test_leaves_camera_alone_when_not_given(self, observatory):
        camera = MockCamera()
        camera.properties["ReadoutMode"] = 1
        camera.properties["Gain"] = 2
        observatory._configure_camera(_paired(camera), _object())
        assert camera.set_calls == []

    def test_no_set_when_already_in_mode(self, observatory):
        camera = MockCamera()
        camera.properties["ReadoutMode"] = 1
        camera.properties["Gain"] = 2
        observatory._configure_camera(
            _paired(camera), _object(readout_mode="Fast", gain="High")
        )
        assert camera.set_calls == []
