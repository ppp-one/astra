"""Tests for the set temperature of the ``cool_camera`` action.

Covers:
    - `CoolCameraActionConfig` takes an optional number for ``temperature``.
    - `Observatory.save_set_temperature` writes a new value to the observatory
      config file and leaves the file alone when nothing changes.
    - `Observatory.run_action` saves the value before it cools, so all the
      cooling in the action already goes to the new temperature.
"""

import re
import shutil
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import astra.observatory as observatory_module
from astra.action_configs import CoolCameraActionConfig, action_value_schema
from astra.config import Config, ObservatoryConfig
from astra.observatory import Observatory


class TestCoolCameraActionConfig:
    def test_temperature_is_optional(self):
        assert CoolCameraActionConfig.from_dict({}).temperature is None

    @pytest.mark.parametrize("value", [-10, -12.5, 5])
    def test_takes_a_number(self, value):
        assert CoolCameraActionConfig.from_dict({"temperature": value}).temperature == (
            value
        )

    @pytest.mark.parametrize("value", ["-10", True, [-10]])
    def test_rejects_a_value_that_is_not_a_number(self, value):
        with pytest.raises(TypeError, match="temperature"):
            CoolCameraActionConfig.from_dict({"temperature": value})

    def test_schedule_editor_shows_a_number_field(self):
        (entry,) = action_value_schema(CoolCameraActionConfig)
        assert entry["name"] == "temperature"
        assert entry["type"] == "number"
        assert entry["required"] is False
        assert entry["default"] is None


@pytest.fixture
def observatory_config(tmp_path):
    """A real observatory config file, made from the template."""
    shutil.copy(
        Config.TEMPLATE_DIR / "observatory_config.yml", tmp_path / "obs_config.yml"
    )
    return ObservatoryConfig(tmp_path, "obs")


def _paired_devices(observatory_config):
    camera_config = observatory_config["Camera"][0]
    return SimpleNamespace(
        observatory_config=observatory_config,
        get_device_config=lambda device_type: camera_config,
    )


def _observatory():
    obs = SimpleNamespace(logger=MagicMock())
    obs.save_set_temperature = types.MethodType(Observatory.save_set_temperature, obs)
    return obs


class TestSaveSetTemperature:
    def test_writes_the_new_value_to_the_file(self, observatory_config):
        _observatory().save_set_temperature(
            "cam0", -10, _paired_devices(observatory_config)
        )

        assert observatory_config["Camera"][0]["temperature"] == -10
        reloaded = ObservatoryConfig(
            observatory_config.config_path, observatory_config.observatory_name
        )
        assert reloaded["Camera"][0]["temperature"] == -10
        # The comment next to the value is kept, ruamel may change its spacing
        assert re.search(
            r"temperature: -10\s+# degrees Celsius",
            observatory_config.file_path.read_text(),
        )

    @pytest.mark.parametrize("value", [None, -20])
    def test_leaves_the_file_alone_when_nothing_changes(
        self, observatory_config, value
    ):
        # The template already sets -20
        observatory_config.save = MagicMock()

        _observatory().save_set_temperature(
            "cam0", value, _paired_devices(observatory_config)
        )

        observatory_config.save.assert_not_called()
        assert observatory_config["Camera"][0]["temperature"] == -20


def test_run_action_cools_to_the_new_temperature(observatory_config, monkeypatch):
    observatory_config.save = MagicMock()
    paired_devices = _paired_devices(observatory_config)
    monkeypatch.setattr(
        observatory_module,
        "PairedDevices",
        SimpleNamespace(from_observatory=lambda **kwargs: paired_devices),
    )
    obs = _observatory()
    obs.devices = {"Camera": {"cam0": MagicMock()}}
    obs.config = {"Camera": []}
    obs.logger.error_free = True
    obs.cool_camera = MagicMock()
    obs.check_conditions = lambda action: True
    obs.schedule_manager = SimpleNamespace(running=True)
    obs.watchdog_running = True
    obs.weather_safe = True
    action = MagicMock(
        action_type="cool_camera",
        device_name="cam0",
        action_value=CoolCameraActionConfig(temperature=-10),
    )

    Observatory.run_action(obs, action)

    observatory_config.save.assert_called_once()
    calls = obs.cool_camera.call_args_list
    assert len(calls) == 2
    for call in calls:
        assert call.args[1] == -10
