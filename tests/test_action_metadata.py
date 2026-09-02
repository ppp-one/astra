"""Unit tests for arbitrary user metadata passthrough on schedule actions.

Covers:
    - `metadata` dict survives ObjectActionConfig/CalibrationActionConfig
      construction and `from_dict()` parsing, while unrelated unknown keys
      are still dropped (existing, intentional behavior).
    - The `dict[str, Any]` type-validation bug that previously crashed on
      any non-empty `metadata` dict (`typing.Any` is not a valid isinstance
      argument) is fixed.
    - `HeaderManager.get_base_header` correctly resolves
      `device_type=action_metadata` FITS header rows from that dict,
      including dtype casting and the "key missing" warning path.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from astra.action_configs import CalibrationActionConfig, ObjectActionConfig
from astra.header_manager import HeaderManager


def make_fits_config(rows: list[dict]) -> pd.DataFrame:
    """Helper to build a fits_config DataFrame indexed by header keyword."""
    index = [r["header"] for r in rows]
    return pd.DataFrame(rows, index=index)


class TestObjectActionConfigMetadata:
    def test_metadata_defaults_to_empty_dict(self):
        config = ObjectActionConfig(object="M42", exptime=30.0)
        assert config.metadata == {}

    def test_metadata_dict_is_preserved_via_constructor(self):
        config = ObjectActionConfig(
            object="M42",
            exptime=30.0,
            metadata={"ID": 47026, "USER": "Foobar", "TRRATE": "sidereal"},
        )
        assert config.metadata == {"ID": 47026, "USER": "Foobar", "TRRATE": "sidereal"}

    def test_metadata_dict_is_preserved_via_from_dict(self):
        action_value = {
            "object": "M42",
            "exptime": 30.0,
            "ra": 83.82208,
            "dec": -5.39111,
            "metadata": {
                "ID": 47026,
                "USER": "Foobar",
                "EMAIL": "foobar@email.com",
                "TRRATE": "sidereal",
                "ACTIVITY": "multi_mode",
                "PROCESS": "Normal",
            },
        }
        config = ObjectActionConfig.from_dict(action_value)
        assert config.metadata["ID"] == 47026
        assert config.metadata["USER"] == "Foobar"
        assert config.metadata["EMAIL"] == "foobar@email.com"
        assert config.metadata["TRRATE"] == "sidereal"
        assert config.metadata["ACTIVITY"] == "multi_mode"
        assert config.metadata["PROCESS"] == "Normal"

    def test_unknown_top_level_keys_are_still_dropped(self):
        """Only keys nested under `metadata` survive; top-level unknown keys
        (the original bug reported) are still filtered out by
        merge_config_dicts, since only known dataclass fields are kept."""
        action_value = {
            "object": "M42",
            "exptime": 30.0,
            "ID": 47026,  # top-level, NOT under "metadata" -> dropped
            "metadata": {"ID": 47026},  # nested -> preserved
        }
        config = ObjectActionConfig.from_dict(action_value)
        assert not hasattr(config, "ID")
        assert "ID" not in config
        assert config.metadata == {"ID": 47026}

    def test_populated_metadata_does_not_raise_on_validate(self):
        """Regression test: dict[str, Any] fields with non-empty values used
        to crash validate() because `isinstance(v, typing.Any)` is invalid.
        Successful construction (which calls validate() via __post_init__)
        is sufficient to prove the fix."""
        config = ObjectActionConfig(
            object="M42",
            exptime=30.0,
            metadata={"ID": 47026, "DARK": False, "DURATION": 38, "USER": "Foobar"},
        )
        assert config.metadata["DARK"] is False


class TestCalibrationActionConfigMetadata:
    def test_metadata_defaults_to_empty_dict(self):
        config = CalibrationActionConfig(exptime=[0.0, 5.0], n=[3, 3])
        assert config.metadata == {}

    def test_metadata_dict_is_preserved_via_from_dict(self):
        action_value = {
            "exptime": [0.0, 5.0],
            "n": [3, 3],
            "metadata": {"PROCESS": "Normal", "DARK": True},
        }
        config = CalibrationActionConfig.from_dict(action_value)
        assert config.metadata == {"PROCESS": "Normal", "DARK": True}


class TestGetBaseHeaderActionMetadata:
    def _base_kwargs(self, metadata):
        return dict(
            paired_devices=MagicMock(),
            logger=MagicMock(),
            action_value=ObjectActionConfig(
                object="M42", exptime=38.0, metadata=metadata
            ),
        )

    def test_writes_string_metadata_value(self):
        fits_config = make_fits_config(
            [
                {
                    "header": "USER",
                    "comment": "Requesting user",
                    "device_type": "action_metadata",
                    "device_command": "USER",
                    "dtype": "str",
                    "fixed": True,
                }
            ]
        )
        kwargs = self._base_kwargs({"USER": "Foobar"})
        hdr = HeaderManager.get_base_header(fits_config=fits_config, **kwargs)
        assert hdr["USER"] == "Foobar"

    def test_casts_int_metadata_value(self):
        fits_config = make_fits_config(
            [
                {
                    "header": "ID",
                    "comment": "Source plan ID",
                    "device_type": "action_metadata",
                    "device_command": "ID",
                    "dtype": "int",
                    "fixed": True,
                }
            ]
        )
        kwargs = self._base_kwargs({"ID": "47026"})  # string input, int dtype
        hdr = HeaderManager.get_base_header(fits_config=fits_config, **kwargs)
        assert hdr["ID"] == 47026
        assert isinstance(hdr["ID"], int)

    def test_casts_bool_metadata_value(self):
        fits_config = make_fits_config(
            [
                {
                    "header": "DARK",
                    "comment": "Dark frame flag from plan",
                    "device_type": "action_metadata",
                    "device_command": "DARK",
                    "dtype": "bool",
                    "fixed": True,
                }
            ]
        )
        kwargs = self._base_kwargs({"DARK": False})
        hdr = HeaderManager.get_base_header(fits_config=fits_config, **kwargs)
        assert hdr["DARK"] is False

    def test_missing_key_warns_and_leaves_header_unset(self):
        fits_config = make_fits_config(
            [
                {
                    "header": "EMAIL",
                    "comment": "User contact email",
                    "device_type": "action_metadata",
                    "device_command": "EMAIL",
                    "dtype": "str",
                    "fixed": True,
                }
            ]
        )
        kwargs = self._base_kwargs({})  # no EMAIL key present
        hdr = HeaderManager.get_base_header(fits_config=fits_config, **kwargs)
        assert "EMAIL" not in hdr
        kwargs["logger"].warning.assert_called_once()

    def test_multiple_action_metadata_rows(self):
        fits_config = make_fits_config(
            [
                {
                    "header": "USER",
                    "comment": "user",
                    "device_type": "action_metadata",
                    "device_command": "USER",
                    "dtype": "str",
                    "fixed": True,
                },
                {
                    "header": "TRRATE",
                    "comment": "tracking rate",
                    "device_type": "action_metadata",
                    "device_command": "TRRATE",
                    "dtype": "str",
                    "fixed": True,
                },
                {
                    "header": "ACTIVITY",
                    "comment": "activity mode",
                    "device_type": "action_metadata",
                    "device_command": "ACTIVITY",
                    "dtype": "str",
                    "fixed": True,
                },
            ]
        )
        kwargs = self._base_kwargs(
            {"USER": "Foobar", "TRRATE": "sidereal", "ACTIVITY": "multi_mode"}
        )
        hdr = HeaderManager.get_base_header(fits_config=fits_config, **kwargs)
        assert hdr["USER"] == "Foobar"
        assert hdr["TRRATE"] == "sidereal"
        assert hdr["ACTIVITY"] == "multi_mode"

    def test_action_metadata_row_not_treated_as_ascom_device(self):
        """Regression test: action_metadata rows must not be swallowed by the
        generic 'direct ascom command headers' branch, which previously
        matched any device_type not in a fixed exclusion list."""
        fits_config = make_fits_config(
            [
                {
                    "header": "PROCESS",
                    "comment": "processing tag",
                    "device_type": "action_metadata",
                    "device_command": "PROCESS",
                    "dtype": "str",
                    "fixed": True,
                }
            ]
        )
        paired_devices = MagicMock()
        # Simulate a PairedDevices object that would raise if asked to resolve
        # a device_type it doesn't recognise, to prove that code path is
        # never reached for action_metadata rows.
        paired_devices.__contains__ = MagicMock(
            side_effect=AssertionError(
                "action_metadata should not be looked up as a paired device"
            )
        )
        kwargs = self._base_kwargs({"PROCESS": "Normal"})
        kwargs["paired_devices"] = paired_devices
        hdr = HeaderManager.get_base_header(fits_config=fits_config, **kwargs)
        assert hdr["PROCESS"] == "Normal"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
