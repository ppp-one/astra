"""Tests for subframe configuration in action configs."""

import pytest

from astra.action_configs import (
    AutofocusConfig,
    CalibrateGuidingActionConfig,
    CalibrationActionConfig,
    FlatsActionConfig,
    ObjectActionConfig,
    PointingModelActionConfig,
)


class TestSubframeValidation:
    """Test subframe validation in action configs."""

    def test_object_action_with_valid_subframe(self):
        """Test ObjectActionConfig with valid subframe parameters."""
        config = ObjectActionConfig(
            object="M31",
            exptime=300.0,
            ra=10.68,
            dec=41.27,
            subframe_width=512,
            subframe_height=512,
        )
        assert config.has_subframe()
        assert config.subframe_width == 512
        assert config.subframe_height == 512
        assert config.subframe_center_x == 0.5
        assert config.subframe_center_y == 0.5

    def test_object_action_without_subframe(self):
        """Test ObjectActionConfig without subframe (default full frame)."""
        config = ObjectActionConfig(object="M31", exptime=300.0, ra=10.68, dec=41.27)
        assert not config.has_subframe()
        assert config.subframe_width is None
        assert config.subframe_height is None

    def test_subframe_with_custom_center(self):
        """Test subframe with custom center position."""
        config = ObjectActionConfig(
            object="Target",
            exptime=60.0,
            ra=100.0,
            dec=20.0,
            subframe_width=256,
            subframe_height=256,
            subframe_center_x=0.75,
            subframe_center_y=0.25,
        )
        assert config.has_subframe()
        assert config.subframe_center_x == 0.75
        assert config.subframe_center_y == 0.25

    def test_subframe_negative_width_raises(self):
        """Test that negative width raises ValueError."""
        with pytest.raises(ValueError, match="subframe_width must be positive"):
            ObjectActionConfig(
                object="Test",
                exptime=10.0,
                subframe_width=-100,
                subframe_height=100,
            )

    def test_subframe_negative_height_raises(self):
        """Test that negative height raises ValueError."""
        with pytest.raises(ValueError, match="subframe_height must be positive"):
            ObjectActionConfig(
                object="Test",
                exptime=10.0,
                subframe_width=100,
                subframe_height=-100,
            )

    def test_subframe_zero_width_raises(self):
        """Test that zero width raises ValueError."""
        with pytest.raises(ValueError, match="subframe_width must be positive"):
            ObjectActionConfig(
                object="Test",
                exptime=10.0,
                subframe_width=0,
                subframe_height=100,
            )

    def test_subframe_invalid_center_x_raises(self):
        """Test that invalid center_x raises ValueError."""
        with pytest.raises(
            ValueError, match="subframe_center_x must be between 0 and 1"
        ):
            ObjectActionConfig(
                object="Test",
                exptime=10.0,
                subframe_width=100,
                subframe_height=100,
                subframe_center_x=1.5,
            )

    def test_subframe_invalid_center_y_raises(self):
        """Test that invalid center_y raises ValueError."""
        with pytest.raises(
            ValueError, match="subframe_center_y must be between 0 and 1"
        ):
            ObjectActionConfig(
                object="Test",
                exptime=10.0,
                subframe_width=100,
                subframe_height=100,
                subframe_center_y=-0.1,
            )

    def test_subframe_only_width_specified_raises(self):
        """Test that specifying only width raises ValueError."""
        with pytest.raises(
            ValueError,
            match="Both subframe_width and subframe_height must be specified together",
        ):
            ObjectActionConfig(
                object="Test",
                exptime=10.0,
                subframe_width=100,
            )

    def test_subframe_only_height_specified_raises(self):
        """Test that specifying only height raises ValueError."""
        with pytest.raises(
            ValueError,
            match="Both subframe_width and subframe_height must be specified together",
        ):
            ObjectActionConfig(
                object="Test",
                exptime=10.0,
                subframe_height=100,
            )

    def test_nonsidereal_start_lead_time_defaults_to_60_seconds(self):
        """Test default lead wait used for non-sidereal pre-pointing."""
        config = ObjectActionConfig(object="M31", exptime=300.0, ra=10.68, dec=41.27)
        assert config.nonsidereal_start_lead_time_seconds == 60.0

    def test_nonsidereal_start_lead_time_negative_raises(self):
        """Negative lead wait is invalid."""
        with pytest.raises(
            ValueError,
            match="nonsidereal_start_lead_time_seconds must be >= 0",
        ):
            ObjectActionConfig(
                object="M31",
                exptime=300.0,
                ra=10.68,
                dec=41.27,
                nonsidereal_start_lead_time_seconds=-1,
            )


class TestSubframeInAllImagingConfigs:
    """Test that subframe works across all imaging action configs."""

    def test_calibration_config_with_subframe(self):
        """Test CalibrationActionConfig with subframe."""
        config = CalibrationActionConfig(
            exptime=[0.0, 1.0],
            n=[10, 10],
            subframe_width=512,
            subframe_height=512,
        )
        assert config.has_subframe()

    def test_flats_config_with_subframe(self):
        """Test FlatsActionConfig with subframe."""
        config = FlatsActionConfig(
            filter=["Clear", "Red"],
            n=[10, 10],
            subframe_width=1024,
            subframe_height=1024,
        )
        assert config.has_subframe()

    def test_calibrate_guiding_config_with_subframe(self):
        """Test CalibrateGuidingActionConfig with subframe."""
        config = CalibrateGuidingActionConfig(
            subframe_width=256,
            subframe_height=256,
        )
        assert config.has_subframe()

    def test_pointing_model_config_with_subframe(self):
        """Test PointingModelActionConfig with subframe."""
        config = PointingModelActionConfig(
            subframe_width=512,
            subframe_height=512,
        )
        assert config.has_subframe()

    def test_autofocus_config_with_subframe(self):
        """Test AutofocusConfig with subframe."""
        config = AutofocusConfig(
            exptime=3.0,
            subframe_width=400,
            subframe_height=400,
        )
        assert config.has_subframe()


class TestSubframeFromDict:
    """Test subframe parameters work with from_dict method."""

    def test_object_config_from_dict_with_subframe(self):
        """Test creating ObjectActionConfig from dict with subframe."""
        config_dict = {
            "object": "Target",
            "exptime": 100.0,
            "ra": 50.0,
            "dec": 30.0,
            "subframe_width": 768,
            "subframe_height": 768,
            "subframe_center_x": 0.6,
            "subframe_center_y": 0.4,
        }
        config = ObjectActionConfig.from_dict(config_dict)
        assert config.has_subframe()
        assert config.subframe_width == 768
        assert config.subframe_height == 768
        assert config.subframe_center_x == 0.6
        assert config.subframe_center_y == 0.4

    def test_object_config_from_dict_without_subframe(self):
        """Test creating ObjectActionConfig from dict without subframe."""
        config_dict = {
            "object": "Target",
            "exptime": 100.0,
        }
        config = ObjectActionConfig.from_dict(config_dict)
        assert not config.has_subframe()

    def test_object_config_from_dict_with_defaults(self):
        """Test creating ObjectActionConfig with default subframe values."""
        config_dict = {
            "object": "Target",
            "exptime": 100.0,
        }
        default_dict = {
            "subframe_width": 512,
            "subframe_height": 512,
            "bin": 2,
        }
        config = ObjectActionConfig.from_dict(config_dict, default_dict)
        assert config.has_subframe()
        assert config.subframe_width == 512
        assert config.subframe_height == 512
        assert config.bin == 2


class TestSubframeToJsonable:
    """Test that subframe parameters are properly serialized."""

    def test_subframe_in_to_jsonable(self):
        """Test that to_jsonable includes subframe parameters."""
        config = ObjectActionConfig(
            object="M31",
            exptime=300.0,
            ra=10.68,
            dec=41.27,
            subframe_width=512,
            subframe_height=512,
            subframe_center_x=0.75,
            subframe_center_y=0.25,
        )
        jsonable = config.to_jsonable()
        assert jsonable["subframe_width"] == 512
        assert jsonable["subframe_height"] == 512
        assert jsonable["subframe_center_x"] == 0.75
        assert jsonable["subframe_center_y"] == 0.25
