"""Tests for subframe setup in observatory.py."""

from unittest.mock import MagicMock

import pytest

from astra.action_configs import ObjectActionConfig
from astra.observatory import Observatory
from astra.paired_devices import PairedDevices


class MockCamera:
    """Mock camera device for testing."""

    def __init__(self):
        self.properties = {
            "BinX": 1,
            "BinY": 1,
            "CameraXSize": 2048,
            "CameraYSize": 2048,
            "StartX": 0,
            "StartY": 0,
            "NumX": 2048,
            "NumY": 2048,
        }

    def get(self, prop):
        return self.properties[prop]

    def set(self, prop, value):
        self.properties[prop] = value


class TestSubframeSetup:
    """Test subframe setup in observatory."""

    @pytest.fixture
    def mock_observatory(self):
        """Create a mock observatory with logger."""
        obs = MagicMock(spec=Observatory)
        obs.logger = MagicMock()
        obs.logger.info = MagicMock()
        obs.logger.debug = MagicMock()
        obs.logger.warning = MagicMock()
        # Bind the actual method to the mock
        obs._setup_camera_subframe = Observatory._setup_camera_subframe.__get__(obs)
        return obs

    @pytest.fixture
    def mock_paired_devices(self):
        """Create mock paired devices."""
        paired = MagicMock(spec=PairedDevices)
        paired.__getitem__ = lambda self, key: "TestCamera" if key == "Camera" else None
        return paired

    def test_subframe_centered_512x512(self, mock_observatory, mock_paired_devices):
        """Test centered 512x512 subframe on 2048x2048 sensor."""
        camera = MockCamera()
        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            subframe_width=512,
            subframe_height=512,
            subframe_center_x=0.5,
            subframe_center_y=0.5,
        )

        mock_observatory._setup_camera_subframe(
            camera, action_value, mock_paired_devices
        )

        # Center 512x512 on 2048x2048 sensor:
        # StartX = (2048 - 512*1) * 0.5 = 768
        # StartY = (2048 - 512*1) * 0.5 = 768
        assert camera.get("StartX") == 768
        assert camera.get("StartY") == 768
        assert camera.get("NumX") == 512
        assert camera.get("NumY") == 512

    def test_subframe_with_binning(self, mock_observatory, mock_paired_devices):
        """Test subframe with 2x2 binning."""
        camera = MockCamera()
        camera.set("BinX", 2)
        camera.set("BinY", 2)

        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            bin=2,
            subframe_width=256,  # 256 binned pixels
            subframe_height=256,  # 256 binned pixels
            subframe_center_x=0.5,
            subframe_center_y=0.5,
        )

        mock_observatory._setup_camera_subframe(
            camera, action_value, mock_paired_devices
        )

        # 256 binned pixels = 512 unbinned pixels
        # StartX = (2048 - 512) * 0.5 = 768
        assert camera.get("StartX") == 768
        assert camera.get("StartY") == 768
        assert camera.get("NumX") == 256
        assert camera.get("NumY") == 256

    def test_subframe_top_left_corner(self, mock_observatory, mock_paired_devices):
        """Test subframe positioned at top-left corner."""
        camera = MockCamera()
        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            subframe_width=400,
            subframe_height=400,
            subframe_center_x=0.0,
            subframe_center_y=0.0,
        )

        mock_observatory._setup_camera_subframe(
            camera, action_value, mock_paired_devices
        )

        # Top-left: StartX = 0, StartY = 0
        assert camera.get("StartX") == 0
        assert camera.get("StartY") == 0
        assert camera.get("NumX") == 400
        assert camera.get("NumY") == 400

    def test_subframe_bottom_right_corner(self, mock_observatory, mock_paired_devices):
        """Test subframe positioned at bottom-right corner."""
        camera = MockCamera()
        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            subframe_width=400,
            subframe_height=400,
            subframe_center_x=1.0,
            subframe_center_y=1.0,
        )

        mock_observatory._setup_camera_subframe(
            camera, action_value, mock_paired_devices
        )

        # Bottom-right: StartX = 2048 - 400 = 1648
        assert camera.get("StartX") == 1648
        assert camera.get("StartY") == 1648
        assert camera.get("NumX") == 400
        assert camera.get("NumY") == 400

    def test_subframe_custom_position(self, mock_observatory, mock_paired_devices):
        """Test subframe with custom center position."""
        camera = MockCamera()
        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            subframe_width=512,
            subframe_height=512,
            subframe_center_x=0.75,  # 3/4 to the right
            subframe_center_y=0.25,  # 1/4 from top
        )

        mock_observatory._setup_camera_subframe(
            camera, action_value, mock_paired_devices
        )

        # StartX = (2048 - 512) * 0.75 = 1152
        # StartY = (2048 - 512) * 0.25 = 384
        assert camera.get("StartX") == 1152
        assert camera.get("StartY") == 384
        assert camera.get("NumX") == 512
        assert camera.get("NumY") == 512

    def test_subframe_exceeds_sensor_width_raises(
        self, mock_observatory, mock_paired_devices
    ):
        """Test that subframe exceeding sensor width raises ValueError."""
        camera = MockCamera()
        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            subframe_width=3000,  # Exceeds 2048
            subframe_height=512,
        )

        with pytest.raises(ValueError, match="exceeds sensor width"):
            mock_observatory._setup_camera_subframe(
                camera, action_value, mock_paired_devices
            )

    def test_subframe_exceeds_sensor_height_raises(
        self, mock_observatory, mock_paired_devices
    ):
        """Test that subframe exceeding sensor height raises ValueError."""
        camera = MockCamera()
        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            subframe_width=512,
            subframe_height=3000,  # Exceeds 2048
        )

        with pytest.raises(ValueError, match="exceeds sensor height"):
            mock_observatory._setup_camera_subframe(
                camera, action_value, mock_paired_devices
            )

    def test_subframe_with_binning_exceeds_sensor(
        self, mock_observatory, mock_paired_devices
    ):
        """Test that subframe with binning exceeding sensor raises ValueError."""
        camera = MockCamera()
        camera.set("BinX", 2)
        camera.set("BinY", 2)

        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            bin=2,
            subframe_width=1500,  # 1500 * 2 = 3000 unbinned > 2048
            subframe_height=512,
        )

        with pytest.raises(ValueError, match="exceeds sensor width"):
            mock_observatory._setup_camera_subframe(
                camera, action_value, mock_paired_devices
            )

    def test_subframe_rectangular(self, mock_observatory, mock_paired_devices):
        """Test non-square subframe."""
        camera = MockCamera()
        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            subframe_width=1024,
            subframe_height=512,
            subframe_center_x=0.5,
            subframe_center_y=0.5,
        )

        mock_observatory._setup_camera_subframe(
            camera, action_value, mock_paired_devices
        )

        # StartX = (2048 - 1024) * 0.5 = 512
        # StartY = (2048 - 512) * 0.5 = 768
        assert camera.get("StartX") == 512
        assert camera.get("StartY") == 768
        assert camera.get("NumX") == 1024
        assert camera.get("NumY") == 512

    def test_subframe_full_sensor_size(self, mock_observatory, mock_paired_devices):
        """Test subframe equal to full sensor size."""
        camera = MockCamera()
        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            subframe_width=2048,
            subframe_height=2048,
        )

        mock_observatory._setup_camera_subframe(
            camera, action_value, mock_paired_devices
        )

        # Should work - effectively full frame
        assert camera.get("StartX") == 0
        assert camera.get("StartY") == 0
        assert camera.get("NumX") == 2048
        assert camera.get("NumY") == 2048

    def test_logging_called(self, mock_observatory, mock_paired_devices):
        """Test that logging is called with correct information."""
        camera = MockCamera()
        action_value = ObjectActionConfig(
            object="Test",
            exptime=10.0,
            subframe_width=512,
            subframe_height=512,
        )

        mock_observatory._setup_camera_subframe(
            camera, action_value, mock_paired_devices
        )

        # Check that logger.info was called
        mock_observatory.logger.info.assert_called()
        call_args = mock_observatory.logger.info.call_args[0][0]
        assert "512×512" in call_args
        assert "binned pixels" in call_args

        # Check that logger.debug was called
        mock_observatory.logger.debug.assert_called()
        debug_args = mock_observatory.logger.debug.call_args[0][0]
        assert "StartX=" in debug_args
        assert "NumX=" in debug_args
