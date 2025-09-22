"""Unit tests for image_handler module."""

import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pytest
from alpaca.camera import ImageMetadata
from astropy.io import fits

from astra.image_handler import (
    FilenameTemplates,
    ImageHandler,
    JinjaFilenameTemplates,
)


class TestImageHandler:
    def test_initialization(self):
        header = fits.Header()
        image_directory = Path("/tmp/test_images")
        templates = FilenameTemplates()
        handler = ImageHandler(header, image_directory, templates)
        assert handler.header is header
        assert handler.image_directory == image_directory
        assert isinstance(handler.filename_templates, FilenameTemplates)
        assert handler.last_image_path is None
        assert handler.last_image_timestamp is None

    def test_create_image_dir_user_specified(self):
        """Test creating directory with user-specified path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            user_dir = Path(temp_dir) / "custom_dir"

            result = ImageHandler.create_image_dir(user_specified_dir=str(user_dir))

            assert result == user_dir
            assert user_dir.exists()
            assert user_dir.is_dir()

    def test_create_image_dir_already_exists(self):
        """Test behavior when user-specified directory already exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            user_dir = Path(temp_dir) / "existing_dir"
            user_dir.mkdir()

            result = ImageHandler.create_image_dir(user_specified_dir=str(user_dir))

            assert result == user_dir
            assert user_dir.exists()

    def test_create_image_dir_auto_generated(self, temp_config):
        """Test creating directory with auto-generated date-based path."""
        schedule_time = datetime(2024, 5, 15, 10, 0, 0, tzinfo=UTC)
        site_long = -120.0  # 8 hours west

        result = ImageHandler.create_image_dir(schedule_time, site_long)

        # Local date should be 2024-05-15 10:00 - 8:00 = 2024-05-15 02:00
        expected_date = "20240515"
        expected_path = Path(temp_config.paths.images) / expected_date

        assert result == expected_path
        assert expected_path.exists()
        assert expected_path.is_dir()

    def test_create_image_dir_date_calculation(self, temp_config):
        """Test date calculation for auto-generated directory."""
        # Test crossing date boundary
        schedule_time = datetime(2024, 5, 15, 2, 0, 0, tzinfo=UTC)
        site_long = 120.0  # 8 hours east

        result = ImageHandler.create_image_dir(schedule_time, site_long)

        # Local date should be 2024-05-15 02:00 + 8:00 = 2024-05-15 10:00
        expected_date = "20240515"
        expected_path = Path(temp_config.paths.images) / expected_date

        assert result == expected_path

    def test_create_image_dir_default_parameters(self, temp_config):
        """Test function with default parameters."""
        result = ImageHandler.create_image_dir()
        expected_date = datetime.now(UTC).strftime("%Y%m%d")
        expected_path = Path(temp_config.paths.images) / expected_date
        assert result == expected_path
        assert expected_path.exists()

    def create_mock_image_info(self, element_type: int, rank: int) -> ImageMetadata:
        """Helper to create mock ImageMetadata."""
        mock_info = Mock(spec=ImageMetadata)
        mock_info.ImageElementType = element_type
        mock_info.Rank = rank
        return mock_info

    def create_test_header(self, **kwargs) -> fits.Header:
        """Helper to create FITS header with default values."""
        defaults = {
            "FILTER": "V",
            "IMAGETYP": "Light Frame",
            "OBJECT": "M31",
            "EXPTIME": 60.0,
        }
        defaults.update(kwargs)

        header = fits.Header()
        for key, value in defaults.items():
            header[key] = value
        return header

    def test_transform_image_to_array_uint16_type_0(self):
        """Test transformation with ImageElementType 0."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        assert result.shape == (2, 2)
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_transform_image_to_array_uint16_type_1(self):
        """Test transformation with ImageElementType 1."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(1, 2)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_transform_image_to_array_uint16_type_2_low_maxadu(self):
        """Test transformation with ImageElementType 2 and low maxadu."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(2, 2)
        maxadu = 65535

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16

    def test_transform_image_to_array_int32_type_2_high_maxadu(self):
        """Test transformation with ImageElementType 2 and high maxadu."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(2, 2)
        maxadu = 70000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.int32

    def test_transform_image_to_array_float64_type_3(self):
        """Test transformation with ImageElementType 3."""
        image = [[1.5, 2.7], [3.1, 4.9]]
        info = self.create_mock_image_info(3, 2)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.float64
        np.testing.assert_array_almost_equal(result, [[1.5, 3.1], [2.7, 4.9]])

    def test_3d_image_rank_3(self):
        """Test transformation with 3D image (Rank 3)."""
        # RGB image: 2x2 pixels, 3 channels
        image = [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]]
        info = self.create_mock_image_info(0, 3)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        assert result.shape == (3, 2, 2)
        # Should transpose from (2, 2, 3) to (3, 2, 2)
        expected = [[[1, 7], [4, 10]], [[2, 8], [5, 11]], [[3, 9], [6, 12]]]
        np.testing.assert_array_equal(result, expected)

    def test_numpy_array_input(self):
        """Test transformation with numpy array input."""
        image = np.array([[1, 2], [3, 4]], dtype=np.int32)
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_transform_image_to_array_invalid_type(self):
        """Test error handling for invalid ImageElementType."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(4, 2)
        maxadu = 1000

        with pytest.raises(ValueError, match="Unknown ImageElementType: 4"):
            ImageHandler._transform_image_to_array(image, maxadu, info)

    def test_transform_image_to_array_3d_rank_3(self):
        """Test transformation with 3D image (Rank 3)."""
        # RGB image: 2x2 pixels, 3 channels
        image = [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]]
        info = self.create_mock_image_info(0, 3)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        assert result.shape == (3, 2, 2)
        # Should transpose from (2, 2, 3) to (3, 2, 2)
        expected = [[[1, 7], [4, 10]], [[2, 8], [5, 11]], [[3, 9], [6, 12]]]
        np.testing.assert_array_equal(result, expected)

    def test_transform_image_to_array_numpy_array(self):
        """Test transformation with numpy array input."""
        image = np.array([[1, 2], [3, 4]], dtype=np.int32)
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_transform_image_to_array_list_input(self):
        """Test transformation with list input."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_save_image_updates_last_path_and_timestamp(self, temp_config):
        header = fits.Header()
        image_directory = Path(temp_config.paths.images) / "handler_test"
        image_directory.mkdir(exist_ok=True)
        templates = FilenameTemplates()
        handler = ImageHandler(header, image_directory, templates)
        image = [[1, 2], [3, 4]]
        info = Mock(spec=ImageMetadata)
        info.ImageElementType = 0
        info.Rank = 2
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        # Optionally, check that last_image_path and last_image_timestamp are updated
        # assert handler.last_image_path == result
        # assert handler.last_image_timestamp == exposure_start_datetime

    def test_set_imagetype_header(self):
        header = fits.Header()
        handler = ImageHandler(header)
        handler.header["EXPTIME"] = 0
        use_light = True
        use_light = handler.set_imagetype_header("calibration", use_light)
        assert handler.header["IMAGETYP"] == "Bias Frame"
        assert use_light is False
        handler.header["EXPTIME"] = 10
        use_light = handler.set_imagetype_header("calibration", use_light)
        assert handler.header["IMAGETYP"] == "Dark Frame"
        assert use_light is False
        use_light = handler.set_imagetype_header("object", use_light)
        assert handler.header["IMAGETYP"] == "Light Frame"
        assert use_light is True

    def test_get_observatory_location(self):
        header = fits.Header()
        header["LAT-OBS"] = 10.0
        header["LONG-OBS"] = 20.0
        header["ALT-OBS"] = 100.0
        handler = ImageHandler(header)
        loc = handler.get_observatory_location()
        assert loc.lat.value == 10.0
        assert loc.lon.value == 20.0
        assert abs(loc.height.value - 100.0) < 1e-6

    def _prepare_save_args(self, temp_config, image, header_kwargs, image_directory):
        info = self.create_mock_image_info(0, 2)
        maxadu = 65535
        header = self.create_test_header(**header_kwargs)
        device_name = "TestCamera"
        exposure_start_datetime = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        filepath = Path(temp_config.paths.images) / image_directory
        filepath.mkdir(exist_ok=True)
        handler = ImageHandler(header, filepath)
        return handler, image, info, maxadu, device_name, exposure_start_datetime

    def test_save_light_frame(self, temp_config):
        handler, image, info, maxadu, device_name, exposure_start_datetime = (
            self._prepare_save_args(
                temp_config,
                [[100, 200], [300, 400]],
                {},
                "test_image_directory",
            )
        )
        handler.header["IMAGETYP"] = "light"
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        assert result.is_file()
        assert result.name.startswith("TestCamera_V_M31_60.000_")
        expected_path = (
            Path(temp_config.paths.images) / "test_image_directory" / result.name
        )
        assert result == expected_path
        with fits.open(result) as hdul:
            np.testing.assert_array_equal(hdul[0].data, [[100, 300], [200, 400]])
            assert hdul[0].header["DATE-OBS"] == "2024-05-15T12:00:00.000000"
            assert "UTC datetime file written" in hdul[0].header.comments["DATE-OBS"]
            assert "UTC datetime start of exposure" in hdul[0].header.comments["DATE"]

    def test_save_bias_frame(self, temp_config):
        handler, image, info, maxadu, device_name, exposure_start_datetime = (
            self._prepare_save_args(
                temp_config,
                [[10, 11], [12, 13]],
                {"IMAGETYP": "Bias Frame", "EXPTIME": 0.0},
                "bias_image_directory",
            )
        )
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        assert result.is_file()
        assert result.name.startswith("TestCamera_bias_0.000_")
        with fits.open(result) as hdul:
            np.testing.assert_array_equal(hdul[0].data, [[10, 12], [11, 13]])
            assert hdul[0].header["IMAGETYP"] == "Bias Frame"
            assert hdul[0].header["EXPTIME"] == 0.0

    def test_save_dark_frame(self, temp_config):
        handler, image, info, maxadu, device_name, exposure_start_datetime = (
            self._prepare_save_args(
                temp_config,
                [[20, 21], [22, 23]],
                {"IMAGETYP": "Dark Frame", "EXPTIME": 120.0},
                "dark_image_directory",
            )
        )
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        assert result.is_file()
        assert result.name.startswith("TestCamera_dark_120.000_")
        with fits.open(result) as hdul:
            np.testing.assert_array_equal(hdul[0].data, [[20, 22], [21, 23]])
            assert hdul[0].header["IMAGETYP"] == "Dark Frame"
            assert hdul[0].header["EXPTIME"] == 120.0

    def test_save_other_frame_type(self, temp_config):
        handler, image, info, maxadu, device_name, exposure_start_datetime = (
            self._prepare_save_args(
                temp_config,
                [[30, 31], [32, 33]],
                {"IMAGETYP": "Flat Frame", "EXPTIME": 10.0},
                "flat_image_directory",
            )
        )
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        assert result.is_file()
        assert result.name.startswith("TestCamera_V_flat_10.000_")
        with fits.open(result) as hdul:
            np.testing.assert_array_equal(hdul[0].data, [[30, 32], [31, 33]])
            assert hdul[0].header["IMAGETYP"] == "Flat Frame"
            assert hdul[0].header["EXPTIME"] == 10.0

    def test_save_image_no_header_raises(self, temp_config):
        handler = ImageHandler(
            header=None, image_directory=Path(temp_config.paths.images) / "neg_test"
        )
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        with pytest.raises(ValueError, match="No FITS header specified to save image."):
            handler.save_image(
                image, info, maxadu, device_name, exposure_start_datetime
            )

    def test_save_image_no_image_directory_raises(self, temp_config):
        header = self.create_test_header()
        handler = ImageHandler(header=header, image_directory=None)
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        with pytest.raises(ValueError, match="Image directory is not set."):
            handler.save_image(
                image, info, maxadu, device_name, exposure_start_datetime
            )

    def test_create_image_dir_invalid_path(self):
        # Try to create a directory with an invalid path (should raise OSError or similar)
        invalid_path = "/invalid_path/\0bad_dir"
        with pytest.raises(Exception):
            ImageHandler.create_image_dir(user_specified_dir=invalid_path)

    def test_save_image_invalid_wcs(self, temp_config):
        header = self.create_test_header()
        image_directory = Path(temp_config.paths.images) / "neg_test"
        image_directory.mkdir(exist_ok=True)
        handler = ImageHandler(header, image_directory)
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)

        class BadWCS:
            def to_header(self):
                raise RuntimeError("WCS error!")

        with pytest.raises(RuntimeError, match="WCS error!"):
            handler.save_image(
                image, info, maxadu, device_name, exposure_start_datetime, wcs=BadWCS()
            )

    def test_save_image_missing_header_keys(self, temp_config):
        # Missing FILTER, IMAGETYP, OBJECT, EXPTIME
        header = fits.Header()
        image_directory = Path(temp_config.paths.images) / "neg_missing_keys"
        image_directory.mkdir(exist_ok=True)
        handler = ImageHandler(header, image_directory)
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        # Should not raise, but filename will contain 'NA' and exptime will be nan
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        assert "NA" in result.name

    def test_save_image_exptime_string(self, temp_config):
        header = self.create_test_header(EXPTIME="not_a_float")
        image_directory = Path(temp_config.paths.images) / "neg_exptime_str"
        image_directory.mkdir(exist_ok=True)
        handler = ImageHandler(header, image_directory)
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        # Should raise ValueError when converting EXPTIME to float
        with pytest.raises(ValueError):
            handler.save_image(
                image, info, maxadu, device_name, exposure_start_datetime
            )

    def test_save_image_corrupted_data(self, temp_config):
        header = self.create_test_header()
        image_directory = Path(temp_config.paths.images) / "neg_corrupt_data"
        image_directory.mkdir(exist_ok=True)
        handler = ImageHandler(header, image_directory)
        image = "not_an_array"
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        with pytest.raises(Exception):
            handler.save_image(
                image, info, maxadu, device_name, exposure_start_datetime
            )

    def test_save_image_illegal_device_name(self, temp_config):
        header = self.create_test_header()
        image_directory = Path(temp_config.paths.images) / "neg_illegal_device"
        image_directory.mkdir(exist_ok=True)
        handler = ImageHandler(header, image_directory)
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "Test/Camera:Bad|Name"
        exposure_start_datetime = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        # Should create a file, possibly in a subdirectory
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        # The path should include the subdirectory due to the slash
        assert "Test" in str(result.parent)
        assert "Camera:Bad|Name" in str(result.name)

    def test_save_image_template_missing_arg(self, temp_config):
        header = self.create_test_header()
        image_directory = Path(temp_config.paths.images) / "neg_template_missing_arg"
        image_directory.mkdir(exist_ok=True)
        # Create a template that references a missing argument
        with pytest.raises(ValueError, match="missing_arg"):
            bad_templates = FilenameTemplates(
                light="{device}_{missing_arg}_{exptime:.3f}_{timestamp}.fits"
            )
            handler = ImageHandler(header, image_directory, bad_templates)
            image = [[1, 2], [3, 4]]
            info = self.create_mock_image_info(0, 2)
            maxadu = 1000
            device_name = "TestCamera"
            exposure_start_datetime = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
            handler.save_image(
                image, info, maxadu, device_name, exposure_start_datetime
            )


class FilenameTemplateTests:
    def test_filename_templates(self):
        templates = FilenameTemplates()
        jinja_templates = JinjaFilenameTemplates()
        test_types = ["light", "bias", "dark", "flat", "default"]
        expected = {
            "light": "TestCamera_TestFilter_TestObject_300.123_2025-01-01_00-00-00.fits",
            "bias": "TestCamera_bias_300.123_2025-01-01_00-00-00.fits",
            "dark": "TestCamera_dark_300.123_2025-01-01_00-00-00.fits",
            "flat": "TestCamera_TestFilter_flat_300.123_2025-01-01_00-00-00.fits",
            "default": "TestCamera_TestFilter_default_300.123_2025-01-01_00-00-00.fits",
        }
        for imagetype in test_types:
            filename = templates.render_filename(
                **templates.TEST_ARGS | {"imagetype": imagetype}
            )

            assert filename == expected[imagetype], (
                f"For {imagetype}, got {filename}, expected {expected[imagetype]}"
            )

            assert filename == jinja_templates.render_filename(
                **jinja_templates.TEST_ARGS | {"imagetype": imagetype}
            ), (
                "JinjaFilenameTemplates template does not match standard template. "
                f"For {imagetype}, got {filename}, expected {expected[imagetype]}"
            )

    def test_filename_template_with_subdir(self):
        templates = JinjaFilenameTemplates(
            dark="{{ imagetype.split(' ')[0].upper() }}/" + JinjaFilenameTemplates.dark
        )
        filename = templates.render_filename(
            **templates.TEST_ARGS | {"imagetype": "dark"}
        )
        expected = "DARK/TestCamera_dark_300.123_2025-01-01_00-00-00.fits"
        assert filename == expected, f"Got {filename}, expected {expected}"
