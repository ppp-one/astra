"""Unit tests for image_handler module."""

import datetime
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pytest
from alpaca.camera import ImageMetadata
from astropy.io import fits

from astra.filename_templates import FilenameTemplates, JinjaFilenameTemplates
from astra.header_manager import ObservatoryHeader
from astra.image_handler import ImageHandler


class TestImageHandler:
    def test_initialization(self):
        header = ObservatoryHeader.get_test_header()
        image_directory = Path("/tmp/test_images")
        templates = FilenameTemplates()
        handler = ImageHandler(header, image_directory, templates)
        assert handler.header is header
        assert handler.image_directory == image_directory
        assert isinstance(handler.filename_templates, FilenameTemplates)
        assert handler.last_image_path is None
        assert handler.last_image_timestamp is None

    def create_mock_image_info(self, element_type: int, rank: int) -> ImageMetadata:
        """Helper to create mock ImageMetadata."""
        mock_info = Mock(spec=ImageMetadata)
        mock_info.ImageElementType = element_type
        mock_info.Rank = rank
        return mock_info

    def create_test_header(self, **kwargs) -> ObservatoryHeader:
        """Helper to create FITS header with default values."""
        defaults = {
            "FILTER": "V",
            "IMAGETYP": "Light Frame",
            "OBJECT": "M31",
            "EXPTIME": 60.0,
        }
        defaults.update(kwargs)

        header = ObservatoryHeader.get_test_header()
        for key, value in defaults.items():
            header[key] = value
        return header

    def test_transform_image_to_array_uint16_type_0(self):
        """Test transformation with ImageElementType 0."""
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        assert result.shape == (2, 2)
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_transform_image_to_array_uint16_type_1(self):
        """Test transformation with ImageElementType 1."""
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(1, 2)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_transform_image_to_array_uint16_type_2_low_maxadu(self):
        """Test transformation with ImageElementType 2 and low maxadu."""
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(2, 2)
        maxadu = 65535

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16

    def test_transform_image_to_array_int32_type_2_high_maxadu(self):
        """Test transformation with ImageElementType 2 and high maxadu."""
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(2, 2)
        maxadu = 70000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.int32

    def test_transform_image_to_array_float64_type_3(self):
        """Test transformation with ImageElementType 3."""
        image = np.array([[1.5, 2.7], [3.1, 4.9]])
        info = self.create_mock_image_info(3, 2)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.float64
        np.testing.assert_array_almost_equal(result, [[1.5, 3.1], [2.7, 4.9]])

    def test_3d_image_rank_3(self):
        """Test transformation with 3D image (Rank 3)."""
        # RGB image: 2x2 pixels, 3 channels
        image = np.array([[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]])
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
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(4, 2)
        maxadu = 1000

        with pytest.raises(ValueError, match="Unknown ImageElementType: 4"):
            ImageHandler._transform_image_to_array(image, maxadu, info)

    def test_transform_image_to_array_3d_rank_3(self):
        """Test transformation with 3D image (Rank 3)."""
        # RGB image: 2x2 pixels, 3 channels
        image = np.array([[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]])
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
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000

        result = ImageHandler._transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_save_image_updates_last_path_and_timestamp(self, temp_config):
        header = ObservatoryHeader.get_test_header()
        image_directory = Path(temp_config.paths.images) / "handler_test"
        image_directory.mkdir(exist_ok=True)
        templates = FilenameTemplates()
        handler = ImageHandler(header, image_directory, templates)
        image = np.array([[1, 2], [3, 4]])
        info = Mock(spec=ImageMetadata)
        info.ImageElementType = 0
        info.Rank = 2
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        # Optionally, check that last_image_path and last_image_timestamp are updated
        # assert handler.last_image_path == result
        # assert handler.last_image_timestamp == exposure_start_datetime

    def test_save_image_avoids_filename_collisions(self, temp_config):
        header = ObservatoryHeader.get_test_header()
        image_directory = Path(temp_config.paths.images) / "handler_collision_test"
        image_directory.mkdir(exist_ok=True)
        handler = ImageHandler(
            header,
            image_directory,
            FilenameTemplates.from_dict({"default": "static_file_name.fits"}),
        )

        image = np.array([[1, 2], [3, 4]])
        info = Mock(spec=ImageMetadata)
        info.ImageElementType = 0
        info.Rank = 2
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )

        first = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        second = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )

        assert first.name == "static_file_name.fits"
        assert first.exists()
        assert second.exists()
        assert first != second

    def test_filename_templates_render_same(self):
        jinja_templates = JinjaFilenameTemplates()
        standard_templates = FilenameTemplates()
        test_kwargs = FilenameTemplates.TEST_KWARGS
        test_kwargs.pop("action_type", None)

        for key in standard_templates.__dataclass_fields__.keys():
            jinja_result = jinja_templates.render_filename(key, **test_kwargs)
            standard_result = standard_templates.render_filename(key, **test_kwargs)
            assert jinja_result == standard_result, f"Mismatch in template '{key}'"

    def test_set_imagetype_header(self):
        header = ObservatoryHeader.get_test_header()
        handler = ImageHandler(header)
        handler.header["EXPTIME"] = 0
        use_light = True
        use_light = handler.header.set_imagetype("calibration", use_light)
        assert handler.header["IMAGETYP"] == "Bias Frame"
        assert use_light is False
        handler.header["EXPTIME"] = 10
        use_light = handler.header.set_imagetype("calibration", use_light)
        assert handler.header["IMAGETYP"] == "Dark Frame"
        assert use_light is False
        use_light = handler.header.set_imagetype("object", use_light)
        assert handler.header["IMAGETYP"] == "Light Frame"
        assert use_light is True

    def test_get_observatory_location(self):
        header = ObservatoryHeader.get_test_header()
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
        exposure_start_datetime = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )
        filepath = Path(temp_config.paths.images) / image_directory
        filepath.mkdir(exist_ok=True)
        handler = ImageHandler(header, filepath)
        handler.observing_date = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )
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
        handler.header["ASTRATYP"] = "object"
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        assert result.is_file()
        assert result.name.startswith("TestCamera_V_M31_60.000_")
        expected_path = (
            Path(temp_config.paths.images)
            / "test_image_directory"
            / "20240515"
            / result.name
        )
        assert result == expected_path
        with fits.open(result) as hdul:
            np.testing.assert_array_equal(hdul[0].data, [[100, 300], [200, 400]])
            assert hdul[0].header["DATE-OBS"] == "2024-05-15T12:00:00.000000"
            assert "UTC datetime file written" in hdul[0].header.comments["DATE"]
            assert (
                "UTC datetime start of exposure" in hdul[0].header.comments["DATE-OBS"]
            )

    def test_save_bias_frame(self, temp_config):
        handler, image, info, maxadu, device_name, exposure_start_datetime = (
            self._prepare_save_args(
                temp_config,
                [[10, 11], [12, 13]],
                {"IMAGETYP": "Bias Frame", "EXPTIME": 0.0},
                "bias_image_directory",
            )
        )
        handler.header["ASTRATYP"] = "calibration"
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        assert result.is_file()
        assert result.name.startswith("TestCamera_bias_0.000_")
        expected_path = (
            Path(temp_config.paths.images)
            / "bias_image_directory"
            / "20240515"
            / result.name
        )
        assert result == expected_path
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
        handler.header["ASTRATYP"] = "calibration"
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        assert result.is_file()
        assert result.name.startswith("TestCamera_dark_120.000_")
        expected_path = (
            Path(temp_config.paths.images)
            / "dark_image_directory"
            / "20240515"
            / result.name
        )
        assert result == expected_path
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
        handler.header["ASTRATYP"] = "flats"
        result = handler.save_image(
            image, info, maxadu, device_name, exposure_start_datetime
        )
        assert result.exists()
        assert result.is_file()
        assert result.name.startswith("TestCamera_V_flat_10.000_")
        expected_path = (
            Path(temp_config.paths.images)
            / "flat_image_directory"
            / "20240515"
            / result.name
        )
        assert result == expected_path
        with fits.open(result) as hdul:
            np.testing.assert_array_equal(hdul[0].data, [[30, 32], [31, 33]])
            assert hdul[0].header["IMAGETYP"] == "Flat Frame"
            assert hdul[0].header["EXPTIME"] == 10.0

    def test_save_image_no_image_directory_raises(self):
        header = self.create_test_header()
        handler = ImageHandler(header=header, image_directory=None)
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )
        with pytest.raises(ValueError, match="Image directory is not set."):
            handler.save_image(
                image, info, maxadu, device_name, exposure_start_datetime
            )

    def test_save_image_invalid_wcs(self, temp_config):
        header = self.create_test_header()
        image_directory = Path(temp_config.paths.images) / "neg_test"
        image_directory.mkdir(exist_ok=True)
        handler = ImageHandler(header, image_directory)
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )

        class BadWCS:
            def to_header(self):
                raise RuntimeError("WCS error!")

        with pytest.raises(RuntimeError, match="WCS error!"):
            handler.save_image(
                image, info, maxadu, device_name, exposure_start_datetime, wcs=BadWCS()
            )

    def test_save_image_missing_header_keys(self, temp_config):
        # Missing FILTER, IMAGETYP, OBJECT, EXPTIME
        header = ObservatoryHeader.get_test_header()
        image_directory = Path(temp_config.paths.images) / "neg_missing_keys"
        image_directory.mkdir(exist_ok=True)
        handler = ImageHandler(header, image_directory)
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )
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
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "TestCamera"
        exposure_start_datetime = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )
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
        exposure_start_datetime = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )
        with pytest.raises(Exception):
            handler.save_image(
                image, info, maxadu, device_name, exposure_start_datetime
            )

    def test_save_image_illegal_device_name(self, temp_config):
        header = self.create_test_header()
        image_directory = Path(temp_config.paths.images) / "neg_illegal_device"
        image_directory.mkdir(exist_ok=True)
        handler = ImageHandler(header, image_directory)
        image = np.array([[1, 2], [3, 4]])
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000
        device_name = "Test/Camera:Bad|Name"
        exposure_start_datetime = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )
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
            bad_templates = FilenameTemplates.from_dict(
                {"object": "{device}_{missing_arg}_{exptime:.3f}_{timestamp}.fits"}
            )
            handler = ImageHandler(header, image_directory, bad_templates)
            image = np.array([[1, 2], [3, 4]])
            info = self.create_mock_image_info(0, 2)
            maxadu = 1000
            device_name = "TestCamera"
            exposure_start_datetime = datetime.datetime(
                2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
            )
            handler.save_image(
                image, info, maxadu, device_name, exposure_start_datetime
            )

    def test_has_image_directory(self):
        header = ObservatoryHeader.get_test_header()
        handler = ImageHandler(header, image_directory=Path("/tmp/test"))
        assert handler.has_image_directory() is True
        handler = ImageHandler(header, image_directory=None)
        assert handler.has_image_directory() is False

    def test_set_image_dir(self, temp_config):
        # Test user-specified directory
        with tempfile.TemporaryDirectory() as temp_dir:
            user_dir = Path(temp_dir) / "custom"
            result = ImageHandler.set_image_dir(user_specified_dir=str(user_dir))
            assert result == user_dir
            assert user_dir.exists()
        # Test default (None)
        result_default = ImageHandler.set_image_dir()
        assert result_default == temp_config.paths.images

    @patch("astra.image_handler.HeaderManager.get_base_header")
    @patch("astra.image_handler.FilenameTemplates.from_dict")
    @patch("astra.image_handler.ImageHandler.get_observing_night_date")
    def test_from_action(self, mock_get_date, mock_from_dict, mock_get_header):
        mock_action = Mock()
        mock_action.action_value = {"dir": "/tmp/test"}
        mock_paired_devices = Mock()
        mock_observatory_config = Mock()
        mock_observatory_config.get.return_value = {}
        mock_fits_config = Mock()
        mock_logger = Mock()
        mock_get_header.return_value = ObservatoryHeader.get_test_header()
        mock_from_dict.return_value = FilenameTemplates()
        mock_get_date.return_value = datetime.datetime(2025, 1, 1)

        handler = ImageHandler.from_action(
            mock_action,
            mock_paired_devices,
            mock_observatory_config,
            mock_fits_config,
            mock_logger,
        )
        assert isinstance(handler, ImageHandler)
        assert handler.image_directory == Path("/tmp/test")
        assert handler.observing_date == datetime.datetime(2025, 1, 1)
        mock_get_date.assert_called_once_with(
            mock_action.start_time,
            mock_get_header.return_value.get_observatory_location(),
        )

    def test_init_missing_location_keys_raises(self):
        header = ObservatoryHeader.get_test_header()
        del header["LAT-OBS"]
        with pytest.raises(ValueError, match="LAT-OBS"):
            ImageHandler(header=header, image_directory=Path("/tmp/test"))

    @patch("astra.image_handler.HeaderManager.get_base_header")
    @patch("astra.image_handler.FilenameTemplates.from_dict")
    def test_from_action_missing_location_keys_raises(
        self, mock_from_dict, mock_get_header
    ):
        header = ObservatoryHeader.get_test_header()
        del header["LAT-OBS"]
        mock_get_header.return_value = header
        mock_from_dict.return_value = FilenameTemplates()
        mock_action = Mock()
        mock_action.action_value = {"dir": "/tmp/test"}
        mock_observatory_config = Mock()
        mock_observatory_config.get.return_value = {}
        with pytest.raises(ValueError, match="LAT-OBS"):
            ImageHandler.from_action(
                mock_action, Mock(), mock_observatory_config, Mock(), Mock()
            )

    def test_get_observing_night_date(self):
        from astropy.coordinates import EarthLocation

        lon0 = EarthLocation(lat=0, lon=0, height=0)
        lon_plus3 = EarthLocation(lat=0, lon=45, height=0)  # UTC+3
        lon_minus7 = EarthLocation(lat=0, lon=-105, height=0)  # UTC-7

        # --- Naive datetimes, lon=0 ---
        # Before noon -> previous night
        result = ImageHandler.get_observing_night_date(
            datetime.datetime(2025, 1, 1, 9, 0, 0), lon0
        )
        assert result == datetime.datetime(2024, 12, 31, 0, 0, 0)

        # After noon -> tonight
        result = ImageHandler.get_observing_night_date(
            datetime.datetime(2025, 1, 1, 22, 0, 0), lon0
        )
        assert result == datetime.datetime(2025, 1, 1, 0, 0, 0)

        # Exactly at local noon -> tonight (boundary)
        result = ImageHandler.get_observing_night_date(
            datetime.datetime(2025, 1, 1, 12, 0, 0), lon0
        )
        assert result == datetime.datetime(2025, 1, 1, 0, 0, 0)

        # --- UTC+ observatory: 13:45 UTC = 16:45 local -> tonight ---
        result = ImageHandler.get_observing_night_date(
            datetime.datetime(2026, 3, 20, 13, 45, 0), lon_plus3
        )
        assert result == datetime.datetime(2026, 3, 20, 0, 0, 0)

        # --- UTC-7 observatory: 13:45 UTC = 06:45 local -> previous night ---
        result = ImageHandler.get_observing_night_date(
            datetime.datetime(2026, 3, 20, 13, 45, 0), lon_minus7
        )
        assert result == datetime.datetime(2026, 3, 19, 0, 0, 0)

        # --- Timezone-aware (UTC) datetime ---
        result = ImageHandler.get_observing_night_date(
            datetime.datetime(2025, 1, 1, 9, 0, 0, tzinfo=datetime.UTC), lon0
        )
        assert result == datetime.datetime(2024, 12, 31, 0, 0, 0)

    def test_get_file_path(self, temp_config):
        header = ObservatoryHeader.get_test_header()
        header["ASTRATYP"] = "object"
        header["IMAGETYP"] = "Light Frame"
        header["FILTER"] = "V"
        header["OBJECT"] = "M31"
        header["EXPTIME"] = 60.0
        image_directory = Path(temp_config.paths.images) / "test"
        handler = ImageHandler(header, image_directory)
        handler.observing_date = datetime.datetime(
            2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC
        )
        date = datetime.datetime(2024, 5, 15, 12, 0, 0, tzinfo=datetime.UTC)
        filepath = handler.get_file_path("TestCamera", header, date, 0, image_directory)
        assert filepath.name.startswith("TestCamera_V_M31_60.000_")
        assert filepath.parent == image_directory / "20240515"

    def test_resolve_image_directory(self, temp_config):
        header = ObservatoryHeader.get_test_header()
        handler = ImageHandler(header, Path(temp_config.paths.images))
        # Test with None (uses instance directory)
        result = handler._resolve_image_directory(None)
        assert result == Path(temp_config.paths.images)
        # Test with relative path
        result_rel = handler._resolve_image_directory("subdir")
        assert result_rel == Path(temp_config.paths.images) / "subdir"
        # Test with absolute path
        abs_path = Path("/tmp/abs")
        result_abs = handler._resolve_image_directory(abs_path)
        assert result_abs == abs_path

    def test_image_directory_property(self):
        header = ObservatoryHeader.get_test_header()
        handler = ImageHandler(header)
        # Test setter
        handler.image_directory = "/tmp/new_dir"
        assert handler.image_directory == Path("/tmp/new_dir")
        # Test getter with None raises
        handler._image_directory = None
        with pytest.raises(ValueError, match="Image directory is not set."):
            _ = handler.image_directory
