"""Unit tests for image_handler module."""

import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pytest
from alpaca.camera import ImageMetadata
from astropy.io import fits
from astropy.wcs import WCS

from astra.image_handler import create_image_dir, save_image, transform_image_to_array


class TestCreateImageDir:
    """Tests for create_image_dir function."""

    def test_user_specified_dir_creation(self):
        """Test creating directory with user-specified path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            user_dir = Path(temp_dir) / "custom_dir"

            result = create_image_dir(user_specified_dir=str(user_dir))

            assert result == user_dir
            assert user_dir.exists()
            assert user_dir.is_dir()

    def test_user_specified_dir_already_exists(self):
        """Test behavior when user-specified directory already exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            user_dir = Path(temp_dir) / "existing_dir"
            user_dir.mkdir()

            result = create_image_dir(user_specified_dir=str(user_dir))

            assert result == user_dir
            assert user_dir.exists()

    def test_auto_generated_dir_creation(self, temp_config):
        """Test creating directory with auto-generated date-based path."""
        schedule_time = datetime(2024, 5, 15, 10, 0, 0, tzinfo=UTC)
        site_long = -120.0  # 8 hours west

        result = create_image_dir(schedule_time, site_long)

        # Local date should be 2024-05-15 10:00 - 8:00 = 2024-05-15 02:00
        expected_date = "20240515"
        expected_path = Path(temp_config.paths.images) / expected_date

        assert result == expected_path
        assert expected_path.exists()
        assert expected_path.is_dir()

    def test_auto_generated_dir_date_calculation(self, temp_config):
        """Test date calculation for auto-generated directory."""
        # Test crossing date boundary
        schedule_time = datetime(2024, 5, 15, 2, 0, 0, tzinfo=UTC)
        site_long = 120.0  # 8 hours east

        result = create_image_dir(schedule_time, site_long)

        # Local date should be 2024-05-15 02:00 + 8:00 = 2024-05-15 10:00
        expected_date = "20240515"
        expected_path = Path(temp_config.paths.images) / expected_date

        assert result == expected_path

    def test_default_parameters(self, temp_config):
        """Test function with default parameters."""

        with patch("astra.image_handler.datetime") as mock_datetime:
            mock_now = datetime.now(UTC)
            mock_datetime.now.return_value = mock_now
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(
                *args, **kwargs
            )

            result = create_image_dir()

            expected_date = mock_now.strftime("%Y%m%d")
            expected_path = Path(temp_config.paths.images) / expected_date

            assert result == expected_path
            assert expected_path.exists()


class TestTransformImageToArray:
    """Tests for transform_image_to_array function."""

    def create_mock_image_info(self, element_type: int, rank: int) -> ImageMetadata:
        """Helper to create mock ImageMetadata."""
        mock_info = Mock(spec=ImageMetadata)
        mock_info.ImageElementType = element_type
        mock_info.Rank = rank
        return mock_info

    def test_uint16_element_type_0(self):
        """Test transformation with ImageElementType 0."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000

        result = transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        assert result.shape == (2, 2)
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_uint16_element_type_1(self):
        """Test transformation with ImageElementType 1."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(1, 2)
        maxadu = 1000

        result = transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_uint16_element_type_2_low_maxadu(self):
        """Test transformation with ImageElementType 2 and low maxadu."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(2, 2)
        maxadu = 65535

        result = transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16

    def test_int32_element_type_2_high_maxadu(self):
        """Test transformation with ImageElementType 2 and high maxadu."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(2, 2)
        maxadu = 70000

        result = transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.int32

    def test_float64_element_type_3(self):
        """Test transformation with ImageElementType 3."""
        image = [[1.5, 2.7], [3.1, 4.9]]
        info = self.create_mock_image_info(3, 2)
        maxadu = 1000

        result = transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.float64
        np.testing.assert_array_almost_equal(result, [[1.5, 3.1], [2.7, 4.9]])

    def test_invalid_element_type(self):
        """Test error handling for invalid ImageElementType."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(4, 2)
        maxadu = 1000

        with pytest.raises(ValueError, match="Unknown ImageElementType: 4"):
            transform_image_to_array(image, maxadu, info)

    def test_3d_image_rank_3(self):
        """Test transformation with 3D image (Rank 3)."""
        # RGB image: 2x2 pixels, 3 channels
        image = [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]]
        info = self.create_mock_image_info(0, 3)
        maxadu = 1000

        result = transform_image_to_array(image, maxadu, info)

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

        result = transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])

    def test_list_input(self):
        """Test transformation with list input."""
        image = [[1, 2], [3, 4]]
        info = self.create_mock_image_info(0, 2)
        maxadu = 1000

        result = transform_image_to_array(image, maxadu, info)

        assert result.dtype == np.uint16
        np.testing.assert_array_equal(result, [[1, 3], [2, 4]])


class TestSaveImage:
    """Tests for save_image function."""

    def create_mock_image_info(
        self, element_type: int = 0, rank: int = 2
    ) -> ImageMetadata:
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

    @patch("astra.image_handler.datetime")
    def test_save_light_frame(self, mock_datetime, temp_config):
        """Test saving a light frame image."""

        # Mock datetime for consistent filename
        mock_now = datetime(2024, 5, 15, 12, 30, 45, 123456, tzinfo=UTC)
        mock_datetime.now.return_value = mock_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        image = [[100, 200], [300, 400]]
        info = self.create_mock_image_info()
        maxadu = 65535
        header = self.create_test_header()
        device_name = "TestCamera"
        dateobs = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        folder = "test_folder"

        filepath = Path(temp_config.paths.images) / folder
        filepath.mkdir(exist_ok=True)

        result = save_image(image, info, maxadu, header, device_name, dateobs, folder)

        # Check file was created
        assert result.exists()
        assert result.is_file()

        # Check filename format for light frame
        expected_filename = "TestCamera_V_M31_60.000_20240515_123045.123.fits"
        assert result.name == expected_filename

        # Check file path
        expected_path = Path(temp_config.paths.images) / folder / expected_filename
        assert result == expected_path

        # Verify FITS file content
        with fits.open(result) as hdul:
            # Check data
            np.testing.assert_array_equal(hdul[0].data, [[100, 300], [200, 400]])
            # Check headers
            assert hdul[0].header["DATE-OBS"] == "2024-05-15T12:00:00.000000"
            assert hdul[0].header["DATE"] == "2024-05-15T12:30:45.123456"
            assert "UTC datetime file written" in hdul[0].header.comments["DATE-OBS"]

            assert "UTC datetime start of exposure" in hdul[0].header.comments["DATE"]

    @patch("astra.image_handler.datetime")
    def test_save_bias_frame(self, mock_datetime, temp_config):
        """Test saving a bias frame image."""

        mock_now = datetime(2024, 5, 15, 12, 30, 45, 123456, tzinfo=UTC)
        mock_datetime.now.return_value = mock_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        image = [[50, 51], [52, 53]]
        info = self.create_mock_image_info()
        maxadu = 65535
        header = self.create_test_header(IMAGETYP="Bias Frame", EXPTIME=0.0)
        device_name = "TestCamera"
        dateobs = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        folder = "bias_folder"

        filepath = Path(temp_config.paths.images) / folder
        filepath.mkdir(exist_ok=True)

        result = save_image(image, info, maxadu, header, device_name, dateobs, folder)

        # Check filename format for bias frame
        expected_filename = "TestCamera_Bias Frame_0.000_20240515_123045.123.fits"
        assert result.name == expected_filename

    @patch("astra.image_handler.datetime")
    def test_save_dark_frame(self, mock_datetime, temp_config):
        """Test saving a dark frame image."""
        mock_now = datetime(2024, 5, 15, 12, 30, 45, 123456, tzinfo=UTC)
        mock_datetime.now.return_value = mock_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        image = [[75, 76], [77, 78]]
        info = self.create_mock_image_info()
        maxadu = 65535
        header = self.create_test_header(IMAGETYP="Dark Frame", EXPTIME=120.0)
        device_name = "TestCamera"
        dateobs = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        folder = "dark_folder"

        filepath = Path(temp_config.paths.images) / folder
        filepath.mkdir(exist_ok=True)

        result = save_image(image, info, maxadu, header, device_name, dateobs, folder)

        # Check filename format for dark frame
        expected_filename = "TestCamera_Dark Frame_120.000_20240515_123045.123.fits"
        assert result.name == expected_filename

    @patch("astra.image_handler.datetime")
    def test_save_other_frame_type(self, mock_datetime, temp_config):
        """Test saving a frame with other image type."""

        mock_now = datetime(2024, 5, 15, 12, 30, 45, 123456, tzinfo=UTC)
        mock_datetime.now.return_value = mock_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        image = [[150, 151], [152, 153]]
        info = self.create_mock_image_info()
        maxadu = 65535
        header = self.create_test_header(IMAGETYP="Flat Frame", EXPTIME=10.0)
        device_name = "TestCamera"
        dateobs = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        folder = "flat_folder"

        filepath = Path(temp_config.paths.images) / folder
        filepath.mkdir(exist_ok=True)

        result = save_image(image, info, maxadu, header, device_name, dateobs, folder)

        # Check filename format for other frame type
        expected_filename = "TestCamera_V_Flat Frame_10.000_20240515_123045.123.fits"
        assert result.name == expected_filename

    @patch("astra.image_handler.datetime")
    def test_save_with_wcs(self, mock_datetime, temp_config):
        """Test saving image with WCS information."""

        mock_now = datetime(2024, 5, 15, 12, 30, 45, 123456, tzinfo=UTC)
        mock_datetime.now.return_value = mock_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        # Create a simple WCS
        wcs = WCS(naxis=2)
        wcs.wcs.crpix = [100, 100]
        wcs.wcs.cdelt = [-0.0001, 0.0001]
        wcs.wcs.crval = [180.0, 45.0]
        wcs.wcs.ctype = ["RA---TAN", "DEC--TAN"]

        image = [[100, 200], [300, 400]]
        info = self.create_mock_image_info()
        maxadu = 65535
        header = self.create_test_header()
        device_name = "TestCamera"
        dateobs = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        folder = "wcs_folder"

        filepath = Path(temp_config.paths.images) / folder
        filepath.mkdir(exist_ok=True)

        result = save_image(
            image, info, maxadu, header, device_name, dateobs, folder, wcs=wcs
        )

        # Verify WCS was added to header
        with fits.open(result) as hdul:
            header_keys = list(hdul[0].header.keys())
            assert "CRPIX1" in header_keys
            assert "CRPIX2" in header_keys
            assert "CRVAL1" in header_keys
            assert "CRVAL2" in header_keys
            assert "CTYPE1" in header_keys
            assert "CTYPE2" in header_keys

    @patch("astra.image_handler.datetime")
    def test_filter_name_cleanup(self, mock_datetime, temp_config):
        """Test that filter names with quotes are cleaned up in filename."""
        mock_now = datetime(2024, 5, 15, 12, 30, 45, 123456, tzinfo=UTC)
        mock_datetime.now.return_value = mock_now
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        image = [[100, 200], [300, 400]]
        info = self.create_mock_image_info()
        maxadu = 65535
        header = self.create_test_header(FILTER="'B'")  # Filter name with quotes
        device_name = "TestCamera"
        dateobs = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        folder = "test_folder"

        filepath = Path(temp_config.paths.images) / folder
        filepath.mkdir(exist_ok=True)

        result = save_image(image, info, maxadu, header, device_name, dateobs, folder)

        # Check that quotes were removed from filter name in filename
        expected_filename = "TestCamera_B_M31_60.000_20240515_123045.123.fits"
        assert result.name == expected_filename

    @patch("astra.image_handler.transform_image_to_array")
    def test_transform_function_called(self, mock_transform, temp_config):
        """Test that transform_image_to_array is called with correct parameters."""
        # Mock the transform function to return a simple array
        mock_transform.return_value = np.array([[1, 2], [3, 4]], dtype=np.uint16)

        image = [[100, 200], [300, 400]]
        info = self.create_mock_image_info()
        maxadu = 65535
        header = self.create_test_header()
        device_name = "TestCamera"
        dateobs = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        folder = "test_folder"

        filepath = Path(temp_config.paths.images) / folder
        filepath.mkdir(exist_ok=True)

        save_image(image, info, maxadu, header, device_name, dateobs, folder)

        # Verify transform function was called with correct parameters
        mock_transform.assert_called_once_with(image, maxadu=maxadu, image_info=info)

    def test_header_preservation(self, temp_config):
        """Test that original header values are preserved."""

        image = [[100, 200], [300, 400]]
        info = self.create_mock_image_info()
        maxadu = 65535
        header = self.create_test_header()
        header["CUSTOM"] = ("test_value", "Custom test header")
        device_name = "TestCamera"
        dateobs = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
        folder = "test_folder"

        filepath = Path(temp_config.paths.images) / folder
        filepath.mkdir(exist_ok=True)

        result = save_image(image, info, maxadu, header, device_name, dateobs, folder)

        # Verify original headers are preserved
        with fits.open(result) as hdul:
            assert hdul[0].header["FILTER"] == "V"
            assert hdul[0].header["IMAGETYP"] == "Light Frame"
            assert hdul[0].header["OBJECT"] == "M31"
            assert hdul[0].header["EXPTIME"] == 60.0
            assert hdul[0].header["CUSTOM"] == "test_value"

    def test_edge_cases_exptime_precision(self, temp_config):
        """Test filename generation with various exposure time precisions."""
        mock_now = datetime(2024, 5, 15, 12, 30, 45, 123456, tzinfo=UTC)
        with patch("astra.image_handler.datetime") as mock_datetime:
            mock_datetime.now.return_value = mock_now
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(
                *args, **kwargs
            )

            image = [[100, 200], [300, 400]]
            info = self.create_mock_image_info()
            maxadu = 65535
            header = self.create_test_header(EXPTIME=1.2346)
            device_name = "TestCamera"
            dateobs = datetime(2024, 5, 15, 12, 0, 0, tzinfo=UTC)
            folder = "test_folder"
            filepath = Path(temp_config.paths.images) / folder
            filepath.mkdir(exist_ok=True)

            result = save_image(
                image, info, maxadu, header, device_name, dateobs, folder
            )

            # Check that exposure time is formatted to 3 decimal places
            assert "1.235" in result.name
