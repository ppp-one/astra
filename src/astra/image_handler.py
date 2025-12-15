"""
Astronomical image processing and FITS file management utilities.

This module provides functions for handling astronomical images captured from
observatory cameras. It manages image directory creation, data type conversion,
and FITS file saving with proper headers and metadata.

Key features:
    - Automatic directory creation with date-based naming
    - Image data type conversion and array reshaping for FITS compatibility
    - FITS file saving with comprehensive metadata and WCS support
    - Intelligent filename generation based on observation parameters

The module handles various image types including light frames, bias frames,
dark frames, and calibration images, ensuring proper metadata preservation
and file organization for astronomical data processing pipelines.

"""

import datetime
import logging
from pathlib import Path
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from alpaca.camera import ImageMetadata
from astropy.coordinates import AltAz, EarthLocation, get_sun
from astropy.io import fits
from astropy.time import Time
from astropy.wcs.utils import WCS

from astra.config import Config
from astra.config import ObservatoryConfig
from astra.filename_templates import FilenameTemplates
from astra.header_manager import HeaderManager, ObservatoryHeader
from astra.logger import ObservatoryLogger
from astra.paired_devices import PairedDevices
from astra.scheduler import Action

__all__ = ["ImageHandler"]


class ImageHandler:
    """
    Class that stores image_directory and header.

    Attributes:
        header (fits.Header): FITS header template for images.
        image_directory (Path | None): Directory path to save images.
            If None, must be set before saving images.
        last_image_path (Path | None): Path of the last saved image.
        last_image_timestamp (datetime | None): Timestamp of the last saved image.
        filename_templates (FilenameTemplates): Templates for generating filenames.
            Uses Python str.format() syntax by default. For more advanced logic,
            use JinjaFilenameTemplates class.
        logger (logging.Logger): Logger for logging messages.

    Methods:
        save_image(...): Save an image as a FITS file with proper headers and filename.
        from_action(...): Create an ImageHandler instance from an action and observatory.
        get_observatory_location(): Get the observatory location as an EarthLocation object.
        has_image_directory(): Check if the image_directory is set.

    Examples:
        >>> from astra.image_handler import ImageHandler
        >>> from astra.header_manager import ObservatoryHeader
        >>> from pathlib import Path
        >>> header = ObservatoryHeader.get_test_header()
        >>> header['FILTER'] = 'V'
        >>> image_handler = ImageHandler(header=header, image_directory=Path("images"))
        >>> image_handler.image_directory
        PosixPath('images')
        >>> image_handler.header['FILTER']
        'V'
    """

    def __init__(
        self,
        header: ObservatoryHeader,
        image_directory: Path | None = None,
        filename_templates: FilenameTemplates | None = None,
        logger: logging.Logger | None = None,
        observing_date: datetime.datetime | None = None,
    ):
        self.header = header
        self._image_directory = Path(image_directory) if image_directory else None
        self.last_image_path: Path | None = None
        self.last_image_timestamp: datetime.datetime | None = None

        self.observing_date = (
            observing_date
            if observing_date is not None
            else self.get_default_observing_date()
        )
        self.filename_templates = (
            filename_templates
            if isinstance(filename_templates, FilenameTemplates)
            else FilenameTemplates()
        )
        self.logger = logging.getLogger(__name__) if logger is None else logger

    @property
    def image_directory(self) -> Path:
        if self._image_directory is None:
            raise ValueError("Image directory is not set.")
        return self._image_directory

    @image_directory.setter
    def image_directory(self, image_directory: Path | str) -> None:
        self._image_directory = Path(image_directory)

    def has_image_directory(self) -> bool:
        return self._image_directory is not None

    @classmethod
    def from_action(
        cls,
        action: Action,
        paired_devices: PairedDevices,
        observatory_config: ObservatoryConfig,
        fits_config: pd.DataFrame,
        logger: ObservatoryLogger,
    ):
        """Create ImageHandler from an action and observatory."""
        action_value = action.action_value
        header = HeaderManager.get_base_header(
            paired_devices, action_value, fits_config, logger
        )

        image_directory = cls.set_image_dir(user_specified_dir=action_value.get("dir"))

        filename_templates = FilenameTemplates.from_dict(
            observatory_config.get("Misc", {}).get("filename_templates", {})
        )
        location = header.get_observatory_location()
        observing_date = cls.get_observing_night_date(
            datetime.datetime.now(datetime.UTC), location
        )

        return cls(
            header=header,
            image_directory=image_directory,
            filename_templates=filename_templates,
            logger=logger,
            observing_date=observing_date,
        )

    def save_image(
        self,
        image: Union[List[int], np.ndarray],
        image_info: ImageMetadata,
        maxadu: int,
        device_name: str,
        exposure_start_datetime: datetime.datetime,
        sequence_counter: int = 0,
        header: ObservatoryHeader | None = None,
        image_directory: str | Path | None = None,
        wcs: Optional[WCS] = None,
    ) -> Path:
        """
        Save an astronomical image as a FITS file with proper headers and filename.

        Transforms raw image data, updates FITS headers with observation metadata,
        optionally adds WCS information, and saves as a FITS file with an automatically
        generated filename based on image properties.

        Parameters:
            image (list[int] | np.ndarray): Raw image data to save.
            image_info (ImageMetadata): Image metadata for data type determination.
            maxadu (int): Maximum ADU value for the image.
            header (fits.Header): FITS header containing FILTER, IMAGETYP, OBJECT, EXPTIME.
            device_name (str): Camera/device name for filename generation.
            exposure_start_datetime (datetime): UTC datetime when exposure started.
            image_directory (str): Subdirectory name within the images directory.
            wcs (WCS, optional): World Coordinate System information. Defaults to None.

        Returns:
            Path: Path to the saved FITS file.

        Note:
            Filename formats:
                - Light frames: "{device}_{filter}_{object}_{exptime}_{timestamp}.fits"
                - Bias/Dark: "{device}_{imagetype}_{exptime}_{timestamp}.fits"
                - Other: "{device}_{filter}_{imagetype}_{exptime}_{timestamp}.fits"

            Headers automatically updated with DATE-OBS, DATE, and WCS (if provided).
        """
        image_directory_path = self._resolve_image_directory(image_directory)

        if header is None:
            if self.header is None:
                raise ValueError("No FITS header specified to save image.")
            header = self.header

        image_array = self._transform_image_to_array(
            image, maxadu=maxadu, image_info=image_info
        )

        date = header.update_fits_header_times(exposure_start_datetime)

        # add WCS information
        if wcs:
            header.extend(wcs.to_header(), update=True)

        # create FITS HDU
        hdu = fits.PrimaryHDU(image_array, header=header)

        filepath = self.get_file_path(
            device_name=device_name,
            header=header,
            date=date,
            sequence_counter=sequence_counter,
            image_directory=image_directory_path,
        )

        # save FITS file
        hdu.writeto(filepath, output_verify="silentfix")

        self.last_image_path = filepath
        self.last_image_timestamp = date

        return filepath

    def get_file_path(
        self,
        device_name: str,
        header: fits.Header,
        date: datetime.datetime,
        sequence_counter: int,
        image_directory: Path,
    ) -> Path:
        """Generate a file path for saving an image based on metadata and templates."""
        filename = self.filename_templates.render_filename(
            action_type=str(header.get("ASTRATYP", "default")).lower(),
            device=device_name,
            imagetype=str(header.get("IMAGETYP", "default")),
            filter_name=str(header.get("FILTER", "NA")).replace("'", ""),
            object_name=header.get("OBJECT", "NA"),
            exptime=float(header.get("EXPTIME", float("nan"))),  # type: ignore
            sequence_counter=sequence_counter,
            timestamp=date.strftime("%Y%m%d_%H%M%S.%f")[:-3],
            datetime_timestamp=date,
            action_date=self.observing_date.strftime("%Y%m%d"),
            action_datetime=self.observing_date,
            datetime=datetime,
        )

        filepath = image_directory / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)

        return filepath

    def _resolve_image_directory(self, image_directory: str | Path | None) -> Path:
        """
        Resolve the image directory path, combining user-specified and default directories.
        """
        if image_directory is None:
            if self._image_directory is None:
                raise ValueError("Image directory is not set.")
            return self._image_directory
        if not isinstance(image_directory, Path):
            image_directory = Path(image_directory)
        if not image_directory.is_absolute():
            return Config().paths.images / image_directory
        return image_directory

    @staticmethod
    def set_image_dir(
        user_specified_dir: Optional[str] = None,
    ) -> Path | None:
        """
        Create a directory for storing astronomical images.

        Creates a directory for image storage using either a user-specified path
        or an auto-generated date-based path. The auto-generated path uses the
        local date calculated from the schedule start time and site longitude.

        Parameters:
            schedule_start_time (datetime, optional): Start time of the observing schedule.
                Defaults to current UTC time.
            site_long (float, optional): Site longitude in degrees for local time conversion.
                Defaults to 0.
            user_specified_dir (str | None, optional): Custom directory path. If provided,
                this overrides auto-generation. Defaults to None.

        Returns:
            Path: Path object pointing to the created directory.

        Note:
            Auto-generated directory format is YYYYMMDD based on local date calculated
            as schedule_start_time + (site_long / 15) hours.
        """
        if user_specified_dir:
            image_directory = Path(user_specified_dir)
            image_directory.mkdir(parents=True, exist_ok=True)
        else:
            image_directory = Config().paths.images

        return image_directory

    @staticmethod
    def _transform_image_to_array(
        image: Union[List[int], np.ndarray], maxadu: int, image_info: ImageMetadata
    ) -> np.ndarray:
        """
        Transform raw image data to a FITS-compatible numpy array.

        Converts raw image data to the appropriate data type and shape for FITS files.
        Handles data type selection based on image element type and maximum ADU value,
        and applies necessary array transpositions for FITS conventions.

        Parameters:
            image (list[int] | np.ndarray): Raw image data as list or numpy array.
            maxadu (int): Maximum ADU (Analog-to-Digital Unit) value for the image.
            image_info (ImageMetadata): Metadata containing ImageElementType (0-3) and
                Rank (2 for grayscale, 3 for color).

        Returns:
            np.ndarray: Properly shaped and typed array ready for FITS file creation.
                2D images are transposed, 3D images use transpose(2, 1, 0).

        Raises:
            ValueError: If ImageElementType is not in range 0-3.

        Note:
            ImageElementType mapping: 0,1→uint16; 2→uint16 (≤65535) or int32 (>65535); 3→float64.
            Transpose operations match FITS conventions where first axis = columns, second = rows.
        """
        if not isinstance(image, np.ndarray):
            image = np.array(image)

        # Determine the image data type
        if image_info.ImageElementType == 0 or image_info.ImageElementType == 1:
            imgDataType = np.uint16
        elif image_info.ImageElementType == 2:
            if maxadu <= 65535:
                imgDataType = np.uint16  # Required for BZERO & BSCALE to be written
            else:
                imgDataType = np.int32
        elif image_info.ImageElementType == 3:
            imgDataType = np.float64
        else:
            raise ValueError(f"Unknown ImageElementType: {image_info.ImageElementType}")

        # Make a numpy array of the correct shape for astropy.io.fits
        if image_info.Rank == 2:
            image_array = np.array(image, dtype=imgDataType).transpose()
        else:
            image_array = np.array(image, dtype=imgDataType).transpose(2, 1, 0)

        return image_array

    def get_observatory_location(self):
        return self.header.get_observatory_location()

    @staticmethod
    def get_observing_night_date(
        observation_time: datetime.datetime, location: EarthLocation
    ) -> datetime.datetime:
        """
        Calculate the observing night date based on the sun's position.

        If the sun is up, the date is the current local date.
        If the sun is down:
            - If it's morning (before noon), the date is yesterday.
            - If it's evening (after noon), the date is today.

        Parameters:
            observation_time (datetime.datetime): The time of observation (UTC).
            location (EarthLocation): The location of the observatory.

        Returns:
            datetime.datetime: The observing night date (at midnight).
        """
        # Calculate sun altitude
        time = Time(observation_time, location=location)
        sun = get_sun(time)
        altaz = sun.transform_to(AltAz(obstime=time, location=location))

        # Get local time
        longitude = location.lon.deg
        local_time = observation_time + datetime.timedelta(hours=longitude / 15)

        if altaz.alt.deg > 0:
            # Sun is Up -> Today
            obs_date = local_time.date()
        else:
            # Sun is Down
            if local_time.hour < 12:
                # Morning -> Yesterday
                obs_date = local_time.date() - datetime.timedelta(days=1)
            else:
                # Evening -> Today
                obs_date = local_time.date()

        return datetime.datetime.combine(obs_date, datetime.time.min)

    @staticmethod
    def get_default_observing_date(longitude: float = 0):
        dt = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
            hours=longitude / 15
        )
        return datetime.datetime.combine(dt.date(), datetime.time.min)

    def __repr__(self):
        return (
            f"ImageHandler(header={dict(self.header)}, "
            f"image_directory={self.image_directory}, "
            f"last_image_path={self.last_image_path}, "
            f"last_image_timestamp={self.last_image_timestamp}, "
            f"filename_templates={self.filename_templates})"
        )
