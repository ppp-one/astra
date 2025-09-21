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

Example:
    # Create directory and save an astronomical image
    folder = create_image_dir(schedule_start_time, site_longitude)
    filepath = save_image(
        image_data, image_info, maxadu, header,
        camera_name, obs_time, folder_name
    )
"""

import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Union

import numpy as np
from alpaca.camera import ImageMetadata
from astropy.io import fits
from astropy.wcs.utils import WCS
from jinja2 import Template

import astra
from astra import Config
from astra.paired_devices import PairedDevices
from astra.scheduler import Action

__all__ = ["ImageHandler", "FilenameTemplates", "JinjaFilenameTemplates"]


@dataclass
class FilenameTemplates:
    """

    Examples:

    >>> from astra.image_handler import FilenameTemplates

    Default templates
    >>> templates = FilenameTemplates()
    >>> templates.render_filename(
    ... **templates.TEST_ARGS | {"imagetype": "light frame"}
    ... )
    'TestCamera_TestFilter_TestObject_300.123_2025-01-01_00-00-00.fits'

    Lets create a template with more advanced logic using using Jinja2
    >>> flat_template = (
    ...     # use subdirs
    ...     "{{ imagetype.split('_')[0].upper() }}/{{ device }}_"
    ...     # customise timestamp format
    ...     + "{{ datetime_timestamp.strftime('%Y%m%d_%H%M%S.%f')[:-5] }}_"
    ...     # Add custom logic
    ...     + "{{ 'Dusk' if (datetime_timestamp + datetime.timedelta(hours=5)).hour > 12 else 'Dawn' }}"
    ...     + ".fits"
    ... )
    >>> filename_templates = FilenameTemplates.from_dict(
    ...     {"flat": flat_template}
    ... )
    >>> filename_templates.render_filename(
    ... **filename_templates.TEST_ARGS | {"imagetype": "flat frame"}
    ... )
    'FLAT/TestCamera_20250101_000000.0_Dawn.fits'

    """

    light: str = "{device}_{filter_name}_{object_name}_{exptime:.3f}_{timestamp}.fits"
    bias: str = "{device}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
    dark: str = "{device}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
    flat: str = "{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
    default: str = "{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"

    TEST_ARGS = {
        "device": "TestCamera",
        "filter_name": "TestFilter",
        "object_name": "TestObject",
        "imagetype": "light",
        "exptime": 300.123456,
        "timestamp": "2025-01-01_00-00-00",
        "datetime_timestamp": datetime.datetime(2025, 1, 1, 0, 0, 0, 0),
        "datetime": datetime,
        "sequence": 0,
    }
    SEQUENCES = ["light", "bias", "dark", "flat", "default"]

    @property
    def SUPPORTED_ARGS(self) -> set[str]:
        return set(self.TEST_ARGS.keys())

    def __post_init__(self):
        if self._has_jinja_templates([getattr(self, key) for key in self.SEQUENCES]):
            raise ValueError(
                "FilenameTemplates contains Jinja2 syntax. "
                "Please use JinjaFilenameTemplates class instead."
            )
        self._validate()

    @classmethod
    def from_dict(cls, template_dict: dict[str, str]) -> "FilenameTemplates":
        valid_keywords = {
            key: value for key, value in template_dict.items() if key in cls.SEQUENCES
        }

        if cls._has_jinja_templates(list(valid_keywords.values())):
            return JinjaFilenameTemplates(**valid_keywords)  # type: ignore

        return cls(**valid_keywords)

    def render_filename(self, imagetype, **kwargs) -> str:
        imagetype_standardised = self._get_imagetype(imagetype)

        return getattr(self, imagetype_standardised).format(
            imagetype=imagetype_standardised, **kwargs
        )

    def _get_template(self, imagetype: str) -> str:
        if not hasattr(self, imagetype):
            imagetype = self._get_imagetype(imagetype)
        return getattr(self, imagetype)

    def _get_imagetype(self, imagetype: str) -> str:
        imagetype_lower = imagetype.lower()
        for name in self.SEQUENCES:
            if name in imagetype_lower:
                return name
        return "default"

    def _validate(self):
        for name in self.SEQUENCES:
            if not name.startswith("_"):
                try:
                    self.render_filename(**self.TEST_ARGS | {"imagetype": name})
                except Exception as e:
                    raise ValueError(
                        f"Error rendering template for '{name}'. "
                        f"Template: '{getattr(self, name)}'. Exception: {e}."
                    )

    @staticmethod
    def _has_jinja_templates(templates: list) -> bool:
        return any(["{{" in item and "}}" in item for item in templates])


@dataclass
class JinjaFilenameTemplates(FilenameTemplates):
    light: str = "{{ device }}_{{ filter_name }}_{{ object_name }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
    bias: str = (
        "{{ device }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
    )
    dark: str = (
        "{{ device }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
    )
    flat: str = "{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
    default: str = "{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"

    _compiled_templates: dict[str, Template] = field(default_factory=dict)

    def __post_init__(self):
        self._validate_templates()
        self._compiled_templates = {}

        for name in self.SEQUENCES:
            template_str = getattr(self, name)
            self._compiled_templates[name] = Template(template_str)

        self._validate()

    def render_filename(self, imagetype, **kwargs) -> str:
        imagetype_standardised = self._get_imagetype(imagetype)

        return self._compiled_templates[imagetype_standardised].render(
            imagetype=imagetype_standardised, **kwargs
        )

    def _validate_templates(self):
        import re

        pattern = re.compile(r"{{\s*([\w]+)[^}]*}}")
        for name in self.SEQUENCES:
            template = getattr(self, name)
            if not isinstance(template, str):
                continue
            for match in pattern.findall(template):
                if match not in self.SUPPORTED_ARGS:
                    raise ValueError(
                        f"Template '{name}' uses unsupported argument: {{{{{match}}}}}."
                    )


class ImageHandler:
    """
    Class that stores folder and header.
    Can be passed around instead of (folder, hdr) tuple.
    # TODO rename folder to directory?
    """

    def __init__(
        self,
        header: fits.Header,
        folder: Path | None = None,
        filename_templates: FilenameTemplates | None = None,
    ):
        self.header = header
        self._folder = folder
        self.last_image_path: Path | None = None
        self.last_image_timestamp: datetime.datetime | None = None

        self.filename_templates = (
            filename_templates
            if isinstance(filename_templates, FilenameTemplates)
            else FilenameTemplates()
        )

    @property
    def folder(self) -> Path:
        if self._folder is None:
            raise ValueError("No folder specified to save image.")
        return self._folder

    @folder.setter
    def folder(self, folder: Path | str) -> None:
        self._folder = Path(folder)

    def has_folder(self) -> bool:
        return self._folder is not None

    @classmethod
    def from_action(
        cls,
        action: Action | dict,
        observatory: "astra.Observatory",  # type: ignore
        paired_devices: PairedDevices,
        create_folder=True,
    ):
        action_value = action["action_value"]
        hdr = observatory.base_header(paired_devices, action_value)
        cls._add_action_and_image_type(action, observatory, hdr)

        folder = cls.create_image_dir(
            schedule_start_time=action_value.get(
                "schedule_start_time", datetime.datetime.now(datetime.UTC)
            ),
            site_long=hdr.get("LONG-OBS"),
            user_specified_dir=action_value.get("dir"),
            create_folder=create_folder,
        )

        filename_templates = FilenameTemplates.from_dict(
            observatory.config.get("Misc", {}).get("filename_templates", {})
        )

        return cls(
            header=hdr,
            folder=folder,
            filename_templates=filename_templates,
        )

    def save_image(
        self,
        image: Union[List[int], np.ndarray],
        image_info: ImageMetadata,
        maxadu: int,
        device_name: str,
        exposure_start_datetime: datetime.datetime,
        image_sequence_number: int = 0,
        hdr: fits.Header | None = None,
        folder: str | Path | None = None,
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
            hdr (fits.Header): FITS header containing FILTER, IMAGETYP, OBJECT, EXPTIME.
            device_name (str): Camera/device name for filename generation.
            exposure_start_datetime (datetime): UTC datetime when exposure started.
            folder (str): Subfolder name within the images directory.
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
        if folder is None:
            if self.folder is None:
                raise ValueError("No folder specified to save image.")
            folder = self.folder

        if hdr is None:
            if self.header is None:
                raise ValueError("No FITS header specified to save image.")
            hdr = self.header

        image_array = self._transform_image_to_array(
            image, maxadu=maxadu, image_info=image_info
        )

        date = self._update_fits_header_times(hdr, exposure_start_datetime)

        # add WCS information
        if wcs:
            hdr.extend(wcs.to_header(), update=True)

        # create FITS HDU
        hdu = fits.PrimaryHDU(image_array, header=hdr)

        filename = self.filename_templates.render_filename(
            device=device_name,
            filter_name=str(hdr.get("FILTER", "NA")).replace("'", ""),
            object_name=hdr.get("OBJECT", "NA"),
            imagetype=str(hdr.get("IMAGETYP", "default")),
            exptime=float(hdr.get("EXPTIME", float("nan"))),  # type: ignore
            timestamp=date.strftime("%Y%m%d_%H%M%S.%f")[:-3],
            timestamp_date=date.strftime("%Y%m%d"),
            timestamp_time=date.strftime("%H%M%S.%f")[:-3],
            sequence=image_sequence_number,
        )
        filepath = Config().paths.images / folder / filename

        # Ensure that directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)

        # save FITS file
        hdu.writeto(filepath)

        return filepath

    @staticmethod
    def create_image_dir(
        schedule_start_time: datetime.datetime | None = None,
        site_long: float = 0,
        user_specified_dir: Optional[str] = None,
        create_folder: bool = True,
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
        if not create_folder:
            return None

        if schedule_start_time is None:
            schedule_start_time = datetime.datetime.now(datetime.UTC)

        if user_specified_dir:
            folder = Path(user_specified_dir)
        else:
            date_str = (
                schedule_start_time + datetime.timedelta(hours=site_long / 15)
            ).strftime("%Y%m%d")
            folder = Config().paths.images / date_str
        folder.mkdir(parents=True, exist_ok=True)

        return folder

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

    def _update_fits_header_times(
        self, header: fits.Header, exposure_start_datetime: datetime.datetime
    ) -> datetime.datetime:
        header["DATE-OBS"] = (
            exposure_start_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "UTC datetime file written",
        )
        date = datetime.datetime.now(datetime.UTC)
        header["DATE"] = (
            date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "UTC datetime start of exposure",
        )
        return date

    @staticmethod
    def _add_action_and_image_type(row, observatory, header):
        if "object" == row["action_type"]:
            header["IMAGETYP"] = "Light Frame"
        elif "flats" == row["action_type"]:
            if observatory.speculoos:
                header["IMAGETYP"] = "FLAT"
            else:
                header["IMAGETYP"] = "Flat Frame"

        observatory.logger.debug(
            f"Finished pre_sequence for {row['device_name']} {row['action_type']} {row['action_value']}"
        )

    def set_imagetype_header(self, action_type: str, use_light: bool) -> bool:
        if action_type == "calibration":
            if self.header["EXPTIME"] == 0:
                self.header["IMAGETYP"] = "Bias Frame"
                use_light = False
            else:
                self.header["IMAGETYP"] = "Dark Frame"
                use_light = False
        elif action_type == "object":
            self.header["IMAGETYP"] = "Light Frame"
            use_light = True

        return use_light

    def get_observatory_location(self):
        from astropy import units as u
        from astropy.coordinates import EarthLocation

        obs_lat: float = self.header["LAT-OBS"]  # type: ignore
        obs_lon: float = self.header["LONG-OBS"]  # type: ignore
        obs_alt: float = self.header["ALT-OBS"]  # type: ignore
        obs_location = EarthLocation(
            lat=u.Quantity(obs_lat, u.deg),
            lon=u.Quantity(obs_lon, u.deg),
            height=u.Quantity(obs_alt, u.m),
        )
        return obs_location
