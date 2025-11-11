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
import time

import numpy as np
import pandas as pd
from astropy import units as u
from astropy.coordinates import EarthLocation, SkyCoord
from astropy.io import fits

import astra
from astra import utils
from astra.config import ObservatoryConfig
from astra.database_manager import DatabaseManager
from astra.logger import ObservatoryLogger
from astra.paired_devices import PairedDevices
from astra.scheduler import Action, BaseActionConfig

__all__ = ["HeaderManager", "ObservatoryHeader"]


class ObservatoryHeader(fits.Header):
    """A FITS header subclass with observatory-specific properties and methods.

    Attributes:
        REQUIRED_KEYS (list): List of required header keys for validation.
    Properties:
        ra (float): Right Ascension in hours.
        dec (float): Declination in degrees.
        airmass (float): Airmass value.

    Methods:
        validate(): Validate that all required keys are present in the header.
        convert_ra_from_hours_to_degrees(): Convert RA from hours to degrees.
        get_target_sky_coordinates(): Get target coordinates as a SkyCoord object.
        get_observatory_location(): Get observatory location as an EarthLocation object.
        get_test_header(): Class method to generate a test header with sample values.

    Examples:
        >>> from astra.image_handler import ObservatoryHeader
        >>> header = ObservatoryHeader.get_test_header()
        >>> header.validate()

    """

    REQUIRED_KEYS = [
        "LONG-OBS",
        "LAT-OBS",
        "RA",
        "DEC",
        "EXPTIME",
        "DATE-OBS",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def validate(self):
        missing = [k for k in self.REQUIRED_KEYS if k not in self]
        if missing:
            raise ValueError(f"Missing required header keys: {missing}")

    @property
    def ra(self) -> float:
        """Right Ascension"""
        return float(self["RA"])  # type: ignore

    @property
    def dec(self) -> float:
        return float(self["DEC"])  # type: ignore

    @property
    def airmass(self):
        return float(self["AIRMASS"])  # type: ignore

    def convert_ra_from_hours_to_degrees(self):
        self["RA"] = self.ra * (360 / 24)

    def get_target_sky_coordinates(self) -> SkyCoord:
        return SkyCoord(self["RA"], self["DEC"], unit=(u.deg, u.deg), frame="icrs")

    def get_observatory_location(self):
        obs_lat: float = self["LAT-OBS"]  # type: ignore
        obs_lon: float = self["LONG-OBS"]  # type: ignore
        obs_alt: float = self["ALT-OBS"]  # type: ignore
        return EarthLocation(
            lat=u.Quantity(obs_lat, u.deg),
            lon=u.Quantity(obs_lon, u.deg),
            height=u.Quantity(obs_alt, u.m),
        )

    @classmethod
    def get_test_header(cls) -> "ObservatoryHeader":
        header = cls(
            {
                "LONG-OBS": -70.403,
                "LAT-OBS": -24.625,
                "ALT-OBS": 2400,
                "RA": 14.053488,
                "DEC": -47.3756,
                "AIRMASS": 1.2,
                "DATE-END": "2025-01-01T00:30:00.000",
                "ALTITUDE": 2400,
                "EXPTIME": 300,
                "DATE-OBS": "2025-01-01T00:00:00.000",
            }
        )
        return header

    def update_fits_header_times(
        self, exposure_start_datetime: datetime.datetime
    ) -> datetime.datetime:
        self["DATE-OBS"] = (
            exposure_start_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "UTC datetime start of exposure",
        )
        date = datetime.datetime.now(datetime.UTC)
        self["DATE"] = (
            date.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "UTC datetime file written",
        )
        return date

    def set_imagetype(self, action_type: str, use_light: bool) -> bool:
        """Set the IMAGETYP header based on action type.
        Returns True if the image is a light frame, False otherwise.
        """
        use_light = True
        if action_type == "calibration":
            if self["EXPTIME"] == 0:
                self["IMAGETYP"] = "Bias Frame"
            else:
                self["IMAGETYP"] = "Dark Frame"
            use_light = False
        elif action_type == "flats":
            self["IMAGETYP"] = "Flat Frame"
        elif action_type in ["object", "autofocus", "calibrate_guiding", "pointing"]:
            self["IMAGETYP"] = "Light Frame"
        else:
            self["IMAGETYP"] = "Unknown Frame"

        return use_light

    def set_action_type(self, action: Action) -> None:
        """Set the action type related headers based on the Action object."""
        self["ASTRATYP"] = (action.action_type, "Type of action performed by astra")

    def add_times(
        self, fits_config: pd.DataFrame, location: EarthLocation, target: SkyCoord
    ) -> None:
        """Add comprehensive time information to FITS header.

        Calculates and adds various time formats to FITS header including
        Julian Day variants, Modified Julian Day, and astronomical time corrections.
        Also computes airmass from altitude.

        Args:
            self (dict): FITS header dictionary to modify in-place.
            fits_config (pd.DataFrame): Configuration with header specifications.
            location (EarthLocation): Observer's geographic location.
            target (SkyCoord): Target celestial coordinates.
        """
        exposure_start_datetime = pd.to_datetime(self["DATE-OBS"])  # type : ignore

        dateend = exposure_start_datetime + datetime.timedelta(
            seconds=float(self["EXPTIME"])  # type : ignore
        )
        jd = utils.to_jd(exposure_start_datetime)
        jdend = utils.to_jd(dateend)

        mjd = jd - 2400000.5
        mjdend = jdend - 2400000.5

        hjd, bjd, lstsec, ha = utils.time_conversion(jd, location, target)

        for row_header, row in fits_config[fits_config["fixed"] == False].iterrows():  # noqa: E712
            if row["device_type"] == "astra":
                if row_header == "JD-OBS":
                    self[row_header] = (jd, row["comment"])
                elif row_header == "JD-END":
                    self[row_header] = (jdend, row["comment"])
                elif row_header == "HJD-OBS":
                    self[row_header] = (hjd, row["comment"])
                elif row_header == "BJD-OBS":
                    self[row_header] = (bjd, row["comment"])
                elif row_header == "MJD-OBS":
                    self[row_header] = (mjd, row["comment"])
                elif row_header == "MJD-END":
                    self[row_header] = (mjdend, row["comment"])
                elif row_header == "DATE-END":
                    self[row_header] = (
                        dateend.strftime("%Y-%m-%dT%H:%M:%S.%f"),
                        row["comment"],
                    )
                elif row_header == "LST":
                    self[row_header] = (lstsec, row["comment"])
                elif row_header == "HA":
                    self[row_header] = (ha, row["comment"])
                else:
                    pass

    def add_airmass(self, fits_config: pd.DataFrame) -> None:
        z = (90 - self["ALTITUDE"]) * np.pi / 180  # type: ignore
        self["AIRMASS"] = (
            (
                (1.002432 * np.cos(z) ** 2 + 0.148386 * np.cos(z) + 0.0096467)
                / (
                    np.cos(z) ** 3
                    + 0.149864 * np.cos(z) ** 2
                    + 0.0102963 * np.cos(z)
                    + 0.000303978
                )
            ),
            fits_config.loc["AIRMASS", "comment"],
        )  # https://doi.org/10.1364/AO.33.001108, https://en.wikipedia.org/wiki/Air_mass_(astronomy)


class HeaderManager:
    """Manages the creation and updating of FITS headers for astronomical images."""

    @staticmethod
    def get_base_header(
        paired_devices: PairedDevices,
        action_value: BaseActionConfig,
        fits_config: pd.DataFrame,
        logger: ObservatoryLogger,
    ) -> ObservatoryHeader:
        """
        Create a base FITS header with observatory and observation information.

        Constructs a comprehensive FITS header containing fixed observatory parameters,
        current device status, astronomical coordinates, and observation metadata.
        The header is built from the FITS configuration file and current system state.

        Parameters:
            paired_devices (PairedDevices): Object containing the devices being used
                for the current observation sequence.
            action_value (dict): Dictionary containing observation parameters from
                the schedule, including target coordinates, filters, and settings.

        Returns:
            fits.Header: A complete FITS header object containing:
                - Fixed observatory information (location, instrument details)
                - Current astronomical conditions (coordinates, time)
                - Device-specific parameters (telescope pointing, filter position)
                - Observation metadata (object name, exposure settings)

        Header Categories:
            - astra: Observatory and software version information
            - astropy_default: Standard astronomical coordinate systems
            - Device-specific: Current status from telescopes, cameras, etc.

        Note:
            - Some header values are populated from real-time device polling
            - Coordinate transformations are performed for various reference frames
            - Observatory location and timing information is automatically included
        """

        logger.info("Creating base header")

        hdr = ObservatoryHeader()
        for row_header, fits_row in fits_config.iterrows():
            if fits_row["device_type"] == "astra" and fits_row["fixed"] is True:
                # custom headers
                if row_header == "FILTER":
                    device = paired_devices.filter_wheel
                    pos = device.get("Position")
                    names = device.get("Names")
                    hdr[row_header] = (names[pos], fits_row["comment"])
                elif row_header == "XPIXSZ":
                    device = paired_devices.camera
                    binx = device.get("BinX")
                    xpixsize = device.get("PixelSizeX")
                    hdr[row_header] = (binx * xpixsize, fits_row["comment"])
                elif row_header == "YPIXSZ":
                    device = paired_devices.camera
                    biny = device.get("BinY")
                    ypixsize = device.get("PixelSizeY")
                    hdr[row_header] = (biny * ypixsize, fits_row["comment"])
                elif row_header == "APTAREA":
                    device = paired_devices.telescope
                    val = device.get("ApertureArea")
                    hdr[row_header] = (val, fits_row["comment"])
                elif row_header == "APTDIA":
                    device = paired_devices.telescope
                    val = device.get("ApertureDiameter")
                    hdr[row_header] = (val, fits_row["comment"])
                elif row_header == "FOCALLEN":
                    device = paired_devices.telescope
                    val = device.get("FocalLength")
                    hdr[row_header] = (val, fits_row["comment"])
                elif row_header == "OBJECT":
                    if row_header.lower() in action_value:
                        hdr[row_header] = (
                            action_value[row_header.lower()],
                            fits_row["comment"],
                        )
                elif row_header in ["EXPTIME", "IMAGETYP"]:
                    hdr[row_header] = (None, fits_row["comment"])
                elif row_header == "ASTRA":
                    hdr[row_header] = (astra.ASTRA_VER, fits_row["comment"])
                else:
                    logger.warning(f"Unknown header: {fits_row['header']}")

            elif (
                fits_row["device_type"]
                not in ["astropy_default", "astra", "astra_fixed", ""]
            ) and fits_row["fixed"] is True:
                # direct ascom command headers
                device_type = fits_row["device_type"]

                if device_type in paired_devices:
                    device = paired_devices.get_device(device_type)
                    assert device is not None, (
                        f"{device_type} not found in paired_devices"
                    )

                    val = device.get(fits_row["device_command"])

                    hdr[row_header] = (val, fits_row["comment"])

            elif fits_row["device_type"] == "astra_fixed":
                # fixed headers, ensure datatype
                try:
                    if fits_row["dtype"] == "float":
                        hdr[row_header] = (
                            float(fits_row["device_command"]),
                            fits_row["comment"],
                        )
                    elif fits_row["dtype"] == "int":
                        hdr[row_header] = (
                            int(fits_row["device_command"]),
                            fits_row["comment"],
                        )
                    elif fits_row["dtype"] == "str":
                        hdr[row_header] = (
                            str(fits_row["device_command"]),
                            fits_row["comment"],
                        )
                    elif fits_row["dtype"] == "bool":
                        hdr[row_header] = (
                            bool(fits_row["device_command"]),
                            fits_row["comment"],
                        )
                    else:
                        hdr[row_header] = (
                            fits_row["device_command"],
                            fits_row["comment"],
                        )
                        logger.error(f"Unknown data type: {fits_row['dtype']}")
                except ValueError as e:
                    logger.report_device_issue(
                        device_type="Headers",
                        device_name="",
                        message=f"Invalid value for data type: {fits_row}",
                        exception=e,
                    )

        logger.info("Base header created")

        return hdr

    @staticmethod
    def final_headers(
        database_manager: DatabaseManager,
        logger: ObservatoryLogger,
        observatory_config: ObservatoryConfig,
        devices: dict,
        fits_config: pd.DataFrame,
    ) -> None:
        """
        Complete FITS headers with interpolated device data.

        Post-processes captured images by adding dynamic header information that
        wasn't available at exposure time. Uses polled device data to interpolate
        accurate values for each image timestamp, ensuring complete and accurate
        FITS headers for scientific analysis.

        The process:
        1. Retrieves incomplete images from the database
        2. Groups images by camera for efficient processing
        3. Queries polled device data around image timestamps
        4. Interpolates device values to exact exposure times
        5. Updates FITS files with complete headers
        6. Marks images as header-complete in database

        Key Features:
        - Time-interpolated device values for precise timestamps
        - Handles multiple cameras and device types simultaneously
        - Preserves original headers while adding missing information
        - Robust error handling with detailed logging

        Data Sources:
        - Device polling data from database
        - FITS configuration file for header mapping
        - Original image FITS headers for timing information

        Error Handling:
        - Individual image failures don't stop batch processing
        - All errors are logged and added to error_source
        - Database consistency maintained even with partial failures

        Note:
            - Typically run after observation sequences complete
            - Critical for ensuring complete scientific metadata
            - May take significant time for large image sets
        """
        try:
            logger.info("Completing headers")
            df_images = HeaderManager._get_incomplete_images(database_manager)
            if df_images.empty:
                logger.info("No headers to complete, as there are no images.")
                return

            for camera_name in df_images["camera_name"].unique():
                logger.info(f"Processing images from camera: {camera_name}")

                df_images_filt = HeaderManager._filter_images_by_camera(
                    df_images, camera_name
                )
                logger.info(f"{df_images_filt.shape[0]} images to process.")

                paired_devices = HeaderManager._get_paired_devices(
                    camera_name, devices, observatory_config
                )
                df_images_filt = HeaderManager._prepare_image_times(df_images_filt)
                df_poll = HeaderManager._get_polling_data(
                    database_manager, df_images_filt
                )
                df_poll_unique = HeaderManager._get_unique_poll_headers(
                    df_poll, fits_config, paired_devices
                )
                df_inp = HeaderManager._interpolate_poll_data(
                    df_poll, df_poll_unique, df_images_filt
                )
                HeaderManager._update_fits_files(
                    df_images_filt,
                    df_inp,
                    df_poll_unique,
                    database_manager,
                    logger,
                    fits_config,
                )
            logger.info("Completing headers... Done.")
        except Exception as e:
            logger.report_device_issue(
                "Headers", "", "Error completing headers", exception=e
            )

    @staticmethod
    def _get_incomplete_images(database_manager):
        return database_manager.execute_select_to_df(
            "SELECT * FROM images WHERE complete_hdr = 0;", table="images"
        )

    @staticmethod
    def _filter_images_by_camera(df_images, camera_name):
        return df_images[df_images["camera_name"] == camera_name]

    @staticmethod
    def _get_paired_devices(camera_name, devices, observatory_config):
        return PairedDevices.from_camera_name(
            camera_name=camera_name,
            devices=devices,
            observatory_config=observatory_config,
        )

    @staticmethod
    def _prepare_image_times(df_images_filt: pd.DataFrame) -> pd.DataFrame:
        df_images_filt["date_obs"] = pd.to_datetime(
            df_images_filt["date_obs"], format="%Y-%m-%d %H:%M:%S.%f"
        )
        df_images_filt = df_images_filt.sort_values(by="date_obs").reset_index(
            drop=True
        )
        df_images_filt["jd_obs"] = (
            df_images_filt["date_obs"].apply(utils.to_jd).sort_values()
        )
        while df_images_filt["jd_obs"].duplicated().sum() > 0:
            df_images_filt["jd_obs"] = df_images_filt["jd_obs"].mask(
                df_images_filt["jd_obs"].duplicated(),
                df_images_filt["jd_obs"] + 1e-9,
            )
        return df_images_filt.sort_values(by="jd_obs").reset_index(drop=True)

    @staticmethod
    def _get_polling_data(
        database_manager: DatabaseManager, df_images_filt: pd.DataFrame
    ) -> pd.DataFrame:
        t0 = df_images_filt["date_obs"].iloc[0] - pd.Timedelta("10 sec")
        t1 = df_images_filt["date_obs"].iloc[-1] + pd.Timedelta("10 sec")
        df_poll = database_manager.execute_select_to_df(
            f'SELECT * FROM polling WHERE datetime BETWEEN "{str(t0)}" AND "{str(t1)}";',
            table="polling",
        )
        df_poll["jd"] = pd.to_datetime(
            df_poll["datetime"], format="%Y-%m-%d %H:%M:%S.%f"
        ).apply(utils.to_jd)
        return df_poll

    @staticmethod
    def _get_unique_poll_headers(
        df_poll: pd.DataFrame, fits_config: pd.DataFrame, paired_devices: pd.DataFrame
    ) -> pd.DataFrame:
        df_poll_unique = df_poll[
            ["device_type", "device_name", "device_command"]
        ].drop_duplicates()
        df_poll_unique = df_poll_unique[
            df_poll_unique.apply(
                lambda x: (
                    x["device_type"] in fits_config["device_type"].values
                    and x["device_command"] in fits_config["device_command"].values
                ),
                axis=1,
            )
        ]
        df_poll_unique["header"] = df_poll_unique.apply(
            lambda x: fits_config[
                (fits_config["device_type"] == x["device_type"])
                & (fits_config["device_command"] == x["device_command"])
            ].index[0],
            axis=1,
        )
        df_poll_unique["comment"] = df_poll_unique.apply(
            lambda x: fits_config[
                (fits_config["device_type"] == x["device_type"])
                & (fits_config["device_command"] == x["device_command"])
            ]["comment"].values[0],
            axis=1,
        )
        df_poll_unique = df_poll_unique[
            df_poll_unique["device_name"].isin(paired_devices.values())
        ]
        return df_poll_unique

    @staticmethod
    def _interpolate_poll_data(
        df_poll: pd.DataFrame,
        df_poll_unique: pd.DataFrame,
        df_images_filt: pd.DataFrame,
    ) -> pd.DataFrame:
        df_inp = pd.DataFrame(
            columns=df_poll_unique["header"], index=df_images_filt["jd_obs"]
        )
        for _, poll_row in df_poll_unique.iterrows():
            df_poll_filtered = (
                df_poll[
                    (df_poll["device_type"] == poll_row["device_type"])
                    & (df_poll["device_name"] == poll_row["device_name"])
                    & (df_poll["device_command"] == poll_row["device_command"])
                ]
                .sort_values(by="jd")
                .set_index("jd")
            )

            df_poll_filtered["device_value"] = pd.to_numeric(
                df_poll_filtered["device_value"].replace(
                    {"True": "1.0", "False": "0.0"}
                ),
                errors="coerce",
            ).fillna(-1)

            df_inp[poll_row["header"]] = utils.interpolate_dfs(
                df_images_filt["jd_obs"], df_poll_filtered["device_value"]
            )["device_value"].fillna(0)
        return df_inp

    @staticmethod
    def _update_fits_files(
        df_images_filt: pd.DataFrame,
        df_inp: pd.DataFrame,
        df_poll_unique: pd.DataFrame,
        database_manager: DatabaseManager,
        logger: ObservatoryLogger,
        fits_config: pd.DataFrame,
    ):
        for row_index, row in df_images_filt.iterrows():
            try:
                HeaderManager._update_single_fits_file(
                    row_index,
                    row,
                    df_inp,
                    df_poll_unique,
                    fits_config,
                )
                time.sleep(0)
            except FileNotFoundError:
                logger.warning(f"Error completing headers: {row['filepath']}")
            finally:
                database_manager.execute(
                    f'''UPDATE images SET complete_hdr = 1 WHERE filename="{row["filepath"]}"'''
                )

    @staticmethod
    def _update_single_fits_file(
        row_index: int,
        row: pd.Series,
        df_inp: pd.DataFrame,
        df_poll_unique: pd.DataFrame,
        fits_config: pd.DataFrame,
    ):
        with fits.open(row["filepath"], mode="update") as filehandle:
            header = ObservatoryHeader(filehandle[0].header)  # type: ignore
            for header_entry in df_inp.columns:
                # Only write if not already present in header (preserve target coordinates)
                if header_entry not in header:
                    header[header_entry] = (
                        df_inp.iloc[row_index][header_entry],
                        df_poll_unique[df_poll_unique["header"] == header_entry][
                            "comment"
                        ].values[0],
                    )

            # Only convert RA from hours to degrees if it came from polled data (not already in degrees)
            if "RA-DEG" not in header:
                header.convert_ra_from_hours_to_degrees()
            else:
                # Remove the marker flag - it's just for internal tracking
                del header["RA-DEG"]

            location = header.get_observatory_location()
            target = header.get_target_sky_coordinates()
            header.add_times(fits_config, location, target)
            header.add_airmass(fits_config)

            # Assign the modified header back to the HDU
            filehandle[0].header = header
            filehandle[0].add_checksum()  # type: ignore
            filehandle.flush()
