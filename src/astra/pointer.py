"""
Astronomical pointing correction and plate solving utilities.

This module provides comprehensive tools for correcting telescope pointing errors
by analyzing astronomical images and comparing detected stars with catalog data.
The primary workflow involves star detection, world coordinate system (WCS)
computation, and pointing offset calculation.

Key Features:
- Multi-scale star detection algorithms for robust identification in noisy images
- Gaia star catalog integration for precise astrometric reference
- World Coordinate System (WCS) computation using the twirl library
- Automatic pointing correction calculation and validation
- Support for FITS file processing with metadata extraction
- Background subtraction and image cleaning utilities

The module supports two main use cases:
1. Real-time pointing correction during observations
2. Post-processing analysis of astronomical images

Typical Workflow:
    1. Load or generate an astronomical image
    2. Detect stars using multi-scale algorithms
    3. Query Gaia catalog for reference stars in the field
    4. Compute WCS transformation between image and sky coordinates
    5. Calculate pointing correction based on target vs actual position
    6. Apply corrections to telescope pointing

Classes:
    PointingCorrection: Stores target vs actual pointing coordinates
    ImageStarMapping: Handles star detection and catalog matching
    PointingCorrectionHandler: Main interface for complete pointing analysis

Functions:
    find_stars: Basic star detection using DAOStarFinder
    gaia_db_query: Query Gaia catalog for reference stars

Example:
    # Process a FITS file for pointing correction
    corrector = PointingCorrectionHandler.from_fits_file(
        "observation.fits",
        target_ra=150.25,
        target_dec=2.18
    )

    # Get the pointing offset
    ra_offset = corrector.pointing_correction.offset_ra
    dec_offset = corrector.pointing_correction.offset_dec

    print(f"Pointing error: RA={ra_offset:.3f}°, Dec={dec_offset:.3f}°")
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union

import astropy.units as u
import cabaret
import numpy as np
import pandas as pd
import twirl
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.stats import sigma_clipped_stats
from astropy.units import Quantity
from astropy.wcs.utils import WCS, pixel_to_skycoord
from matplotlib import pyplot as plt
from photutils.detection import DAOStarFinder

from astra import Config
from astra.utils import clean_image

logger = logging.getLogger(__name__)


def calculate_pointing_correction_from_fits(
    filepath: str | Path | None,
    dark_frame: str | Path | None = None,
    target_ra: float | None = None,
    target_dec: float | None = None,
    filter_band: Optional[str] = None,
):
    """
    Create a PointingCorrectionHandler instance from a FITS file.

    Performs the complete plate solving workflow from a FITS file, including
    metadata extraction, image cleaning, star detection, and pointing correction.

    Parameters:
        filepath (str or Path): Path to the FITS file.
        dark_frame (str or Path, optional): Path to a dark frame for subtraction.
            Defaults to None.
        target_ra (float, optional): Target right ascension in degrees. If not
            provided, it's read from FITS header. Defaults to None.
        target_dec (float, optional): Target declination in degrees. If not
            provided, it's read from FITS header. Defaults to None.
        filter_band (str, optional): Filter band used for observation. Defaults to None.

    Returns:
        PointingCorrectionHandler: Instance with computed pointing correction
            and image-star mapping.
    """
    image, header = _read_fits_file(filepath)

    if dark_frame is not None:
        dark_image, _ = _read_fits_file(dark_frame)
        image = image - dark_image

    if target_ra is None:
        target_ra = header["RA"]
    if target_dec is None:
        target_dec = header["DEC"]

    plate_scale, dateobs = _extract_plate_scale_and_dateobs(header)

    return calculate_pointing_correction_from_image(
        image,
        target_ra=target_ra,
        target_dec=target_dec,
        dateobs=dateobs,
        plate_scale=plate_scale,
        filter_band=filter_band,
    )


def calculate_pointing_correction_from_image(
    image: np.ndarray,
    target_ra: float,
    target_dec: float,
    dateobs: datetime,
    plate_scale: float,
    filter_band: Optional[str] = None,
):
    """
    Create a PointingCorrectionHandler instance from an image array.

    Performs the complete plate solving workflow: image cleaning, star detection,
    Gaia catalog matching, WCS computation, and pointing correction calculation.

    Parameters:
        image (np.ndarray): The 2D astronomical image data.
        target_ra (float): Target right ascension in degrees.
        target_dec (float): Target declination in degrees.
        dateobs (datetime): Observation date for proper motion corrections.
        plate_scale (float): Image plate scale in degrees per pixel.
        filter_band (str, optional): Filter band used for observation.

    Returns:
        PointingCorrectionHandler: Instance with computed pointing correction.
        image_star_mapping (ImageStarMapping): Mapping of image stars to Gaia stars.
        int: Number of stars used from the image for plate solving.

    Raises:
        Exception: If insufficient stars are detected for plate solving,
            if the offset is larger than the field of view, or if too few
            stars are matched.
    """
    image_clean = clean_image(image)

    # assume 2" FWHM
    stars_in_image = find_stars(
        image_clean,
        threshold=7,
        fwhm=(2 / 3600) / plate_scale,
    )

    # Limit number of stars and gaia stars to use for plate solve
    stars_in_image_used = min(len(stars_in_image), 24)

    if stars_in_image_used < 4:
        raise Exception("Not enough stars detected to plate solve")

    stars_in_image = stars_in_image[0:stars_in_image_used]
    gaia_star_coordinates = _get_gaia_star_coordinates(
        target_ra,
        target_dec,
        image_clean,
        dateobs,
        plate_scale,
        filter_band=filter_band,
        fov_scale=1.1,
        limit=int(2 * stars_in_image_used),
    )

    image_star_mapping = ImageStarMapping.from_gaia_coordinates(
        stars_in_image, gaia_star_coordinates
    )

    plating_ra, plating_dec = image_star_mapping.get_plating_center(
        image_shape=image_clean.shape
    )

    pointing_correction = PointingCorrection(
        target_ra=target_ra,
        target_dec=target_dec,
        plating_ra=plating_ra,
        plating_dec=plating_dec,
    )

    _verify_offset_within_fov(pointing_correction, plate_scale, image_clean.shape)

    _verify_plate_solve(
        image_star_mapping,
        number_of_stars_to_match=np.floor(stars_in_image_used * 0.75),
    )

    return pointing_correction, image_star_mapping, stars_in_image_used


def find_stars(
    data: np.ndarray,
    threshold: float = 5.0,
    fwhm: float = 3.0,
) -> np.ndarray:
    """
    Find stars using DAOStarFinder algorithm.

    Uses the photutils DAOStarFinder algorithm to detect point sources in
    astronomical images. The function performs background subtraction and
    returns star coordinates sorted by brightness.

    Parameters:
        data (np.ndarray): The 2D image data array.
        threshold (float, optional): Detection threshold in units of background
            standard deviation. Higher values detect fewer, brighter stars.
            Defaults to 5.0.
        fwhm (float, optional): Expected Full Width at Half Maximum of stars
            in pixels. Should match the typical seeing conditions. Defaults to 3.0.
        peakmax (float, optional): Maximum ADU value to consider for star detection. Defaults to 65535.

    Returns:
        np.ndarray: Array of detected star coordinates sorted by brightness.
            Shape is (N, 2) where N is the number of stars, and each row is (x, y).
            Returns an empty array if no stars are found.
    """
    # Calculate background statistics
    mean, median, std = sigma_clipped_stats(data, sigma=3.0)

    # Use DAOStarFinder for star detection
    dao_find = DAOStarFinder(
        fwhm=fwhm,
        threshold=threshold * std,
        exclude_border=True,
        min_separation=2 * fwhm,
    )
    dao_sources = dao_find(data)

    if dao_sources is None or len(dao_sources) == 0:
        return np.array([]).reshape(0, 2)

    # Sort sources by flux (brightness) in descending order
    sorted_indices = np.argsort(dao_sources["flux"])[::-1]
    dao_sources = dao_sources[sorted_indices]

    # Filter sources based on peak value
    dao_sources = dao_sources[dao_sources["peak"] > mean + threshold * std]

    # Convert to (x, y) coordinates
    coordinates = np.column_stack([dao_sources["xcentroid"], dao_sources["ycentroid"]])

    return coordinates


@dataclass
class PointingCorrection:
    """
    Class to store the pointing correction between the desired target center and the plating center.

    Stores target and actual telescope pointing coordinates and provides methods
    to calculate offsets, angular separations, and proxy coordinates for correction.
    The proxy coordinates represent where the telescope should point to achieve
    the desired target position.

    Attributes:
        target_ra (float): The right ascension of the target center in degrees.
        target_dec (float): The declination of the target center in degrees.
        plating_ra (float): The right ascension of the plating center in degrees.
        plating_dec (float): The declination of the plating center in degrees.

    Properties:
        offset_ra: RA offset (plating - target) in degrees
        offset_dec: Dec offset (plating - target) in degrees
        angular_separation: Angular distance between target and actual position
        proxy_ra: RA coordinate to point telescope to reach target
        proxy_dec: Dec coordinate to point telescope to reach target

    Example:
        >>> from astra.pointer import PointingCorrection
        >>> pointing_correction = PointingCorrection(
        ...     target_ra=10.685, target_dec=41.269,
        ...     plating_ra=10.68471, plating_dec=41.26917
        ... )
        >>> print(f"RA offset: {pointing_correction.offset_ra:.4f} degrees")
    """

    target_ra: float
    target_dec: float
    plating_ra: float
    plating_dec: float

    @property
    def offset_ra(self):
        """
        Calculate the RA offset between plating and target coordinates.

        Returns:
            float: RA offset (plating_ra - target_ra) in degrees. Positive values
                indicate the telescope pointed east of the target.
        """
        return self.plating_ra - self.target_ra

    @property
    def offset_dec(self):
        """
        Calculate the Dec offset between plating and target coordinates.

        Returns:
            float: Dec offset (plating_dec - target_dec) in degrees. Positive values
                indicate the telescope pointed north of the target.
        """
        return self.plating_dec - self.target_dec

    @property
    def angular_separation(self) -> float:
        """
        Calculate the angular separation between target and plating coordinates.

        Returns:
            float: Angular distance between target and actual position in degrees.
                Always positive, represents the magnitude of the pointing error.
        """
        target_coord = SkyCoord(ra=self.target_ra, dec=self.target_dec, unit="deg")
        plating_coord = SkyCoord(ra=self.plating_ra, dec=self.plating_dec, unit="deg")
        return target_coord.separation(plating_coord).deg

    @property
    def proxy_ra(self):
        """
        Calculate the RA coordinate to point the telescope to reach the target RA.

        This assumes that the offset is independent of the telescope's position in the sky.
        The proxy coordinate compensates for the systematic pointing error.

        Returns:
            float: RA coordinate in degrees where the telescope should point to
                effectively arrive at the target RA.

        See Also:
            ConstantDistorter: Verify sign convention of correction.
        """
        return self.target_ra - self.offset_ra

    @property
    def proxy_dec(self):
        """
        Calculate the Dec coordinate to point the telescope to reach the target Dec.

        This assumes that the offset is independent of the telescope's position in the sky.
        The proxy coordinate compensates for the systematic pointing error.

        Returns:
            float: Dec coordinate in degrees where the telescope should point to
                effectively arrive at the target Dec.

        See Also:
            ConstantDistorter: Verify sign convention of correction.
        """
        return self.target_dec - self.offset_dec

    def __repr__(self):
        return (
            "PointingCorrection("
            f"target_ra={self.target_ra}, target_dec={self.target_dec}, "
            f" plating_ra={self.plating_ra}, plating_dec={self.plating_dec})"
        )


@dataclass
class ImageStarMapping:
    """
    A class to handle the mapping of stars detected in an image to their corresponding
    Gaia star coordinates using World Coordinate System (WCS) transformations.

    Manages the relationship between pixel coordinates of detected stars and their
    celestial coordinates from the Gaia catalog. Provides methods for coordinate
    transformations, star matching, and validation of the plate solving process.

    Attributes:
        wcs (WCS): The World Coordinate System object used for mapping celestial
            coordinates to pixel coordinates.
        stars_in_image (np.ndarray): An array of detected star coordinates in the
            image, represented in pixel format (x, y).
        gaia_stars_in_image (np.ndarray): An array of Gaia star coordinates
            projected into the image's pixel space using the WCS transformation.

    Methods:
        from_gaia_coordinates: Class method to create instance from star coordinates
        get_plating_center: Calculate the sky coordinates of image center
        skycoord_to_pixels: Convert sky coordinates to pixel coordinates
        pixels_to_skycoord: Convert pixel coordinates to sky coordinates
        find_gaia_match: Match detected stars to Gaia catalog stars
        number_of_matched_stars: Count stars matched within threshold
        plot: Visualize detected and catalog stars
    """

    wcs: WCS
    stars_in_image: np.ndarray
    gaia_stars_in_image: np.ndarray

    @classmethod
    def from_gaia_coordinates(cls, stars_in_image: np.ndarray, gaia_stars: np.ndarray):
        """
        Create an ImageStarMapping instance from detected stars and Gaia coordinates.

        Computes the World Coordinate System (WCS) transformation using the twirl
        library to map between pixel and sky coordinates.

        Parameters:
            stars_in_image (np.ndarray): Array of detected star coordinates in pixels (x, y).
            gaia_stars (np.ndarray): Array of Gaia star coordinates in degrees (ra, dec).

        Returns:
            ImageStarMapping: New instance with computed WCS and transformed coordinates.
        """
        wcs = twirl.compute_wcs(stars_in_image, gaia_stars)
        gaia_stars_in_image = np.array(SkyCoord(gaia_stars, unit="deg").to_pixel(wcs)).T
        return cls(wcs, stars_in_image, gaia_stars_in_image)

    def get_plating_center(self, image_shape: Tuple[int, int]) -> Tuple[float, float]:
        """
        Calculate the sky coordinates of the image center (plating center).

        Parameters:
            image_shape (Tuple[int, int]): Shape of the image as (height, width).

        Returns:
            Tuple[float, float]: RA and Dec coordinates of the image center in degrees.
        """
        plating_center = pixel_to_skycoord(
            image_shape[1] / 2, image_shape[0] / 2, self.wcs
        )
        return plating_center.ra.deg, plating_center.dec.deg

    def skycoord_to_pixels(self, ra: float, dec: float) -> Tuple[float, float]:
        """
        Convert sky coordinates to pixel coordinates using the WCS.

        Parameters:
            ra (float): Right ascension in degrees.
            dec (float): Declination in degrees.

        Returns:
            Tuple[float, float]: Pixel coordinates (x, y) in the image.
        """
        return SkyCoord(ra, dec, unit="deg").to_pixel(self.wcs)

    def pixels_to_skycoord(self, pixels: np.ndarray):
        """
        Convert pixel coordinates to sky coordinates using the WCS.

        Parameters:
            pixels (np.ndarray): Array of pixel coordinates.

        Returns:
            Array of sky coordinates corresponding to the input pixels.
        """
        return self.wcs.pixel_to_world_values(pixels)

    def find_gaia_match(self):
        """
        Find the closest Gaia star match for each detected star in the image.

        Calculates the distance between each detected star and all Gaia stars
        in pixel space, then returns the closest match and distance for each.

        Returns:
            Tuple[np.ndarray, np.ndarray]:
                - Matched Gaia star positions in pixel coordinates
                - Distances to the closest matches in pixels
        """
        squared_distances = np.sum(
            (
                self.stars_in_image[:, np.newaxis]
                - self.gaia_stars_in_image[np.newaxis, :, :]
            )
            ** 2,
            axis=-1,
        )
        match_index = np.argmin(squared_distances, axis=1)
        distance = np.sqrt(np.min(squared_distances, axis=1))

        return self.gaia_stars_in_image[match_index], distance

    def number_of_matched_stars(self, pixel_threshold: int = 10):
        """
        Count the number of stars matched within a pixel threshold.

        Parameters:
            pixel_threshold (int, optional): Maximum distance in pixels to consider
                a match valid. Defaults to 10.

        Returns:
            int: Number of detected stars that have a Gaia match within the threshold.
        """
        distance_to_closest_star = self.find_gaia_match()[1]

        return np.sum(distance_to_closest_star < pixel_threshold)

    def plot(self, ax=None, transpose=False, **kwargs):
        """
        Plot detected stars and their Gaia matches for visualization.

        Parameters:
            ax (matplotlib.axes.Axes, optional): Axes to plot on. If None, creates new figure.
            transpose (bool, optional): If True, transpose x and y coordinates for plotting.
                Defaults to False.
            **kwargs: Additional keyword arguments passed to scatter plot.
        """

        if ax is None:
            fig, ax = plt.subplots()

        default_dict = {
            "facecolors": "none",
            "edgecolors": "r",
            "linewidth": 2,
            "marker": "o",
        }

        gaia_stars = self.find_gaia_match()
        gaia_stars_in_pixel_range = gaia_stars[0][gaia_stars[1] < 20]

        dim = (1, 0) if transpose else (0, 1)

        ax.scatter(
            self.stars_in_image[::, dim[0]],
            self.stars_in_image[:, dim[1]],
            label="Detected stars",
            s=80,
            **(default_dict | kwargs),
        )
        ax.scatter(
            gaia_stars_in_pixel_range[:, dim[0]],
            gaia_stars_in_pixel_range[:, dim[1]],
            label="Matched Gaia stars",
            s=160,
            **(default_dict | {"edgecolors": "dodgerblue"} | kwargs),
        )
        ax.scatter(
            self.gaia_stars_in_image[:, dim[0]],
            self.gaia_stars_in_image[:, dim[1]],
            label="All Gaia stars",
            s=40,
            **(default_dict | {"edgecolors": "yellow"} | kwargs),
        )
        ax.legend()


def _read_fits_file(filepath: str | Path):
    """
    Read image data and header from a FITS file.

    Parameters:
        filepath (str or Path): Path to the FITS file.

    Returns:
        Tuple[np.ndarray, Header]: Image data and header.
    """
    with fits.open(filepath) as hdul:
        header = hdul[0].header
        image = hdul[0].data
    return image, header


def _extract_plate_scale_and_dateobs(header):
    """
    Extract plate scale and observation date from FITS header.

    Parameters:
        header (fits.Header): FITS header containing metadata.

    Returns:
        Tuple[datetime, float]: Observation date and plate scale in degrees per pixel.
    """
    dateobs = pd.to_datetime(header["DATE-OBS"])

    # get units of FOCALLEN through comments if available
    focallen_comment = header.comments["FOCALLEN"].lower()
    if "mm" in focallen_comment or "millimeter" in focallen_comment:
        focallen_unit = 1e-3
    else:
        focallen_unit = 1.0  # default to m

    plate_scale = np.arctan(
        (header["XPIXSZ"] * 1e-6) / (header["FOCALLEN"] * focallen_unit)
    ) * (180 / np.pi)  # deg/pixel
    return plate_scale, dateobs


def _map_filter_band_to_gaia_tmass(filter_band: Optional[str]) -> Optional[str]:
    """
    Map a given filter band to the corresponding Gaia or 2MASS band.

    Parameters:
        filter_band (str, optional): The filter band used for observation.
    Returns:
        str: Corresponding Gaia or 2MASS band, or defaults to G if not found.
    """

    gaia_tmass_filter_mappings = {
        "G": ["r", "clear", "C"],
        "BP": ["u", "g"],
        "RP": ["i", "z", "I+z", "Exo"],
        "J": ["J", "Y", "YJ", "zYJ"],
        "H": ["H"],
        "KS": ["K", "Ks"],
    }

    if filter_band is None:
        return "G"

    for gaia_band, bands in gaia_tmass_filter_mappings.items():
        if filter_band.lower().strip("'") in [b.lower() for b in bands]:
            return gaia_band

    return "G"


def _get_gaia_star_coordinates(
    ra,
    dec,
    image_clean,
    dateobs,
    plate_scale,
    filter_band=None,
    fov_scale=1.1,
    limit=24,
):
    """
    Get Gaia star coordinates for a given field of view.

    Parameters:
        ra (float): Right ascension of field center in degrees.
        dec (float): Declination of field center in degrees.
        image_clean (np.ndarray): The cleaned image data.
        dateobs (datetime): Observation date.
        plate_scale (float): Plate scale in degrees per pixel.
        filter_band (str, optional): Filter band used for observation.
        fov_scale (float, optional): Factor to scale field of view. Defaults to 1.1.
        limit (int, optional): Maximum number of stars to query. Defaults to 24.

    Returns:
        np.ndarray: Array of Gaia star coordinates.
    """
    # Get fov from image shape and plate scale
    fov = np.array(image_clean.shape) * plate_scale * fov_scale

    gaia_tmass_filter = _map_filter_band_to_gaia_tmass(filter_band)

    if Config().gaia_db.is_file():
        use_tmass = gaia_tmass_filter in ["J", "H", "KS"]
        logger.debug(
            f"Using {'2MASS J' if use_tmass else 'Gaia G'} filter band for local query"
        )
        # Query gaia database for stars in the fov
        gaia_star_coordinates = local_gaia_db_query(
            center=(ra, dec), fov=fov, limit=limit, dateobs=dateobs, tmass=use_tmass
        )
        return gaia_star_coordinates
    else:
        logger.debug("Using online Gaia archive for star query")
        logger.debug(f"Using Gaia/2MASS filter band: {gaia_tmass_filter}")
        table = cabaret.GaiaQuery.query(
            center=(ra, dec),
            radius=np.max(fov) / 2,
            filter_band=gaia_tmass_filter,
            limit=limit,
            timeout=60,
        )

        table_filt = cabaret.GaiaQuery._apply_proper_motion(table, dateobs).copy()

        return np.array([table_filt["ra"].value.data, table_filt["dec"].value.data]).T


def _verify_offset_within_fov(
    pointing_correction: PointingCorrection,
    plate_scale: float,
    image_shape: Tuple[int, int],
):
    """
    Verify that the pointing offset is within the field of view.

    Parameters:
        pointing_correction (PointingCorrection): The pointing correction object.
        plate_scale (float): Plate scale in degrees per pixel.
        image_shape (Tuple[int, int]): Shape of the image.

    Raises:
        Exception: If the offset is larger than the field of view.
    """
    # Check that the offset is not larger than the fov
    # Get fov from image shape and plate scale
    fov = np.array(image_shape) * plate_scale
    if pointing_correction.angular_separation > max(fov):
        raise Exception("Pointing error is larger than the field of view")


def _verify_plate_solve(
    image_star_mapping: ImageStarMapping,
    pixel_threshold: int = 20,
    number_of_stars_to_match: int = 4,
):
    """
    Verify the plate solve by checking the number of matched stars.

    Parameters:
        image_star_mapping (ImageStarMapping): The image-star mapping object.
        pixel_threshold (int, optional): Pixel distance threshold for a match.
            Defaults to 20.
        number_of_stars_to_match (int, optional): Minimum number of stars to match.
            Defaults to 4.

    Raises:
        Exception: If too few stars are matched.
    """
    # Check that we have at least a certain number of stars matched
    number_of_matched_stars = image_star_mapping.number_of_matched_stars(
        pixel_threshold=pixel_threshold
    )
    if number_of_matched_stars < number_of_stars_to_match:
        raise Exception(
            f"Plate solve failed: only {number_of_matched_stars / number_of_stars_to_match:.2%} stars matched"
        )


def local_gaia_db_query(
    center: Union[Tuple[float, float], SkyCoord],
    fov: Union[float, Quantity],
    limit: int = 1000,
    tmass: bool = False,
    dateobs: Optional[datetime] = None,
) -> np.ndarray:
    """
    Query the Gaia archive to retrieve the RA-DEC coordinates of stars within a given field-of-view.

    Retrieves star positions from a local Gaia database within a rectangular region
    centered on the specified coordinates. Optionally applies proper motion corrections
    for a specific observation date and sorts by magnitude (Gaia G-band or 2MASS J-band).

    Parameters:
        center (tuple or SkyCoord): The sky coordinates of the center of the FOV.
            If a tuple is given, it should contain the RA and DEC in degrees.
        fov (float or Quantity): The field-of-view size in degrees. If a float is given,
            it is assumed to be in degrees. Can be a single value (square FOV) or
            tuple (RA_fov, Dec_fov).
        limit (int, optional): The maximum number of sources to retrieve from the
            Gaia archive. Defaults to 1000.
        tmass (bool, optional): Whether to sort by 2MASS J magnitudes instead of
            Gaia G magnitudes. Defaults to False.
        dateobs (datetime, optional): The date of the observation. If given, the
            proper motions of the sources will be applied to update positions
            from J2015.5 to the observation date. Defaults to None.

    Returns:
        np.ndarray: An array of shape (n, 2) containing the RA-DEC coordinates
            of the retrieved sources in degrees, sorted by magnitude (brightest first).

    Raises:
        ImportError: If required database utilities are not available.
    """

    if isinstance(center, SkyCoord):
        ra = center.ra.deg
        dec = center.dec.deg
    else:
        ra, dec = center

    logger.debug(f"Querying local Gaia DB around RA={ra}, DEC={dec} with FOV={fov}")

    if not isinstance(fov, u.Quantity):
        fov = fov * u.deg
        logger.debug(f"Converted FOV to Quantity: {fov}")

    if fov.ndim == 0:
        ra_fov = dec_fov = fov.to(u.deg).value
    elif fov.ndim == 1:
        ra_fov, dec_fov = fov.to(u.deg).value
    else:
        ra_fov = fov[0].to(u.deg).value
        dec_fov = fov[1].to(u.deg).value

    # Dec bounds are straightforward
    min_dec = max(dec - dec_fov / 2, -90.0)
    max_dec = min(dec + dec_fov / 2, 90.0)

    # For RA, account for spherical geometry
    # At declination `dec`, the RA angular size scales as 1/cos(dec)
    cos_dec = np.cos(np.radians(dec))

    # Check if we're near a pole (within 5 degrees)
    if abs(dec) > 85:
        # Near poles, query all RA values
        min_ra = 0.0
        max_ra = 360.0
        logger.debug("Near pole - querying all RA values")
    else:
        # Adjust RA FOV for spherical geometry
        ra_fov_adjusted = ra_fov / cos_dec if cos_dec > 0.01 else 360.0

        min_ra = ra - ra_fov_adjusted / 2
        max_ra = ra + ra_fov_adjusted / 2

        # Handle RA wraparound at 0/360
        if min_ra < 0:
            min_ra += 360
        if max_ra > 360:
            max_ra -= 360

    # Handle RA wraparound case
    crosses_zero = min_ra > max_ra

    table = local_db_query(
        Config().gaia_db, min_dec, max_dec, min_ra, max_ra, crosses_zero=crosses_zero
    )

    if tmass:
        table = table.sort_values(by=["j_m"]).reset_index(drop=True)
    else:
        table = table.sort_values(by=["phot_g_mean_mag"]).reset_index(drop=True)

    table = table.map(lambda x: np.nan if x == "" else x)
    table.dropna(inplace=True)

    # Limit number of stars
    table = table[0:limit]

    # Add proper motion to ra and dec
    if dateobs is not None:
        dateobs = dateobs.year + (dateobs.timetuple().tm_yday - 1) / 365.25  # type: ignore
        years = dateobs - 2015.5  # type: ignore
        table["ra"] += years * table["pmra"] / 1000 / 3600
        table["dec"] += years * table["pmdec"] / 1000 / 3600

    return np.array([table["ra"].values, table["dec"].values]).T


def local_db_query(
    db: Union[str, Path],
    min_dec: float,
    max_dec: float,
    min_ra: float,
    max_ra: float,
    crosses_zero: bool = False,
) -> pd.DataFrame:
    """Query astronomical database for objects within coordinate bounds.

    Performs federated query across sharded SQLite database tables to retrieve
    astronomical catalog data within specified declination and right ascension ranges.

    Args:
        db (Union[str, Path]): Path to the SQLite database file.
        min_dec (float): Minimum declination in degrees.
        max_dec (float): Maximum declination in degrees.
        min_ra (float): Minimum right ascension in degrees.
        max_ra (float): Maximum right ascension in degrees.
        crosses_zero (bool, optional): Whether the RA range crosses the 0-degree line.
            Defaults to False.

    Returns:
        pd.DataFrame: Combined results from all relevant database shards.
    """

    conn = sqlite3.connect(db)

    # Determine the relevant shard(s) based on declination
    arr = np.arange(np.floor(min_dec), np.ceil(max_dec) + 1, 1)
    relevant_shard_ids = set()
    for i in range(len(arr) - 1):
        shard_id = f"{arr[i]:.0f}_{arr[i + 1]:.0f}"
        relevant_shard_ids.add(shard_id)

    # Execute the federated query across the relevant shard(s)
    df_total = pd.DataFrame()
    for shard_id in relevant_shard_ids:
        shard_table_name = f"{shard_id}"

        if crosses_zero:
            # Query in two parts: [min_ra, 360] OR [0, max_ra]
            q = f"""SELECT * FROM `{shard_table_name}` 
                    WHERE dec BETWEEN {min_dec} AND {max_dec} 
                    AND (ra >= {min_ra} OR ra <= {max_ra})"""
        else:
            q = f"""SELECT * FROM `{shard_table_name}` 
                    WHERE dec BETWEEN {min_dec} AND {max_dec} 
                    AND ra BETWEEN {min_ra} AND {max_ra}"""

        df = pd.read_sql_query(q, conn)
        df_total = pd.concat([df, df_total], axis=0)

    conn.close()
    return df_total
