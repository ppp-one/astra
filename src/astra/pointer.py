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
    ConstantDistorter: Testing utility for verifying correction sign conventions

Functions:
    find_stars_dao: Basic star detection using DAOStarFinder
    find_stars_multiscale: Robust multi-scale star detection
    remove_duplicates: Clean up duplicate star detections
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

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Union

import astropy.units as u
import numpy as np
import pandas as pd
import twirl
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.stats import SigmaClip, sigma_clipped_stats
from astropy.units import Quantity
from astropy.wcs.utils import WCS, pixel_to_skycoord
from matplotlib import pyplot as plt
from photutils.background import Background2D, MedianBackground
from photutils.detection import DAOStarFinder
from scipy import ndimage

from astra import Config
from astra.utils import db_query


def find_stars_dao(
    data: np.ndarray, threshold: float = 5.0, fwhm: float = 3.0
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

    Returns:
        np.ndarray: Array of (x, y) coordinates sorted by brightness (brightest first).
            Returns empty array with shape (0, 2) if no stars are detected.
    """
    # Calculate background statistics
    mean, median, std = sigma_clipped_stats(data, sigma=3.0)

    # Use DAOStarFinder for star detection
    daofind = DAOStarFinder(fwhm=fwhm, threshold=threshold * std)
    sources = daofind(data - median)

    if sources is None or len(sources) == 0:
        return np.array([]).reshape(0, 2)

    # Convert to (x, y) coordinates
    coordinates = np.column_stack([sources["xcentroid"], sources["ycentroid"]])

    # Sort by flux (brightness)
    fluxes = sources["flux"]
    return coordinates[np.argsort(fluxes)[::-1]]


def remove_duplicates(detections: np.ndarray, width: int, height: int) -> np.ndarray:
    """
    Remove duplicate detections using priority-based selection.

    Filters out duplicate star detections by keeping only the highest priority
    detection within a 10-pixel radius. Priority is calculated based on central
    position preference and smaller detection scales.

    Parameters:
        detections (np.ndarray): Array of detections with [x, y, scale] for each detection.
        width (int): Image width in pixels.
        height (int): Image height in pixels.

    Returns:
        np.ndarray: Filtered array of unique detections [x, y, scale] sorted by priority.
    """
    if len(detections) <= 1:
        return detections

    # Calculate priority scores (prefer central position and smaller scale)
    center = np.array([width / 2, height / 2])
    positions = detections[:, :2]
    scales = detections[:, 2]

    center_distances = np.linalg.norm(positions - center, axis=1)
    center_scores = 1.0 / (1.0 + center_distances / max(width, height))
    scale_scores = 1.0 / scales  # Prefer smaller scales

    priority_scores = center_scores * scale_scores

    # Sort by priority (highest first)
    sorted_indices = np.argsort(priority_scores)[::-1]
    sorted_detections = detections[sorted_indices]

    # Remove duplicates (within 10 pixels)
    unique_detections = []
    for detection in sorted_detections:
        is_duplicate = False
        pos = detection[:2]

        for existing in unique_detections:
            if np.linalg.norm(pos - existing[:2]) < 10.0:
                is_duplicate = True
                break

        if not is_duplicate:
            unique_detections.append(detection)

    return np.array(unique_detections)


def find_stars_multiscale(
    data: np.ndarray,
    scales: list = [1, 2, 3],
    threshold: float = 5.0,
    edge_buffer: int = 15,
) -> np.ndarray:
    """
    Multi-scale star detection for noisy astronomical images.

    Performs star detection at multiple smoothing scales to improve robustness
    in noisy images. Each scale uses Gaussian smoothing followed by DAOStarFinder.
    Results are combined and duplicates removed based on priority scoring.

    Parameters:
        data (np.ndarray): The 2D image data array.
        scales (list, optional): List of smoothing scales to try (in pixels).
            Larger scales detect extended sources better. Defaults to [1, 2, 3].
        threshold (float, optional): Detection threshold in units of background
            standard deviation. Defaults to 5.0.
        edge_buffer (int, optional): Minimum distance from image edges to accept
            a detection (in pixels). Helps avoid edge artifacts. Defaults to 15.

    Returns:
        np.ndarray: Array of (x, y) coordinates of detected stars, sorted by
            brightness (brightest first). Returns empty array with shape (0, 2)
            if no stars are detected.
    """
    all_detections = []
    height, width = data.shape

    for scale in scales:
        # Smooth the image with proper edge handling
        smoothed = ndimage.gaussian_filter(data, sigma=scale, mode="reflect")

        # Detect stars
        stars = find_stars_dao(smoothed, threshold=threshold, fwhm=scale * 2)

        if len(stars) > 0:
            # Filter out stars too close to edges
            valid_stars = []
            for star in stars:
                x, y = star
                if (
                    edge_buffer <= x < width - edge_buffer
                    and edge_buffer <= y < height - edge_buffer
                ):
                    brightness = data[int(y), int(x)]
                    valid_stars.append([x, y, scale, brightness])

            if valid_stars:
                all_detections.extend(valid_stars)

    if not all_detections:
        return np.array([]).reshape(0, 2)

    # Convert to numpy array for easier manipulation
    all_detections = np.array(all_detections)

    # Remove duplicates
    unique_stars = remove_duplicates(all_detections, width, height)

    # Sort by brightness (descending order)
    sorted_indices = np.argsort(unique_stars[:, 3])[::-1]
    sorted_stars = unique_stars[sorted_indices]

    # Return only x, y coordinates
    return sorted_stars[:, :2]


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
        desired_center = SkyCoord(self.target_ra, self.target_dec, unit=[u.deg, u.deg])
        plating_center = SkyCoord(
            self.plating_ra, self.plating_dec, unit=[u.deg, u.deg]
        )
        return desired_center.separation(plating_center).deg

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
        return float(plating_center.ra.deg), float(plating_center.dec.deg)

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

    def plot(self, ax=None, matched=False, transpose=False, **kwargs):
        """
        Plot detected stars and their Gaia matches for visualization.

        Parameters:
            ax (matplotlib.axes.Axes, optional): Axes to plot on. If None, creates new figure.
            matched (bool, optional): If True, only show matched stars. If False, show
                all Gaia stars. Defaults to False.
            transpose (bool, optional): If True, transpose x and y coordinates for plotting.
                Defaults to False.
            **kwargs: Additional keyword arguments passed to scatter plot.
        """

        if ax is None:
            fig, ax = plt.subplots()

        default_dict = {"s": 40, "facecolors": "none", "edgecolors": "r"}

        gaia_stars = self.find_gaia_match()[0]
        dim = (1, 0) if transpose else (0, 1)

        ax.scatter(
            self.stars_in_image[::, dim[0]],
            self.stars_in_image[:, dim[1]],
            label="Detected stars",
            **(default_dict | kwargs),
        )
        ax.scatter(
            gaia_stars[:, dim[0]],
            gaia_stars[:, dim[1]],
            label="Gaia stars",
            **(default_dict | {"edgecolors": "dodgerblue", "ls": "--"} | kwargs),
        )
        if not matched:
            non_matched_gaia_stars = np.array(
                [star for star in self.gaia_stars_in_image if star not in gaia_stars]
            )

            ax.scatter(
                non_matched_gaia_stars[:, dim[0]],
                non_matched_gaia_stars[:, dim[1]],
                label="Non matched Gaia stars",
                **(default_dict | {"edgecolors": "dodgerblue", "ls": ":"} | kwargs),
            )


class PointingCorrectionHandler:
    """A handler for performing pointing corrections on astronomical images.

    This class is responsible for managing the process of correcting the pointing of
    astronomical images based on detected stars and their corresponding coordinates
    from the Gaia database. It provides methods to create an instance from an image
    or a FITS file, and it includes functionality for cleaning images, extracting
    relevant metadata, and verifying the results of the plate solving process.

    Attributes
    ----------
    pointing_correction: PointingCorrection
        The pointing correction between the desired target center and the plating center.
    image_star_mapping: ImageStarMapping
        The mapping of stars detected in the image to their corresponding Gaia star coordinates.

    Examples
    --------
    Here is an example of how to use the PointingCorrectionHandler on a simulated image:

    ```python
    import datetime
    import cabaret
    import matplotlib.pyplot as plt
    from astra.pointer import PointingCorrectionHandler

    # Create an observatory with a camera
    observatory = cabaret.Observatory(
        name="MyObservatory",
        camera=cabaret.Camera(
            height=1024,  # Height of the camera in pixels
            width=1024,   # Width of the camera in pixels
        ),
    )

    # Define target coordinates
    ra = 100  # Target right ascension in degrees
    dec = 34  # Target declination in degrees

    # Simulate real observed coordinates (with a small offset)
    real_ra, real_dec = ra + 0.01, dec - 0.02

    # Define the observation time
    dateobs = datetime.datetime(2025, 3, 1, 21, 1, 35, 86730, tzinfo=datetime.timezone.utc)

    # Generate an image based on the target coordinates and observation time
    data = observatory.generate_image(
        ra=real_ra,      # Right ascension in degrees
        dec=real_dec,    # Declination in degrees
        exp_time=30,     # Exposure time in seconds
        dateobs=dateobs,  # Time of observation
    )

    # Create a PointingCorrectionHandler instance from the generated image
    pointing_corrector = PointingCorrectionHandler.from_image(
        data,
        target_ra=ra,                     # Target right ascension
        target_dec=dec,                   # Target declination
        dateobs=dateobs,                  # Observation date
        plate_scale=observatory.camera.plate_scale / 3600,  # Plate scale in degrees per pixel
    )

    # Optional: Display the generated image
    plt.imshow(data, cmap='gray')
    plt.title("Generated Image")
    plt.colorbar(label='Pixel Intensity')
    plt.show()

    # Print the pointing correction details
    print(pointing_corrector)
    ```
    """

    def __init__(
        self,
        pointing_correction: PointingCorrection,
        image_star_mapping: ImageStarMapping,
    ):
        self.pointing_correction = pointing_correction
        self.image_star_mapping = image_star_mapping

    @classmethod
    def from_image(
        cls,
        image: np.ndarray,
        target_ra: float,
        target_dec: float,
        dateobs: datetime,
        plate_scale: float,
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

        Returns:
            PointingCorrectionHandler: Instance with computed pointing correction
                and image-star mapping.

        Raises:
            Exception: If insufficient stars are detected for plate solving,
                if the offset is larger than the field of view, or if too few
                stars are matched.
        """
        image_clean = cls._clean_image(image)

        # Detect stars in the image
        stars_in_image = find_stars_multiscale(
            image_clean, scales=[1, 2, 3], threshold=7, edge_buffer=10
        )

        # Limit number of stars and gaia stars to use for plate solve
        number_of_stars_to_use = min(len(stars_in_image), 16)

        if number_of_stars_to_use < 4:
            raise Exception("Not enough stars detected for plate solve")

        stars_in_image = stars_in_image[0:number_of_stars_to_use]
        gaia_star_coordinates = cls._get_gaia_star_coordinates(
            target_ra,
            target_dec,
            image_clean,
            dateobs,
            plate_scale,
            fov_scale=1.2,
            limit=2 * number_of_stars_to_use,
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

        cls._verify_offset_within_fov(
            pointing_correction, plate_scale, image_clean.shape
        )
        cls._verify_plate_solve(
            image_star_mapping,
            pixel_threshold=20,
            number_of_stars_to_match=np.floor(number_of_stars_to_use * 0.8),
        )

        return cls(
            pointing_correction=pointing_correction,
            image_star_mapping=image_star_mapping,
        )

    @classmethod
    def from_fits_file(
        cls,
        filepath: str | Path | None,
        dark_frame: str | Path | None = None,
        target_ra: float | None = None,
        target_dec: float | None = None,
    ):
        """
        Create a PointingCorrectionHandler instance from a FITS file.

        Reads a FITS file, extracts metadata, and performs plate solving.
        Optionally applies dark frame subtraction and allows override of
        target coordinates.

        Parameters:
            filepath (str | Path): Path to the FITS file to process.
            dark_frame (str | Path | None, optional): Path to dark frame for
                subtraction. If None, no dark subtraction is performed. Defaults to None.
            target_ra (float | None, optional): Target right ascension in degrees.
                If None, uses RA from FITS header. Defaults to None.
            target_dec (float | None, optional): Target declination in degrees.
                If None, uses DEC from FITS header. Defaults to None.

        Returns:
            PointingCorrectionHandler: Instance with computed pointing correction.

        Raises:
            FileNotFoundError: If the FITS file or dark frame file doesn't exist.
            KeyError: If required metadata is missing from FITS header.
        """
        image, header = cls._read_fits_file(filepath)

        if dark_frame:
            dark_image, _ = cls._read_fits_file(dark_frame)
            image = image - dark_image

        if target_dec is None:
            target_dec = float(header["DEC"])
        if target_ra is None:
            target_ra = float(header["RA"])

        dateobs, plate_scale = cls._extract_plate_scale_and_dateobs(header)

        return cls.from_image(image, target_ra, target_dec, dateobs, plate_scale)

    @staticmethod
    def _read_fits_file(filepath: str | Path):
        """
        Read a FITS file and return the image data and header.

        Parameters:
            filepath (str | Path): Path to the FITS file to read.

        Returns:
            Tuple[np.ndarray, fits.Header]: Image data as int16 array and FITS header.

        Raises:
            FileNotFoundError: If the specified file does not exist.
        """
        if not Path(filepath).exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        with fits.open(filepath) as hdu:
            header = hdu[0].header
            image = hdu[0].data.astype(np.int16)

        return image, header

    @staticmethod
    def _extract_plate_scale_and_dateobs(header):
        """
        Extract plate scale and observation date from FITS header.

        Parameters:
            header (fits.Header): FITS header containing metadata.

        Returns:
            Tuple[datetime, float]: Observation date and plate scale in degrees per pixel.
        """
        dateobs = pd.to_datetime(header["DATE-OBS"])
        plate_scale = np.arctan(
            (header["XPIXSZ"] * 1e-6) / (header["FOCALLEN"] * 1e-3)
        ) * (180 / np.pi)  # deg/pixel
        return dateobs, plate_scale

    @staticmethod
    def _get_gaia_star_coordinates(
        ra, dec, image_clean, dateobs, plate_scale, fov_scale=1.1, limit=24
    ):
        """
        Retrieve Gaia star coordinates for the image field of view.

        Parameters:
            ra (float): Central right ascension in degrees.
            dec (float): Central declination in degrees.
            image_clean (np.ndarray): Cleaned image data for shape calculation.
            dateobs (datetime): Observation date for proper motion corrections.
            plate_scale (float): Plate scale in degrees per pixel.
            fov_scale (float, optional): Factor to expand FOV for star query. Defaults to 1.1.
            limit (int, optional): Maximum number of stars to retrieve. Defaults to 24.

        Returns:
            np.ndarray: Array of Gaia star coordinates in degrees.
        """
        fov = plate_scale * np.array(image_clean.shape)
        fov[0] *= 1 / np.abs(np.cos(dec * np.pi / 180))
        # TODO: put tmass option in config
        return gaia_db_query(
            (ra, dec), fov_scale * fov, tmass=True, dateobs=dateobs, limit=limit
        )

    @staticmethod
    def _clean_image(data: np.ndarray) -> np.ndarray:
        """
        Clean astronomical image by removing background and applying filters.

        Performs background subtraction using 2D background estimation,
        applies median filtering, and corrects for horizontal banding artifacts.

        Parameters:
            data (np.ndarray): Raw image data.

        Returns:
            np.ndarray: Cleaned image data with background removed and artifacts corrected.
        """

        data = data.astype(np.int16)

        bkg = Background2D(
            data,
            (32, 32),
            filter_size=(3, 3),
            sigma_clip=SigmaClip(sigma=3.0),
            bkg_estimator=MedianBackground(),
        )
        bkg_clean = data - bkg.background

        med_clean = ndimage.median_filter(bkg_clean, size=5, mode="mirror")
        band_corr = np.median(med_clean, axis=1).reshape(-1, 1)
        image_clean = med_clean - band_corr
        image_clean = np.clip(image_clean, 1, None)

        return image_clean

    @staticmethod
    def _verify_offset_within_fov(
        pointing_correction: PointingCorrection,
        plate_scale: float,
        image_shape: Tuple[int, int],
    ):
        """
        Verify that the calculated pointing offset is within the image field of view.

        Parameters:
            pointing_correction (PointingCorrection): The calculated pointing correction.
            plate_scale (float): Plate scale in degrees per pixel.
            image_shape (Tuple[int, int]): Image dimensions as (height, width).

        Raises:
            Exception: If the pointing offset exceeds the field of view, indicating
                a failed plate solve.
        """
        if max(plate_scale * np.array(image_shape)) < abs(
            pointing_correction.angular_separation
        ):
            raise Exception("Plate solve failed, offset larger than field of view")

    @staticmethod
    def _verify_plate_solve(
        image_star_mapping: ImageStarMapping,
        pixel_threshold: int = 20,
        number_of_stars_to_match: int = 4,
    ):
        """
        Verify that sufficient stars were matched for a reliable plate solve.

        Parameters:
            image_star_mapping (ImageStarMapping): The star mapping results.
            pixel_threshold (int, optional): Maximum distance in pixels to consider
                a match valid. Defaults to 20.
            number_of_stars_to_match (int, optional): Minimum number of stars that
                must be matched. Defaults to 4.

        Raises:
            Exception: If insufficient stars are matched, indicating a failed plate solve.
        """
        number_of_matched_stars = image_star_mapping.number_of_matched_stars(
            pixel_threshold
        )

        # tolerate 10% less stars matched
        if number_of_matched_stars < number_of_stars_to_match:
            raise Exception(
                f"Plate solve failed: only {number_of_matched_stars:.0f} stars matched out of {number_of_stars_to_match:.0f} required."
            )

    def __repr__(self):
        return (
            f"PointingCorrectionHandler(pointing_correction={self.pointing_correction}, "
            f"image_star_mapping={self.image_star_mapping})"
        )


def gaia_db_query(
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

    Example:
        >>> from astropy.coordinates import SkyCoord
        >>> from datetime import datetime
        >>> center = SkyCoord(ra=10.68458, dec=41.26917, unit='deg')
        >>> fov = 0.1  # degrees
        >>> stars = gaia_db_query(center, fov, limit=50)
        >>> print(f"Found {len(stars)} stars")
    """

    if isinstance(center, SkyCoord):
        ra = center.ra.deg
        dec = center.dec.deg
    else:
        ra, dec = center

    if not isinstance(fov, u.Quantity):
        fov = fov * u.deg

    if fov.ndim == 1:
        ra_fov, dec_fov = fov.to(u.deg).value
    else:
        ra_fov = fov[0].to(u.deg).value
        dec_fov = fov[1].to(u.deg).value

    min_dec = dec - dec_fov / 2
    max_dec = dec + dec_fov / 2
    min_ra = ra - ra_fov / 2
    max_ra = ra + ra_fov / 2

    table = db_query(Config().gaia_db, min_dec, max_dec, min_ra, max_ra)
    if tmass:
        table = table.sort_values(by=["j_m"]).reset_index(drop=True)
    else:
        table = table.sort_values(by=["phot_g_mean_mag"]).reset_index(drop=True)

    table.replace("", np.nan, inplace=True)
    table.dropna(inplace=True)

    # limit number of stars
    table = table[0:limit]

    # add proper motion to ra and dec
    if dateobs is not None:
        # calculate fractional year
        dateobs = dateobs.year + (dateobs.timetuple().tm_yday - 1) / 365.25  # type: ignore

        years = dateobs - 2015.5  # type: ignore
        table["ra"] += years * table["pmra"] / 1000 / 3600
        table["dec"] += years * table["pmdec"] / 1000 / 3600

    return np.array([table["ra"].values, table["dec"].values]).T


class ConstantDistorter:
    """
    Testing utility for verifying pointing correction sign conventions.

    A simple model that applies a constant offset to telescope coordinates,
    used to verify that pointing corrections are applied with the correct
    sign convention. Helps ensure that calculated corrections will move
    the telescope in the right direction.

    Parameters:
        error (float, optional): The constant pointing error to simulate in degrees.
            Defaults to 1.

    Methods:
        plated_to_target_coords: Apply correction from measured to true coordinates
        target_to_plated_coords: Apply error from target to measured coordinates
        test: Demonstrate the correction process with example coordinates

    Example:
        >>> distorter = ConstantDistorter(error=0.1)
        >>> distorter.test(target_coords=100.0)
        # This will demonstrate how the correction process works
    """

    def __init__(self, error: float = 1):
        """
        Initialize the ConstantDistorter with a specified pointing error.

        Parameters:
            error (float, optional): The constant pointing error to simulate in degrees.
                Defaults to 1.
        """
        self.error = error

    def plated_to_target_coords(self, real):
        """
        Convert measured (plated) coordinates to true target coordinates.

        Parameters:
            real (float): The measured coordinate from plate solving.

        Returns:
            float: The true target coordinate that corresponds to the measured position.
        """
        return real + self.error

    def target_to_plated_coords(self, target):
        """
        Convert target coordinates to expected measured (plated) coordinates.

        Parameters:
            target (float): The intended target coordinate.

        Returns:
            float: The coordinate that would be measured if pointing at the target.
        """
        return target - self.error

    def test(self, target_coords=0):
        """
        Demonstrate the correction process with example coordinates.

        Shows how pointing corrections work by simulating the complete cycle:
        target -> measured -> corrected target -> final measured position.

        Parameters:
            target_coords (float, optional): Example target coordinate to test with.
                Defaults to 0.
        """
        real = self.target_to_plated_coords(target=target_coords)
        proxy_target_coords = target_coords - (real - target_coords)
        final_real = self.target_to_plated_coords(proxy_target_coords)
        print(
            f"Pointing to the target coordinate {target_coords}"
            f"results in the following coordinate {real} found by plating.\n"
            f"If we now point to the proxy target coordinate {proxy_target_coords} "
            f"we will actually arrive at {final_real} i.e. our original target coordinate."
        )
        if not final_real == target_coords:
            print("Sign convention is wrong.")
