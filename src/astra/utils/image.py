"""Image cleaning and background subtraction utilities."""

import numpy as np
from astropy.stats import SigmaClip, sigma_clipped_stats
from donuts.image import Image
from photutils.background import Background2D, MedianBackground
from scipy import ndimage

# smallest connected group of pixels above the noise floor that is kept as a
# star; smaller groups are hot pixel clusters that survive the median filter
MIN_SOURCE_PIXELS = 10

# number of 3x3 median passes; two passes also remove larger hot pixel
# clusters and clusters that touch each other
MEDIAN_PASSES = 2

# step between pixels used to estimate the noise floor
NOISE_SAMPLE_STEP = 4


class CustomImageClass(Image):
    """Enhanced image processing class with background subtraction and cleaning."""

    def preconstruct_hook(self) -> None:
        """
        Apply image preprocessing before Donuts star detection.

        Removes the background (including amp glow), hot pixels and hot pixel
        clusters, so that the shift is measured on stars only. Fixed detector
        defects are identical in the reference and check images and would
        otherwise pull the measured shift towards zero.
        """
        # if greater than 2Kx2K, crop to 2Kx2K for speed
        shapex, shapey = self.raw_image.shape
        if shapex > 2048 and shapey > 2048:
            self.raw_image = self.raw_image[
                shapex // 2 - 1024 : shapex // 2 + 1024,
                shapey // 2 - 1024 : shapey // 2 + 1024,
            ]

        data = clean_image(self.raw_image)
        _, median, std = sigma_clipped_stats(
            data[::NOISE_SAMPLE_STEP, ::NOISE_SAMPLE_STEP], sigma=3.0
        )

        # remove noise floor
        data -= median + 7 * std
        data[data < 0] = 0

        self.raw_image = data


def subtract_background(data: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Subtract a smooth 2D background, such as sky gradients and amp glow.

    Parameters:
        data (np.ndarray): The 2D image data.

    Returns:
        tuple: (background-subtracted image as float32, map of the local
            background noise with the same shape)
    """
    sigma_clip = SigmaClip(sigma=3.0)
    bkg_estimator = MedianBackground()

    # Convert to float32, handling both regular and masked arrays
    data = data.astype(np.float32)
    if np.ma.isMaskedArray(data):
        data = data.filled(fill_value=np.nan)

    box_size = 32
    bkg = Background2D(
        data,
        (box_size, box_size),
        filter_size=(3, 3),
        sigma_clip=sigma_clip,
        bkg_estimator=bkg_estimator,  # type: ignore
    )

    # repeat each box value instead of the slower smooth interpolation of
    # bkg.background_rms; the steps between boxes do not bias star positions
    noise_map = np.repeat(
        np.repeat(bkg.background_rms_mesh, box_size, axis=0), box_size, axis=1
    )[: data.shape[0], : data.shape[1]]

    return data - bkg.background, noise_map


def median_filter_3x3(data: np.ndarray) -> np.ndarray:
    """
    Exact 3x3 median filter with reflected edges.

    Gives the same result as ``scipy.ndimage.median_filter(data, size=3,
    mode="reflect")``, but about 10x faster, because it uses only element-wise
    minimum and maximum operations.

    Parameters:
        data (np.ndarray): The 2D image data.

    Returns:
        np.ndarray: The filtered image.
    """
    padded = np.pad(data, 1, mode="symmetric")

    # sort each vertical triple of pixels into low, middle and high
    top, centre, bottom = padded[:-2], padded[1:-1], padded[2:]
    low = np.minimum(top, centre)
    high = np.maximum(top, centre)
    mid = np.maximum(low, np.minimum(high, bottom))
    low = np.minimum(low, bottom)
    high = np.maximum(high, bottom)

    # the median of 3x3 is the median of: the largest low, the middle mid
    # and the smallest high of the three neighbouring columns
    max_low = np.maximum(np.maximum(low[:, :-2], low[:, 1:-1]), low[:, 2:])
    min_high = np.minimum(np.minimum(high[:, :-2], high[:, 1:-1]), high[:, 2:])
    mid_mid = _median_of_three(mid[:, :-2], mid[:, 1:-1], mid[:, 2:])

    return _median_of_three(max_low, mid_mid, min_high)


def _median_of_three(a: np.ndarray, b: np.ndarray, c: np.ndarray) -> np.ndarray:
    return np.maximum(np.minimum(a, b), np.minimum(np.maximum(a, b), c))


def remove_small_sources(
    data: np.ndarray, min_pixels: int, threshold: float = 0.0
) -> np.ndarray:
    """
    Set connected groups of pixels above ``threshold`` to zero, if the group
    has fewer than ``min_pixels`` pixels.

    Parameters:
        data (np.ndarray): The 2D image data, with the background at zero.
        min_pixels (int): Smallest group size to keep.
        threshold (float, optional): Pixels above this value form the groups.
            Defaults to 0.

    Returns:
        np.ndarray: The image with small groups removed (modified in place).
    """
    labels, _ = ndimage.label(data > threshold)
    sizes = np.bincount(labels.ravel())
    small = sizes < min_pixels
    small[0] = False  # label 0 is the pixels below the threshold
    data[small[labels]] = 0
    return data


def clean_image(data: np.ndarray) -> np.ndarray:
    """
    Remove the background, hot pixels and hot pixel clusters from an image.

    Used for both plate solving and guiding. The result is divided by the
    relative local noise, so that a single threshold works over the whole
    image: noise peaks in amp glow regions do not pass it.

    Parameters:
        data (np.ndarray): The 2D image data.

    Returns:
        np.ndarray: The cleaned image as float32, with the background at zero.
    """
    data, noise_map = subtract_background(data)
    for _ in range(MEDIAN_PASSES):
        data = median_filter_3x3(data)
    data = np.nan_to_num(data, nan=0.0)

    _, median, std = sigma_clipped_stats(
        data[::NOISE_SAMPLE_STEP, ::NOISE_SAMPLE_STEP], sigma=3.0
    )
    data = (data - median) / (noise_map / np.nanmedian(noise_map))

    return remove_small_sources(data, MIN_SOURCE_PIXELS, threshold=7 * std)
