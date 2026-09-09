"""Image cleaning and background subtraction utilities."""

import numpy as np
from astropy.stats import SigmaClip, sigma_clipped_stats
from donuts.image import Image
from photutils.background import Background2D, MedianBackground
from scipy import ndimage


class CustomImageClass(Image):
    """Enhanced image processing class with background subtraction and cleaning."""

    def preconstruct_hook(self) -> None:
        """
        Apply image preprocessing before Donuts star detection.

        Performs background subtraction, noise reduction, and systematic
        correction to improve star detection reliability.
        """
        # if greater than 2Kx2K, crop to 2Kx2K for speed
        shapex, shapey = self.raw_image.shape
        if shapex > 2048 and shapey > 2048:
            self.raw_image = self.raw_image[
                shapex // 2 - 1024 : shapex // 2 + 1024,
                shapey // 2 - 1024 : shapey // 2 + 1024,
            ]

        self.raw_image = clean_image(self.raw_image)
        mean, median, std = sigma_clipped_stats(self.raw_image, sigma=3.0)

        # remove noise floor
        self.raw_image -= median + 7 * std
        self.raw_image[self.raw_image < 0] = 0


def clean_image(data: np.ndarray) -> np.ndarray:
    """
    Clean an image by subtracting the background.

    Parameters:
        data (np.ndarray): The 2D image data.

    Returns:
        np.ndarray: The background-subtracted image.
    """

    sigma_clip = SigmaClip(sigma=3.0)
    bkg_estimator = MedianBackground()

    # Convert to float32, handling both regular and masked arrays
    data = data.astype(np.float32)
    if np.ma.isMaskedArray(data):
        data = data.filled(fill_value=np.nan)

    bkg = Background2D(
        data,
        (32, 32),
        filter_size=(3, 3),
        sigma_clip=sigma_clip,
        bkg_estimator=bkg_estimator,  # type: ignore
    )

    bkg_clean = data - bkg.background

    med_clean = ndimage.median_filter(
        bkg_clean, size=5, mode="mirror"
    )  # slow but needed

    # add minimum back to avoid negative values
    med_clean += np.abs(np.nanmin(med_clean))

    return med_clean
