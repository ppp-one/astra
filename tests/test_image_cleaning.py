import types

import numpy as np
from scipy import ndimage

from astra.utils import image


def _scipy_median_blur(padded: np.ndarray, ksize: int) -> np.ndarray:
    """Stand-in for cv2.medianBlur: scipy median of the unpadded image."""
    inner = padded[2:-2, 2:-2]
    return np.pad(ndimage.median_filter(inner, size=ksize, mode="mirror"), 2)


def test_clean_image_median_matches_scipy(monkeypatch):
    """The OpenCV 5x5 median gives the same result as scipy, edges included."""
    rng = np.random.default_rng(0)
    data = rng.normal(1000.0, 20.0, (130, 100)).astype(np.float32)
    data[rng.integers(0, 130, 50), rng.integers(0, 100, 50)] = 60000.0  # hot pixels
    data[0, :] += 500.0  # bright edge row, to check the edge handling

    result = image.clean_image(data)

    monkeypatch.setattr(
        image, "cv2", types.SimpleNamespace(medianBlur=_scipy_median_blur)
    )
    np.testing.assert_array_equal(result, image.clean_image(data))
