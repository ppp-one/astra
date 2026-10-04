import numpy as np
import pytest
from astropy.io import fits
from donuts import Donuts
from scipy import ndimage

from astra.utils.image import (
    CustomImageClass,
    median_filter_3x3,
    remove_small_sources,
)


@pytest.mark.parametrize("shape", [(64, 64), (37, 51), (3, 3), (1, 5)])
def test_median_filter_3x3_matches_scipy(shape):
    rng = np.random.default_rng(0)
    data = rng.normal(100, 10, shape).astype(np.float32)

    expected = ndimage.median_filter(data, size=3, mode="reflect")

    np.testing.assert_array_equal(median_filter_3x3(data), expected)


def test_remove_small_sources():
    data = np.zeros((20, 20), dtype=np.float32)
    data[2:4, 2:5] = 5.0  # 6 pixel cluster
    data[10:15, 10:15] = 1.0  # 25 pixel star

    result = remove_small_sources(data, min_pixels=10)

    assert not result[2:4, 2:5].any()
    assert (result[10:15, 10:15] == 1.0).all()


def _sparse_field(shift_x: float, shift_y: float) -> np.ndarray:
    """Few stars, amp glow, and fixed hot pixels and one fixed hot cluster."""
    ny, nx = 1024, 1024
    yy, xx = np.mgrid[0:ny, 0:nx]
    stars = [(300, 200, 5000), (700, 650, 3000), (150, 800, 2000), (850, 300, 1500)]
    sigma = 2.0

    image = np.full((ny, nx), 1000.0)
    image += 2000 * np.exp(-((xx - nx) ** 2 + (yy - ny) ** 2) / (2 * 200**2))
    for x, y, peak in stars:
        image += peak * np.exp(
            -((xx - x - shift_x) ** 2 + (yy - y - shift_y) ** 2) / (2 * sigma**2)
        )

    rng = np.random.default_rng(int(shift_x * 100 + shift_y * 10) + 7)
    image += rng.normal(0, 10, image.shape)

    # fixed detector defects, the same in every frame
    defects = np.random.default_rng(1)
    image.flat[defects.integers(0, image.size, 500)] = 12000
    image[500:502, 500:503] = 12000  # 3x2 cluster survives a 3x3 median
    image[270:273, 134:137] = 19000  # two 3x3 clusters, one pixel apart,
    image[271:274, 138:141] = 19000  # join into one group after a 3x3 median

    return image.astype(np.float32)


def test_guiding_shift_ignores_fixed_hot_cluster(tmp_path):
    ref_path = tmp_path / "ref.fits"
    check_path = tmp_path / "check.fits"
    fits.writeto(ref_path, _sparse_field(0.0, 0.0))
    fits.writeto(check_path, _sparse_field(1.6, -0.7))

    donuts_ref = Donuts(
        ref_path,
        normalise=False,
        subtract_bkg=False,
        downweight_edges=False,
        image_class=CustomImageClass,
    )
    shift = donuts_ref.measure_shift(check_path)

    # Donuts returns the correction that moves the check image onto the reference
    assert shift.x.value == pytest.approx(-1.6, abs=0.1)
    assert shift.y.value == pytest.approx(0.7, abs=0.1)


def _cabaret_frame(camera, sources, dx: float, dy: float, seed: int) -> np.ndarray:
    """Simulated frame with the stars moved by (dx, dy) pixels, plus amp glow."""
    from astropy.coordinates import SkyCoord
    from cabaret import Site, Telescope, generate_image

    wcs = camera.get_wcs(SkyCoord(150.0, 20.0, unit="deg"))
    wcs.wcs.crpix = [wcs.wcs.crpix[0] + dx, wcs.wcs.crpix[1] + dy]
    image = generate_image(
        ra=150.0,
        dec=20.0,
        exp_time=10.0,
        camera=camera,
        telescope=Telescope(focal_length=8.0, diameter=1.0),
        site=Site(seeing=1.3, sky_background=1000 / (np.pi * 0.25 * 0.52**2 * 10)),
        sources=sources,
        wcs=wcs,
        seed=seed,
        airmass=1.2,
    ).astype(np.float64)

    yy, xx = np.mgrid[0 : camera.height, 0 : camera.width]
    glow = 3000 * np.exp(
        -((xx - camera.width) ** 2 + (yy - camera.height) ** 2) / (2 * 150**2)
    )
    image += np.random.default_rng(seed + 1).poisson(glow)

    return np.clip(image, 0, 65535).astype(np.uint16)


def test_guiding_shift_on_simulated_sparse_field_with_defects(tmp_path):
    """Few stars, with hot pixels, hot clusters, hot columns, amp glow and noisy pixels."""
    from astropy.coordinates import SkyCoord
    from cabaret import Camera, Sources
    from cabaret.camera import ConstantPixelDefect

    camera = Camera(
        width=1024,
        height=1024,
        plate_scale=0.52,  # FWHM 2.5 px for 1.3" seeing
        read_noise=8,
        pixel_defects={
            "hot": {"type": "constant", "value": 8000, "rate": 1e-3, "seed": 1},
            "noisy": {"type": "noise", "rate": 2e-3, "noise_level": 500, "seed": 2},
            "columns": {
                "type": "column",
                "value": 2500,
                "rate": 3 / 1024,
                "dim": 1,
                "seed": 3,
            },
        },
    )
    rng = np.random.default_rng(4)
    cluster_pixels = []
    for _ in range(40):
        h, w = [(1, 2), (2, 2), (2, 3), (3, 3), (2, 4)][rng.integers(5)]
        y, x = rng.integers(10, 1014, 2)
        cluster_pixels += [(y + i, x + j) for i in range(h) for j in range(w)]
    clusters = ConstantPixelDefect(name="clusters", value=20000)
    clusters.set_pixels(np.array(cluster_pixels), camera)
    camera.pixel_defects["clusters"] = clusters

    wcs = camera.get_wcs(SkyCoord(150.0, 20.0, unit="deg"))
    star_px = rng.uniform(30, 994, (6, 2))
    electrons = 10 ** rng.uniform(np.log10(2e3), np.log10(6e4), 6)
    sky = wcs.pixel_to_world(star_px[:, 0], star_px[:, 1])
    sources = Sources.from_arrays(
        ra=sky.ra.deg, dec=sky.dec.deg, fluxes=electrons / (0.8 * np.pi * 0.25 * 10)
    )

    ref_path = tmp_path / "ref.fits"
    fits.writeto(ref_path, _cabaret_frame(camera, sources, 0.0, 0.0, seed=10))
    donuts_ref = Donuts(
        ref_path,
        normalise=False,
        subtract_bkg=False,
        downweight_edges=False,
        image_class=CustomImageClass,
    )

    for i, (dx, dy) in enumerate([(2.3, -1.4), (-4.1, 3.6)]):
        check_path = tmp_path / f"check{i}.fits"
        fits.writeto(check_path, _cabaret_frame(camera, sources, dx, dy, seed=11 + i))

        shift = donuts_ref.measure_shift(check_path)

        assert shift.x.value == pytest.approx(-dx, abs=0.2)
        assert shift.y.value == pytest.approx(-dy, abs=0.2)
