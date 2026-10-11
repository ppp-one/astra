import logging

import numpy as np
import pytest
from astropy.coordinates import SkyCoord
from astropy.wcs import WCS

from astra import pointer
from astra.logger import ObservatoryLogger
from astra.pointer import ImageStarMapping, PlateSolveError, _verify_plate_solve

PLATE_SCALE = 0.35 / 3600  # deg/pixel


def make_wcs(scale_x=PLATE_SCALE, scale_y=PLATE_SCALE, skew_deg=0.0):
    wcs = WCS(naxis=2)
    wcs.wcs.ctype = ["RA---TAN", "DEC--TAN"]
    wcs.wcs.crval = [100.0, 34.0]
    wcs.wcs.crpix = [512, 512]
    skew = np.radians(skew_deg)
    wcs.wcs.cd = [[-scale_x, scale_y * np.sin(skew)], [0.0, scale_y * np.cos(skew)]]
    return wcs


def make_mapping(wcs, n_stars=12, offset_px=0.0):
    """Gaia stars that land `offset_px` from each detected star under `wcs`."""
    rng = np.random.default_rng(0)
    stars = rng.uniform(50, 950, size=(n_stars, 2))
    sky = wcs.pixel_to_world(stars[:, 0] + offset_px, stars[:, 1])
    gaia = np.column_stack([sky.ra.deg, sky.dec.deg])
    gaia_in_image = np.array(SkyCoord(gaia, unit="deg").to_pixel(wcs)).T
    return ImageStarMapping(wcs, stars, gaia_in_image), stars, gaia


def test_no_twirl_match_gives_clear_error(monkeypatch):
    monkeypatch.setattr(pointer.twirl, "compute_wcs", lambda *args, **kwargs: None)
    stars = np.zeros((4, 2))
    gaia = np.zeros((8, 2))

    with pytest.raises(PlateSolveError, match="no match between the 4 image stars"):
        pointer.ImageStarMapping.from_gaia_coordinates(stars, gaia)


def test_triangles_first_then_quads(monkeypatch):
    good_wcs = make_wcs()
    _, stars, gaia = make_mapping(good_wcs)
    calls = []

    def fake_compute_wcs(stars, gaia, **kwargs):
        calls.append(kwargs)
        # triangles give a solution with the wrong plate scale, quads a good one
        return make_wcs(scale_x=2 * PLATE_SCALE) if len(calls) == 1 else good_wcs

    monkeypatch.setattr(pointer.twirl, "compute_wcs", fake_compute_wcs)
    mapping = ImageStarMapping.from_gaia_coordinates(
        stars, gaia, plate_scale=PLATE_SCALE
    )

    assert calls == [{"asterism": 3, "min_match": 0.5}, {}]
    assert mapping.wcs is good_wcs


def test_triangle_exception_falls_back_to_quads(monkeypatch, caplog):
    good_wcs = make_wcs()
    _, stars, gaia = make_mapping(good_wcs)
    calls = []

    def fake_compute_wcs(stars, gaia, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise ValueError("Initial guess is outside of provided bounds")
        return good_wcs

    monkeypatch.setattr(pointer.twirl, "compute_wcs", fake_compute_wcs)
    with caplog.at_level("INFO", logger="astra.pointer"):
        mapping = ImageStarMapping.from_gaia_coordinates(
            stars, gaia, plate_scale=PLATE_SCALE
        )

    assert calls == [{"asterism": 3, "min_match": 0.5}, {}]
    assert mapping.wcs is good_wcs
    assert "triangles raised: Initial guess" in caplog.text
    assert "Plate solved with quads (quads fallback)" in caplog.text


def test_solve_result_goes_to_given_logger(monkeypatch):
    # The observatory logger has no parent, so records must be sent to it
    # directly to reach its DatabaseLoggingHandler (the UI log).
    class ListHandler(logging.Handler):
        def __init__(self):
            super().__init__()
            self.messages = []

        def emit(self, record):
            self.messages.append(record.getMessage())

    observatory_logger = ObservatoryLogger("test_obs", level=logging.INFO)
    handler = ListHandler()
    observatory_logger.addHandler(handler)

    good_wcs = make_wcs()
    _, stars, gaia = make_mapping(good_wcs)
    monkeypatch.setattr(pointer.twirl, "compute_wcs", lambda *a, **k: good_wcs)
    ImageStarMapping.from_gaia_coordinates(
        stars, gaia, plate_scale=PLATE_SCALE, logger=observatory_logger
    )

    assert any(m.startswith("Plate solved with triangles") for m in handler.messages)


def test_exceptions_from_both_attempts_give_plate_solve_error(monkeypatch):
    def fake_compute_wcs(*args, **kwargs):
        raise ValueError("bad match")

    monkeypatch.setattr(pointer.twirl, "compute_wcs", fake_compute_wcs)
    _, stars, gaia = make_mapping(make_wcs())

    with pytest.raises(PlateSolveError, match="triangles: bad match; quads: bad match"):
        ImageStarMapping.from_gaia_coordinates(stars, gaia, plate_scale=PLATE_SCALE)


def test_without_plate_scale_only_quads_are_used(monkeypatch):
    wrong_scale_wcs = make_wcs(scale_x=4 * PLATE_SCALE, scale_y=4 * PLATE_SCALE)
    _, stars, gaia = make_mapping(make_wcs())
    calls = []

    def fake_compute_wcs(stars, gaia, **kwargs):
        calls.append(kwargs)
        return wrong_scale_wcs

    monkeypatch.setattr(pointer.twirl, "compute_wcs", fake_compute_wcs)
    mapping = ImageStarMapping.from_gaia_coordinates(stars, gaia)

    assert calls == [{}]
    assert mapping.wcs is wrong_scale_wcs


def test_good_triangle_solve_skips_quads(monkeypatch, caplog):
    good_wcs = make_wcs()
    _, stars, gaia = make_mapping(good_wcs)
    calls = []

    def fake_compute_wcs(stars, gaia, **kwargs):
        calls.append(kwargs)
        return good_wcs

    monkeypatch.setattr(pointer.twirl, "compute_wcs", fake_compute_wcs)
    with caplog.at_level("INFO", logger="astra.pointer"):
        ImageStarMapping.from_gaia_coordinates(stars, gaia, plate_scale=PLATE_SCALE)

    assert calls == [{"asterism": 3, "min_match": 0.5}]
    assert (
        "Plate solved with triangles (no fallback): plate scale error +0.0% x +0.0%, "
        'axis ratio 1.000, skew 0.00 deg, 12/12 stars matched within 3.5" (10.0 px)'
    ) in caplog.text


def test_both_attempts_fail_reports_both(monkeypatch):
    _, stars, gaia = make_mapping(make_wcs())
    monkeypatch.setattr(
        pointer.twirl,
        "compute_wcs",
        lambda *args, **kwargs: make_wcs(scale_x=2 * PLATE_SCALE),
    )

    with pytest.raises(PlateSolveError, match="triangles: .*quads: "):
        ImageStarMapping.from_gaia_coordinates(stars, gaia, plate_scale=PLATE_SCALE)


def test_verify_accepts_good_solve():
    mapping, _, _ = make_mapping(
        make_wcs(scale_x=1.04 * PLATE_SCALE, scale_y=1.03 * PLATE_SCALE)
    )
    _verify_plate_solve(mapping, PLATE_SCALE)


@pytest.mark.parametrize(
    "wcs, match",
    [
        (
            make_wcs(scale_x=1.06 * PLATE_SCALE, scale_y=1.06 * PLATE_SCALE),
            "plate scale",
        ),
        (
            make_wcs(scale_x=0.94 * PLATE_SCALE, scale_y=0.94 * PLATE_SCALE),
            "plate scale",
        ),
        (
            make_wcs(scale_x=1.03 * PLATE_SCALE, scale_y=0.99 * PLATE_SCALE),
            "not square",
        ),
        (make_wcs(skew_deg=1.5), "not square"),
    ],
)
def test_verify_rejects_bad_geometry(wcs, match):
    mapping, _, _ = make_mapping(wcs)
    with pytest.raises(PlateSolveError, match=match):
        _verify_plate_solve(mapping, PLATE_SCALE)


def test_verify_match_radius_is_in_arcsec():
    # 3.5" is 10 px at 0.35"/px, so 9 px offsets match and 11 px do not
    mapping, _, _ = make_mapping(make_wcs(), offset_px=9)
    _verify_plate_solve(mapping, PLATE_SCALE)

    mapping, _, _ = make_mapping(make_wcs(), offset_px=11)
    with pytest.raises(PlateSolveError, match="0 of 12 detected stars"):
        _verify_plate_solve(mapping, PLATE_SCALE)

    # at 0.7"/px, 3.5" is 5 px
    mapping, _, _ = make_mapping(
        make_wcs(scale_x=2 * PLATE_SCALE, scale_y=2 * PLATE_SCALE), offset_px=6
    )
    with pytest.raises(PlateSolveError, match="5.0 px"):
        _verify_plate_solve(mapping, 2 * PLATE_SCALE)


def test_verify_needs_four_matches_and_no_fraction():
    # 4 of 20 matched (20%) is enough; 3 is not
    mapping, _, _ = make_mapping(make_wcs(), n_stars=20)
    far = np.full((16, 2), 1e4)
    mapping.gaia_stars_in_image = np.vstack([mapping.gaia_stars_in_image[:4], far])
    _verify_plate_solve(mapping, PLATE_SCALE)

    mapping.gaia_stars_in_image = np.vstack([mapping.gaia_stars_in_image[:3], far])
    with pytest.raises(PlateSolveError, match="3 of 20"):
        _verify_plate_solve(mapping, PLATE_SCALE)
