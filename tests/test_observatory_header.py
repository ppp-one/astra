from datetime import datetime

import astropy.units as u
import pandas as pd
import pytest
from astropy.coordinates import EarthLocation, SkyCoord

from astra.header_manager import ObservatoryHeader


def make_fits_config(headers):
    """Helper to create a DataFrame for testing add_times."""
    return pd.DataFrame(
        [
            {
                "header": h,
                "comment": f"comment for {h}",
                "device_type": "astra",
                "fixed": False,
            }
            for h in headers
        ],
        index=headers,
    )


@pytest.fixture
def base_inputs():
    hdr = ObservatoryHeader(
        {
            "DATE-OBS": "2000-01-01T12:00:00",
            "EXPTIME": 60.0,
            "ALTITUDE": 45.0,  # degrees
            "LONG-OBS": 0.0,
            "LAT-OBS": 51.4779,
            "ALT-OBS": 46.0,
            "RA": 10.0,
            "DEC": 20.0,
        }
    )
    location = EarthLocation.of_site("greenwich")
    target = SkyCoord(ra=10 * u.deg, dec=20 * u.deg, frame="icrs")
    return hdr, location, target


def test_add_times_adds_expected_keys(base_inputs):
    hdr, location, target = base_inputs
    headers_to_add = [
        "JD-OBS",
        "JD-END",
        "HJD-OBS",
        "BJD-OBS",
        "MJD-OBS",
        "MJD-END",
        "DATE-END",
        "LST",
        "HA",
    ]
    fits_config = make_fits_config(headers_to_add)

    hdr.add_times(fits_config, location, target)

    for h in headers_to_add:
        assert h in hdr
        comment = hdr.comments[h]
        assert isinstance(comment, str)
        assert comment.startswith("comment")


def test_add_times_date_end_format(base_inputs):
    hdr, location, target = base_inputs
    fits_config = make_fits_config(["DATE-END", "AIRMASS"])
    hdr.add_times(fits_config, location, target)
    hdr.add_airmass(fits_config)
    val = hdr["DATE-END"]
    # Should be a tuple (value, comment)
    if isinstance(val, tuple):
        date_str, comment = val
    else:
        date_str = val
    # Ensure correct format: YYYY-MM-DDTHH:MM:SS.microseconds
    datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")


def test_add_times_airmass_reasonable(base_inputs):
    hdr, location, target = base_inputs
    fits_config = make_fits_config(["AIRMASS"])
    hdr.add_times(fits_config, location, target)
    hdr.add_airmass(fits_config)
    assert "AIRMASS" in hdr
    airmass = hdr["AIRMASS"]
    if isinstance(airmass, tuple):
        airmass = airmass[0]
    assert 1.0 <= airmass <= 2.0  # with altitude=45°, airmass should be ~1.4


def test_add_times_skips_fixed_entries(base_inputs):
    hdr, location, target = base_inputs
    fits_config = pd.DataFrame(
        [
            {
                "comment": "test",
                "device_type": "astra",
                "fixed": True,
            },
        ],
        index=["JD-OBS"],
    )
    hdr.add_times(fits_config, location, target)
    assert "JD-OBS" not in hdr


class TestObservatoryHeader:
    def test_validate(self):
        header = ObservatoryHeader.get_test_header()
        header["ALTITUDE"] = 45.0
        header["EXPTIME"] = 60.0
        header["DATE-OBS"] = "2024-05-15T12:00:00"
        header.validate()  # Should not raise
        del header["LONG-OBS"]
        with pytest.raises(ValueError, match="Missing required header keys"):
            header.validate()

    def test_convert_ra_from_hours_to_degrees(self):
        header = ObservatoryHeader.get_test_header()
        original_ra = header.ra
        header._convert_ra_from_hours_to_degrees()
        assert header["RA"] == original_ra * (360 / 24)

    def test_get_target_sky_coordinates(self):
        header = ObservatoryHeader.get_test_header()
        coords = header.get_target_sky_coordinates()
        assert coords.ra.value == header.ra  # RA in hours, as SkyCoord handles it
        assert coords.dec.value == header.dec
