from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from astra.database_manager import DatabaseManager
from astra.guiding import Guider, GuiderManager

PARAMS = {
    "PIX2TIME": {"+x": 100.0, "-x": 100.0, "+y": 100.0, "-y": 100.0},
    "DIRECTIONS": {"+x": "East", "-x": "West", "+y": "North", "-y": "South"},
    "RA_AXIS": "x",
    "PID_COEFFS": {
        "x": {"p": 0.7, "i": 0.02, "d": 0.0},
        "y": {"p": 0.5, "i": 0.02, "d": 0.0},
        "set_x": 0.0,
        "set_y": 0.0,
    },
}


@pytest.fixture
def db(temp_config):
    # temp_config is per session, so each test gets its own database file
    return DatabaseManager(f"test_guiding_{uuid4().hex}", logger=MagicMock())


@pytest.fixture
def guider(db):
    return Guider(
        SimpleNamespace(device_name="telescope_Europa"),
        logger=MagicMock(),
        database_manager=db,
        params=PARAMS,
    )


@pytest.fixture
def image(tmp_path):
    path = tmp_path / "camera_Europa_z_Sp0354+2416_10.000_20261022_065600.000.fits"
    path.write_bytes(b"")
    return path


def _reference(guider, field="Sp0354+2416", filt="z", exptime=10, pierside=0):
    return guider.getReferenceImage(field, filt, exptime, "camera_Europa", pierside)


@pytest.mark.parametrize(
    "field, filt",
    [("Sp0354+2416", "z'"), ("O'Brien's field", "z"), ("Sp0354+2416", "r''")],
)
def test_reference_with_quotes_can_be_set_found_and_cleared(guider, image, field, filt):
    guider.setReferenceImage(field, filt, 10, str(image), "camera_Europa", 0)
    assert _reference(guider, field, filt) is not None

    guider.clearReferenceImage(field, filt, 10, "camera_Europa", 0)

    assert _reference(guider, field, filt) is None


def test_reference_values_are_stored_as_before(guider, db, image):
    """Rows written with the old '%s' formatting must still match."""
    guider.setReferenceImage("Sp0354+2416", "z", 18.0, str(image), "camera_Europa", 1)

    row = db.execute("SELECT field, filter, exptime, pierside FROM autoguider_ref")
    assert row == [("Sp0354+2416", "z", "18.0", 1)]


def test_messages_with_quotes_are_logged(guider, db):
    guider.logMessageToDb("camera_Europa", "Problem with 'file.fits'")

    assert db.execute("SELECT message FROM autoguider_info_log") == [
        ("Problem with 'file.fits'",)
    ]


class FakePairedDevices(dict):
    def __init__(self):
        super().__init__(Telescope="telescope_Europa", Camera="camera_Europa")
        self.telescope = SimpleNamespace(get=lambda name: 0)
        self.camera = SimpleNamespace(get=lambda name: 1)


def test_start_guider_clears_reference_for_filter_with_quote(guider, image):
    """The guider loop stores z' as z, so the reset must clear z."""
    guider.setReferenceImage("Sp0354+2416", "z", 10, str(image), "camera_Europa", 0)
    manager = GuiderManager({"telescope_Europa": guider}, MagicMock(), MagicMock())
    image_handler = SimpleNamespace(
        header={"OBJECT": "Sp0354+2416", "FILTER": "z'", "EXPTIME": 10}
    )

    manager.start_guider(image_handler, FakePairedDevices(), MagicMock())

    manager.logger.warning.assert_not_called()
    assert _reference(guider) is None
