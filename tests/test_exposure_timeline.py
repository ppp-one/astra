import datetime
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from astropy.io import fits

from astra.exposure_timeline import ExposureTimeline
from astra.guiding import Guider


def test_no_exposure_recorded():
    timeline = ExposureTimeline()

    assert not timeline.has_exposures()
    assert timeline.exposure_start_of("image.fits") is None


def test_last_saved_image_is_linked_to_its_exposure():
    timeline = ExposureTimeline()
    timeline.exposure_started()
    timeline.image_saved("a.fits")
    timeline.exposure_started()
    start = time.monotonic()
    timeline.image_saved("b.fits")

    assert timeline.has_exposures()
    assert timeline.exposure_start_of("b.fits") == pytest.approx(start, abs=0.05)
    assert timeline.exposure_start_of("a.fits") is None  # only the newest is kept


class FakeTelescope:
    """Records when each pulse is sent; IsPulseGuiding follows the durations."""

    device_name = "T1"

    def __init__(self):
        self.pulses = []
        self._busy_until = 0.0

    def get(self, name, **kwargs):
        if name == "Declination":
            return 0.0
        if name == "PulseGuide":
            now = time.monotonic()
            self.pulses.append((now, kwargs["Direction"], kwargs["Duration"]))
            self._busy_until = now + kwargs["Duration"] / 1000
            return None
        if name == "IsPulseGuiding":
            return time.monotonic() < self._busy_until
        raise AssertionError(f"unexpected call {name}")


def _guider(telescope) -> Guider:
    guider = Guider(
        telescope,
        logger=MagicMock(),
        database_manager=MagicMock(),
        params={
            "PIX2TIME": {"+x": 100.0, "-x": 100.0, "+y": 100.0, "-y": 100.0},
            "DIRECTIONS": {"+x": "East", "-x": "West", "+y": "North", "-y": "South"},
            "RA_AXIS": "x",
            "PID_COEFFS": {
                "x": {"p": 1.0, "i": 0.0, "d": 0.0},
                "y": {"p": 1.0, "i": 0.0, "d": 0.0},
                "set_x": 0.0,
                "set_y": 0.0,
            },
        },
    )
    guider.running = True
    return guider


def test_guide_records_when_the_pulses_have_finished():
    """The pulses are sent at once; the frame selection needs their end."""
    telescope = FakeTelescope()
    timeline = ExposureTimeline()

    _guider(telescope).guide(-1.0, -0.5, 1, "cam", timeline=timeline)

    assert [p[2] for p in telescope.pulses] == [50, 100]  # y first, then x
    last_sent = telescope.pulses[-1][0]
    assert last_sent + 0.1 <= timeline.last_pulse_end < last_sent + 0.2


def test_guide_without_pulses_records_nothing():
    timeline = ExposureTimeline()

    _guider(FakeTelescope()).guide(0.0, 0.0, 1, "cam", timeline=timeline)

    assert timeline.last_pulse_end == 0.0


def _image_measured(tmp_path, pulse_during_first: bool) -> str:
    """
    Save "first.fits", with or without the last pulse ending during its
    exposure, then "second.fits". Returns the image the guider measures.
    """
    for name in ("first.fits", "second.fits"):
        header = fits.Header({"FILTER": "r", "OBJECT": "field", "EXPTIME": 1.0})
        fits.PrimaryHDU(header=header).writeto(tmp_path / name)

    timeline = ExposureTimeline()
    image_handler = SimpleNamespace(
        exposure_timeline=timeline,
        last_image_path=None,
        last_image_timestamp=None,
        header={"EXPTIME": 0.0},
    )

    def save(name):
        timeline.exposure_started()
        timeline.image_saved(tmp_path / name)
        image_handler.last_image_path = tmp_path / name
        image_handler.last_image_timestamp = datetime.datetime.now(datetime.UTC)

    guider = _guider(FakeTelescope())
    guider.MIN_GUIDE_INTERVAL = 0.0
    result = {}
    waiter = threading.Thread(
        target=lambda: result.update(
            image=guider.waitForImage("cam", image_handler)[0]
        ),
        daemon=True,
    )

    if not pulse_during_first:
        timeline.pulse_finished()
        time.sleep(0.01)
    save("first.fits")
    if pulse_during_first:
        time.sleep(0.01)
        timeline.pulse_finished()
    image_handler.last_image_timestamp = None  # nothing new yet for the guider
    waiter.start()

    time.sleep(0.05)
    image_handler.last_image_timestamp = datetime.datetime.now(datetime.UTC)
    time.sleep(0.3)
    if waiter.is_alive():
        save("second.fits")
    waiter.join(timeout=2)
    guider.running = False

    return result["image"].name


def test_wait_for_image_skips_images_exposed_during_the_last_pulse(tmp_path):
    """Even a short overlap is skipped: the frame choice must not depend on timing."""
    assert _image_measured(tmp_path, pulse_during_first=True) == "second.fits"


def test_wait_for_image_uses_images_exposed_after_the_last_pulse(tmp_path):
    assert _image_measured(tmp_path, pulse_during_first=False) == "first.fits"


def _handler(timeline):
    return SimpleNamespace(
        exposure_timeline=timeline,
        last_image_path=None,
        last_image_timestamp=None,
        header={"EXPTIME": 0.0},
    )


def test_wait_for_image_stops_at_once_when_the_guider_is_stopped(tmp_path):
    """An abrupt end while waiting on an image the exposure loop never recorded."""
    timeline = ExposureTimeline()
    timeline.exposure_started()  # exposure started, but its image is never saved
    image_handler = _handler(timeline)
    guider = _guider(FakeTelescope())
    guider.MIN_GUIDE_INTERVAL = 0.0
    result = {}
    waiter = threading.Thread(
        target=lambda: result.update(out=guider.waitForImage("cam", image_handler)),
        daemon=True,
    )
    waiter.start()
    # an image appears that the timeline does not know
    image_handler.last_image_path = tmp_path / "unknown.fits"
    image_handler.last_image_timestamp = datetime.datetime.now(datetime.UTC)
    time.sleep(0.2)

    guider.running = False
    waiter.join(timeout=1)

    assert not waiter.is_alive()
    assert result["out"] == (None, None, None, None)


def test_aborted_exposure_does_not_block_the_next_image(tmp_path):
    header = fits.Header({"FILTER": "r", "OBJECT": "field", "EXPTIME": 1.0})
    fits.PrimaryHDU(header=header).writeto(tmp_path / "next.fits")
    timeline = ExposureTimeline()
    image_handler = _handler(timeline)
    guider = _guider(FakeTelescope())
    guider.MIN_GUIDE_INTERVAL = 0.0

    timeline.exposure_started()  # aborted: no image is saved for it
    timeline.exposure_started()
    timeline.image_saved(tmp_path / "next.fits")
    image_handler.last_image_path = tmp_path / "next.fits"
    result = {}
    waiter = threading.Thread(
        target=lambda: result.update(
            image=guider.waitForImage("cam", image_handler)[0]
        ),
        daemon=True,
    )
    waiter.start()
    time.sleep(0.05)
    image_handler.last_image_timestamp = datetime.datetime.now(datetime.UTC)
    waiter.join(timeout=2)
    guider.running = False

    assert result["image"] == tmp_path / "next.fits"
