import math
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from alpaca.telescope import AlignmentModes
from astropy.io import fits

from astra import guiding
from astra.exposure_timeline import ExposureTimeline
from astra.guiding import IMAGES_TO_STABILISE, MAX_CULLED_IN_A_ROW, Guider

NAN = (math.nan, math.nan)


class FakeTelescope:
    """Records the image the guider was measuring when each pulse was sent."""

    device_name = "T1"

    def __init__(self):
        self.pulses = []
        self.frame = None

    def get(self, name, **kwargs):
        if name == "AlignmentMode":
            return AlignmentModes.algPolar
        if name == "SideOfPier":
            return 0
        if name == "Declination":
            return 0.0
        if name == "PulseGuide":
            self.pulses.append((self.frame, kwargs["Direction"], kwargs["Duration"]))
            return None
        if name == "IsPulseGuiding":
            return False
        raise AssertionError(f"unexpected call {name}")


def _guider(telescope) -> Guider:
    guider = Guider(
        telescope,
        logger=MagicMock(),
        database_manager=MagicMock(),
        params={
            "PIX2TIME": {"+x": 1.0, "-x": 1.0, "+y": 1.0, "-y": 1.0},
            "DIRECTIONS": {"+x": "East", "-x": "West", "+y": "North", "-y": "South"},
            "RA_AXIS": "x",
            "PID_COEFFS": {
                "x": {"p": 0.7, "i": 0.02, "d": 0.0},
                "y": {"p": 0.7, "i": 0.02, "d": 0.0},
                "set_x": 0.0,
                "set_y": 0.0,
            },
        },
    )
    guider.running = True
    return guider


def _run_loop(tmp_path, monkeypatch, shifts):
    """
    Run the guider loop on one image per shift in `shifts` and return the
    telescope and the guider. Frame i of the list is the i-th image measured.
    """
    image = tmp_path / "image.fits"
    fits.PrimaryHDU().writeto(image)

    telescope = FakeTelescope()
    guider = _guider(telescope)

    class FakeDonuts:
        def __init__(self, *args, **kwargs):
            pass

        def measure_shift(self, check_file):
            telescope.frame = len(measured)
            x, y = shifts[len(measured)]
            measured.append((x, y))
            return SimpleNamespace(
                x=SimpleNamespace(value=x), y=SimpleNamespace(value=y)
            )

    measured = []
    # the first image is the reference, then one image per shift
    images_left = [len(shifts) + 1]

    def wait_for_image(camera_name, image_handler):
        if images_left[0] == 0:
            guider.running = False
            return None, None, None, None
        images_left[0] -= 1
        return image, "field", "zYJ", 18.0

    monkeypatch.setattr(guiding, "Donuts", FakeDonuts)
    monkeypatch.setattr(guider, "waitForImage", wait_for_image)
    monkeypatch.setattr(guider, "getReferenceImage", lambda *args: str(image))

    image_handler = SimpleNamespace(
        last_image_path=image, exposure_timeline=ExposureTimeline()
    )
    guider.guider_loop("cam", image_handler)

    guider.logger.report_device_issue.assert_not_called()
    assert len(measured) == len(shifts)
    return telescope, guider


def _pulsed_frames(telescope):
    return sorted({frame for frame, _, _ in telescope.pulses})


def test_guide_ignores_a_nan_shift():
    telescope = FakeTelescope()
    guider = _guider(telescope)

    applied, pidx, pidy, _, _ = guider.guide(math.nan, 1.0, -1, "cam")

    assert (applied, pidx, pidy) == (False, 0.0, 0.0)
    assert telescope.pulses == []
    assert guider.BUFF_X == [] and guider.BUFF_Y == []


def test_guide_still_corrects_after_a_nan_shift():
    """A NaN once stayed in the PID integrator and stopped all later pulses."""
    telescope = FakeTelescope()
    guider = _guider(telescope)

    guider.guide(math.nan, math.nan, -1, "cam")
    _, pidx, pidy, _, _ = guider.guide(2.0, 2.0, -1, "cam")

    assert math.isfinite(pidx) and math.isfinite(pidy)
    assert len(telescope.pulses) == 2


def test_loop_keeps_guiding_after_nan_shifts(tmp_path, monkeypatch):
    """The pattern of the night of 2026-10-19 on Callisto, up to the first > 20 px shift."""
    stable = [(1.0, 1.0)] * 4
    shifts = stable + [NAN] * 4 + [(10.0, 13.0), (11.0, 15.0), (12.5, 17.7)]

    telescope, guider = _run_loop(tmp_path, monkeypatch, shifts)

    assert _pulsed_frames(telescope)[-3:] == [8, 9, 10]
    assert all(math.isfinite(v) for v in guider.BUFF_X + guider.BUFF_Y)
    assert guider.logger.warning.call_count == 4


def test_nan_shifts_do_not_use_up_the_stabilisation(tmp_path, monkeypatch):
    """The first real shift is still corrected with the wider stabilisation limit."""
    shifts = [NAN] * IMAGES_TO_STABILISE + [(30.0, 0.0)]

    telescope, _ = _run_loop(tmp_path, monkeypatch, shifts)

    assert _pulsed_frames(telescope) == [IMAGES_TO_STABILISE]


def test_loop_stabilises_again_after_too_many_large_shifts(tmp_path, monkeypatch):
    stable = [(1.0, 1.0)] * 4
    shifts = stable + [(25.0, 25.0)] * (MAX_CULLED_IN_A_ROW + 1)

    telescope, guider = _run_loop(tmp_path, monkeypatch, shifts)

    last = len(shifts) - 1
    assert last in _pulsed_frames(telescope)
    assert not set(range(4, last)) & set(_pulsed_frames(telescope))
    # the stabilisation clips to 40 px, so the whole shift is pulled back
    assert {duration for frame, _, duration in telescope.pulses if frame == last} == {
        25
    }
    guider.logger.warning.assert_called_once()


@pytest.mark.parametrize("good_frame", range(5, 4 + MAX_CULLED_IN_A_ROW))
def test_good_shift_resets_the_large_shift_count(tmp_path, monkeypatch, good_frame):
    shifts = [(1.0, 1.0)] * 4 + [(25.0, 25.0)] * MAX_CULLED_IN_A_ROW
    shifts.insert(good_frame, (1.0, 1.0))

    telescope, guider = _run_loop(tmp_path, monkeypatch, shifts)

    assert max(_pulsed_frames(telescope)) == good_frame
    guider.logger.warning.assert_not_called()
