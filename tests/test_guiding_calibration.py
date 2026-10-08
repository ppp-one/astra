import numpy as np
import pytest
from alpaca.telescope import GuideDirections

from astra.calibrate_guiding import calibration_from_shifts
from astra.guiding import Guider

PULSE_TIME = 5000.0  # ms
MOVE = 50.0  # pixels per calibration pulse, so 100 ms per pixel


def _moves(angle: float, mirrored: bool) -> dict[str, np.ndarray]:
    """Change of the measured shift per ms of pulse, for a rotated camera."""
    a = np.radians(angle)
    east = -np.array([np.cos(a), np.sin(a)]) * MOVE / PULSE_TIME
    north = -np.array([-np.sin(a), np.cos(a)]) * MOVE / PULSE_TIME
    if mirrored:
        north = -north
    return {"East": east, "West": -east, "North": north, "South": -north}


def _calibrate(angle: float, mirrored: bool) -> dict:
    shifts = {name: move * PULSE_TIME for name, move in _moves(angle, mirrored).items()}
    return calibration_from_shifts(shifts, PULSE_TIME)


class FakeMount:
    """Moves the measured shift along the true, rotated axes."""

    device_name = "T"

    def __init__(self, moves: dict[str, np.ndarray]):
        self.moves = moves
        self.total = np.zeros(2)

    def get(self, name, **kwargs):
        if name == "Declination":
            return 0.0
        if name == "PulseGuide":
            direction = GuideDirections(kwargs["Direction"]).name.removeprefix("guide")
            self.total += kwargs["Duration"] * self.moves[direction]
            return None
        if name == "IsPulseGuiding":
            return False
        raise AssertionError(name)


def _guider(config: dict, mount: FakeMount) -> Guider:
    from unittest.mock import MagicMock

    params = dict(config)
    params["DIRECTIONS"] = dict(config["DIRECTIONS"])
    params["PID_COEFFS"] = {
        "x": {"p": 1.0, "i": 0.0, "d": 0.0},
        "y": {"p": 1.0, "i": 0.0, "d": 0.0},
        "set_x": 0.0,
        "set_y": 0.0,
    }
    guider = Guider(
        mount, logger=MagicMock(), database_manager=MagicMock(), params=params
    )
    guider.running = True
    return guider


ANGLES = [0, 10, 30, 44, 45, 46, 90, 135, 180, -30]


@pytest.mark.parametrize("mirrored", [False, True])
@pytest.mark.parametrize("angle", ANGLES)
def test_guide_corrects_a_shift_at_any_camera_angle(angle, mirrored):
    config = _calibrate(angle, mirrored)
    mount = FakeMount(_moves(angle, mirrored))
    shift = np.array([3.0, -2.0])

    # images_to_stabilise > 0: the guider uses P = 1, so it corrects the full shift
    _guider(config, mount).guide(shift[0], shift[1], 1, "cam")

    np.testing.assert_allclose(shift + mount.total, 0.0, atol=0.02)


@pytest.mark.parametrize("mirrored", [False, True])
@pytest.mark.parametrize("angle", ANGLES)
def test_calibration_reports_the_remaining_rotation(angle, mirrored):
    config = _calibrate(angle, mirrored)

    # the axes are relabelled to the nearest ones, so at most 45 degrees remain
    expected = (angle + 45) % 90 - 45
    assert abs(config["ANGLE"]) == pytest.approx(abs(expected), abs=0.01)
    if abs(expected) < 45:
        assert config["ANGLE"] == pytest.approx(expected, abs=0.01)
    for value in config["PIX2TIME"].values():
        assert value == pytest.approx(PULSE_TIME / MOVE)
    assert set(config["DIRECTIONS"]) == {"+x", "-x", "+y", "-y"}
    assert set(config["DIRECTIONS"].values()) == {"North", "South", "East", "West"}


def test_aligned_camera_gives_the_previous_calibration():
    config = _calibrate(0, mirrored=False)

    assert config["RA_AXIS"] == "x"
    assert config["ANGLE"] == 0.0
    assert config["DIRECTIONS"] == {
        "+x": "East",
        "-x": "West",
        "+y": "North",
        "-y": "South",
    }


def test_guider_without_angle_in_config_is_unchanged():
    config = _calibrate(0, mirrored=False)
    del config["ANGLE"]
    mount = FakeMount(_moves(0, mirrored=False))

    guider = _guider(config, mount)
    guider.guide(3.0, -2.0, 1, "cam")

    assert guider.ANGLE == 0.0
    np.testing.assert_allclose(mount.total, [-3.0, 2.0], atol=0.02)


def test_calibration_rejects_pulses_that_do_not_move_back():
    shifts = {name: move * PULSE_TIME for name, move in _moves(0, False).items()}
    shifts["West"] = shifts["East"]

    with pytest.raises(ValueError, match="opposite directions"):
        calibration_from_shifts(shifts, PULSE_TIME)


def test_calibration_rejects_moves_that_are_not_perpendicular():
    shifts = {name: move * PULSE_TIME for name, move in _moves(0, False).items()}
    north = _moves(40, False)["North"] * PULSE_TIME  # Dec 40 degrees off, RA not
    shifts["North"], shifts["South"] = north, -north

    with pytest.raises(ValueError, match="right angles"):
        calibration_from_shifts(shifts, PULSE_TIME)
