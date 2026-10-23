"""Tests for Observatory.pointing_correction with a simulated mount."""

from unittest.mock import MagicMock, patch

import pytest

from astra.observatory import Observatory
from astra.pointer import PointingCorrection

TARGET = (23.4618, 30.6602)
# the mount points this far from where it was sent (degrees)
MOUNT_ERROR = (-0.87 / 60, -0.56 / 60)
THRESHOLD_ARCMIN = 0.1


class SimulatedMount:
    """A mount that reports the position it was sent to, but points
    MOUNT_ERROR away from it."""

    def __init__(self, ra, dec):
        self.ra = ra
        self.dec = dec
        self.slews = []

    def get(self, key, **kwargs):
        if key == "RightAscension":
            return self.ra / 15
        if key == "Declination":
            return self.dec
        if key == "SlewToCoordinatesAsync":
            self.ra = kwargs["RightAscension"] * 15
            self.dec = kwargs["Declination"]
            self.slews.append((self.ra, self.dec))
            return None
        raise KeyError(key)

    def plate_solve(self, filepath, target_ra, target_dec, **kwargs):
        correction = PointingCorrection(
            target_ra=target_ra,
            target_dec=target_dec,
            plating_ra=self.ra + MOUNT_ERROR[0],
            plating_dec=self.dec + MOUNT_ERROR[1],
        )
        mapping = MagicMock()
        mapping.number_of_matched_stars.return_value = 19
        mapping.gaia_stars_in_image = [None] * 40
        return correction, mapping, 20


@pytest.fixture
def setup():
    obs = Observatory.__new__(Observatory)
    obs.logger = MagicMock()
    obs.wait_for_slew = MagicMock()
    # a J2000 mount, so no frame conversion
    obs.to_mount_coordinates = lambda name, ra, dec: (ra, dec)

    mount = SimulatedMount(*TARGET)
    paired = MagicMock()
    paired.__getitem__ = lambda self, key: "mount"
    paired.telescope = mount
    paired.get_device_config.return_value = {"pointing_threshold": THRESHOLD_ARCMIN}

    action = MagicMock()
    action.device_name = "camera"
    action.action_value = {"object": "M33", "ra": TARGET[0], "dec": TARGET[1]}

    with (
        patch(
            "astra.observatory.calculate_pointing_correction_from_fits",
            side_effect=mount.plate_solve,
        ),
        patch("time.sleep"),
    ):
        yield obs, mount, paired, action


def test_first_correction_cancels_the_mount_error(setup):
    obs, mount, paired, action = setup

    complete, _ = obs.pointing_correction(action, "image.fits", paired)

    assert not complete
    assert mount.slews[-1] == pytest.approx(
        (TARGET[0] - MOUNT_ERROR[0], TARGET[1] - MOUNT_ERROR[1])
    )


def test_second_attempt_does_not_undo_the_first(setup):
    """A small residual on the second attempt must not send the mount back to
    the target, which was the cause of the swinging seen on 2026-10-22."""
    obs, mount, paired, action = setup
    obs.pointing_correction(action, "image.fits", paired)
    corrected = mount.slews[-1]

    # the mount lands a little off, so the second attempt needs a small slew
    mount.dec += 0.12 / 60
    complete, _ = obs.pointing_correction(action, "image.fits", paired)

    assert not complete
    assert mount.slews[-1] == pytest.approx(corrected)


def test_corrections_converge(setup):
    obs, mount, paired, action = setup

    results = [
        obs.pointing_correction(action, "image.fits", paired)[0] for _ in range(3)
    ]

    assert results == [False, True, True]
    assert len(mount.slews) == 1


def test_ra_wraps_at_zero(setup):
    obs, mount, paired, action = setup
    action.action_value.update(ra=0.001)
    mount.ra = 0.001

    obs.pointing_correction(action, "image.fits", paired)

    assert 0 <= mount.slews[-1][0] < 360
    assert mount.slews[-1][0] == pytest.approx((0.001 - MOUNT_ERROR[0]) % 360)
