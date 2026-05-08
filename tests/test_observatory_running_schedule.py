"""
Pytest tests for Observatory schedule action types.
Tests each action type individually to ensure they complete without setting error_free to False.
"""

import json
import logging
import time
import threading
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest
import requests

from astra.observatory import Observatory, ObservatoryConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
if not any(getattr(handler, "_astra_test_handler", False) for handler in logger.handlers):
    test_handler = logging.StreamHandler()
    test_handler.setLevel(logging.INFO)
    test_handler.setFormatter(logging.Formatter("%(message)s"))
    test_handler._astra_test_handler = True
    logger.addHandler(test_handler)
tracking_logger = logging.getLogger(f"{__name__}.tracking")
tracking_logger.setLevel(logging.WARNING)
tracking_logger.propagate = False
if not any(getattr(handler, "_astra_tracking_handler", False) for handler in tracking_logger.handlers):
    tracking_handler = logging.StreamHandler()
    tracking_handler.setLevel(logging.WARNING)
    tracking_handler.setFormatter(logging.Formatter("%(message)s"))
    tracking_handler._astra_tracking_handler = True
    tracking_logger.addHandler(tracking_handler)


def check_simulators_available(server_url="http://localhost:11111"):
    """Check if Alpaca simulators are running."""
    try:
        logger.info("Checking if Alpaca simulators are running...")
        response = requests.get(f"{server_url}/api/v1/camera/0/connected", timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


@pytest.fixture
def schedule_manager(observatory: Observatory):
    """Manage test schedule creation and cleanup."""
    # schedule_path = temp_config.paths.schedules / f"{observatory.name}.jsonl"
    schedule_path = observatory.schedule_manager.schedule_path

    @contextmanager
    def create_test_schedule(schedule_data):
        try:
            logger.info("Creating test schedule...")
            # Create test schedule
            with open(schedule_path, "w") as f:
                f.write(json.dumps(schedule_data) + "\n")

            logger.info(f"Test schedule created at {schedule_path}")
            time.sleep(
                3
            )  # Give some time for the observatory to pick up the new schedule

            yield schedule_path

        finally:
            logger.info("Cleaning up test schedule...")
            # Clean up test schedule
            schedule_path.unlink(missing_ok=True)

    yield create_test_schedule


def create_schedule_data(
    action_type: str,
    temp_config,
    inject_weather_alert: bool = False,
    inject_weather_alert_delay: int = 30,
) -> dict:
    """Create schedule data for the specified action type."""
    # Start the action in 5 seconds from now to give plenty of buffer
    base_time = datetime.now(UTC) + timedelta(seconds=5)

    # Get the camera device name from the first available observatory
    observatory_config = ObservatoryConfig.from_config(temp_config)

    camera_devices = observatory_config["Camera"]
    device_name = camera_devices[0]["device_name"]

    action_configs = {
        "cool_camera": {"action_value": {}, "duration": 1},  # minutes
        "calibration": {
            "action_value": {"exptime": [0.1, 0.1], "n": [1, 1]},
            "duration": 1,
        },
        "close": {"action_value": {}, "duration": 1},
        "open": {"action_value": {}, "duration": 1},
        "object": {
            "action_value": {
                "object": "test_target",
                "ra": 10.0,
                "dec": 70.0,
                "exptime": 1,  # Very short exposure
                "filter": "Clear",
                "guiding": True,
                "pointing": False,
            },
            "duration": 1,  # Shorter duration
        },
        "autofocus": {
            "action_value": {
                "exptime": 1.0,
                "filter": "Clear",
                "focus_measure_operator": "hfr",
                "j_mag_range": [5, 10],
                "ra": 121.48813,
                "dec": 4.28434,
                "search_range_is_relative": True,
                "search_range": 500,
                "n_steps": [
                    5,
                ],
                "n_exposures": [
                    1,
                ],
                "star_find_threshold": 6,
            },
            "duration": 2,  # Give it a bit more time
        },
        "flats": {
            "action_value": {"filter": ["Clear"], "n": [5]},
            "duration": 2,
        },
        "nonsidereal_object": {
            "action_type": "object",
            "action_value": {
                "object": "mars",
                "lookup_name": "mars",
                "exptime": 1,
                "filter": "Clear",
                "guiding": False,
                "pointing": False,
                "nonsidereal_recenter_interval": 10,
                "nonsidereal_start_lead_time_seconds": 15
            },
            "duration": 1,
        },
        "asteroid": {
            "action_type": "object",
            "action_value": {
                "object": "433 Eros",
                "lookup_name": "A898 PA",
                "exptime": 1,
                "filter": "Clear",
                "guiding": False,
                "pointing": False,
                "nonsidereal_recenter_interval": 100,
                "nonsidereal_start_lead_time_seconds": 15,
            },
            "duration": 1,
        },
        "tle": {
            "action_type": "object",
            "action_value": {
                "object": "ISS",
                "lookup_name": "TLE",
                "tle": "1 25544U 98067A   26119.49027627  .00007115  00000-0  13705-3 0  9999\n2 25544  51.6319 181.1364 0007113   4.2135 355.8912 15.49020532564209",
                "exptime": 1,
                "filter": "Clear",
                "guiding": False,
                "pointing": False,
                "nonsidereal_recenter_interval": 100,
                "nonsidereal_start_lead_time_seconds": 15,
            },
            "duration": 1,
        },
    }

    if action_type not in action_configs:
        raise ValueError(f"Unknown action type: {action_type}")

    config = action_configs[action_type]

    action = {
        "device_name": device_name,
        "action_type": config.get("action_type", action_type),
        "action_value": config["action_value"],
        "start_time": base_time.isoformat(),
        "end_time": (base_time + timedelta(minutes=config["duration"])).isoformat(),
        "_duration": config["duration"],  # For internal use only
    }

    if inject_weather_alert:
        action["_inject_weather_alert"] = True
        action["_inject_weather_alert_delay"] = inject_weather_alert_delay

    return action


def wait_for_schedule_completion(
    observatory: Observatory,
    schedule_data: dict,
    server_url,
    config,
) -> tuple[bool, int, bool]:
    """
    Wait for schedule to complete and return results.

    Returns:
        tuple: (success, completed_actions, error_free_maintained)
    """
    import os

    for f in config.paths.images.glob("**/*.fits"):
        try:
            os.remove(f)
        except Exception:
            pass

    # set weather to safe
    logger.info("Reloading observatory state to defaults")
    response = requests.get(f"{server_url}/reload")
    if response.status_code != 200:
        logger.error(f"Failed to reload observatory state: {response.text}")
        assert False, "Failed to reload observatory state."

    # Prepare for flats if needed
    if schedule_data["action_type"] == "flats":
        prepare_flats(server_url, sunset=True)

    # clear all tables for schedule run
    logger.info("Clearing images and polling tables...")
    observatory.database_manager.execute("DELETE FROM images")
    observatory.database_manager.execute("DELETE FROM polling")

    logger.info("Schedule data:", schedule_data)
    timeout = schedule_data["_duration"] * 60 + 120  # duration in seconds + buffer
    start_time = time.time()
    error_free_maintained = True

    # count number of images in Config().paths.images
    initial_n_images = len(list(config.paths.images.glob("**/*.fits")))
    logger.info(f"Initial number of images: {initial_n_images}")

    logger.info("pytest Starting schedule...")
    observatory.start_schedule()

    # Wait for schedule to start
    wait_start = time.time()
    while (
        not observatory.schedule_manager.running
        and (time.time() - wait_start) < timeout
    ):
        time.sleep(0.5)

    if not observatory.schedule_manager.running:
        return False, 0, error_free_maintained

    # Monitor execution
    weather_alert_injected = False

    while True:
        if (time.time() - start_time) > timeout:
            raise TimeoutError(
                "Schedule did not complete in expected time."
                f" {observatory.schedule_manager.get_completion_status()}"
            )
        if not observatory.logger.error_free:
            error_free_maintained = False
            break

        if observatory.schedule_manager.schedule is not None:
            if observatory.schedule_manager.schedule.is_completed():
                observatory.schedule_manager.stop_schedule(
                    thread_manager=observatory.thread_manager
                )
                break

        if schedule_data.get("_inject_weather_alert", False) and (
            time.time() - start_time
        ) > schedule_data.get("_inject_weather_alert_delay", 30):
            if not weather_alert_injected:
                logger.info("Injecting weather alert...")
                # Inject a weather alert halfway through the schedule duration
                response = requests.put(
                    f"{server_url}/api/v1/safetymonitor/0/issafe",
                    data={"IsSafe": False},
                )
                if response.status_code != 200:
                    logger.error(f"Failed to inject weather alert: {response.text}")
                    assert False, "Failed to inject weather alert."
                else:
                    logger.info("Weather alert injected successfully.")
                    weather_alert_injected = True
                    time.sleep(10)  # Wait for 10 seconds before checking status
            else:
                # check that dome and telescope closed
                response = requests.get(f"{server_url}/api/v1/telescope/0/atpark")

                telescope_atpark = response.json().get("Value", False)

                response = requests.get(f"{server_url}/api/v1/dome/0/atpark")

                dome_atpark = response.json().get("Value", False)

                if not (telescope_atpark and dome_atpark):
                    logger.error("Telescope or dome is not parked.")
                    assert False, "Telescope or dome did not park after weather alert."
                else:
                    logger.info("Telescope and dome are parked.")
                    observatory.schedule_manager.stop_schedule(
                        thread_manager=observatory.thread_manager
                    )
                    break

        if schedule_data.get("_inject_weather_alert", False) and (
            time.time() - start_time
        ) > schedule_data.get("_inject_weather_alert_delay", 30):
            if not weather_alert_injected:
                logger.info("Injecting weather alert...")
                # Inject a weather alert halfway through the schedule duration
                response = requests.put(
                    f"{server_url}/api/v1/safetymonitor/0/issafe",
                    data={"IsSafe": False},
                )
                if response.status_code != 200:
                    logger.error(f"Failed to inject weather alert: {response.text}")
                    assert False, "Failed to inject weather alert."
                else:
                    logger.info("Weather alert injected successfully.")
                    weather_alert_injected = True
                    time.sleep(10)  # Wait for 10 seconds before checking status
            else:
                # check that dome and telescope closed
                response = requests.get(f"{server_url}/api/v1/telescope/0/atpark")

                telescope_atpark = response.json().get("Value", False)

                response = requests.get(f"{server_url}/api/v1/dome/0/atpark")

                dome_atpark = response.json().get("Value", False)

                if not (telescope_atpark and dome_atpark):
                    logger.error("Telescope or dome is not parked.")
                    assert False, "Telescope or dome did not park after weather alert."
                else:
                    logger.info("Telescope and dome are parked.")

        time.sleep(1)

    # count number of images in Config().paths.images
    final_n_images = len(list(config.paths.images.glob("**/*.fits")))
    n_images = final_n_images - initial_n_images

    if schedule_data["action_type"] == "object" and not schedule_data.get(
        "_inject_weather_alert", False
    ):
        print(f"Number of images taken: {n_images}")
        assert n_images != 0, "Images were not taken during object action."

    if schedule_data["action_type"] == "flats" and not schedule_data.get(
        "_inject_weather_alert", False
    ):
        print(f"Number of images taken: {n_images}")
        assert n_images != 0, "Flats were not taken during flats action."

    # Wait for all headers to be complete
    complete_headers = 1
    while complete_headers > 0:
        if (time.time() - start_time) > timeout:
            raise TimeoutError("complete_headers did not complete in expected time.")
        complete_headers = observatory.database_manager.execute_select(
            "SELECT COUNT(*) FROM images WHERE complete_hdr=0"
        )[0][0]
        logger.info(f"Number of incomplete headers: {complete_headers}")
        time.sleep(1)

    # count number of images in Config().paths.images
    final_n_images = len(list(config.paths.images.glob("**/*.fits")))
    n_images = final_n_images - initial_n_images
    if schedule_data["action_type"] == "object":
        logger.info(f"Number of images taken: {n_images}")
        assert n_images != 0, "Images were not taken during object action."

    # Check if weather alert was injected
    if not schedule_data.get("_inject_weather_alert", False):
        final_completed = (
            sum(action.completed for action in observatory.schedule_manager.schedule)
            if observatory.schedule_manager.schedule is not None
            else 0
        )
    else:
        final_completed = 1

    assert final_completed > 0, "No actions were completed in the schedule."

    return final_completed > 0, final_completed, error_free_maintained


def set_location_for_body_visibility(server_url, body_name: str, latitude: float = 0.0, tle: str = None):
    """Set telescope simulator location so that body is near the meridian and above the horizon."""
    from astropy.coordinates import AltAz, EarthLocation, get_body, solar_system_ephemeris, SkyCoord
    from astropy.time import Time
    from astroquery.jplhorizons import Horizons
    import astropy.units as u

    t = Time.now()
    if body_name.lower() in solar_system_ephemeris.bodies:
        body = get_body(body_name, t)
            
    elif tle is None:
        obj = Horizons(id=body_name, location='500', epochs=t.jd)
        eph = obj.ephemerides()
        body = SkyCoord(ra=eph["RA"].data * u.deg, dec=eph["DEC"].data * u.deg, obstime=t).transform_to('gcrs')
        latitude = body.dec.deg
    else:
        obj = Horizons(id='TLE', location='500', epochs=t.jd)
        eph = obj.ephemerides(optional_settings={"TLE": tle})
        body = SkyCoord(ra=eph["RA"].data * u.deg, dec=eph["DEC"].data * u.deg, obstime=t).transform_to('gcrs')
        latitude = body.dec.deg
    gmst = t.sidereal_time("mean", "greenwich").deg
    transit_lon = ((body.ra.deg - gmst + 180) % 360) - 180

    loc = EarthLocation(lat=latitude * u.deg, lon=transit_lon * u.deg, height=0 * u.m)
    alt = body.transform_to(AltAz(obstime=t, location=loc)).alt.deg
    assert alt > 0, (
        f"{body_name} is not above the horizon at lat={latitude} (alt={alt:.1f}°)"
    )

    r = requests.put(
        f"{server_url}/api/v1/telescope/0/sitelatitude", data={"SiteLatitude": latitude}
    )
    assert r.status_code == 200, "Failed to set observatory latitude."
    r = requests.put(
        f"{server_url}/api/v1/telescope/0/sitelongitude",
        data={"SiteLongitude": transit_lon},
    )
    assert r.status_code == 200, "Failed to set observatory longitude."

    r = requests.put(
        f"{server_url}/api/v1/telescope/0/siteelevation",
        data={"SiteElevation": 1000.0},
    )
    assert r.status_code == 200, "Failed to set observatory elevation."


def set_safety_monitor_safe(server_url):
    """Set the safety monitor to safe."""
    r = requests.put(
        f"{server_url}/api/v1/safetymonitor/0/issafe", data={"IsSafe": True}
    )
    if r.status_code != 200:
        logger.error(f"Failed to set safety monitor to safe: {r.text}")
        assert False, "Failed to set safety monitor to safe."


def _wait_for_schedule_running(observatory: Observatory, timeout_s: float = 60.0) -> None:
    """Wait until scheduler thread reports a running state."""
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        if observatory.schedule_manager.running:
            return
        time.sleep(0.5)
    raise TimeoutError("Schedule did not enter running state in time.")


def _get_telescope_radec(server_url: str) -> tuple[float, float]:
    """Return telescope (RA deg, Dec deg) from Alpaca endpoints."""
    ra_hours = (
        requests.get(f"{server_url}/api/v1/telescope/0/rightascension", timeout=5)
        .json()
        .get("Value")
    )
    dec_deg = (
        requests.get(f"{server_url}/api/v1/telescope/0/declination", timeout=5)
        .json()
        .get("Value")
    )
    return float(ra_hours) * 15.0, float(dec_deg)


def _assert_telescope_position_matches_expected(
    server_url: str,
    expected_ra_deg: float,
    expected_dec_deg: float,
    *,
    max_separation_deg: float = 0.1,
) -> None:
    """Assert the telescope is pointed near the expected RA/Dec."""
    import astropy.units as u
    from astropy.coordinates import SkyCoord

    actual_ra_deg, actual_dec_deg = _get_telescope_radec(server_url)
    actual_coord = SkyCoord(ra=actual_ra_deg * u.deg, dec=actual_dec_deg * u.deg)
    expected_coord = SkyCoord(
        ra=expected_ra_deg * u.deg,
        dec=expected_dec_deg * u.deg,
    )
    separation_deg = actual_coord.separation(expected_coord).deg

    assert separation_deg <= max_separation_deg, (
        "Telescope was not pre-pointed to the expected non-sidereal position: "
        f"actual=({actual_ra_deg:.6f}, {actual_dec_deg:.6f}) deg, "
        f"expected=({expected_ra_deg:.6f}, {expected_dec_deg:.6f}) deg, "
        f"separation={separation_deg:.4f} deg"
    )


def _get_site_location(server_url: str):
    """Build EarthLocation from simulator site coordinates."""
    import astropy.units as u
    from astropy.coordinates import EarthLocation

    lat = (
        requests.get(f"{server_url}/api/v1/telescope/0/sitelatitude", timeout=5)
        .json()
        .get("Value")
    )
    lon = (
        requests.get(f"{server_url}/api/v1/telescope/0/sitelongitude", timeout=5)
        .json()
        .get("Value")
    )
    elevation = (
        requests.get(f"{server_url}/api/v1/telescope/0/siteelevation", timeout=5)
        .json()
        .get("Value")
    )
    return EarthLocation(
        lat=float(lat) * u.deg,
        lon=float(lon) * u.deg,
        height=float(elevation) * u.m,
    )


def _precompute_tracking_path(
    body_name: str,
    schedule_data: dict,
    obs_location,
    sample_interval_s: float,
    *,
    tle: str | None = None,
):
    """Precompute ephemeris interpolation functions for tracking validation.

    Mirrors schedule-side setup so the predicted path is directly comparable to
    the interpolators prepared during schedule loading.
    """
    from astropy.time import Time

    from astra.utils import precompute_ephemeris

    schedule_start = datetime.fromisoformat(schedule_data["start_time"])
    schedule_end = datetime.fromisoformat(schedule_data["end_time"])
    if schedule_start.tzinfo is None:
        schedule_start = schedule_start.replace(tzinfo=UTC)
    if schedule_end.tzinfo is None:
        schedule_end = schedule_end.replace(tzinfo=UTC)

    # Schedule validation precomputes from action start through action end + 0.5 h.
    duration_hours = (schedule_end - schedule_start).total_seconds() / 3600.0 + 0.5

    recenter_interval_s = float(
        schedule_data.get("action_value", {}).get("nonsidereal_recenter_interval", 0)
    )
    if recenter_interval_s > 0:
        interval_minutes = recenter_interval_s / 60.0
    else:
        interval_minutes = max(sample_interval_s / 60.0, 0.01)

    ephem_start = Time(schedule_start)
    logger.info(
        "Precomputing tracking path: ephem_start=%s, duration_hours=%.3f, interval_minutes=%.3f",
        ephem_start.isot,
        duration_hours,
        interval_minutes,
    )
    ra_interp, dec_interp, ra_rate_interp, dec_rate_interp = precompute_ephemeris(
        body_name=body_name,
        start_time=ephem_start,
        duration_hours=duration_hours,
        obs_location=obs_location,
        interval_minutes=interval_minutes,
        tle_data=tle,
        return_rates=True,
    )
    return ephem_start, ra_interp, dec_interp, ra_rate_interp, dec_rate_interp


def _get_tracking_activation_time(schedule_data: dict) -> datetime:
    """Return when non-sidereal tracking rates should become active."""
    lead_time_seconds = float(
        schedule_data["action_value"].get("nonsidereal_start_lead_time_seconds", 60.0)
    )
    start_time = datetime.fromisoformat(schedule_data["start_time"])
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=UTC)
    return start_time + timedelta(seconds=lead_time_seconds)


def _wait_until_datetime(target_time: datetime, timeout_s: float = 120.0) -> None:
    """Wait until the requested UTC timestamp is reached."""
    if target_time.tzinfo is None:
        target_time = target_time.replace(tzinfo=UTC)
    deadline = time.time() + timeout_s
    while True:
        remaining_s = (target_time - datetime.now(UTC)).total_seconds()
        if remaining_s <= 0:
            return
        if time.time() > deadline:
            raise TimeoutError(f"Timed out waiting for {target_time.isoformat()}.")
        time.sleep(min(1.0, remaining_s))


def _max_sample_count_for_interval(
    schedule_data: dict,
    sample_interval_s: float,
    *,
    start_time: datetime | None = None,
    safety_margin_s: float = 5.0,
) -> int:
    """Return maximum feasible sample count for the schedule and interval."""
    end_time = datetime.fromisoformat(schedule_data["end_time"])
    if start_time is None:
        start_time = datetime.now(UTC)
    elif start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=UTC)
    remaining_s = (end_time - start_time).total_seconds() - safety_margin_s
    if remaining_s <= 0:
        return 1
    return max(1, int(remaining_s // sample_interval_s) + 1)


def _tracking_direction(rate: float, *, tolerance: float = 1e-6) -> str:
    """Translate a numeric rate into a readable direction label."""
    if rate > tolerance:
        return "positive"
    if rate < -tolerance:
        return "negative"
    return "stationary"


def _sample_tracking_rate_alignment(
    server_url: str,
    ephem_start,
    ra_interp,
    dec_interp,
    ra_rate_interp,
    dec_rate_interp,
    sample_count: int,
    sample_interval_s: float,
    end_time: datetime | None = None,
) -> list[dict[str, float | str]]:
    """Sample actual and predicted rate directions over the tracking window."""
    import astropy.units as u
    from astropy.coordinates import SkyCoord
    from astropy.time import Time

    samples: list[dict[str, float | str]] = []
    for i in range(sample_count):
        if end_time is not None:
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=UTC)
            if datetime.now(UTC) >= end_time:
                break
        if i > 0:
            if end_time is not None:
                remaining_s = (end_time - datetime.now(UTC)).total_seconds()
                if remaining_s <= 0:
                    break
                time.sleep(min(sample_interval_s, remaining_s))
            else:
                time.sleep(sample_interval_s)

        obs_time = Time.now()
        elapsed_s = (obs_time - ephem_start).to_value("s")
        expected_ra_deg = float(ra_interp(elapsed_s)) % 360.0
        expected_dec_deg = float(dec_interp(elapsed_s))
        expected_ra_rate = float(ra_rate_interp(elapsed_s))
        expected_dec_rate = float(dec_rate_interp(elapsed_s))
        exp_coord = SkyCoord(ra=expected_ra_deg * u.deg, dec=expected_dec_deg * u.deg)

        ra_deg, dec_deg = _get_telescope_radec(server_url)
        tel_coord = SkyCoord(ra=ra_deg * u.deg, dec=dec_deg * u.deg)

        tracking_logger.warning(
            "Tracking comparison t+%.1fs: actual RA=%.6f deg, Dec=%.6f deg; "
            "predicted RA=%.6f deg, Dec=%.6f deg",
            float(elapsed_s),
            ra_deg,
            dec_deg,
            expected_ra_deg,
            expected_dec_deg,
        )

        actual_ra_rate = float(
            requests.get(f"{server_url}/api/v1/telescope/0/rightascensionrate", timeout=5)
            .json()
            .get("Value", 0.0)
        )
        actual_dec_rate = float(
            requests.get(f"{server_url}/api/v1/telescope/0/declinationrate", timeout=5)
            .json()
            .get("Value", 0.0)
        )
        separation = tel_coord.separation(exp_coord).deg

        samples.append(
            {
                "elapsed_s": float(elapsed_s),
                "expected_ra_rate": expected_ra_rate,
                "expected_dec_rate": expected_dec_rate,
                "actual_ra_rate": actual_ra_rate,
                "actual_dec_rate": actual_dec_rate,
                "separation_deg": float(separation),
                "expected_ra_direction": _tracking_direction(expected_ra_rate),
                "expected_dec_direction": _tracking_direction(expected_dec_rate),
                "actual_ra_direction": _tracking_direction(actual_ra_rate),
                "actual_dec_direction": _tracking_direction(actual_dec_rate),
            }
        )

    return samples

def prepare_flats(server_url, sunset=True):
    """Prepare flats by setting sunlight conditions and placing
    observatory where the sun is setting or rising."""
    # Set system time to noon to trigger flats condition

    r = requests.put(f"{server_url}/sunlight/?state=True")
    if r.status_code != 200:
        assert False, "Failed to set sunlight condition."

    import astropy.units as u
    import numpy as np
    from astropy.coordinates import AltAz, EarthLocation, get_sun
    from astropy.time import Time

    def get_sun_terminator_longitude(
        dateobs: datetime, latitude_deg: float, sunset: bool = False
    ):
        """
        Return the longitude where the Sun is rising or setting at the given UTC time and latitude.
        """
        # Input validation
        if not -90 <= latitude_deg <= 90:
            raise ValueError("Latitude must be between -90 and +90 degrees")

        t = Time(dateobs, scale="utc")
        sun = get_sun(t)

        # Search grid of longitudes with high resolution
        longs = np.linspace(-180, 180, 1441) * u.deg  # ~0.25° resolution
        lats = np.full_like(longs.value, latitude_deg) * u.deg

        locs = EarthLocation.from_geodetic(longs, lats, height=0 * u.m)
        altaz = sun.transform_to(AltAz(obstime=t, location=locs))
        altitudes = altaz.alt.deg

        # Find zero crossings (altitude changing sign)
        sign_changes = np.diff(np.sign(altitudes))
        idx = np.where(sign_changes != 0)[0]

        if len(idx) == 0:
            return None  # No sunrise/sunset at this latitude/time

        candidates = []

        for i in idx:
            # Linear interpolation for more accurate longitude
            x0, x1 = longs[i].value, longs[i + 1].value
            y0, y1 = altitudes[i], altitudes[i + 1]

            # Avoid division by zero
            if y1 - y0 == 0:
                continue

            longitude = x0 - y0 * (x1 - x0) / (y1 - y0)

            # Determine if this is sunrise or sunset
            is_sunrise = sign_changes[i] > 0  # altitude increasing = sunrise
            is_sunset = sign_changes[i] < 0  # altitude decreasing = sunset

            if (sunset and is_sunset) or (not sunset and is_sunrise):
                candidates.append(longitude)

        if not candidates:
            return None

        # If multiple candidates (rare), return the first one
        # This can happen near polar regions or at certain times of year
        return candidates[0]

    lat = -24.6252  # Paranal
    long = get_sun_terminator_longitude(
        datetime.now(UTC), lat, sunset=sunset
    ) + 3 / np.cos(np.radians(lat))
    assert long is not None, "Could not determine sun terminator longitude."

    # Set observatory location
    r = requests.put(
        f"{server_url}/api/v1/telescope/0/sitelatitude", data={"SiteLatitude": lat}
    )
    if r.status_code != 200:
        assert False, "Failed to set observatory latitude."

    r = requests.put(
        f"{server_url}/api/v1/telescope/0/sitelongitude", data={"SiteLongitude": long}
    )
    if r.status_code != 200:
        assert False, "Failed to set observatory longitude."


@pytest.mark.slow
@pytest.mark.network
class TestScheduleActionTypes:
    """Test each schedule action type individually."""

    def test_cool_camera_action(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test cool_camera action type."""
        schedule_data = create_schedule_data("cool_camera", temp_config)

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                f"error_free became False during cool_camera action. Error sources: {observatory.logger.error_source}"
            )
            assert success, (
                f"cool_camera action did not complete successfully. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_calibration_action(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test calibration action type."""
        schedule_data = create_schedule_data("calibration", temp_config)

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                "error_free became False during calibration action. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert success, (
                "calibration action did not complete successfully. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_close_action(self, observatory, schedule_manager, server_url, temp_config):
        """Test close action type."""
        schedule_data = create_schedule_data("close", temp_config)

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                "error_free became False during close action. Error sources: "
                f"{observatory.logger.error_source}"
            )
            assert success, (
                "close action did not complete successfully. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_close_action_with_weather_alert(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test close action type with weather alert."""
        schedule_data = create_schedule_data(
            "close",
            temp_config,
            inject_weather_alert=True,
            inject_weather_alert_delay=0,
        )

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                "error_free became False during close action. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert success, (
                f"close action did not complete successfully. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_open_action(self, observatory, schedule_manager, server_url, temp_config):
        """Test open action type."""
        set_safety_monitor_safe(server_url)
        schedule_data = create_schedule_data("open", temp_config)

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                f"error_free became False during open action. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert success, (
                f"open action did not complete successfully. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_open_action_with_weather_alert(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test open action type with weather alert."""
        set_safety_monitor_safe(server_url)
        schedule_data = create_schedule_data(
            "open", temp_config, inject_weather_alert=True, inject_weather_alert_delay=0
        )

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                f"error_free became False during open action. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert success, (
                f"open action did not complete successfully. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_object_action(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test object action type."""
        set_safety_monitor_safe(server_url)
        schedule_data = create_schedule_data("object", temp_config)

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                f"error_free became False during object action. Error sources: "
                f"{observatory.logger.error_source}"
            )
            assert success, (
                f"object action did not complete successfully. Error sources: "
                f"{observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_object_action_with_weather_alert(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test object action type with weather alert."""
        schedule_data = create_schedule_data(
            "object", temp_config, inject_weather_alert=True
        )

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                f"error_free became False during object action. Error sources: "
                f"{observatory.logger.error_source}"
            )
            assert success, (
                f"object action did not complete successfully. Error sources: "
                f"{observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_nonsidereal_object_action(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test object action with non-sidereal tracking via lookup_name.

        Verifies that a sequence using lookup_name='mars' completes without errors,
        takes at least one image, and that tracking rates are reset on teardown.
        """
        set_safety_monitor_safe(server_url)
        set_location_for_body_visibility(server_url, "mars")
        schedule_data = create_schedule_data("nonsidereal_object", temp_config)

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                "error_free became False during nonsidereal_object action. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert success, (
                "nonsidereal_object action did not complete successfully. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

        # Verify tracking rates were reset via the simulator endpoint
        ra_rate = (
            requests.get(f"{server_url}/api/v1/telescope/0/rightascensionrate")
            .json()
            .get("Value")
        )
        dec_rate = (
            requests.get(f"{server_url}/api/v1/telescope/0/declinationrate")
            .json()
            .get("Value")
        )
        assert ra_rate == 0.0, (
            f"RightAscensionRate was not reset after sequence: {ra_rate}"
        )
        assert dec_rate == 0.0, (
            f"DeclinationRate was not reset after sequence: {dec_rate}"
        )

    def test_nonsidereal_object_action_with_weather_alert(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Non-sidereal object tracking must stop safely during weather alert.

        Verifies that when safety monitor switches to unsafe mid-sequence, the
        schedule is interrupted and mount tracking/rates are stopped/reset.
        """
        set_safety_monitor_safe(server_url)
        set_location_for_body_visibility(server_url, "mars")
        schedule_data = create_schedule_data(
            "nonsidereal_object",
            temp_config,
            inject_weather_alert=True,
            inject_weather_alert_delay=30,
        )

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                "error_free became False during nonsidereal weather alert test. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert success, (
                "nonsidereal_object action did not complete successfully under "
                f"weather alert. Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

        tracking = (
            requests.get(f"{server_url}/api/v1/telescope/0/tracking")
            .json()
            .get("Value")
        )
        ra_rate = (
            requests.get(f"{server_url}/api/v1/telescope/0/rightascensionrate")
            .json()
            .get("Value")
        )
        dec_rate = (
            requests.get(f"{server_url}/api/v1/telescope/0/declinationrate")
            .json()
            .get("Value")
        )

        assert tracking is False, "Tracking should be stopped after weather alert"
        assert ra_rate == 0.0, (
            f"RightAscensionRate was not reset after weather alert: {ra_rate}"
        )
        assert dec_rate == 0.0, (
            f"DeclinationRate was not reset after weather alert: {dec_rate}"
        )

    def test_jpl_horizons_object_action(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test object action with non-sidereal tracking via JPL Horizons api.

        Verifies that a sequence using lookup_name='mars' completes without errors,
        takes at least one image, and that tracking rates are reset on teardown.
        """
        set_safety_monitor_safe(server_url)
        set_location_for_body_visibility(server_url, "A898 PA")
        schedule_data = create_schedule_data("asteroid", temp_config)

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                "error_free became False during nonsidereal_object action. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert success, (
                "nonsidereal_object action did not complete successfully. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

        # Verify tracking rates were reset via the simulator endpoint
        ra_rate = (
            requests.get(f"{server_url}/api/v1/telescope/0/rightascensionrate")
            .json()
            .get("Value")
        )
        dec_rate = (
            requests.get(f"{server_url}/api/v1/telescope/0/declinationrate")
            .json()
            .get("Value")
        )
        assert ra_rate == 0.0, (
            f"RightAscensionRate was not reset after sequence: {ra_rate}"
        )
        assert dec_rate == 0.0, (
            f"DeclinationRate was not reset after sequence: {dec_rate}"
        )
    def test_tle_object_action(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test object action with non-sidereal tracking via TLE.

        Verifies that a sequence using lookup_name='tle' completes without errors,
        takes at least one image, and that tracking rates are reset on teardown.
        """
        set_safety_monitor_safe(server_url)
        set_location_for_body_visibility(server_url, "tle", tle="1 25544U 98067A   26119.49027627  .00007115  00000-0  13705-3 0  9999\n2 25544  51.6319 181.1364 0007113   4.2135 355.8912 15.49020532564209")
        schedule_data = create_schedule_data("tle", temp_config)

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                "error_free became False during nonsidereal_object action. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert success, (
                "nonsidereal_object action did not complete successfully. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

        # Verify tracking rates were reset via the simulator endpoint
        ra_rate = (
            requests.get(f"{server_url}/api/v1/telescope/0/rightascensionrate")
            .json()
            .get("Value")
        )
        dec_rate = (
            requests.get(f"{server_url}/api/v1/telescope/0/declinationrate")
            .json()
            .get("Value")
        )
        assert ra_rate == 0.0, (
            f"RightAscensionRate was not reset after sequence: {ra_rate}"
        )
        assert dec_rate == 0.0, (
            f"DeclinationRate was not reset after sequence: {dec_rate}"
        )

    def test_tle_object_action_with_weather_alert(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """TLE non-sidereal tracking must stop safely during weather alert."""
        tle = (
            "1 25544U 98067A   26119.49027627  .00007115  00000-0  13705-3 0  9999\n"
            "2 25544  51.6319 181.1364 0007113   4.2135 355.8912 15.49020532564209"
        )
        set_safety_monitor_safe(server_url)
        set_location_for_body_visibility(server_url, "tle", tle=tle)
        schedule_data = create_schedule_data(
            "tle",
            temp_config,
            inject_weather_alert=True,
            inject_weather_alert_delay=30,
        )

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                "error_free became False during TLE weather alert test. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert success, (
                "tle action did not complete successfully under weather alert. "
                f"Error sources: {observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

        tracking = (
            requests.get(f"{server_url}/api/v1/telescope/0/tracking")
            .json()
            .get("Value")
        )
        ra_rate = (
            requests.get(f"{server_url}/api/v1/telescope/0/rightascensionrate")
            .json()
            .get("Value")
        )
        dec_rate = (
            requests.get(f"{server_url}/api/v1/telescope/0/declinationrate")
            .json()
            .get("Value")
        )

        assert tracking is False, "Tracking should be stopped after weather alert"
        assert ra_rate == 0.0, (
            f"RightAscensionRate was not reset after weather alert: {ra_rate}"
        )
        assert dec_rate == 0.0, (
            f"DeclinationRate was not reset after weather alert: {dec_rate}"
        )

    def test_precompute_tracking_path_matches_schedule_interpolators(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Helper precomputed path should closely match schedule-generated ephemeris."""
        tle = (
            "1 25544U 98067A   26119.49027627  .00007115  00000-0  13705-3 0  9999\n"
            "2 25544  51.6319 181.1364 0007113   4.2135 355.8912 15.49020532564209"
        )
        set_safety_monitor_safe(server_url)
        set_location_for_body_visibility(server_url, "TLE", tle=tle)
        obs_location = _get_site_location(server_url)
        schedule_data = create_schedule_data("tle", temp_config)
        start_time = datetime.fromisoformat(schedule_data["start_time"])
        schedule_data["end_time"] = (start_time + timedelta(minutes=10)).isoformat()
        schedule_data["_duration"] = 10

        lead_time_s = float(
            schedule_data["action_value"].get("nonsidereal_start_lead_time_seconds", 60.0)
        )
        recenter_interval_s = float(
            schedule_data["action_value"].get("nonsidereal_recenter_interval", 60.0)
        )
        action_duration_s = (
            datetime.fromisoformat(schedule_data["end_time"])
            - datetime.fromisoformat(schedule_data["start_time"])
        ).total_seconds()

        with schedule_manager(schedule_data):
            schedule = observatory.schedule_manager.read()
            assert schedule is not None and len(schedule) > 0, "Failed to load schedule."
            action = schedule[0]

            schedule_ra_interp = action.action_value.get("_ra_interp")
            schedule_dec_interp = action.action_value.get("_dec_interp")
            schedule_ra_rate_interp = action.action_value.get("_ra_rate_interp")
            schedule_dec_rate_interp = action.action_value.get("_dec_rate_interp")

            assert schedule_ra_interp is not None, "Schedule RA interpolator missing."
            assert schedule_dec_interp is not None, "Schedule Dec interpolator missing."
            assert schedule_ra_rate_interp is not None, "Schedule RA rate interpolator missing."
            assert schedule_dec_rate_interp is not None, "Schedule Dec rate interpolator missing."

            _, helper_ra_interp, helper_dec_interp, helper_ra_rate_interp, helper_dec_rate_interp = _precompute_tracking_path(
                body_name="TLE",
                schedule_data=schedule_data,
                obs_location=obs_location,
                sample_interval_s=recenter_interval_s,
                tle=tle,
            )

            max_helper_time_s = max(0.0, action_duration_s - lead_time_s)
            sample_times_s = [
                0.0,
                max_helper_time_s * 0.25,
                max_helper_time_s * 0.5,
                max_helper_time_s * 0.75,
                max_helper_time_s,
            ]

            def angular_sep_deg(a_deg: float, b_deg: float) -> float:
                return abs((a_deg - b_deg + 180.0) % 360.0 - 180.0)

            for t_s in sample_times_s:

                helper_ra = float(helper_ra_interp(t_s)) % 360.0
                helper_dec = float(helper_dec_interp(t_s))
                helper_ra_rate = float(helper_ra_rate_interp(t_s))
                helper_dec_rate = float(helper_dec_rate_interp(t_s))

                schedule_ra = float(schedule_ra_interp(t_s)) % 360.0
                schedule_dec = float(schedule_dec_interp(t_s))
                schedule_ra_rate = float(schedule_ra_rate_interp(t_s))
                schedule_dec_rate = float(schedule_dec_rate_interp( t_s))

                assert angular_sep_deg(helper_ra, schedule_ra) < 0.2, (
                    f"RA interpolator mismatch at t={t_s:.1f}s: "
                    f"helper={helper_ra:.6f}, schedule={schedule_ra:.6f}"
                )
                assert abs(helper_dec - schedule_dec) < 0.2, (
                    f"Dec interpolator mismatch at t={t_s:.1f}s: "
                    f"helper={helper_dec:.6f}, schedule={schedule_dec:.6f}"
                )
                assert abs(helper_ra_rate - schedule_ra_rate) < 50.0, (
                    f"RA rate interpolator mismatch at t={t_s:.1f}s: "
                    f"helper={helper_ra_rate:.6f}, schedule={schedule_ra_rate:.6f}"
                )
                assert abs(helper_dec_rate - schedule_dec_rate) < 200.0, (
                    f"Dec rate interpolator mismatch at t={t_s:.1f}s: "
                    f"helper={helper_dec_rate:.6f}, schedule={schedule_dec_rate:.6f}"
                )

    def test_jpl_horizons_rates_keep_pointing_on_target_over_time(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Mount pointing should remain close to JPL-Horizons target at multiple times."""
        set_safety_monitor_safe(server_url)
        body_name = "A898 PA"
        set_location_for_body_visibility(server_url, body_name)
        obs_location = _get_site_location(server_url)
        schedule_data = create_schedule_data("asteroid", temp_config)
        sample_interval_s = 2.0
        end_time = datetime.fromisoformat(schedule_data["end_time"])
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        with schedule_manager(schedule_data):
            ephem_start, ra_interp, dec_interp, ra_rate_interp, dec_rate_interp = _precompute_tracking_path(
                body_name=body_name,
                schedule_data=schedule_data,
                obs_location=obs_location,
                sample_interval_s=sample_interval_s,
            )
            observatory.start_schedule()
            _wait_for_schedule_running(observatory)
            _wait_until_datetime(_get_tracking_activation_time(schedule_data))
            sample_count = _max_sample_count_for_interval(
                schedule_data,
                sample_interval_s,
                start_time=datetime.now(UTC),
            )

            samples = _sample_tracking_rate_alignment(
                server_url=server_url,
                ephem_start=ephem_start,
                ra_interp=ra_interp,
                dec_interp=dec_interp,
                ra_rate_interp=ra_rate_interp,
                dec_rate_interp=dec_rate_interp,
                sample_count=sample_count,
                sample_interval_s=sample_interval_s,
                end_time=end_time,
            )

            observatory.schedule_manager.stop_schedule(
                thread_manager=observatory.thread_manager
            )

        assert samples, "No tracking-rate samples were collected."
        assert max(sample["separation_deg"] for sample in samples) < 0.5, (
            "JPL Horizons non-sidereal tracking drifted too far from expected "
            f"target coordinates. Separations (deg): {[sample['separation_deg'] for sample in samples]}"
        )

    def test_tle_rates_keep_pointing_on_target_over_time(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Mount pointing should remain close to TLE target at multiple times."""
        tle = (
            "1 25544U 98067A   26119.49027627  .00007115  00000-0  13705-3 0  9999\n2 25544  51.6319 181.1364 0007113   4.2135 355.8912 15.49020532564209"
        )
        set_safety_monitor_safe(server_url)
        set_location_for_body_visibility(server_url, "TLE", tle=tle)
        obs_location = _get_site_location(server_url)
        schedule_data = create_schedule_data("tle", temp_config)
        sample_interval_s = 2.0
        end_time = datetime.fromisoformat(schedule_data["end_time"])
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=UTC)

        with schedule_manager(schedule_data):
            ephem_start, ra_interp, dec_interp, ra_rate_interp, dec_rate_interp = _precompute_tracking_path(
                body_name="TLE",
                schedule_data=schedule_data,
                obs_location=obs_location,
                sample_interval_s=sample_interval_s,
                tle=tle,
            )
            observatory.start_schedule()
            _wait_for_schedule_running(observatory)
            _wait_until_datetime(_get_tracking_activation_time(schedule_data))
            sample_count = _max_sample_count_for_interval(
                schedule_data,
                sample_interval_s,
                start_time=datetime.now(UTC),
            )

            samples = _sample_tracking_rate_alignment(
                server_url=server_url,
                ephem_start=ephem_start,
                ra_interp=ra_interp,
                dec_interp=dec_interp,
                ra_rate_interp=ra_rate_interp,
                dec_rate_interp=dec_rate_interp,
                sample_count=sample_count,
                sample_interval_s=sample_interval_s,
                end_time=end_time,
            )

            observatory.schedule_manager.stop_schedule(
                thread_manager=observatory.thread_manager
            )

        assert samples, "No tracking-rate samples were collected."
        assert max(sample["separation_deg"] for sample in samples) < 0.5, (
            "TLE non-sidereal tracking drifted too far from expected target "
            f"coordinates. Separations (deg): {[sample['separation_deg'] for sample in samples]}"
        )

    def test_tle_prepointed_before_tracking_rates_apply(
        self, observatory, schedule_manager, server_url, temp_config, monkeypatch
    ):
        """The telescope should be on the pre-point target before rates turn on."""
        tle = (
            "1 25544U 98067A   26119.49027627  .00007115  00000-0  13705-3 0  9999\n2 25544  51.6319 181.1364 0007113   4.2135 355.8912 15.49020532564209"
        )
        set_safety_monitor_safe(server_url)
        set_location_for_body_visibility(server_url, "TLE", tle=tle)
        obs_location = _get_site_location(server_url)
        schedule_data = create_schedule_data("tle", temp_config)

        from astra.nonsidereal import NonSiderealManager

        apply_rates_called = threading.Event()
        rate_assertion_errors: list[str] = []
        captured_prepoint: dict[str, float] = {}
        original_apply_rates = NonSiderealManager.apply_rates
        original_prepoint_coordinates = NonSiderealManager.prepoint_coordinates

        def wrapped_prepoint_coordinates(self, lead_time_seconds: float = 60.0):
            result = original_prepoint_coordinates(self, lead_time_seconds=lead_time_seconds)
            if result is not None:
                captured_prepoint["ra_deg"] = float(result[0])
                captured_prepoint["dec_deg"] = float(result[1])
            return result

        def wrapped_apply_rates(self, telescope):
            try:
                assert "ra_deg" in captured_prepoint and "dec_deg" in captured_prepoint, (
                    "Pre-point coordinates were not captured before rates were applied."
                )

                _assert_telescope_position_matches_expected(
                    server_url,
                    captured_prepoint["ra_deg"],
                    captured_prepoint["dec_deg"],
                    max_separation_deg=0.5,
                )

                ra_rate_before = float(
                    requests.get(
                        f"{server_url}/api/v1/telescope/0/rightascensionrate",
                        timeout=5,
                    )
                    .json()
                    .get("Value", 0.0)
                )
                dec_rate_before = float(
                    requests.get(
                        f"{server_url}/api/v1/telescope/0/declinationrate",
                        timeout=5,
                    )
                    .json()
                    .get("Value", 0.0)
                )
                assert ra_rate_before == 0.0, (
                    f"RightAscensionRate should still be zero before activation: {ra_rate_before}"
                )
                assert dec_rate_before == 0.0, (
                    f"DeclinationRate should still be zero before activation: {dec_rate_before}"
                )

                original_apply_rates(self, telescope)

                ra_rate_after = float(telescope.get("RightAscensionRate"))
                dec_rate_after = float(telescope.get("DeclinationRate"))
                assert abs(ra_rate_after) > 0.0 or abs(dec_rate_after) > 0.0, (
                    "Non-sidereal rates were not applied after the pre-point slew."
                )
            except Exception as exc:
                rate_assertion_errors.append(str(exc))
                raise
            finally:
                apply_rates_called.set()

        monkeypatch.setattr(NonSiderealManager, "prepoint_coordinates", wrapped_prepoint_coordinates)
        monkeypatch.setattr(NonSiderealManager, "apply_rates", wrapped_apply_rates)

        with schedule_manager(schedule_data):
            observatory.start_schedule()
            _wait_for_schedule_running(observatory)

            assert apply_rates_called.wait(timeout=120), (
                "Non-sidereal rates were never applied during the schedule."
            )
            if rate_assertion_errors:
                pytest.fail(rate_assertion_errors[0])

            observatory.schedule_manager.stop_schedule(
                thread_manager=observatory.thread_manager
            )

    def test_autofocus_action(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test autofocus action type"""
        set_safety_monitor_safe(server_url)
        schedule_data = create_schedule_data("autofocus", temp_config)

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                f"error_free became False during autofocus action. Error sources: "
                f"{observatory.logger.error_source}"
            )
            assert success, (
                f"autofocus action did not complete successfully. Error sources: "
                f"{observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_autofocus_action_with_weather_alert(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test autofocus action type with weather alert"""
        set_safety_monitor_safe(server_url)
        schedule_data = create_schedule_data(
            "autofocus", temp_config, inject_weather_alert=True
        )

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                f"error_free became False during autofocus action. Error sources: "
                f"{observatory.logger.error_source}"
            )
            assert success, (
                f"autofocus action did not complete successfully. Error sources: "
                f"{observatory.logger.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_flats_action(self, observatory, schedule_manager, server_url, temp_config):
        """Test flats action type"""
        set_safety_monitor_safe(server_url)
        schedule_data = create_schedule_data("flats", temp_config)

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                f"error_free became False during flats action. Error sources: {observatory.error_source}"
            )
            assert success, (
                f"flats action did not complete successfully. Error sources: {observatory.error_source}"
            )
            assert completed > 0, "No actions were completed"

    def test_flats_action_with_weather_alert(
        self, observatory, schedule_manager, server_url, temp_config
    ):
        """Test flats action type with weather alert"""
        schedule_data = create_schedule_data(
            "flats", temp_config, inject_weather_alert=True
        )

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory, schedule_data, server_url, temp_config
            )

            assert error_free_maintained, (
                f"error_free became False during flats action. Error sources: {observatory.error_source}"
            )
            assert success, (
                f"flats action did not complete successfully. Error sources: {observatory.error_source}"
            )
            assert completed > 0, "No actions were completed"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
