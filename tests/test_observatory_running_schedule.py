"""
Pytest tests for Observatory schedule action types.
Tests each action type individually to ensure they complete without setting error_free to False.
"""

import json
import logging
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest
import requests

from astra.observatory import Observatory, ObservatoryConfig

logger = logging.getLogger(__name__)


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


def set_location_for_body_visibility(server_url, body_name: str, latitude: float = 0.0):
    """Set telescope simulator location so that body is near the meridian and above the horizon."""
    from astropy.coordinates import AltAz, EarthLocation, get_body
    from astropy.time import Time

    t = Time.now()
    body = get_body(body_name, t)
    gmst = t.sidereal_time("mean", "greenwich").deg
    transit_lon = ((body.ra.deg - gmst + 180) % 360) - 180

    import astropy.units as u

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


def set_safety_monitor_safe(server_url):
    """Set the safety monitor to safe."""
    r = requests.put(
        f"{server_url}/api/v1/safetymonitor/0/issafe", data={"IsSafe": True}
    )
    if r.status_code != 200:
        logger.error(f"Failed to set safety monitor to safe: {r.text}")
        assert False, "Failed to set safety monitor to safe."


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
