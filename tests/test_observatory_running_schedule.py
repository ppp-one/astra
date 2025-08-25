"""
Pytest tests for Observatory schedule action types.
Tests each action type individually to ensure they complete without setting error_free to False.
"""

import pytest
import time
import json
import requests
from pathlib import Path
from datetime import datetime, timedelta, UTC
from contextlib import contextmanager
from threading import Thread
from glob import glob

from astra import Config
from astra.observatory import Observatory

import logging

logger = logging.getLogger(__name__)

CONFIG = Config()
OBSERVATORIES: dict = {}


def check_simulators_available():
    """Check if Alpaca simulators are running."""
    try:
        logger.info("Checking if Alpaca simulators are running...")
        response = requests.get(
            "http://localhost:11111/api/v1/camera/0/connected", timeout=5
        )
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def clean_up_observatories():
    """Clean up observatory devices properly."""
    for obs in OBSERVATORIES.values():
        # Stop any running schedules
        if obs.schedule_running:
            obs.stop_schedule()

        # Stop watchdog
        if obs.watchdog_running:
            obs.watchdog_running = False

        # Get all the devices and stop them
        for device_type in obs.devices:
            for device_name in obs.devices[device_type]:
                device = obs.devices[device_type][device_name]
                try:
                    device.stop()
                except Exception:
                    pass


@pytest.fixture(scope="session", autouse=True)
def setup_observatories():
    """Setup observatories for testing."""
    if not check_simulators_available():
        pytest.skip("Alpaca simulators not available on localhost:11111")

    # Load observatories with truncation for faster testing
    config_files = glob(str(CONFIG.paths.observatory_config / "*_config.yml"))

    for config_filename in config_files:
        obs = Observatory(config_filename)  # Remove truncation factor
        OBSERVATORIES[obs.name] = obs
        obs.connect_all()

        # Wait a bit for connections to stabilize
        time.sleep(5)

    if not OBSERVATORIES:
        logger.warning(f"No observatories loaded from {config_files} at {CONFIG.paths}")
        pytest.skip("No observatories loaded")

    yield OBSERVATORIES

    # Cleanup after all tests
    clean_up_observatories()


@pytest.fixture(scope="function")
def observatory():
    """Get an observatory for testing."""
    if not OBSERVATORIES:
        pytest.skip("No observatories available")

    # Get the first available observatory
    logger.info("Selecting an observatory for testing...")
    obs_name = list(OBSERVATORIES.keys())[0]
    obs = OBSERVATORIES[obs_name]

    yield obs

    # Cleanup after test
    if obs.schedule_running:
        logger.info("Stopping schedule...")
        obs.stop_schedule()
        time.sleep(1)


@pytest.fixture
def schedule_manager(observatory):
    """Manage test schedule creation and cleanup."""
    config = Config()
    schedule_path = config.paths.schedules / f"{observatory.name}.jsonl"
    backup_path = None

    # Backup existing schedule
    if schedule_path.exists():
        backup_path = schedule_path.with_suffix(".jsonl.test_backup")
        schedule_path.rename(backup_path)

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

    # Restore original schedule
    if backup_path and backup_path.exists():
        backup_path.rename(schedule_path)


def create_schedule_data(action_type: str, device_name: str = None) -> dict:
    """Create schedule data for the specified action type."""
    # Start the action in 15 seconds from now to give plenty of buffer
    base_time = datetime.now(UTC) + timedelta(seconds=5)

    # Use default device name if not provided
    if device_name is None:
        device_name = f"camera_{list(OBSERVATORIES.keys())[0]}"

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
                "ra": 100.0,
                "dec": -30.0,
                "exptime": 1,  # Very short exposure
                "filter": "Clear",
                "guiding": False,
                "pointing": False,
            },
            "duration": 1,  # Shorter duration
        },
        # "flats": {
        #     "action_value": {"filter": ["Clear"], "n": [1]},
        #     "duration": 1,
        # },  # Just 1 flat, shorter duration
    }

    if action_type not in action_configs:
        raise ValueError(f"Unknown action type: {action_type}")

    config = action_configs[action_type]

    return {
        "device_name": device_name,
        "action_type": action_type,
        "action_value": config["action_value"],
        "start_time": base_time.isoformat(),
        "end_time": (base_time + timedelta(minutes=config["duration"])).isoformat(),
    }


def wait_for_schedule_completion(
    observatory, timeout: int = 120
) -> tuple[bool, int, bool]:
    """
    Wait for schedule to complete and return results.

    Returns:
        tuple: (success, completed_actions, error_free_maintained)
    """
    start_time = time.time()
    error_free_maintained = True

    logger.info("pytest Starting schedule...")
    observatory.start_schedule()

    # Validate schedule is loaded and ready
    # if observatory.schedule is None:
    #     return False, 0, error_free_maintained

    # if observatory.schedule.iloc[-1]["end_time"] < datetime.now(UTC):
    #     return False, 0, error_free_maintained

    # Wait for schedule to start
    wait_start = time.time()
    while not observatory.schedule_running and (time.time() - wait_start) < 15:
        time.sleep(0.5)

    if not observatory.schedule_running:
        return False, 0, error_free_maintained

    # Monitor execution
    last_completed = 0
    while observatory.schedule_running and (time.time() - start_time) < timeout:
        if not observatory.error_free:
            error_free_maintained = False
            break

        if observatory.schedule is not None:
            completed = observatory.schedule["completed"].sum()
            if completed > last_completed:
                last_completed = completed
                if completed >= len(observatory.schedule):
                    break

        time.sleep(1)

    print(observatory.schedule["completed"])

    final_completed = (
        observatory.schedule["completed"].sum()
        if observatory.schedule is not None
        else 0
    )

    return final_completed > 0, final_completed, error_free_maintained


class TestScheduleActionTypes:
    """Test each schedule action type individually."""

    def test_cool_camera_action(self, observatory, schedule_manager):
        """Test cool_camera action type."""
        schedule_data = create_schedule_data("cool_camera")

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory
            )

            assert (
                error_free_maintained
            ), f"error_free became False during cool_camera action. Error sources: {observatory.error_source}"
            assert (
                success
            ), f"cool_camera action did not complete successfully. Error sources: {observatory.error_source}"
            assert completed > 0, "No actions were completed"

    def test_calibration_action(self, observatory, schedule_manager):
        """Test calibration action type."""
        schedule_data = create_schedule_data("calibration")

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory
            )

            assert (
                error_free_maintained
            ), f"error_free became False during calibration action. Error sources: {observatory.error_source}"
            assert (
                success
            ), f"calibration action did not complete successfully. Error sources: {observatory.error_source}"
            assert completed > 0, "No actions were completed"

    def test_close_action(self, observatory, schedule_manager):
        """Test close action type."""
        schedule_data = create_schedule_data("close")

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory
            )

            assert (
                error_free_maintained
            ), f"error_free became False during close action. Error sources: {observatory.error_source}"
            assert (
                success
            ), f"close action did not complete successfully. Error sources: {observatory.error_source}"
            assert completed > 0, "No actions were completed"

    def test_open_action(self, observatory, schedule_manager):
        """Test open action type."""
        schedule_data = create_schedule_data("open")

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory
            )

            assert (
                error_free_maintained
            ), f"error_free became False during open action. Error sources: {observatory.error_source}"
            assert (
                success
            ), f"open action did not complete successfully. Error sources: {observatory.error_source}"
            assert completed > 0, "No actions were completed"

    def test_object_action(self, observatory, schedule_manager):
        """Test object action type."""
        schedule_data = create_schedule_data("object")

        with schedule_manager(schedule_data):
            success, completed, error_free_maintained = wait_for_schedule_completion(
                observatory
            )

            assert (
                error_free_maintained
            ), f"error_free became False during object action. Error sources: {observatory.error_source}"
            assert (
                success
            ), f"object action did not complete successfully. Error sources: {observatory.error_source}"
            assert completed > 0, "No actions were completed"

    # def test_flats_action(self, observatory, schedule_manager):
    #     """Test flats action type."""
    #     schedule_data = create_schedule_data("flats")

    #     with schedule_manager(schedule_data):
    #         success, completed, error_free_maintained = wait_for_schedule_completion(
    #             observatory
    #         )

    #         assert (
    #             error_free_maintained
    #         ), f"error_free became False during flats action. Error sources: {observatory.error_source}"
    #         assert (
    #             success
    #         ), f"flats action did not complete successfully. Error sources: {observatory.error_source}"
    #         assert completed > 0, "No actions were completed"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
