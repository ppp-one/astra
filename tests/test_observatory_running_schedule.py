"""
Pytest tests for Observatory schedule action types.
Tests each action type individually to ensure they complete without setting error_free to False.
"""

import pytest
import time
import json
import requests
import yaml
from datetime import datetime, timedelta, UTC
from contextlib import contextmanager
from glob import glob

from astra import Config
from astra.observatory import Observatory

import logging

logger = logging.getLogger(__name__)

OBSERVATORIES: dict = {}


@pytest.fixture(scope="session")
def temp_config(tmp_path_factory):
    """Create a temporary config for testing that uses real Astra templates."""
    tmp_path = tmp_path_factory.mktemp("astra_test")
    logger.info(f"Creating temporary config in {tmp_path}")

    # Create temporary paths
    config_path = tmp_path / "config" / "astra_config.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)

    folder_assets = tmp_path / "assets"
    folder_assets.mkdir(parents=True, exist_ok=True)

    gaia_db = tmp_path / "gaia.db"
    gaia_db.touch()  # Create empty Gaia DB file

    # Create config data
    config_data = {
        "observatory_name": "test_observatory",
        "folder_assets": str(folder_assets),
        "gaia_db": str(gaia_db),
    }

    # Write config file
    with open(config_path, "w") as f:
        yaml.dump(config_data, f)

    # Store original Config class attributes and singleton
    original_config_path = Config.CONFIG_PATH
    original_template_dir = Config.TEMPLATE_DIR
    original_instance = Config._instance

    try:
        # Patch the Config class paths - keep real template dir to use actual templates
        Config.CONFIG_PATH = config_path
        # Config.TEMPLATE_DIR stays the same to use real templates from source
        Config._instance = None  # Clear singleton to force fresh initialization

        # Create config instance - this will copy real templates to temp directory
        config = Config(allow_default=True)

        # Verify that the observatory config file was created from the template
        observatory_config_file = (
            config.paths.observatory_config / "test_observatory_config.yml"
        )
        if not observatory_config_file.exists():
            raise FileNotFoundError(
                f"Observatory config file was not created: {observatory_config_file}"
            )

        logger.info(f"Temporary config created successfully with paths:")
        logger.info(f"  Images: {config.paths.images}")
        logger.info(f"  Schedules: {config.paths.schedules}")
        logger.info(f"  Observatory config: {config.paths.observatory_config}")

        yield config

    finally:
        # Restore original Config class attributes
        Config.CONFIG_PATH = original_config_path
        Config.TEMPLATE_DIR = original_template_dir
        Config._instance = original_instance


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
def setup_observatories(temp_config):
    """Setup observatories for testing."""
    if not check_simulators_available():
        pytest.skip("Alpaca simulators not available on localhost:11111")

    # Import all modules that have global CONFIG instances
    import astra.observatory as obs_module

    # Store original config (they're all the same singleton instance)
    original_config = obs_module.CONFIG
    logger.info(f"Original config paths: {original_config.paths.images}")
    logger.info(f"Temp config paths: {temp_config.paths.images}")

    # Verify we're actually using different configs
    assert (
        original_config != temp_config
    ), "Temp config should be different from original"
    assert str(original_config.paths.images) != str(
        temp_config.paths.images
    ), "Image paths should be different"

    # Patch all modules with temp config
    obs_module.CONFIG = temp_config

    logger.info("Patched all module CONFIG references to use temp_config")

    try:
        # Load observatories with test config
        config_files = glob(str(temp_config.paths.observatory_config / "*_config.yml"))
        logger.info(f"Found observatory config files: {config_files}")

        for config_filename in config_files:
            logger.info(f"Loading observatory from {config_filename}")
            obs = Observatory(config_filename)
            OBSERVATORIES[obs.name] = obs
            obs.connect_all()

            # Wait a bit for connections to stabilize
            time.sleep(5)

        if not OBSERVATORIES:
            logger.warning(
                f"No observatories loaded from {config_files} at {temp_config.paths}"
            )
            pytest.skip("No observatories loaded")

        logger.info(
            f"Successfully loaded {len(OBSERVATORIES)} observatories: {list(OBSERVATORIES.keys())}"
        )
        yield OBSERVATORIES

    finally:
        # Cleanup after all tests
        clean_up_observatories()

        # Restore original config to all modules
        obs_module.CONFIG = original_config

        logger.info("Restored original CONFIG references to all modules")


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
def schedule_manager(observatory, temp_config):
    """Manage test schedule creation and cleanup."""
    schedule_path = temp_config.paths.schedules / f"{observatory.name}.jsonl"

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


def create_schedule_data(action_type: str, device_name: str = None) -> dict:
    """Create schedule data for the specified action type."""
    # Start the action in 5 seconds from now to give plenty of buffer
    base_time = datetime.now(UTC) + timedelta(seconds=5)

    # Use default device name if not provided
    if device_name is None:
        # Get the camera device name from the first available observatory
        if OBSERVATORIES:
            obs = list(OBSERVATORIES.values())[0]
            # Get the first camera device name from the observatory's config
            if hasattr(obs, "_config") and "Camera" in obs._config:
                camera_devices = obs._config["Camera"]
                if camera_devices and len(camera_devices) > 0:
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
