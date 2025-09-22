import logging
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import pytest
import requests
from unittest.mock import create_autospec, MagicMock

from astra.config import Config, ObservatoryConfig
from astra.logger import ObservatoryLogger

logger = logging.getLogger(__name__)


def set_max_safe_duration(obj, new_value):
    """
    Recursively set every occurrence of the key 'max_safe_duration' in a nested
    dict/list structure to new_value. Returns the modified object (in-place).
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "max_safe_duration":
                obj[k] = new_value
            else:
                set_max_safe_duration(v, new_value)
    elif isinstance(obj, list):
        for item in obj:
            set_max_safe_duration(item, new_value)
    return obj


@pytest.fixture(scope="session")
def server_url():
    host = "localhost"
    port = 11111
    url = f"http://{host}:{port}"

    # If server is already running (e.g. externally), skip starting a new one
    try:
        response = requests.get(url, timeout=1)
        if response.status_code == 200:
            yield url
            return
    except requests.RequestException:
        pass

    log_dir = Path(__file__).parent / "tests" / "simulator_logs"
    log_dir.mkdir(exist_ok=True)
    with (
        open(log_dir / "stdout.log", "wb") as stdout_log,
        open(log_dir / "stderr.err", "wb") as stderr_log,
    ):
        proc = subprocess.Popen(
            ["alpaca-simulators", "--host", host, "--port", str(port)],
            stdout=stdout_log,  # type: ignore
            stderr=stderr_log,  # type: ignore
            env=os.environ,
            text=True,
        )

        # wait until server responds or process dies
        deadline = time.time() + 15
        while time.time() < deadline:
            if proc.poll() is not None:
                out, err = proc.communicate()
                raise RuntimeError(
                    f"Simulator exited early (code={proc.returncode}).\nstdout:\n{out}\nstderr:\n{err}"
                )
            try:
                requests.get(url, timeout=1)
                break
            except requests.RequestException:
                time.sleep(0.2)
        else:
            proc.terminate()
            proc.wait(timeout=5)
            raise RuntimeError("Timed out waiting for simulator to start")

        try:
            yield url
        finally:
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()


@pytest.fixture(scope="session", autouse=True)
# @pytest.fixture(scope="session")
def temp_config(tmp_path_factory):
    """
    Minimal fixture: point CONFIG_PATH to tmp, create assets + gaia db,
    initialise Config by passing observatory_name and folder_assets,
    then remove config and assets and reset the singleton.
    """
    # Prepare temp assets folder and gaia db file
    tmp_path = tmp_path_factory.mktemp("astra_test")
    assets_dir = tmp_path / "astra_assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    # Temporary config file path
    config_path = tmp_path / "config" / "astra_config.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)

    # Copy path gaia database path from initialised config if available
    if Config.CONFIG_PATH.exists():
        gaia_db_file = Config().gaia_db
    else:
        gaia_db_file = assets_dir / "dummy_gaia_db.db"
        gaia_db_file.touch()

    # Patch Config path
    Config.CONFIG_PATH = config_path

    # Clear any potentially existing singleton instance
    Config._instance = None

    config = Config(
        observatory_name="test_observatory",
        folder_assets=assets_dir,
        gaia_db=gaia_db_file,
        propagate_observatory_name=True,
    )

    observatory_config = ObservatoryConfig.from_config(config)
    set_max_safe_duration(observatory_config, 30 / 60)
    observatory_config.save()

    # Reload!
    config._instance = None
    config = Config()

    logging.info("Temporary config created successfully with paths:")
    logging.info(f"  Assets: {config.paths.assets}")
    logging.info(f"  Images: {config.paths.images}")
    logging.info(f"  Schedules: {config.paths.schedules}")
    logging.info(f"  Observatory config: {config.paths.observatory_config}")
    logging.info(f"  Logs: {config.paths.logs}")

    try:
        yield config
    finally:
        # cleanup assets and config file
        try:
            if config.folder_assets.exists():
                shutil.rmtree(config.folder_assets)
        except Exception:
            pass

        try:
            if Config.CONFIG_PATH.exists():
                Config.CONFIG_PATH.unlink()
        except Exception:
            pass

        # reset singleton
        Config._instance = None


@pytest.fixture
def observatory_config(temp_config):
    return ObservatoryConfig.from_config(temp_config)


@pytest.fixture
def observatory(temp_config):
    from astra.observatory import Observatory

    server_url = "http://localhost:11111"

    logger.info("Reloading observatory state to defaults during setup")
    response = requests.get(f"{server_url}/reload")
    assert response.status_code == 200

    observatory_config = ObservatoryConfig.from_config(temp_config)
    observatory = Observatory(observatory_config.observatory_name)

    observatory.connect_all_devices()
    time.sleep(5)

    logger.info("Successfully loaded observatory.")
    yield observatory

    # Cleanup on teardown
    logger.info("Tearing down observatory.")
    if observatory.schedule_manager.running:
        observatory.schedule_manager.stop_schedule(observatory.thread_manager)
    # Stop watchdog
    if observatory.watchdog_running:
        observatory.watchdog_running = False
    for device_type in observatory.devices:
        for device_name in observatory.devices[device_type]:
            try:
                observatory.devices[device_type][device_name].stop()
            except Exception:
                pass


@pytest.fixture
def mock_observatory_logger():
    logger = create_autospec(ObservatoryLogger, instance=True)
    logger.info = MagicMock()
    logger.warning = MagicMock()
    logger.error = MagicMock()
    logger.report_device_issue = MagicMock()
    logger.debug = MagicMock()
    return logger
