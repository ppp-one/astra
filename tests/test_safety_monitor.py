import pytest
from unittest.mock import MagicMock
from datetime import datetime, UTC, timedelta
from astra.safety_monitor import SafetyMonitor


@pytest.fixture
def mock_config():
    return {
        "SafetyMonitor": [{"device_name": "SafeMon", "max_safe_duration": 30}],
        "ObservingConditions": [
            {
                "closing_limits": {
                    "Temperature": [{"lower": 0, "upper": 30, "max_safe_duration": 10}],
                    "SkyTemperature": [
                        {"lower": -50, "upper": 0, "max_safe_duration": 10}
                    ],
                }
            }
        ],
    }


@pytest.fixture
def mock_logger():
    logger = MagicMock()
    logger.info = MagicMock()
    logger.warning = MagicMock()
    logger.error = MagicMock()
    logger.report_device_issue = MagicMock()
    logger.debug = MagicMock()
    return logger


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.execute_select = MagicMock(return_value=[(0, None)])
    db.execute_select_to_df = MagicMock(
        return_value=MagicMock(
            shape=(1, 3),
            pivot=lambda *a, **k: MagicMock(
                index=[0],
                columns=["Temperature", "SkyTemperature"],
                sort_index=lambda: MagicMock(),
                apply=lambda *a, **k: MagicMock(),
                interpolate=lambda *a, **k: MagicMock(),
                __getitem__=lambda s, k: MagicMock(shape=(1,)),
                max=lambda: 0,
            ),
        )
    )
    return db


@pytest.fixture
def mock_device_manager():
    dm = MagicMock()
    dm.devices = {
        "SafetyMonitor": {
            "SafeMon": MagicMock(
                poll_latest=MagicMock(
                    return_value={"IsSafe": {"datetime": MagicMock(), "value": True}}
                )
            )
        }
    }
    return dm


@pytest.fixture
def safety_monitor(mock_config, mock_db, mock_logger, mock_device_manager):
    return SafetyMonitor(mock_config, mock_db, mock_logger, mock_device_manager)


def test_init_sets_config(safety_monitor):
    assert safety_monitor.device_type == "SafetyMonitor"
    assert safety_monitor.device_name == "SafeMon"
    assert safety_monitor.max_safe_duration == 30


def test_device_property(safety_monitor, mock_device_manager):
    assert (
        safety_monitor.device is mock_device_manager.devices["SafetyMonitor"]["SafeMon"]
    )


def test_weather_safe_property(safety_monitor):
    safety_monitor._weather_safe = True
    assert safety_monitor.weather_safe is True


def test_time_to_safe_property(safety_monitor):
    safety_monitor._time_to_safe = 5.5
    assert safety_monitor.time_to_safe == 5.5


def test_check_safety_monitor_safe(safety_monitor):
    safety_monitor.device_manager.devices["SafetyMonitor"][
        "SafeMon"
    ].poll_latest.return_value = {
        "IsSafe": {"datetime": datetime.now(UTC) - timedelta(seconds=5), "value": True}
    }
    result = safety_monitor.check_safety_monitor(30)
    assert isinstance(result, list)


def test_check_internal_conditions(safety_monitor):
    result = safety_monitor.check_internal_conditions()
    assert isinstance(result, tuple)
    assert len(result) == 3


def test_update_status_safe(safety_monitor):
    safety_monitor.device_manager.devices["SafetyMonitor"][
        "SafeMon"
    ].poll_latest.return_value = {
        "IsSafe": {"datetime": datetime.now(UTC) - timedelta(seconds=5), "value": True}
    }
    safety_monitor.database_manager.execute_select.return_value = [(0, None)]
    safety_monitor.database_manager.execute_select_to_df.return_value = MagicMock(
        shape=(1, 3),
        pivot=lambda *a, **k: MagicMock(
            index=[0],
            columns=["Temperature", "SkyTemperature"],
            sort_index=lambda: MagicMock(),
            apply=lambda *a, **k: MagicMock(),
            interpolate=lambda *a, **k: MagicMock(),
            __getitem__=lambda s, k: MagicMock(shape=(1,)),
            max=lambda: 0,
        ),
    )
    result = safety_monitor.update_status()
    assert result in (True, None)
    assert safety_monitor.logger.debug.called
