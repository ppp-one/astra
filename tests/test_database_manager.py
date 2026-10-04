import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from astra.database_manager import DatabaseManager


@pytest.fixture
def db_manager(temp_config):
    logger = MagicMock()
    logger.info = MagicMock()
    logger.warning = MagicMock()
    logger.error = MagicMock()
    logger.report_device_issue = MagicMock()
    return DatabaseManager("test_obs", logger=logger)


def test_create_database(db_manager):
    cursor = db_manager.create_database()
    assert cursor is not None
    # Check tables exist
    tables = [
        row[0]
        for row in cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    ]
    assert "polling" in tables
    assert "images" in tables
    assert "log" in tables


def test_execute_and_select(db_manager):
    db_manager.create_database()
    db_manager.execute(
        "INSERT INTO polling VALUES ('type', 'name', 'cmd', 'val', '2023-01-01T00:00:00')"
    )
    result = db_manager.execute_select("SELECT * FROM polling")
    assert isinstance(result, list)
    assert result[0][0] == "type"


def test_execute_select_to_df(db_manager):
    db_manager.create_database()
    db_manager.execute(
        "INSERT INTO polling VALUES ('type', 'name', 'cmd', 'val', '2023-01-01T00:00:00')"
    )
    df = db_manager.execute_select_to_df("SELECT * FROM polling", table="polling")
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["device_type"] == "type"


@patch("psutil.disk_usage")
@patch("pandas.read_sql_query")
def test_backup_runs(mock_read_sql, mock_disk, db_manager, temp_config):
    mock_disk.return_value.percent = 10
    mock_read_sql.return_value = pd.DataFrame({"datetime": ["20230101_000000"]})
    with patch("pandas.DataFrame.to_csv") as mock_csv:
        with patch("sqlite3.connect") as mock_connect:
            mock_db = MagicMock()
            mock_connect.return_value = mock_db
            # Patch logger.info directly to ensure it's a MagicMock
            db_manager.logger.info = MagicMock()
            db_manager.backup()
            assert db_manager.logger.info.call_count > 0
            assert mock_csv.called
            assert mock_db.close.called


def test_is_now_backup_time(db_manager):
    db_manager.backup_time = db_manager.backup_time.replace(hour=0, minute=0)
    with patch("astra.database_manager.datetime") as mock_dt:
        mock_dt.now.return_value = db_manager.backup_time.replace(tzinfo=None)
        assert db_manager.is_now_backup_time()


def test_maybe_run_backup(db_manager):
    thread_manager = MagicMock()
    db_manager.run_backup = True
    with patch.object(db_manager, "is_now_backup_time", return_value=True):
        db_manager.maybe_run_backup(thread_manager)
        assert thread_manager.start_thread.called
        assert not db_manager.run_backup
    with patch.object(db_manager, "is_now_backup_time", return_value=False):
        db_manager.maybe_run_backup(thread_manager)
        assert db_manager.run_backup


def test_create_database_adds_history_tables_and_index(db_manager):
    cursor = db_manager.create_database()
    names = {
        row[0]
        for row in cursor.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')"
        )
    }
    assert {"schedule_snapshots", "schedule_events"} <= names
    assert {"idx_polling_series", "idx_schedule_events_datetime"} <= names


def test_read_only_connection_rejects_writes(db_manager):
    import sqlite3

    db_manager.create_database()
    conn = db_manager.read_only_connection()
    try:
        assert conn.execute("SELECT COUNT(*) FROM polling").fetchone() is not None
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("DELETE FROM polling")
    finally:
        conn.close()


def test_read_only_connection_aborts_slow_query(db_manager):
    import sqlite3

    db_manager.create_database()
    conn = db_manager.read_only_connection(timeout_s=0.2)
    slow = (
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) "
        "SELECT COUNT(*) FROM n"
    )
    try:
        with pytest.raises(sqlite3.OperationalError, match="interrupted"):
            conn.execute(slow).fetchone()
    finally:
        conn.close()
