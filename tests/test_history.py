import json
import sqlite3
from datetime import datetime, timedelta
from unittest.mock import MagicMock
from uuid import uuid4

import pandas as pd
import pytest

from astra import history
from astra.database_manager import DatabaseManager
from astra.history import SeriesKey

NOW = datetime(2026, 1, 10, 12, 0, 0)

FITS_CONFIG = pd.DataFrame(
    [
        ["float", False, "Camera", "CCDTemperature", "[Celsius] CCD temperature"],
        ["int", False, "Camera", "CameraState", "Camera status"],
        ["float", False, "Telescope", "RightAscension", "[deg] Target RA"],
        ["str", False, "FilterWheel", "Position", "FilterWheel position"],
        ["float", False, "Focuser", "Position", "[steps] Focuser position"],
    ],
    columns=["dtype", "fixed", "device_type", "device_command", "comment"],
    index=pd.Index(["CCD-TEMP", "CAM-STAT", "RA", "FW-POS", "FOCUSPOS"], name="header"),
)


def t(seconds: float) -> str:
    return history.to_db_time(NOW - timedelta(days=3) + timedelta(seconds=seconds))


@pytest.fixture
def db():
    """Database with the Astra schema, filled by direct inserts."""
    manager = DatabaseManager(f"history_{uuid4().hex}", logger=MagicMock())
    manager.create_database()
    # Wait for the worker to create the schema
    manager.execute_select("SELECT 1")
    writer = sqlite3.connect(manager.db_path)
    yield manager, writer
    writer.close()


def insert(writer, key: SeriesKey, rows):
    writer.executemany(
        "INSERT INTO polling VALUES (?, ?, ?, ?, ?)",
        [(*key.params(), value, time) for time, value in rows],
    )
    writer.commit()


def test_numeric_series_is_reduced_to_max_points(db):
    manager, writer = db
    key = SeriesKey("Telescope", "tel", "Altitude")
    # 3 days at 1 s, with one hour missing and some values that are not numbers
    rows = [
        (t(s), "None" if s % 1000 == 0 else str(30 + (s % 600) / 10))
        for s in range(0, 3 * 24 * 3600)
        if not 3600 <= s < 7200
    ]
    insert(writer, key, rows)

    conn = manager.read_only_connection()
    start, end = NOW - timedelta(days=3), NOW
    result = history.query_numeric(conn, key, start, end, max_points=1500)
    conn.close()

    assert 0 < len(result["t"]) <= 1500
    assert result["bucket_s"] == 173
    # Text values such as 'None' are skipped, not read as 0
    assert min(result["min"]) >= 30
    # No bucket inside the missing hour
    start_ms = history.to_epoch_ms(start)
    gap = [x for x in result["t"] if start_ms + 3700_000 <= x < start_ms + 7000_000]
    assert gap == []
    assert len(json.dumps(result)) < 100_000


def test_changes_only_and_value_before_start(db):
    manager, writer = db
    key = SeriesKey("Telescope", "tel", "Tracking")
    values = ["True"] * 10 + ["False"] * 10 + ["True"] * 10
    insert(writer, key, [(t(s * 60), v) for s, v in enumerate(values)])

    conn = manager.read_only_connection()
    start = NOW - timedelta(days=3) + timedelta(minutes=5)
    result = history.query_changes(conn, key, start, NOW, dtype="bool")
    conn.close()

    # The value at the start, then the two changes
    assert result["v"] == [True, False, True]
    assert result["t"][0] == history.to_epoch_ms(start)
    assert result["last_t"] == history.to_epoch_ms(history.parse_time(t(29 * 60)))
    assert result["gaps"] == []
    assert result["truncated"] is False


def test_changes_report_gaps(db):
    manager, writer = db
    key = SeriesKey("Dome", "dome", "ShutterStatus")
    # Samples every 10 s, then nothing for one hour, then samples again
    seconds = list(range(0, 600, 10)) + list(range(4200, 4800, 10))
    insert(writer, key, [(t(s), "1") for s in seconds])

    conn = manager.read_only_connection()
    start = NOW - timedelta(days=3)
    result = history.query_changes(conn, key, start, NOW, dtype="state")
    # A range that starts long after the last sample before it
    late = history.query_changes(
        conn, key, start + timedelta(seconds=3000), NOW, dtype="state"
    )
    conn.close()

    def ms(seconds):
        return history.to_epoch_ms(start + timedelta(seconds=seconds))

    assert result["v"] == [1]
    assert result["gaps"] == [[ms(590), ms(4200)]]
    assert late["gaps"][0] == [ms(3000), ms(4200)]


def test_changes_are_truncated(db):
    manager, writer = db
    key = SeriesKey("Camera", "cam", "CameraState")
    insert(writer, key, [(t(s), str(s % 2)) for s in range(100)])

    conn = manager.read_only_connection()
    result = history.query_changes(
        conn, key, NOW - timedelta(days=3), NOW, dtype="state", max_changes=10
    )
    conn.close()

    assert len(result["t"]) == 10
    assert result["v"][:2] == [0, 1]
    assert result["truncated"] is True


def test_sql_injection_is_treated_as_text(db):
    manager, writer = db
    insert(writer, SeriesKey("Telescope", "tel", "Altitude"), [(t(0), "10")])

    conn = manager.read_only_connection()
    key = SeriesKey("Telescope", "tel' OR '1'='1", "Altitude")
    result = history.query_numeric(conn, key, NOW - timedelta(days=3), NOW)
    changes = history.query_changes(conn, key, NOW - timedelta(days=3), NOW)
    conn.close()

    assert result["t"] == []
    assert changes["t"] == []


def test_list_parameters_resolves_types_and_units(db):
    manager, writer = db
    for key, value in [
        (SeriesKey("Camera", "cam", "CCDTemperature"), "-20.0"),
        (SeriesKey("Camera", "cam", "CameraState"), "2"),
        (SeriesKey("SafetyMonitor", "safe", "IsSafe"), "True"),
        (SeriesKey("Telescope", "tel", "RightAscension"), "5.5"),
        (SeriesKey("Telescope", "tel", "SideOfPier"), "0"),
        (SeriesKey("ObservingConditions", "wx", "Pressure"), "1013"),
        (SeriesKey("FilterWheel", "fw", "Position"), "1"),
        (SeriesKey("Custom", "dev", "Mode"), "fast"),
        (SeriesKey("Custom", "dev", "Count"), "12"),
    ]:
        insert(writer, key, [(t(0), value)])

    conn = manager.read_only_connection()
    extra = {SeriesKey("FilterWheel", "fw", "Position"): {-1: "Moving", 0: "V", 1: "R"}}
    parameters = {
        (p["device_type"], p["device_command"]): p
        for p in history.list_parameters(conn, FITS_CONFIG, extra)
    }
    conn.close()

    def get(device_type, command):
        p = parameters[(device_type, command)]
        return p["dtype"], p["unit"]

    assert get("Camera", "CCDTemperature") == ("float", "°C")
    assert get("Camera", "CameraState") == ("state", "")
    assert parameters[("Camera", "CameraState")]["labels"]["2"] == "Exposing"
    assert get("SafetyMonitor", "IsSafe") == ("bool", "")
    assert get("Telescope", "RightAscension") == ("float", "h")
    assert get("Telescope", "SideOfPier") == ("state", "")
    assert get("ObservingConditions", "Pressure") == ("float", "hPa")
    assert get("FilterWheel", "Position") == ("state", "")
    assert parameters[("FilterWheel", "Position")]["labels"]["1"] == "R"
    assert get("Custom", "Mode") == ("str", "")
    assert get("Custom", "Count") == ("int", "")


def test_clamp_range():
    start, end = history.clamp_range(
        NOW - timedelta(days=10), NOW + timedelta(days=1), now=NOW
    )
    assert start == NOW - timedelta(days=history.RETENTION_DAYS)
    assert end == NOW

    with pytest.raises(ValueError):
        history.clamp_range(NOW, NOW - timedelta(hours=1), now=NOW)
    with pytest.raises(ValueError):
        history.clamp_range(NOW - timedelta(days=9), NOW - timedelta(days=8), now=NOW)


def test_parse_time_returns_naive_utc():
    assert history.parse_time("2026-01-10T13:00:00+01:00") == datetime(2026, 1, 10, 12)
    assert history.parse_time("2026-01-10T12:00:00Z") == datetime(2026, 1, 10, 12)
    assert history.parse_time("2026-01-10 12:00:00.5") == datetime(
        2026, 1, 10, 12, 0, 0, 500000
    )


def test_query_schedule(db):
    manager, writer = db
    jsonl = "\n".join(
        json.dumps(row)
        for row in [
            {
                "device_name": "cam",
                "action_type": "object",
                "action_value": {"object": "M42"},
                "start_time": "2026-01-10T01:00:00+00:00",
                "end_time": "2026-01-10T02:00:00+00:00",
            },
            {
                "device_name": "cam",
                "action_type": "close",
                "action_value": {},
                "start_time": "2026-01-10T05:00:00+00:00",
                "end_time": "2026-01-10T05:10:00+00:00",
            },
        ]
    )
    writer.execute(
        "INSERT INTO schedule_snapshots VALUES (1, ?, 'abc', 2, '', '', ?)",
        ("2026-01-09 20:00:00.000000", jsonl),
    )
    writer.executemany(
        "INSERT INTO schedule_events VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("2026-01-09 20:00:00.000000", "loaded", 1, None, None, None, ""),
            ("2026-01-10 01:00:00.000000", "action_started", 1, 0, "cam", "object", ""),
            (
                "2026-01-10 02:00:00.000000",
                "action_finished",
                1,
                0,
                "cam",
                "object",
                "",
            ),
        ],
    )
    writer.commit()

    conn = manager.read_only_connection()
    result = history.query_schedule(
        conn, datetime(2026, 1, 10, 0, 30), datetime(2026, 1, 10, 6)
    )
    snapshot = history.get_snapshot(conn, 1)
    missing = history.get_snapshot(conn, 99)
    conn.close()

    assert result["loaded_at_start"] == 1
    assert [e["event"] for e in result["events"]] == [
        "loaded",
        "action_started",
        "action_finished",
    ]
    actions = result["snapshots"]["1"]["actions"]
    assert [a["label"] for a in actions] == ["M42", "close"]
    assert actions[0]["start"] == history.to_epoch_ms(datetime(2026, 1, 10, 1))
    assert snapshot == jsonl
    assert missing is None
