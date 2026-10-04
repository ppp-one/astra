"""Read polled device history and schedule history for the History page.

The polling table can hold millions of rows. These functions reduce the data
in SQL before it leaves the database, so a response stays small whatever the
time range:

    - Numeric series are averaged into at most ``max_points`` time buckets,
      with the minimum and maximum of each bucket.
    - Boolean, state and text series return only the times where the value
      changed.

All functions take an open ``sqlite3.Connection``, preferably one from
``DatabaseManager.read_only_connection``, and use parameterized queries.
"""

import json
import math
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd

# The database keeps polled rows for this many days (see DatabaseManager.backup)
RETENTION_DAYS = 3

DB_TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

# ASCOM properties that are always booleans
BOOL_COMMANDS = {
    "AtHome",
    "AtPark",
    "CoolerOn",
    "IsMoving",
    "IsSafe",
    "Slewing",
    "Tracking",
    "WeatherSafe",
}

# ASCOM enum properties, value -> label
STATE_LABELS = {
    "CameraState": {
        0: "Idle",
        1: "Waiting",
        2: "Exposing",
        3: "Reading",
        4: "Download",
        5: "Error",
    },
    "ShutterStatus": {0: "Open", 1: "Closed", 2: "Opening", 3: "Closing", 4: "Error"},
    "SideOfPier": {-1: "Unknown", 0: "East", 1: "West"},
}

# ASCOM ObservingConditions units, used when the FITS config has none
OBSERVING_CONDITIONS_UNITS = {
    "CloudCover": "%",
    "DewPoint": "°C",
    "Humidity": "%",
    "Pressure": "hPa",
    "RainRate": "mm/h",
    "SkyBrightness": "lux",
    "SkyQuality": "mag/arcsec²",
    "SkyTemperature": "°C",
    "StarFWHM": "arcsec",
    "Temperature": "°C",
    "WindDirection": "°",
    "WindGust": "m/s",
    "WindSpeed": "m/s",
}

# Polled values are in ASCOM units, which can differ from the FITS header unit
COMMAND_UNITS = {"RightAscension": "h"}

FITS_UNIT_NAMES = {"Celsius": "°C", "deg": "°", "micron": "µm"}

# A device value is numeric when it has a digit and only number characters
NUMERIC_SQL = "(device_value GLOB '*[0-9]*' AND device_value NOT GLOB '*[^0-9eE.+-]*')"

SERIES_WHERE = "device_type = ? AND device_name = ? AND device_command = ?"

# No sample for longer than this means the device was not polled (for example,
# Astra was not running), so a state is not drawn over that time
GAP_S = 300


@dataclass(frozen=True)
class SeriesKey:
    """Identifies one polled series in the polling table."""

    device_type: str
    device_name: str
    device_command: str

    def params(self) -> tuple[str, str, str]:
        return (self.device_type, self.device_name, self.device_command)


def to_db_time(value: datetime) -> str:
    """Format a datetime as the naive UTC string stored in the database."""
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.strftime(DB_TIME_FORMAT)


def parse_time(value: str) -> datetime:
    """Parse an ISO time string. A time without a timezone is taken as UTC.

    Returns:
        datetime: Naive datetime in UTC, the same as the database values.
    """
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    return parsed.to_pydatetime()


def to_epoch_ms(value: datetime) -> int:
    """Convert a naive UTC datetime to milliseconds since the Unix epoch."""
    return int(value.replace(tzinfo=UTC).timestamp() * 1000)


def clamp_range(
    start: datetime, end: datetime, now: datetime | None = None
) -> tuple[datetime, datetime]:
    """Limit a time range to the data the database keeps.

    Args:
        start (datetime): Naive UTC start time.
        end (datetime): Naive UTC end time.
        now (datetime | None): Naive UTC current time, for tests.

    Raises:
        ValueError: If the range is empty after the limit is applied.
    """
    if now is None:
        now = datetime.now(UTC).replace(tzinfo=None)
    start = max(start, now - timedelta(days=RETENTION_DAYS))
    end = min(end, now)
    if end <= start:
        raise ValueError("The time range is empty or outside the last 3 days.")
    return start, end


def _fits_unit(comment) -> str:
    """Get the unit from a FITS comment such as '[Celsius] CCD temperature'."""
    if not isinstance(comment, str):
        return ""
    match = re.match(r"\s*\[([^\]]+)\]", comment)
    if match is None:
        return ""
    unit = match.group(1).strip()
    return FITS_UNIT_NAMES.get(unit, unit)


def _infer_dtype(value: str | None) -> str:
    """Guess the data type of a polled value from its text."""
    if value is None:
        return "str"
    if value in ("True", "False"):
        return "bool"
    try:
        int(value)
        return "int"
    except ValueError:
        pass
    try:
        float(value)
        return "float"
    except ValueError:
        return "str"


def _fits_rows(fits_config: pd.DataFrame | None) -> dict[tuple[str, str], dict]:
    """Map (device_type, device_command) to dtype, unit and header name."""
    rows: dict[tuple[str, str], dict] = {}
    if fits_config is None or fits_config.empty:
        return rows

    for header, row in fits_config.iterrows():
        device_type = row.get("device_type")
        device_command = row.get("device_command")
        if not isinstance(device_type, str) or not isinstance(device_command, str):
            continue
        rows.setdefault(
            (device_type, device_command),
            {
                "dtype": row.get("dtype") if isinstance(row.get("dtype"), str) else "",
                "unit": _fits_unit(row.get("comment")),
                "header": str(header),
            },
        )
    return rows


def _is_weather_value(key: SeriesKey) -> bool:
    """True for an ASCOM ObservingConditions value, which is always a float."""
    return (
        key.device_type == "ObservingConditions"
        and key.device_command in OBSERVING_CONDITIONS_UNITS
    )


def resolve_parameter(
    key: SeriesKey,
    fits_rows: dict[tuple[str, str], dict],
    latest_value: str | None = None,
    extra_labels: dict[SeriesKey, dict] | None = None,
) -> dict:
    """Describe one series: its data type, unit and value labels.

    Data types are ``float`` and ``int`` (plotted as numbers), and ``bool``,
    ``state`` and ``str`` (plotted as changes).

    Args:
        key (SeriesKey): The series.
        fits_rows (dict): Output of ``_fits_rows``.
        latest_value (str | None): Latest stored value, used to guess the type
            when nothing else is known.
        extra_labels (dict | None): Value labels for a series, for example the
            filter names of a filter wheel.
    """
    command = key.device_command
    fits = fits_rows.get((key.device_type, command), {})
    labels = None

    if extra_labels and key in extra_labels:
        dtype = "state"
        labels = extra_labels[key]
    elif command in STATE_LABELS:
        dtype = "state"
        labels = STATE_LABELS[command]
    elif command in BOOL_COMMANDS:
        dtype = "bool"
    elif fits.get("dtype") in ("bool", "int", "float", "str"):
        dtype = fits["dtype"]
    elif _is_weather_value(key):
        dtype = "float"
    else:
        dtype = _infer_dtype(latest_value)

    unit = (
        COMMAND_UNITS.get(command)
        or fits.get("unit")
        or (OBSERVING_CONDITIONS_UNITS[command] if _is_weather_value(key) else "")
    )

    return {
        "device_type": key.device_type,
        "device_name": key.device_name,
        "device_command": command,
        "dtype": dtype,
        "unit": unit if dtype in ("float", "int") else "",
        "header": fits.get("header", ""),
        "labels": (
            {str(value): label for value, label in labels.items()} if labels else None
        ),
    }


def list_series(conn: sqlite3.Connection) -> list[SeriesKey]:
    """List every series that has rows in the polling table."""
    rows = conn.execute(
        "SELECT DISTINCT device_type, device_name, device_command FROM polling"
    ).fetchall()
    return [SeriesKey(*row) for row in rows if all(isinstance(v, str) for v in row)]


def latest_value(conn: sqlite3.Connection, key: SeriesKey) -> str | None:
    """Return the latest stored value of a series."""
    row = conn.execute(
        f"SELECT device_value FROM polling WHERE {SERIES_WHERE} "
        "ORDER BY datetime DESC LIMIT 1",
        key.params(),
    ).fetchone()
    return row[0] if row else None


def list_parameters(
    conn: sqlite3.Connection,
    fits_config: pd.DataFrame | None = None,
    extra_labels: dict[SeriesKey, dict] | None = None,
) -> list[dict]:
    """Describe every polled series, sorted by device type, name and command."""
    fits_rows = _fits_rows(fits_config)
    parameters = []
    for key in list_series(conn):
        known = (
            key.device_command in STATE_LABELS
            or key.device_command in BOOL_COMMANDS
            or (key.device_type, key.device_command) in fits_rows
            or _is_weather_value(key)
        )
        value = None if known else latest_value(conn, key)
        parameters.append(resolve_parameter(key, fits_rows, value, extra_labels))

    parameters.sort(
        key=lambda p: (p["device_type"], p["device_name"], p["device_command"])
    )
    return parameters


def _compact(value: float | None) -> float | None:
    """Round to 6 significant digits to keep the JSON small."""
    if value is None or not math.isfinite(value):
        return None
    return float(f"{value:.6g}")


def query_numeric(
    conn: sqlite3.Connection,
    key: SeriesKey,
    start: datetime,
    end: datetime,
    max_points: int = 1500,
) -> dict:
    """Average a numeric series into at most ``max_points`` time buckets.

    Values that are not numbers are skipped.

    Returns:
        dict: Columns ``t`` (bucket start, epoch ms), ``mean``, ``min``,
        ``max`` and ``count``, plus ``bucket_s``.
    """
    span_s = (end - start).total_seconds()
    bucket_s = max(1, math.ceil(span_s / max(1, max_points)))
    start_str = to_db_time(start)

    rows = conn.execute(
        f"""SELECT
                CAST((julianday(datetime) - julianday(?)) * 86400.0 / ? AS INTEGER) AS b,
                AVG(CAST(device_value AS REAL)),
                MIN(CAST(device_value AS REAL)),
                MAX(CAST(device_value AS REAL)),
                COUNT(*)
            FROM polling
            WHERE {SERIES_WHERE} AND datetime >= ? AND datetime < ? AND {NUMERIC_SQL}
            GROUP BY b
            ORDER BY b""",
        (start_str, bucket_s, *key.params(), start_str, to_db_time(end)),
    ).fetchall()

    start_ms = to_epoch_ms(start)
    result: dict = {"t": [], "mean": [], "min": [], "max": [], "count": []}
    for bucket, mean, minimum, maximum, count in rows:
        result["t"].append(start_ms + bucket * bucket_s * 1000)
        result["mean"].append(_compact(mean))
        result["min"].append(_compact(minimum))
        result["max"].append(_compact(maximum))
        result["count"].append(count)

    result["bucket_s"] = bucket_s
    return result


def _normalise_value(value: str | None, dtype: str):
    """Convert a stored value text to a JSON value for its data type."""
    if value is None:
        return None
    if dtype == "bool":
        if value in ("True", "1", "1.0"):
            return True
        if value in ("False", "0", "0.0"):
            return False
        return None
    if dtype == "state":
        try:
            return int(float(value))
        except ValueError:
            return value
    return value


def query_changes(
    conn: sqlite3.Connection,
    key: SeriesKey,
    start: datetime,
    end: datetime,
    dtype: str = "str",
    max_changes: int = 5000,
) -> dict:
    """Return only the times where a series changed value.

    The first point is the value that was valid at ``start``, so a plot is not
    empty at its left edge.

    Returns:
        dict: Columns ``t`` (epoch ms) and ``v``, plus ``last_t`` (time of the
        last sample before ``end``, so a plot knows where the data stops),
        ``gaps`` (list of ``[start, end]`` epoch ms with no samples for longer
        than ``GAP_S``) and ``truncated`` (True if more than ``max_changes``
        changes were found).
    """
    start_str = to_db_time(start)
    end_str = to_db_time(end)

    before = conn.execute(
        f"SELECT datetime, device_value FROM polling WHERE {SERIES_WHERE} "
        "AND datetime < ? ORDER BY datetime DESC LIMIT 1",
        (*key.params(), start_str),
    ).fetchone()

    rows = conn.execute(
        f"""SELECT datetime, device_value FROM (
                SELECT datetime, device_value,
                    LAG(device_value) OVER (ORDER BY datetime) AS previous
                FROM polling
                WHERE {SERIES_WHERE} AND datetime >= ? AND datetime < ?
            )
            WHERE previous IS NULL OR previous != device_value
            ORDER BY datetime
            LIMIT ?""",
        (*key.params(), start_str, end_str, max_changes + 1),
    ).fetchall()

    last = conn.execute(
        f"SELECT MAX(datetime) FROM polling WHERE {SERIES_WHERE} AND datetime < ?",
        (*key.params(), end_str),
    ).fetchone()

    gap_rows = conn.execute(
        f"""SELECT previous, datetime FROM (
                SELECT datetime, LAG(datetime) OVER (ORDER BY datetime) AS previous
                FROM polling
                WHERE {SERIES_WHERE} AND datetime >= ? AND datetime < ?
            )
            WHERE previous IS NOT NULL
                AND (julianday(datetime) - julianday(previous)) * 86400.0 > ?
            ORDER BY datetime
            LIMIT ?""",
        (*key.params(), start_str, end_str, GAP_S, max_changes),
    ).fetchall()
    gaps = [
        [to_epoch_ms(parse_time(a)), to_epoch_ms(parse_time(b))] for a, b in gap_rows
    ]

    truncated = len(rows) > max_changes
    rows = rows[:max_changes]

    result: dict = {"t": [], "v": []}
    previous = None
    if before is not None:
        previous = before[1]
        result["t"].append(to_epoch_ms(start))
        result["v"].append(_normalise_value(previous, dtype))
        # The value before the range is old, so there was a gap at the start
        if (start - parse_time(before[0])).total_seconds() > GAP_S:
            first = to_epoch_ms(parse_time(rows[0][0])) if rows else None
            gaps.insert(0, [to_epoch_ms(start), first or to_epoch_ms(end)])

    for time_str, value in rows:
        if value == previous:
            continue
        result["t"].append(to_epoch_ms(parse_time(time_str)))
        result["v"].append(_normalise_value(value, dtype))
        previous = value

    result["last_t"] = (
        to_epoch_ms(parse_time(last[0])) if last and last[0] is not None else None
    )
    result["gaps"] = gaps
    result["truncated"] = truncated
    return result


def query_series(
    conn: sqlite3.Connection,
    parameter: dict,
    start: datetime,
    end: datetime,
    max_points: int = 1500,
) -> dict:
    """Query one series with the method that fits its data type.

    Args:
        parameter (dict): One entry from ``list_parameters``.
    """
    key = SeriesKey(
        parameter["device_type"],
        parameter["device_name"],
        parameter["device_command"],
    )
    if parameter["dtype"] in ("float", "int"):
        data = query_numeric(conn, key, start, end, max_points)
    else:
        data = query_changes(conn, key, start, end, parameter["dtype"])

    return {
        **parameter,
        "start": to_epoch_ms(start),
        "end": to_epoch_ms(end),
        "data": data,
    }


def _action_label(action: dict) -> str:
    """Short label for a schedule action, for example the target name."""
    value = action.get("action_value")
    if isinstance(value, dict):
        for field in ("object", "target", "name"):
            if value.get(field):
                return str(value[field])
    return str(action.get("action_type", ""))


def _parse_snapshot_actions(jsonl: str, max_actions: int = 2000) -> list[dict]:
    """Parse the actions of a stored schedule into short dicts for plotting."""
    actions = []
    for index, line in enumerate(jsonl.splitlines()):
        if len(actions) >= max_actions:
            break
        line = line.strip()
        if not line:
            continue
        try:
            action = json.loads(line)
            start = to_epoch_ms(parse_time(action["start_time"]))
            end = to_epoch_ms(parse_time(action["end_time"]))
        except (ValueError, KeyError, TypeError):
            continue
        actions.append(
            {
                "index": index,
                "device_name": action.get("device_name", ""),
                "action_type": action.get("action_type", ""),
                "label": _action_label(action),
                "start": start,
                "end": end,
            }
        )
    return actions


def query_schedule(
    conn: sqlite3.Connection,
    start: datetime,
    end: datetime,
    max_events: int = 5000,
) -> dict:
    """Return schedule events and the loaded schedules for a time range.

    Events from one day before ``start`` are included, so an action or a run
    that started before the range is still shown. The schedule that was
    loaded at ``start`` is always included.

    Returns:
        dict: ``events`` (list of dicts) and ``snapshots`` (id -> summary with
        its parsed actions).
    """
    events_from = to_db_time(start - timedelta(days=1))
    rows = conn.execute(
        """SELECT datetime, event, snapshot_id, action_index, device_name,
                action_type, message
            FROM schedule_events
            WHERE datetime >= ? AND datetime < ?
            ORDER BY datetime
            LIMIT ?""",
        (events_from, to_db_time(end), max_events),
    ).fetchall()

    events = [
        {
            "t": to_epoch_ms(parse_time(row[0])),
            "event": row[1],
            "snapshot_id": row[2],
            "action_index": row[3],
            "device_name": row[4],
            "action_type": row[5],
            "message": row[6],
        }
        for row in rows
    ]

    snapshot_ids = {e["snapshot_id"] for e in events if e["snapshot_id"] is not None}
    loaded_before = conn.execute(
        "SELECT snapshot_id FROM schedule_events WHERE event = 'loaded' "
        "AND datetime < ? ORDER BY datetime DESC LIMIT 1",
        (to_db_time(start),),
    ).fetchone()
    if loaded_before is not None and loaded_before[0] is not None:
        snapshot_ids.add(loaded_before[0])

    snapshots = {}
    for snapshot_id in sorted(snapshot_ids):
        row = conn.execute(
            "SELECT id, datetime, sha256, n_actions, jsonl "
            "FROM schedule_snapshots WHERE id = ?",
            (snapshot_id,),
        ).fetchone()
        if row is None:
            continue
        snapshots[str(row[0])] = {
            "id": row[0],
            "t": to_epoch_ms(parse_time(row[1])),
            "sha256": row[2],
            "n_actions": row[3],
            "actions": _parse_snapshot_actions(row[4] or ""),
        }

    return {
        "events": events,
        "snapshots": snapshots,
        "loaded_at_start": loaded_before[0] if loaded_before else None,
    }


def get_snapshot(conn: sqlite3.Connection, snapshot_id: int) -> str | None:
    """Return the stored JSONL text of one schedule snapshot."""
    row = conn.execute(
        "SELECT jsonl FROM schedule_snapshots WHERE id = ?", (snapshot_id,)
    ).fetchone()
    return row[0] if row else None
