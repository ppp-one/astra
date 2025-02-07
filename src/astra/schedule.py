import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from astra import Config

CONFIG = Config()


def update_times(df: pd.DataFrame, time_factor: float) -> pd.DataFrame:
    """Update the start and end times to present day factored by the time
    factor."""

    new_rows = []
    prev_start_time = None
    prev_end_time = None
    prev_new_start_time = None
    for i, row in df.iterrows():
        device_type, device_name, action_type, action_value, start_time, end_time = row

        se_time_diff = end_time - start_time
        se_time_diff = se_time_diff / time_factor

        new_start_time = datetime.now(UTC)

        if prev_end_time:
            ss_time_diff = start_time - prev_start_time
            ss_time_diff = ss_time_diff / time_factor

            new_start_time = prev_new_start_time + ss_time_diff

        new_end_time = new_start_time + se_time_diff

        new_row = [
            device_type,
            device_name,
            action_type,
            action_value,
            new_start_time,
            new_end_time,
        ]
        new_rows.append(new_row)

        prev_start_time = start_time
        prev_end_time = end_time

        prev_new_start_time = new_start_time

    return pd.DataFrame(new_rows, columns=df.columns)


def process_schedule(filename, truncate=False) -> pd.DataFrame:
    """ """
    schedule_path = Path(filename)

    if schedule_path.exists() is False:
        raise FileNotFoundError(f"File not found: {filename}")

    # 1. read schedule and convert to a DataFrame
    if schedule_path.suffix == ".csv":
        schedule = pd.read_csv(schedule_path)
    else:
        raise ValueError(f"Unsupported file format: {schedule_path.suffix}")

    # at this point schedule must be a DataFrame
    schedule["start_time"] = pd.to_datetime(schedule.start_time, utc=True)
    schedule["end_time"] = pd.to_datetime(schedule.end_time, utc=True)

    # Sort the schedule by start_time
    schedule = schedule.sort_values(by=["start_time"])

    # for development: Truncate the schedule if self.truncate_schedule is True
    if truncate:
        schedule = update_times(schedule, 10)

    schedule["completed"] = False

    return schedule
