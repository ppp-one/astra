"""
Schedule processing utilities for observatory automation.

This module provides functions for reading, parsing, and manipulating observatory
schedules from various file formats. It handles time conversion, schedule compression
for testing purposes, and ensures proper formatting of schedule data for downstream
processing.

The module supports:
- CSV and JSONL schedule file formats
- Time scaling for development and testing
- Automatic time zone conversion to UTC
- Schedule validation and sorting

Typical usage:
    # Process a schedule file
    schedule_df = process_schedule("my_schedule.jsonl")

    # Process with time compression for testing
    compressed_schedule = process_schedule("schedule.jsonl", truncate_factor=25)
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Union

import pandas as pd
import json

from astra import Config

CONFIG = Config()


def update_times(df: pd.DataFrame, time_factor: float) -> pd.DataFrame:
    """
    Update the start and end times to present day factored by the time factor.

    This function scales the time intervals between schedule entries by a given factor
    and shifts all times to start from the current time. This is useful for testing
    schedules by compressing their duration.

    Parameters:
        df (pd.DataFrame): DataFrame containing schedule data with columns:
            ['device_type', 'device_name', 'action_type', 'action_value', 'start_time', 'end_time']
        time_factor (float): Factor by which to divide time intervals. Values > 1 compress
            the schedule, values < 1 expand it. For example, time_factor=25 makes a 25-hour
            schedule run in 1 hour.

    Returns:
        pd.DataFrame: New DataFrame with updated start_time and end_time columns, scaled by
            time_factor and shifted to start from the current time.
    """

    new_rows = []
    prev_start_time = None
    prev_end_time = None
    prev_new_start_time = None
    for i, row in df.iterrows():
        device_name, action_type, action_value, start_time, end_time = row

        se_time_diff = end_time - start_time
        se_time_diff = se_time_diff / time_factor

        new_start_time = datetime.now(UTC)

        if prev_end_time:
            ss_time_diff = start_time - prev_start_time
            ss_time_diff = ss_time_diff / time_factor

            new_start_time = prev_new_start_time + ss_time_diff

        new_end_time = new_start_time + se_time_diff

        new_row = [
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


def process_schedule(
    filename: Union[str, Path],
    truncate_factor: float | None = None,
) -> pd.DataFrame:
    """
    Process a schedule file and return a DataFrame with parsed schedule data.

    Reads a schedule from a CSV file, converts time columns to datetime objects,
    sorts by start time, and optionally applies time truncation for testing.

    Parameters:
        filename (str or Path): Path to the schedule file. Currently supports CSV
            and JSONL formats.
        truncate_factor (float | None, optional): If specified, the schedule is
            truncated by the factor and moved to the current time. This is useful
            for development/testing to compress long schedules. Defaults to None.

    Returns:
        pd.DataFrame: Processed schedule DataFrame with columns:
            - Original columns from the input file
            - start_time : datetime (converted to UTC)
            - end_time : datetime (converted to UTC)
            - completed : bool (added, defaults to False)
            Sorted by start_time in ascending order.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If the file format is not supported (currently only .csv and .jsonl
            are supported).
    """
    schedule_path = Path(filename)

    if schedule_path.exists() is False:
        raise FileNotFoundError(f"File not found: {filename}")

    # 1. read schedule and convert to a DataFrame
    if schedule_path.suffix == ".csv":
        schedule = pd.read_csv(schedule_path)
    elif schedule_path.suffix == ".jsonl":
        data = []
        with open(schedule_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("//"):
                    continue
                obj = json.loads(line)
                data.append(obj)
        schedule = pd.DataFrame(data)
    else:
        raise ValueError(f"Unsupported file format: {schedule_path.suffix}")

    # at this point schedule must be a DataFrame
    schedule["start_time"] = pd.to_datetime(
        schedule.start_time, utc=True, format="mixed"
    )
    schedule["end_time"] = pd.to_datetime(schedule.end_time, utc=True, format="mixed")

    # Sort the schedule by start_time
    schedule = schedule.sort_values(by=["start_time"])

    # for development: Truncate the schedule if self.truncate_factor is specified
    if truncate_factor:
        schedule = update_times(schedule, truncate_factor)

    schedule["completed"] = False

    return schedule
