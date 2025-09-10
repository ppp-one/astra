"""
Unit tests for schedule processing utilities.

This module contains comprehensive tests for the schedule.py module,
covering schedule reading, processing, time scaling, and error handling.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from astra.schedule import process_schedule, update_times


class TestUpdateTimes:
    """Test cases for the update_times function."""

    def test_update_times_basic_functionality(self):
        """Test basic time scaling functionality."""
        # Create test DataFrame with known time intervals
        start_time1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        end_time1 = datetime(2024, 1, 1, 13, 0, 0, tzinfo=UTC)  # 1 hour duration
        start_time2 = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)  # 1 hour gap
        end_time2 = datetime(2024, 1, 1, 15, 30, 0, tzinfo=UTC)  # 1.5 hour duration

        # DataFrame must match the exact column order expected by update_times
        df = pd.DataFrame(
            {
                "device_name": ["device1", "device2"],
                "action_type": ["action1", "action2"],
                "action_value": ["value1", "value2"],
                "start_time": [start_time1, start_time2],
                "end_time": [end_time1, end_time2],
            }
        )

        time_factor = 2.0
        result = update_times(df, time_factor)

        # Check that we have the same number of rows
        assert len(result) == 2

        # Check that columns are preserved
        assert list(result.columns) == list(df.columns)

        # Check that non-time columns are unchanged
        assert result["device_name"].tolist() == ["device1", "device2"]
        assert result["action_type"].tolist() == ["action1", "action2"]
        assert result["action_value"].tolist() == ["value1", "value2"]

        # Check time scaling
        # First entry should start at current time
        first_duration = result.iloc[0]["end_time"] - result.iloc[0]["start_time"]
        expected_first_duration = timedelta(hours=1) / time_factor
        assert (
            abs(
                first_duration.total_seconds() - expected_first_duration.total_seconds()
            )
            < 1
        )

        # Second entry should maintain scaled intervals
        gap_between = result.iloc[1]["start_time"] - result.iloc[0]["start_time"]
        expected_gap = timedelta(hours=2) / time_factor  # 1 hour duration + 1 hour gap
        assert abs(gap_between.total_seconds() - expected_gap.total_seconds()) < 1

        second_duration = result.iloc[1]["end_time"] - result.iloc[1]["start_time"]
        expected_second_duration = timedelta(hours=1.5) / time_factor
        assert (
            abs(
                second_duration.total_seconds()
                - expected_second_duration.total_seconds()
            )
            < 1
        )

    def test_update_times_single_row(self):
        """Test update_times with a single row DataFrame."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)  # 2 hour duration

        # DataFrame must match the exact column order expected by update_times
        df = pd.DataFrame(
            {
                "device_name": ["device1"],
                "action_type": ["action1"],
                "action_value": ["value1"],
                "start_time": [start_time],
                "end_time": [end_time],
            }
        )

        time_factor = 4.0
        result = update_times(df, time_factor)

        assert len(result) == 1

        # Duration should be scaled
        duration = result.iloc[0]["end_time"] - result.iloc[0]["start_time"]
        expected_duration = timedelta(hours=2) / time_factor
        assert abs(duration.total_seconds() - expected_duration.total_seconds()) < 1

        # Start time should be approximately now
        time_diff = abs(
            (result.iloc[0]["start_time"] - datetime.now(UTC)).total_seconds()
        )
        assert time_diff < 5  # Allow 5 seconds tolerance

    def test_update_times_compression_factor(self):
        """Test different compression factors."""
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        end_time = datetime(2024, 1, 1, 13, 0, 0, tzinfo=UTC)

        # DataFrame must match the exact column order expected by update_times
        df = pd.DataFrame(
            {
                "device_name": ["device1"],
                "action_type": ["action1"],
                "action_value": ["value1"],
                "start_time": [start_time],
                "end_time": [end_time],
            }
        )

        # Test compression factor of 60 (1 hour becomes 1 minute)
        time_factor = 60.0
        result = update_times(df, time_factor)

        duration = result.iloc[0]["end_time"] - result.iloc[0]["start_time"]
        expected_duration = timedelta(minutes=1)
        assert abs(duration.total_seconds() - expected_duration.total_seconds()) < 1

    def test_update_times_empty_dataframe(self):
        """Test update_times with an empty DataFrame."""
        df = pd.DataFrame(
            columns=[
                "device_name",
                "action_type",
                "action_value",
                "start_time",
                "end_time",
            ]
        )
        result = update_times(df, 2.0)

        assert len(result) == 0
        assert list(result.columns) == list(df.columns)


class TestProcessSchedule:
    """Test cases for the process_schedule function."""

    def create_test_jsonl(self, tmp_path, data):
        """Helper method to create a test JSONL file."""
        jsonl_file = tmp_path / "test_schedule.jsonl"
        with open(jsonl_file, "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")
        return jsonl_file

    def test_process_schedule_jsonl_basic(self, tmp_path):
        """Test basic JSONL processing functionality."""
        data = [
            {
                "device_type": "Camera",
                "device_name": "camera1",
                "action_type": "open",
                "action_value": "{}",
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T12:30:00Z",
            },
            {
                "device_type": "Camera",
                "device_name": "camera2",
                "action_type": "close",
                "action_value": "{}",
                "start_time": "2024-01-01T13:00:00Z",
                "end_time": "2024-01-01T13:30:00Z",
            },
        ]

        jsonl_file = self.create_test_jsonl(tmp_path, data)
        result = process_schedule(jsonl_file)

        # Check basic structure
        assert len(result) == 2
        assert "completed" in result.columns
        assert not result["completed"].all()

        # Check datetime conversion
        assert isinstance(result.iloc[0]["start_time"], pd.Timestamp)
        assert isinstance(result.iloc[0]["end_time"], pd.Timestamp)

        # Check sorting by start_time
        assert result.iloc[0]["start_time"] <= result.iloc[1]["start_time"]

        # Check timezone conversion to UTC
        assert result.iloc[0]["start_time"].tz == UTC
        assert result.iloc[0]["end_time"].tz == UTC

    def test_process_schedule_jsonl_with_comments(self, tmp_path):
        """Test JSONL processing with comments and empty lines."""
        jsonl_file = tmp_path / "test_schedule.jsonl"
        with open(jsonl_file, "w") as f:
            f.write("// This is a comment\n")
            f.write("\n")  # Empty line
            f.write(
                json.dumps(
                    {
                        "device_type": "Camera",
                        "device_name": "camera1",
                        "action_type": "open",
                        "action_value": "{}",
                        "start_time": "2024-01-01T12:00:00Z",
                        "end_time": "2024-01-01T12:30:00Z",
                    }
                )
                + "\n"
            )
            f.write("// Another comment\n")
            f.write(
                json.dumps(
                    {
                        "device_type": "Camera",
                        "device_name": "camera2",
                        "action_type": "close",
                        "action_value": "{}",
                        "start_time": "2024-01-01T13:00:00Z",
                        "end_time": "2024-01-01T13:30:00Z",
                    }
                )
                + "\n"
            )

        result = process_schedule(jsonl_file)
        assert len(result) == 2  # Comments and empty lines should be ignored

    def test_process_schedule_sorting(self, tmp_path):
        """Test that schedule is properly sorted by start_time."""
        data = [
            {
                "device_name": "device3",
                "action_type": "action3",
                "action_value": "value3",
                "start_time": "2024-01-01T15:00:00Z",
                "end_time": "2024-01-01T15:30:00Z",
            },
            {
                "device_name": "device1",
                "action_type": "action1",
                "action_value": "value1",
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T12:30:00Z",
            },
            {
                "device_name": "device2",
                "action_type": "action2",
                "action_value": "value2",
                "start_time": "2024-01-01T13:00:00Z",
                "end_time": "2024-01-01T13:30:00Z",
            },
        ]

        jsonl_file = self.create_test_jsonl(tmp_path, data)
        result = process_schedule(jsonl_file)

        # Check that sorting worked
        expected_order = ["device1", "device2", "device3"]
        assert result["device_name"].tolist() == expected_order

        # Verify times are in ascending order
        for i in range(len(result) - 1):
            assert result.iloc[i]["start_time"] <= result.iloc[i + 1]["start_time"]

    def test_process_schedule_with_truncate_factor(self, tmp_path):
        """Test schedule processing with time truncation."""
        data = [
            {
                "device_name": "device1",
                "action_type": "action1",
                "action_value": "value1",
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z",
            },
            {
                "device_name": "device2",
                "action_type": "action2",
                "action_value": "value2",
                "start_time": "2024-01-01T14:00:00Z",
                "end_time": "2024-01-01T15:00:00Z",
            },
        ]

        jsonl_file = self.create_test_jsonl(tmp_path, data)
        truncate_factor = 60.0  # 1 hour becomes 1 minute
        result = process_schedule(jsonl_file, truncate_factor=truncate_factor)

        # Check that truncation was applied
        first_duration = result.iloc[0]["end_time"] - result.iloc[0]["start_time"]
        assert abs(first_duration.total_seconds() - 60) < 5  # Should be ~1 minute

        # Start time should be approximately now
        time_diff = abs(
            (result.iloc[0]["start_time"] - datetime.now(UTC)).total_seconds()
        )
        assert time_diff < 10  # Allow 10 seconds tolerance

    def test_process_schedule_file_not_found(self):
        """Test FileNotFoundError when file doesn't exist."""
        with pytest.raises(FileNotFoundError, match="File not found"):
            process_schedule("nonexistent_file.jsonl")

    def test_process_schedule_unsupported_format(self, tmp_path):
        """Test ValueError for unsupported file formats."""
        unsupported_file = tmp_path / "test.txt"
        unsupported_file.write_text("some content")

        with pytest.raises(ValueError, match="Unsupported file format"):
            process_schedule(unsupported_file)

    def test_process_schedule_pathlib_path(self, tmp_path):
        """Test that function accepts Path objects."""
        data = [
            {
                "device_name": "device1",
                "action_type": "action1",
                "action_value": "value1",
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T12:30:00Z",
            }
        ]

        jsonl_file = self.create_test_jsonl(tmp_path, data)
        result = process_schedule(Path(jsonl_file))  # Pass as Path object

        assert len(result) == 1
        assert "completed" in result.columns

    def test_process_schedule_various_datetime_formats(self, tmp_path):
        """Test processing of various datetime formats."""
        data = [
            {
                "device_name": "device1",
                "action_type": "action1",
                "action_value": "value1",
                "start_time": "2024-01-01T12:00:00Z",  # ISO with Z
                "end_time": "2024-01-01T12:30:00Z",
            },
            {
                "device_name": "device2",
                "action_type": "action2",
                "action_value": "value2",
                "start_time": "2024-01-01 13:00:00+00:00",  # ISO with timezone
                "end_time": "2024-01-01 13:30:00+00:00",
            },
            {
                "device_name": "device3",
                "action_type": "action3",
                "action_value": "value3",
                "start_time": "2024-01-01 14:00:00",  # ISO without timezone
                "end_time": "2024-01-01 14:30:00",
            },
        ]

        jsonl_file = self.create_test_jsonl(tmp_path, data)
        result = process_schedule(jsonl_file)

        # All should be converted to UTC timezone-aware timestamps
        for i in range(len(result)):
            assert result.iloc[i]["start_time"].tz == UTC
            assert result.iloc[i]["end_time"].tz == UTC

    # def test_process_schedule_empty_jsonl(self, tmp_path):
    #     """Test processing of empty JSONL file."""
    #     jsonl_file = tmp_path / "empty.jsonl"
    #     jsonl_file.write_text('')

    #     result = process_schedule(jsonl_file)
    #     assert len(result) == 0
    #     assert 'completed' in result.columns

    def test_process_schedule_complex_action_values(self, tmp_path):
        """Test processing of complex action values."""
        data = [
            {
                "device_type": "Camera",
                "device_name": "camera1",
                "action_type": "object",
                "action_value": "{'object': 'Sp0711-3824', 'filter': 'I+z', 'ra': 107.7545375, 'dec': -38.41298694444444, 'exptime': 13, 'guiding': True, 'pointing': False}",
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T12:30:00Z",
            },
            {
                "device_type": "Camera",
                "device_name": "camera1",
                "action_type": "flats",
                "action_value": "{'filter': ['I+z'], 'n': [10]}",
                "start_time": "2024-01-01T13:00:00Z",
                "end_time": "2024-01-01T13:30:00Z",
            },
        ]

        jsonl_file = self.create_test_jsonl(tmp_path, data)
        result = process_schedule(jsonl_file)

        assert len(result) == 2
        assert result.iloc[0]["action_value"] == data[0]["action_value"]
        assert result.iloc[1]["action_value"] == data[1]["action_value"]

    def test_process_schedule_maintains_column_order(self, tmp_path):
        """Test that column order is maintained during processing."""
        data = [
            {
                "device_type": "Camera",
                "device_name": "camera1",
                "action_type": "open",
                "action_value": "{}",
                "custom_column": "custom_value",
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T12:30:00Z",
            }
        ]

        jsonl_file = self.create_test_jsonl(tmp_path, data)
        result = process_schedule(jsonl_file)

        # Check that custom columns are preserved
        assert "custom_column" in result.columns
        assert result["custom_column"].iloc[0] == "custom_value"

        # Check that completed column is added
        assert "completed" in result.columns
        assert not result["completed"].iloc[0]


class TestIntegration:
    """Integration tests combining multiple functions."""

    def create_test_jsonl(self, tmp_path, data):
        """Helper method to create a test JSONL file."""
        jsonl_file = tmp_path / "test_schedule.jsonl"
        with open(jsonl_file, "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")
        return jsonl_file

    def test_full_workflow_jsonl_with_truncation(self, tmp_path):
        """Test complete workflow: JSONL reading with time truncation."""
        # Create realistic test data similar to the example CSV
        data = [
            {
                # "device_type": "Camera",
                "device_name": "camera_Callisto",
                "action_type": "open",
                "action_value": "{}",
                "start_time": "2024-01-11T23:31:40.915Z",
                "end_time": "2024-01-12T00:16:20.020Z",
            },
            {
                # "device_type": "Camera",
                "device_name": "camera_Callisto",
                "action_type": "object",
                "action_value": "{'object': 'test_object', 'filter': 'I+z', 'ra': 107.7545375, 'dec': -38.41298694444444, 'exptime': 13, 'guiding': True, 'pointing': False}",
                "start_time": "2024-01-12T00:16:20.020Z",
                "end_time": "2024-01-12T04:49:20.020Z",
            },
            {
                # "device_type": "Camera",
                "device_name": "camera_Callisto",
                "action_type": "close",
                "action_value": "{}",
                "start_time": "2024-01-12T10:07:40.253Z",
                "end_time": "2024-01-12T10:12:40.253Z",
            },
        ]

        jsonl_file = self.create_test_jsonl(tmp_path, data)

        # Process with truncation
        truncate_factor = 100.0  # Heavy compression for testing
        result = process_schedule(jsonl_file, truncate_factor=truncate_factor)

        # Verify structure
        assert len(result) == 3
        assert list(result["action_type"]) == ["open", "object", "close"]

        # Verify time compression worked
        total_compressed_duration = (
            result.iloc[-1]["end_time"] - result.iloc[0]["start_time"]
        )
        assert (
            total_compressed_duration.total_seconds() < 3600
        )  # Should be less than 1 hour

        # Verify sorting
        for i in range(len(result) - 1):
            assert result.iloc[i]["start_time"] <= result.iloc[i + 1]["start_time"]

    def test_jsonl_workflow_with_comments_and_truncation(self, tmp_path):
        """Test JSONL workflow with comments and truncation."""
        jsonl_file = tmp_path / "test_with_comments.jsonl"

        with open(jsonl_file, "w") as f:
            f.write("// Observatory schedule for testing\n")
            f.write("\n")
            f.write(
                json.dumps(
                    {
                        # "device_type": "Camera",
                        "device_name": "camera1",
                        "action_type": "open",
                        "action_value": "{}",
                        "start_time": "2024-01-01T10:00:00Z",
                        "end_time": "2024-01-01T10:05:00Z",
                    }
                )
                + "\n"
            )
            f.write("// Start observation sequence\n")
            f.write(
                json.dumps(
                    {
                        # "device_type": "Camera",
                        "device_name": "camera1",
                        "action_type": "object",
                        "action_value": "{'object': 'test', 'exptime': 60}",
                        "start_time": "2024-01-01T10:05:00Z",
                        "end_time": "2024-01-01T12:00:00Z",
                    }
                )
                + "\n"
            )
            f.write("\n")
            f.write("// End sequence\n")
            f.write(
                json.dumps(
                    {
                        # "device_type": "Camera",
                        "device_name": "camera1",
                        "action_type": "close",
                        "action_value": "{}",
                        "start_time": "2024-01-01T12:00:00Z",
                        "end_time": "2024-01-01T12:05:00Z",
                    }
                )
                + "\n"
            )

        result = process_schedule(jsonl_file, truncate_factor=30.0)

        assert len(result) == 3
        assert result["action_type"].tolist() == ["open", "object", "close"]

        # Check that times are compressed and moved to present
        first_start = result.iloc[0]["start_time"]
        time_diff = abs((first_start - datetime.now(UTC)).total_seconds())
        assert time_diff < 30  # Should start within 30 seconds of now
