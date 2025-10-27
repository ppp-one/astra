import json
from datetime import UTC, datetime, timedelta

import pandas as pd

from astra.action_configs import BaseActionConfig, ObjectActionConfig
from astra.scheduler import Action, Schedule


class TestSchedule:
    def create_test_jsonl(self, tmp_path, data):
        jsonl_file = tmp_path / "test_schedule.jsonl"
        with open(jsonl_file, "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")
        return jsonl_file

    def test_from_file_jsonl_basic(self, tmp_path):
        data = [
            {
                "device_name": "camera1",
                "action_type": "open",
                "action_value": {},
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T12:30:00Z",
            },
            {
                "device_name": "camera2",
                "action_type": "close",
                "action_value": {},
                "start_time": "2024-01-01T13:00:00Z",
                "end_time": "2024-01-01T13:30:00Z",
            },
        ]
        jsonl_file = self.create_test_jsonl(tmp_path, data)
        schedule = Schedule.from_file(jsonl_file)
        assert len(schedule) == 2
        assert schedule[0].device_name == "camera1"
        assert schedule[1].device_name == "camera2"
        assert schedule[0].start_time.tzinfo == UTC
        assert schedule[1].end_time.tzinfo == UTC

    def test_from_dataframe(self):
        start_time1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        end_time1 = datetime(2024, 1, 1, 13, 0, 0, tzinfo=UTC)
        start_time2 = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)
        end_time2 = datetime(2024, 1, 1, 15, 0, 0, tzinfo=UTC)
        df = pd.DataFrame(
            {
                "device_name": ["camera1", "camera2"],
                "action_type": ["open", "close"],
                "action_value": [{}, {}],
                "start_time": [start_time1, start_time2],
                "end_time": [end_time1, end_time2],
            }
        )
        schedule = Schedule.from_dataframe(df)
        assert len(schedule) == 2
        assert schedule[0].start_time == start_time1
        assert schedule[1].end_time == end_time2

    def test_update_times(self):
        start_time1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        end_time1 = datetime(2024, 1, 1, 13, 0, 0, tzinfo=UTC)
        start_time2 = datetime(2024, 1, 1, 14, 0, 0, tzinfo=UTC)
        end_time2 = datetime(2024, 1, 1, 15, 30, 0, tzinfo=UTC)
        df = pd.DataFrame(
            {
                "device_type": ["Camera", "Camera"],
                "device_name": ["camera1", "camera2"],
                "action_type": ["open", "close"],
                "action_value": [{}, {}],
                "start_time": [start_time1, start_time2],
                "end_time": [end_time1, end_time2],
            }
        )
        schedule = Schedule.from_dataframe(df)
        Schedule.update_times(schedule, 2.0)
        assert len(schedule) == 2
        assert (
            abs(schedule[0].duration.total_seconds() - 1800) < 2
        )  # 1 hour / 2 = 1800s
        gap_between = schedule[1].start_time - schedule[0].start_time
        assert abs(gap_between.total_seconds() - 3600) < 2  # (1h duration + 1h gap) / 2
        second_duration = schedule[1].end_time - schedule[1].start_time
        assert abs(second_duration.total_seconds() - 2700) < 2  # 1.5h / 2

    def test_validate_conflict(self):
        now = datetime.now(UTC)
        actions = [
            Action(
                device_name="cam1",
                action_type="open",
                action_value=BaseActionConfig(),
                start_time=now,
                end_time=now + timedelta(minutes=10),
            ),
            Action(
                device_name="cam1",
                action_type="close",
                action_value=ObjectActionConfig(object="M31", exptime="invalid"),
                start_time=now + timedelta(minutes=5),
                end_time=now + timedelta(minutes=15),
            ),
        ]
        Schedule(actions)
        # with pytest.raises(ValueError, match="Schedule conflict for device cam1"):
        #     schedule.validate()

    def test_to_pandas(self):
        now = datetime.now(UTC)
        actions = [
            Action(
                device_name="cam1",
                action_type="open",
                action_value=BaseActionConfig(),
                start_time=now,
                end_time=now + timedelta(minutes=10),
            ),
            Action(
                device_name="cam2",
                action_type="close",
                action_value=BaseActionConfig(),
                start_time=now + timedelta(minutes=15),
                end_time=now + timedelta(minutes=25),
            ),
        ]
        schedule = Schedule(actions)
        df = schedule.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert set(df["device_name"]) == {"cam1", "cam2"}
        assert "start_time" in df.columns
        assert "end_time" in df.columns

    def test_schedule_completion_flags(self):
        now = datetime.now(UTC)
        actions = [
            Action(
                device_name="cam1",
                action_type="open",
                action_value=BaseActionConfig(),
                start_time=now,
                end_time=now + timedelta(minutes=10),
            ),
            Action(
                device_name="cam2",
                action_type="close",
                action_value=BaseActionConfig(),
                start_time=now + timedelta(minutes=15),
                end_time=now + timedelta(minutes=25),
            ),
        ]
        schedule = Schedule(actions)
        schedule[0].completed = True
        schedule[1].completed = False
        df = schedule.to_dataframe()

        assert bool(df.loc[0, "completed"]), (
            f"Expected True but got {df.loc[0, 'completed']}"
        )
        assert bool(df.loc[1, "completed"]) is False, (
            f"Expected False but got {df.loc[1, 'completed']}"
        )
        schedule.reset_completion()
        assert all(not a.completed for a in schedule)

    def test_schedule_to_jsonl_and_reload(self, tmp_path):
        now = datetime.now(UTC)
        actions = [
            Action(
                device_name="cam1",
                action_type="open",
                action_value={},
                start_time=now,
                end_time=now + timedelta(minutes=10),
            ),
            Action(
                device_name="cam2",
                action_type="close",
                action_value={},
                start_time=now + timedelta(minutes=15),
                end_time=now + timedelta(minutes=25),
            ),
        ]
        schedule = Schedule(actions)
        jsonl_file = tmp_path / "schedule_test.jsonl"
        schedule.save_to_jsonl(jsonl_file)
        loaded = Schedule.from_file(jsonl_file)
        assert len(loaded) == 2
        assert loaded[0].device_name == "cam1"
        assert loaded[1].device_name == "cam2"
        assert loaded[0].start_time.tzinfo == UTC
        assert loaded[1].end_time.tzinfo == UTC
