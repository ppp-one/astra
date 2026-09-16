import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from astra import timelapse
from astra.timelapse import (
    TimelapseRecorder,
    TimelapseSettings,
    TimelapseSource,
    build_sources,
    make_camera_id,
)

JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 16
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16
FRAME_TIME = datetime(2026, 9, 14, 12, 0, 0, tzinfo=UTC)


def _recorder(tmp_path, *sources):
    return TimelapseRecorder(tmp_path / "timelapse", list(sources))


def _file_source(path, retention_hours=24.0):
    return TimelapseSource(
        id="allsky-sky",
        name="Sky",
        kind="allsky",
        index=0,
        path=path,
        settings=TimelapseSettings(10.0, retention_hours),
    )


def _url_source(url):
    return TimelapseSource(
        id="webcam-outside", name="Outside", kind="webcam", index=0, url=url
    )


def _write_image(path, data, when):
    path.write_bytes(data)
    os.utime(path, (when.timestamp(), when.timestamp()))


def _frame_files(recorder, camera_id):
    return [frame["file"] for frame in recorder.list_frames(camera_id)]


@pytest.mark.parametrize("config", [None, False])
def test_settings_disabled(config):
    assert TimelapseSettings.from_config(config) is None


def test_settings_defaults_and_values():
    assert TimelapseSettings.from_config(True) == TimelapseSettings(10.0, 24.0)
    assert TimelapseSettings.from_config({}) == TimelapseSettings(10.0, 24.0)
    assert TimelapseSettings.from_config(
        {"interval": 5, "retention": 48}
    ) == TimelapseSettings(5.0, 48.0)


@pytest.mark.parametrize(
    "config",
    [
        {"interval": 0},
        {"retention": -1},
        {"interval": "fast"},
        {"interval": None},
        "yes",
    ],
)
def test_settings_rejects_invalid(config):
    with pytest.raises((TypeError, ValueError)):
        TimelapseSettings.from_config(config)


def test_make_camera_id():
    assert make_camera_id("allsky", "East Sky!") == "allsky-east-sky"
    assert make_camera_id("webcam", "***") == "webcam-camera"


def test_build_sources_uses_per_camera_settings():
    sources = build_sources(
        [
            {
                "name": "East Sky",
                "path": "/data/east.jpg",
                "timelapse": {"interval": 5},
            },
            {"name": "West Sky", "path": "/data/west.jpg"},  # no timelapse key
        ],
        [
            {
                "name": "Inside",
                "url": "http://cam/inside",
                "timelapse": True,
            },  # no snapshot
            {
                "name": "Outside",
                "url": "http://cam/outside",
                "snapshot_url": "http://cam/snap.jpg",
                "timelapse": {"interval": 30, "retention": 48},
            },
            {"name": "Door", "snapshot_url": "http://cam/door.jpg"},  # no timelapse key
            {"name": "Bad", "snapshot_url": "http://cam/bad.jpg", "timelapse": "often"},
            "not a dict",
        ],
    )

    assert [s.id for s in sources] == ["allsky-east-sky", "webcam-outside"]
    assert sources[0].path == Path("/data/east.jpg")
    assert sources[0].settings == TimelapseSettings(5.0, 24.0)
    assert sources[1].url == "http://cam/snap.jpg"
    assert sources[1].index == 1
    assert sources[1].settings == TimelapseSettings(30.0, 48.0)


def test_build_sources_makes_ids_unique():
    sources = build_sources(
        [
            {"name": "Sky", "path": "/a.jpg", "timelapse": True},
            {"name": "sky", "path": "/b.jpg", "timelapse": True},
        ],
        [],
    )
    assert [s.id for s in sources] == ["allsky-sky", "allsky-sky-2"]


def test_file_source_saves_frame_only_when_file_changes(tmp_path):
    image = tmp_path / "allsky.jpg"
    _write_image(image, JPEG, FRAME_TIME)
    recorder = _recorder(tmp_path, _file_source(image))
    now = FRAME_TIME + timedelta(hours=1)

    recorder.capture_all(now)
    recorder.capture_all(now)
    assert _frame_files(recorder, "allsky-sky") == ["20260914T120000Z.jpg"]

    _write_image(image, JPEG + b"new", FRAME_TIME + timedelta(minutes=10))
    recorder.capture_all(now)

    frames = recorder.list_frames("allsky-sky")
    assert [f["file"] for f in frames] == [
        "20260914T120000Z.jpg",
        "20260914T121000Z.jpg",
    ]
    assert frames[0]["time"] == "2026-09-14T12:00:00+00:00"


def test_file_older_than_retention_is_not_saved(tmp_path, caplog):
    image = tmp_path / "allsky.jpg"
    _write_image(image, JPEG, FRAME_TIME)
    recorder = _recorder(tmp_path, _file_source(image, retention_hours=24.0))

    recorder.capture_all(FRAME_TIME + timedelta(hours=25))
    recorder.capture_all(FRAME_TIME + timedelta(hours=25))

    assert _frame_files(recorder, "allsky-sky") == []
    stale_warnings = [r for r in caplog.records if "older than" in r.getMessage()]
    assert len(stale_warnings) == 1  # reported once, not on every capture


def test_invalid_or_missing_image_is_not_saved(tmp_path):
    text_file = tmp_path / "allsky.jpg"
    _write_image(text_file, b"<html>error</html>", FRAME_TIME)
    recorder = _recorder(tmp_path, _file_source(text_file))

    recorder.capture_all(FRAME_TIME)
    assert _frame_files(recorder, "allsky-sky") == []

    missing = _recorder(tmp_path, _file_source(tmp_path / "missing.jpg"))
    missing.capture_all(FRAME_TIME)
    assert _frame_files(missing, "allsky-sky") == []


def test_url_source_saves_frame_at_capture_time(tmp_path):
    snapshot = tmp_path / "snapshot"
    snapshot.write_bytes(PNG)
    recorder = _recorder(tmp_path, _url_source(snapshot.as_uri()))

    recorder.capture_all(FRAME_TIME)

    assert _frame_files(recorder, "webcam-outside") == ["20260914T120000Z.png"]


def test_url_source_rejects_response_over_size_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(timelapse, "MAX_SNAPSHOT_BYTES", len(PNG) - 1)
    snapshot = tmp_path / "snapshot"
    snapshot.write_bytes(PNG)
    recorder = _recorder(tmp_path, _url_source(snapshot.as_uri()))

    recorder.capture_all(FRAME_TIME)

    assert _frame_files(recorder, "webcam-outside") == []
    assert "video stream" in recorder.status("webcam-outside")["last_error"]


def test_run_captures_each_camera_at_start(tmp_path):
    image = tmp_path / "allsky.jpg"
    image.write_bytes(JPEG)
    snapshot = tmp_path / "snapshot"
    snapshot.write_bytes(PNG)
    recorder = _recorder(tmp_path, _file_source(image), _url_source(snapshot.as_uri()))

    async def run_briefly():
        task = asyncio.create_task(recorder.run())
        await asyncio.sleep(0.5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run_briefly())

    assert len(_frame_files(recorder, "allsky-sky")) == 1
    assert len(_frame_files(recorder, "webcam-outside")) == 1


def test_prune_removes_expired_frames(tmp_path):
    recorder = _recorder(tmp_path, _file_source(tmp_path / "unused.jpg"))
    folder = tmp_path / "timelapse" / "allsky-sky"
    folder.mkdir(parents=True)
    (folder / "20260913T110000Z.jpg").write_bytes(JPEG)  # 25 h old
    (folder / "20260914T110000Z.jpg").write_bytes(JPEG)  # 1 h old
    (folder / "notes.txt").write_text("keep")

    recorder.prune("allsky-sky", FRAME_TIME)

    assert sorted(p.name for p in folder.iterdir()) == [
        "20260914T110000Z.jpg",
        "notes.txt",
    ]


def test_frame_path_only_serves_frames(tmp_path):
    recorder = _recorder(tmp_path, _file_source(tmp_path / "unused.jpg"))
    folder = tmp_path / "timelapse" / "allsky-sky"
    folder.mkdir(parents=True)
    (folder / "20260914T120000Z.jpg").write_bytes(JPEG)
    (tmp_path / "timelapse" / "secret.jpg").write_bytes(JPEG)

    assert (
        recorder.frame_path("allsky-sky", "20260914T120000Z.jpg")
        == folder / "20260914T120000Z.jpg"
    )
    assert recorder.frame_path("allsky-sky", "../secret.jpg") is None
    assert recorder.frame_path("allsky-sky", "20260914T130000Z.jpg") is None
    assert recorder.frame_path("unknown", "20260914T120000Z.jpg") is None
    assert recorder.list_frames("unknown") == []


def test_status_keeps_last_error_until_a_frame_is_saved(tmp_path):
    image = tmp_path / "allsky.jpg"
    _write_image(image, JPEG, FRAME_TIME)
    recorder = _recorder(tmp_path, _file_source(image, retention_hours=24.0))
    now = FRAME_TIME + timedelta(hours=25)

    recorder.capture_all(now)
    recorder.capture_all(now)  # the file did not change, so the error must stay
    status = recorder.status("allsky-sky")
    assert status["frames"] == []
    assert "older than the 24 h retention period" in status["last_error"]

    _write_image(image, JPEG, now)
    recorder.capture_all(now)
    status = recorder.status("allsky-sky")
    assert status["last_error"] is None
    assert len(status["frames"]) == 1
