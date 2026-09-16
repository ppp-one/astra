"""Time-lapse recording for all-sky cameras and webcams.

Each camera that has a ``timelapse`` key in its config gets a background loop.
The loop saves one frame at the camera's interval and deletes frames older than
the camera's retention period. All-sky cameras are read from the local image
file that other software overwrites. Webcams are read from their
``snapshot_url``, which must return a JPEG or PNG image.

Frames are stored as ``<root>/<camera id>/<UTC timestamp>.<jpg|png>``.
"""

import asyncio
import logging
import re
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http.client import HTTPException
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_MINUTES = 10.0
DEFAULT_RETENTION_HOURS = 24.0
FETCH_TIMEOUT_SECONDS = 10
# Larger snapshot responses are rejected, so a video stream URL cannot fill memory
MAX_SNAPSHOT_BYTES = 20 * 1024 * 1024

TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"
_FRAME_NAME = re.compile(r"^(\d{8}T\d{6}Z)\.(jpg|png)$")


@dataclass
class TimelapseSettings:
    """How often to save frames and how long to keep them."""

    interval_minutes: float = DEFAULT_INTERVAL_MINUTES
    retention_hours: float = DEFAULT_RETENTION_HOURS

    @classmethod
    def from_config(cls, config: object) -> "TimelapseSettings | None":
        """Parse a camera's ``timelapse`` config value.

        Args:
            config (object): ``None`` or ``False`` to disable, ``True`` for the
                defaults, or a dict with optional ``interval`` (minutes) and
                ``retention`` (hours).

        Returns:
            TimelapseSettings | None: Settings, or None if time-lapse is disabled.

        Raises:
            TypeError: If the config is not None, a bool, or a dict.
            ValueError: If a value is not a positive number.
        """
        if config is None or config is False:
            return None
        if config is True:
            return cls()
        if not isinstance(config, dict):
            raise TypeError(f"expected a mapping, got {config!r}")

        try:
            settings = cls(
                interval_minutes=float(
                    config.get("interval", DEFAULT_INTERVAL_MINUTES)
                ),
                retention_hours=float(config.get("retention", DEFAULT_RETENTION_HOURS)),
            )
        except (TypeError, ValueError) as e:
            raise ValueError(f"interval and retention must be numbers: {e}") from e

        if settings.interval_minutes <= 0 or settings.retention_hours <= 0:
            raise ValueError("interval and retention must be positive")
        return settings


@dataclass
class TimelapseSource:
    """A camera to record.

    Attributes:
        id (str): URL-safe identifier, also used as the folder name.
        name (str): Display name from the config.
        kind (str): ``"allsky"`` or ``"webcam"``.
        index (int): Position of the camera in its config list.
        path (Path | None): Local image file (all-sky cameras).
        url (str | None): Snapshot URL (webcams).
        settings (TimelapseSettings): Interval and retention for this camera.
        last_error (str | None): Why the last capture failed, or None after a
            frame is saved.
    """

    id: str
    name: str
    kind: str
    index: int
    path: Path | None = None
    url: str | None = None
    settings: TimelapseSettings = field(default_factory=TimelapseSettings)
    last_error: str | None = field(default=None, compare=False)
    _last_mtime: float | None = field(default=None, repr=False, compare=False)


def make_camera_id(kind: str, name: str) -> str:
    """Build a URL-safe camera id, for example ``allsky-east-sky``.

    Args:
        kind (str): ``"allsky"`` or ``"webcam"``.
        name (str): Camera name from the config.

    Returns:
        str: The camera id.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "camera"
    return f"{kind}-{slug}"


def _feed_settings(feed: dict, label: str) -> TimelapseSettings | None:
    """Parse a feed's ``timelapse`` key. Invalid values are logged and disable it."""
    try:
        return TimelapseSettings.from_config(feed.get("timelapse"))
    except (TypeError, ValueError) as e:
        logger.warning(
            f"Invalid timelapse config for {label}, time-lapse disabled: {e}"
        )
        return None


def build_sources(allsky_feeds: list, webcam_feeds: list) -> list[TimelapseSource]:
    """Create time-lapse sources for the cameras that have a ``timelapse`` key.

    All-sky feeds also need a ``path``. Webcam feeds also need a
    ``snapshot_url``. Feeds without a ``timelapse`` key are skipped.

    Args:
        allsky_feeds (list): List of ``{name, path, timelapse}`` dicts.
        webcam_feeds (list): List of ``{name, url, snapshot_url, timelapse}`` dicts.

    Returns:
        list[TimelapseSource]: One source per recorded camera, with unique ids.
    """
    sources: list[TimelapseSource] = []
    used_ids: set[str] = set()

    def unique_id(kind: str, name: str) -> str:
        base = make_camera_id(kind, name)
        camera_id = base
        suffix = 2
        while camera_id in used_ids:
            camera_id = f"{base}-{suffix}"
            suffix += 1
        used_ids.add(camera_id)
        return camera_id

    # (kind, default name, feeds, key that holds the image source)
    cameras = (
        ("allsky", "All-Sky", allsky_feeds, "path"),
        ("webcam", "Webcam", webcam_feeds, "snapshot_url"),
    )
    for kind, default_name, feeds, source_key in cameras:
        for index, feed in enumerate(feeds):
            if not isinstance(feed, dict):
                continue
            name = str(feed.get("name", default_name))
            settings = _feed_settings(feed, f"{kind} camera '{name}'")
            if settings is None:
                continue
            location = feed.get(source_key)
            if not location:
                logger.warning(
                    f"{kind} camera '{name}' has timelapse set, but no {source_key}"
                )
                continue
            sources.append(
                TimelapseSource(
                    id=unique_id(kind, name),
                    name=name,
                    kind=kind,
                    index=index,
                    path=Path(location) if kind == "allsky" else None,
                    url=str(location) if kind == "webcam" else None,
                    settings=settings,
                )
            )

    return sources


def _image_suffix(data: bytes) -> str | None:
    """Return the file suffix for JPEG or PNG data, or None for other data."""
    if data.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    return None


def _parse_frame_time(filename: str) -> datetime | None:
    """Return the UTC time encoded in a frame file name, or None if invalid."""
    match = _FRAME_NAME.match(filename)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1), TIMESTAMP_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None


class TimelapseRecorder:
    """Save camera frames at each camera's interval and delete expired frames.

    Args:
        root (Path): Folder that holds one subfolder per camera.
        sources (list[TimelapseSource]): Cameras to record.
    """

    def __init__(self, root: Path, sources: list[TimelapseSource]) -> None:
        self.root = Path(root)
        self.sources = {source.id: source for source in sources}

    async def run(self) -> None:
        """Record all cameras until the task is cancelled."""
        await asyncio.gather(
            *(self._run_source(source) for source in self.sources.values())
        )

    async def _run_source(self, source: TimelapseSource) -> None:
        """Record one camera at its own interval."""
        logger.info(
            f"Time-lapse recording '{source.name}' every "
            f"{source.settings.interval_minutes:g} min, keeping "
            f"{source.settings.retention_hours:g} h in {self.root / source.id}"
        )
        while True:
            try:
                await asyncio.to_thread(self.capture, source)
            except Exception:
                # Keep recording after unexpected errors
                logger.exception(f"Time-lapse capture failed for '{source.name}'")
            await asyncio.sleep(source.settings.interval_minutes * 60)

    def capture_all(self, now: datetime | None = None) -> None:
        """Save one frame for each camera and delete expired frames.

        Args:
            now (datetime, optional): Current UTC time. Defaults to now.
        """
        for source in self.sources.values():
            self.capture(source, now)

    def capture(self, source: TimelapseSource, now: datetime | None = None) -> None:
        """Save one frame for a camera and delete its expired frames.

        Errors are logged as warnings and not raised. The error is kept in
        ``source.last_error`` until a frame is saved.

        Args:
            source (TimelapseSource): Camera to capture.
            now (datetime, optional): Current UTC time. Defaults to now.
        """
        now = now or datetime.now(UTC)
        try:
            if self._capture(source, now):
                source.last_error = None
        except (OSError, ValueError, HTTPException) as e:
            source.last_error = str(e)
            logger.warning(f"Time-lapse capture failed for '{source.name}': {e}")
        try:
            self.prune(source.id, now)
        except OSError as e:
            logger.warning(f"Time-lapse cleanup failed for '{source.name}': {e}")

    def _capture(self, source: TimelapseSource, now: datetime) -> bool:
        """Save one frame for a camera.

        Returns:
            bool: True if a frame was saved, False if the image file did not change.

        Raises:
            OSError: If the file or URL cannot be read, or the request times out.
            HTTPException: If the HTTP response is broken.
            ValueError: If the data is not a JPEG or PNG image, or the image
                file is older than the retention period.
        """
        if source.path is not None:
            mtime = source.path.stat().st_mtime
            if mtime == source._last_mtime:
                return False  # no new image since the last frame
            data = source.path.read_bytes()
            if source.path.stat().st_mtime != mtime:
                return False  # the file changed while we read it; try again next time
            # Remember this version even if it is rejected, so it is reported only once
            source._last_mtime = mtime

            frame_time = datetime.fromtimestamp(mtime, UTC)
            retention = source.settings.retention_hours
            if frame_time < now - timedelta(hours=retention):
                # The frame would be deleted at once by prune(), so do not save it
                raise ValueError(
                    f"image file was last updated {frame_time:%Y-%m-%d %H:%M} UTC, "
                    f"which is older than the {retention:g} h retention period"
                )
        else:
            request = urllib.request.Request(
                str(source.url), headers={"User-Agent": "astra-timelapse"}
            )
            with urllib.request.urlopen(
                request, timeout=FETCH_TIMEOUT_SECONDS
            ) as response:
                # Read one byte past the limit to detect responses that are too large
                data = response.read(MAX_SNAPSHOT_BYTES + 1)
            if len(data) > MAX_SNAPSHOT_BYTES:
                raise ValueError(
                    f"response is larger than {MAX_SNAPSHOT_BYTES // 2**20} MB; "
                    "snapshot_url must return one image, not a video stream"
                )
            frame_time = now

        suffix = _image_suffix(data)
        if suffix is None:
            raise ValueError("data is not a JPEG or PNG image")

        folder = self.root / source.id
        folder.mkdir(parents=True, exist_ok=True)
        target = folder / f"{frame_time.strftime(TIMESTAMP_FORMAT)}{suffix}"
        # Write to a temporary file first, so readers never see a partial frame
        partial = target.with_name(target.name + ".part")
        partial.write_bytes(data)
        partial.replace(target)
        return True

    def prune(self, camera_id: str, now: datetime | None = None) -> None:
        """Delete frames older than the camera's retention period.

        Args:
            camera_id (str): Camera id.
            now (datetime, optional): Current UTC time. Defaults to now.
        """
        folder = self.root / camera_id
        if camera_id not in self.sources or not folder.is_dir():
            return
        retention = self.sources[camera_id].settings.retention_hours
        cutoff = (now or datetime.now(UTC)) - timedelta(hours=retention)
        for file in folder.iterdir():
            frame_time = _parse_frame_time(file.name)
            if frame_time is not None and frame_time < cutoff:
                file.unlink(missing_ok=True)

    def list_frames(self, camera_id: str) -> list[dict[str, str]]:
        """List the saved frames for a camera, oldest first.

        Args:
            camera_id (str): Camera id.

        Returns:
            list[dict[str, str]]: Dicts with ``file`` (frame file name) and
                ``time`` (ISO 8601 UTC time).
        """
        folder = self.root / camera_id
        if camera_id not in self.sources or not folder.is_dir():
            return []

        frames = []
        for file in sorted(folder.iterdir()):
            frame_time = _parse_frame_time(file.name)
            if frame_time is not None:
                frames.append({"file": file.name, "time": frame_time.isoformat()})
        return frames

    def status(self, camera_id: str) -> dict:
        """Return the frames and recording status for a camera.

        Args:
            camera_id (str): Camera id.

        Returns:
            dict: ``frames`` (see :meth:`list_frames`) and ``last_error``.

        Raises:
            KeyError: If the camera id is unknown.
        """
        return {
            "frames": self.list_frames(camera_id),
            "last_error": self.sources[camera_id].last_error,
        }

    def frame_path(self, camera_id: str, filename: str) -> Path | None:
        """Return the path of a saved frame.

        Only file names in the frame format are accepted, so a request cannot
        read other files.

        Args:
            camera_id (str): Camera id.
            filename (str): Frame file name from :meth:`list_frames`.

        Returns:
            Path | None: The frame path, or None if the frame does not exist.
        """
        if camera_id not in self.sources or _parse_frame_time(filename) is None:
            return None
        path = self.root / camera_id / filename
        return path if path.is_file() else None
