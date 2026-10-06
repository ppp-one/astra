"""
Timeline of camera exposures, shared between the exposure loop and the guider.

The exposure loop records when each exposure starts and which exposure each
saved image comes from. The guider records when its last correction finished.
With this, the guider only measures images that were exposed after the
correction: an earlier image still shows the error that was corrected.
"""

import threading
import time
from pathlib import Path


class ExposureTimeline:
    """
    Thread-safe record of the exposures of one camera.

    All times are from ``time.monotonic()``.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._exposure_start: float | None = None
        self._last_image: tuple[Path, float] | None = None
        self._pulse_end = 0.0

    def exposure_started(self) -> None:
        """Record the start of an exposure. Call just before ``StartExposure``."""
        with self._lock:
            self._exposure_start = time.monotonic()

    def image_saved(self, path: str | Path) -> None:
        """Record which exposure the saved image comes from."""
        with self._lock:
            if self._exposure_start is not None:
                self._last_image = (Path(path), self._exposure_start)

    def exposure_start_of(self, path: str | Path) -> float | None:
        """Start time of the exposure of the last saved image, or None if
        ``path`` is not the last saved image."""
        with self._lock:
            if self._last_image is None or self._last_image[0] != Path(path):
                return None
            return self._last_image[1]

    def has_exposures(self) -> bool:
        """True once the exposure loop has recorded an exposure."""
        with self._lock:
            return self._exposure_start is not None

    def pulse_finished(self) -> None:
        """Record that the guide pulses have finished."""
        with self._lock:
            self._pulse_end = time.monotonic()

    @property
    def last_pulse_end(self) -> float:
        with self._lock:
            return self._pulse_end
