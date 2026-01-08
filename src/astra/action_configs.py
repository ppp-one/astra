"""Action configuration dataclasses for observatory operations.

Key capabilities:
    - Define structured configurations for various observatory actions
    - Validate required fields and types for action parameters
    - Provide defaults from observatory configuration
    - Support dictionary-like access to action configuration fields
"""

import typing
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

import astropy.units as u
import numpy as np
from astropy.coordinates import AltAz, Angle, EarthLocation, SkyCoord
from astropy.time import Time

from astra.config import Config, ObservatoryConfig


@dataclass
class BaseActionConfig:
    """Base class for action configurations.

    This class serves as a base for specific action configurations,
    providing validation and dictionary-like access to its fields.
    It supports type validation, required fields, and merging with default values,
    centralizing common functionality for all action configurations and ensuring
    that the action values passed by the user are valid.

    Examples:
        >>> from astra.action_configs import AutofocusConfig
        >>> autofocus_config = AutofocusConfig(exptime=3.0)
        >>> exptime in autofocus_config
        True
        >>> autofocus_config['exptime']
        3.0
        >>> autofocus_config.get('not_available')
    """

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_value": {},
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def __post_init__(self):
        self.validate()

    @classmethod
    def from_dict(cls, config_dict: dict, default_dict: dict = {}, logger=None):
        """Create an instance from a dictionary, merging with defaults."""
        kwargs = cls.merge_config_dicts(config_dict, default_dict)

        if logger is not None:
            logger.debug(f"Extracting action values {kwargs} for {cls.__name__}")

        return cls(**kwargs)

    @classmethod
    def defaults_from_observatory_config(
        cls,
        device_name: str,
        device_type: str = "Camera",
        observatory_config: object | None = None,
    ) -> dict:
        """
        Retrieve default values for this action from the observatory configuration.

        Returns a dict suitable for passing as `default_dict` into from_dict.
        """
        # lazy-import to avoid cycle at module import time

        oc = (
            observatory_config
            if observatory_config is not None
            else Config().observatory_config
        )
        if not isinstance(oc, ObservatoryConfig):
            return {}

        action_key = cls.__name__.lower().replace("config", "")

        try:
            if hasattr(oc, "get_device_config"):
                device_conf = oc.get_device_config(device_type, device_name)
                if isinstance(device_conf, dict):
                    return device_conf.get(action_key, {}) or {}
                return {}
        except Exception:
            # Fall through to empty fallback if accessor fails
            return {}

        return {}

    def validate(self):
        """Validate required fields and types of all fields.

        Raises:
            ValueError: If required fields are missing.
            TypeError: If any field has an incorrect type.
        """
        missing = []
        type_errors = []
        for f in self.__dataclass_fields__.values():
            val = getattr(self, f.name)
            # Check required fields
            if f.metadata.get("required") and val is None:
                missing.append(f.name)
            # Type validation for all fields
            err = self._validate_type(f)
            if err:
                type_errors.append(err)
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
        if type_errors:
            raise TypeError(f"Type errors in fields: {type_errors}")

    def _validate_type(self, f):
        val = getattr(self, f.name)
        expected_type = self.__annotations__.get(f.name)
        if val is None or expected_type is None:
            return None

        origin = typing.get_origin(expected_type)
        args = typing.get_args(expected_type)

        if expected_type is float and isinstance(val, int):
            return None

        # Handle Optional/Union types
        if origin is Union:
            allowed_types = [t for t in args if t is not type(None)]
            for t in allowed_types:
                t_origin = typing.get_origin(t)
                if t_origin:
                    if isinstance(val, t_origin):
                        break
                elif isinstance(val, t):
                    break
            else:
                return self.format_type_error(f, allowed_types, type(val))
        # Handle lists and tuples
        elif origin in (list, tuple):
            if not isinstance(val, origin):
                return self.format_type_error(f, origin, type(val))
            elem_type = args[0] if args else None
            if elem_type:
                # Handle Union types inside lists (e.g., List[float | int])
                elem_origin = typing.get_origin(elem_type)
                elem_args = typing.get_args(elem_type)
                if (
                    elem_origin is Union
                    or isinstance(elem_type, type)
                    and elem_type.__module__ == "types"
                    and elem_type.__name__ == "UnionType"
                ):
                    allowed_elem_types = tuple(
                        t for t in elem_args if isinstance(t, type)
                    )
                elif elem_type in (float, int):
                    allowed_elem_types = (float, int)
                else:
                    allowed_elem_types = (elem_type,)
                for v in val:
                    if not isinstance(v, allowed_elem_types):
                        return self.format_type_error(
                            f, allowed_elem_types, type(v), specifier="elements"
                        )

        # Handle dicts
        elif origin is dict:
            if not isinstance(val, dict):
                return self.format_type_error(f, dict, type(val))
            key_type, value_type = args if len(args) == 2 else (None, None)
            if key_type:
                key_origin = typing.get_origin(key_type)
                for k in val.keys():
                    if key_origin:
                        if not isinstance(k, key_origin):
                            return self.format_type_error(
                                f, key_origin, type(k), specifier="keys"
                            )
                    elif not isinstance(k, key_type):
                        return self.format_type_error(
                            f, key_type, type(k), specifier="keys"
                        )
            if value_type:
                value_origin = typing.get_origin(value_type)
                for v in val.values():
                    if value_origin:
                        if not isinstance(v, value_origin):
                            return self.format_type_error(
                                f, value_origin, type(v), specifier="values"
                            )
                    elif not isinstance(v, value_type):
                        return self.format_type_error(
                            f, value_type, type(v), specifier="values"
                        )
        # Handle enums
        elif isinstance(expected_type, type) and issubclass(expected_type, Enum):
            if not isinstance(val, expected_type):
                return self.format_type_error(f, expected_type, type(val))
        # Handle all other types (non-parameterized)
        elif isinstance(expected_type, type):
            if not isinstance(val, expected_type):
                return self.format_type_error(f, expected_type, type(val))
        # Otherwise, skip type check
        return None

    @staticmethod
    def format_type_error(f, expected_type, val, specifier=None):
        return (
            f"{f.name}: "
            + (f"{specifier} " if specifier else "")
            + f"expected {expected_type}, got {val}"
        )

    def get(self, key: str, default=None):
        """
        Get attribute value by key with optional default.

        Args:
            key: Attribute name to retrieve.
            default: Value to return if attribute is not found.
        Returns:
            Attribute value or default if not found.
        """
        return getattr(self, key, default)

    def __getitem__(self, key: str):
        return getattr(self, key)

    def __setitem__(self, key: str, value):
        return setattr(self, key, value)

    def __contains__(self, key: str):
        return hasattr(self, key)

    def keys(self) -> List[str]:
        """Return list of field names in the dataclass."""
        return [
            item
            for item in self.__dataclass_fields__.keys()
            if not item.startswith("_")
        ]

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(self.keys())

    def validate_filters(self, filterwheel_names: dict[str, list[str]]) -> None:
        """Validate that filter(s) exist in the available filterwheels.

        Args:
            filterwheel_names: Dict mapping filterwheel device names to lists of filter names.
                              e.g., {"fw1": ["Clear", "Red", "Green", "Blue"]}

        Raises:
            ValueError: If a filter is specified but doesn't exist in any filterwheel.
        """
        # Get filter value from the config
        filter_value = self.get("filter")

        if filter_value is None or not filterwheel_names:
            return  # No filter specified or no filterwheels available

        # Handle both single filter and list of filters
        filters_to_check = (
            [filter_value] if isinstance(filter_value, str) else filter_value
        )

        # Collect all available filter names from all filterwheels
        all_available_filters = set()
        for fw_filters in filterwheel_names.values():
            all_available_filters.update(fw_filters)

        # Check each filter
        invalid_filters = []
        for f in filters_to_check:
            if f not in all_available_filters:
                invalid_filters.append(f)

        if invalid_filters:
            raise ValueError(
                f"Filter(s) {invalid_filters} not found in available filters: "
                f"{sorted(all_available_filters)}"
            )

    def validate_subframe(self) -> None:
        """Validate subframe parameters.

        Raises:
            ValueError: If subframe parameters are invalid.
        """
        subframe_width = self.get("subframe_width")
        subframe_height = self.get("subframe_height")
        subframe_center_x = self.get("subframe_center_x", 0.5)
        subframe_center_y = self.get("subframe_center_y", 0.5)

        # Check dimensions are positive if specified
        if subframe_width is not None and subframe_width <= 0:
            raise ValueError(f"subframe_width must be positive, got {subframe_width}")
        if subframe_height is not None and subframe_height <= 0:
            raise ValueError(f"subframe_height must be positive, got {subframe_height}")

        # Check center coordinates are in valid range [0, 1]
        if not (0.0 <= subframe_center_x <= 1.0):  # type: ignore
            raise ValueError(
                f"subframe_center_x must be between 0 and 1, got {subframe_center_x}"
            )
        if not (0.0 <= subframe_center_y <= 1.0):  # type: ignore
            raise ValueError(
                f"subframe_center_y must be between 0 and 1, got {subframe_center_y}"
            )

        # If only one dimension is specified, require both
        if (subframe_width is None) != (subframe_height is None):
            raise ValueError(
                "Both subframe_width and subframe_height must be specified together. "
                f"Got: width={subframe_width}, height={subframe_height}"
            )

    def validate_visibility(
        self,
        start_time: Time,
        end_time: Time,
        observatory_location: EarthLocation,
        min_altitude: float = 0.0,
    ):
        """Validate that the target is visible during the scheduled observation window.

        Checks target visibility at the beginning, middle, and end of the planned
        observation to ensure the target remains observable throughout.

        Only implemented for object actions; override in subclasses as needed.
        """
        return None

    def has_subframe(self) -> bool:
        """Check if subframing is enabled.

        Returns:
            True if subframe_width and subframe_height are specified, False otherwise.
        """
        return (
            self.get("subframe_width") is not None
            and self.get("subframe_height") is not None
        )

    @classmethod
    def merge_config_dicts(cls, config_dict: dict, default_dict: dict) -> dict:
        """Merge default_dict and config_dict, keeping only keys in dataclass."""
        if not isinstance(config_dict, dict):
            config_dict = {}
        if not isinstance(default_dict, dict):
            default_dict = {}
        return {k: v for k, v in default_dict.items() if k in cls.__annotations__} | {
            k: v for k, v in config_dict.items() if k in cls.__annotations__
        }

    def to_jsonable(self):
        def convert(val):
            if isinstance(val, Angle):
                return val.deg
            elif isinstance(val, SkyCoord):
                return {"ra": val.ra.deg, "dec": val.dec.deg}  # type: ignore
            elif isinstance(val, Time):
                return val.isot
            elif isinstance(val, Enum):
                return val.value
            elif isinstance(val, dict):
                return {k: convert(v) for k, v in val.items()}
            elif isinstance(val, list):
                return [convert(v) for v in val]
            elif hasattr(val, "__dataclass_fields__"):
                return {k: convert(getattr(val, k)) for k in val.__dataclass_fields__}
            else:
                return val

        return convert(self)


@dataclass
class OpenActionConfig(BaseActionConfig):
    """Open the observatory for observations.

    Steps:
        1. Opens dome shutter
        2. Unparks telescope
        3. Cools camera
    """

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_type": "open",
        "action_value": {},
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def validate(self):
        pass


@dataclass
class CloseActionConfig(BaseActionConfig):
    """Close the observatory safely.

    Steps:
        1. Stop any active guiding operations
        2. Stop telescope slewing and tracking
        3. Park the telescope
        4. Park the dome and close shutter
        5. Cools camera
    """

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_type": "close",
        "action_value": {},
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def validate(self):
        pass


@dataclass
class CompleteHeadersActionConfig(BaseActionConfig):
    """Complete FITS headers after exposures finish.

    Uses paired device telemetry to fill in metadata that was unavailable at
    exposure time. Automatically executed at the end of every schedule.
    """

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_type": "complete_headers",
        "action_value": {},
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def validate(self):
        pass


@dataclass
class CoolCameraActionConfig(BaseActionConfig):
    """Configuration for the ``cool_camera`` schedule action.

    Activates the camera cooler and sets the target temperature with specified tolerance
    from observatory configuration with a 30 minute timeout.
    """

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_type": "cool_camera",
        "action_value": {},
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def validate(self):
        pass


@dataclass
class ObjectActionConfig(BaseActionConfig):
    """Capture a sequence of light frames.

    Workflow:
        1. Pre-sequence setup (pointing, filters, focus, binning, headers)
        2. Capture exposures
        3. Perform pointing correction when ``pointing=true``
        4. Start guiding when ``guiding=true``
        5. Stop exposures, guiding, and tracking at completion
    """

    object: str = field(metadata={"required": True})
    exptime: float = field(metadata={"required": True})
    ra: Optional[float] = None
    dec: Optional[float] = None
    alt: Optional[float] = None
    az: Optional[float] = None
    filter: Optional[str] = None
    focus_shift: Optional[float] = None
    focus_position: Optional[float] = None
    n: Optional[int] = None
    guiding: bool = False
    pointing: bool = False
    bin: int = 1
    dir: Optional[str] = None
    execute_parallel: bool = False
    disable_telescope_movement: bool = False
    reset_guiding_reference: bool = True
    subframe_width: Optional[int] = None
    subframe_height: Optional[int] = None
    subframe_center_x: float = 0.5
    subframe_center_y: float = 0.5

    FIELD_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "object": "Target name.",
        "exptime": "Exposure time per frame in seconds.",
        "ra": "Right Ascension to slew to when overriding current pointing.",
        "dec": "Declination to slew to when overriding current pointing.",
        "alt": "Altitude coordinate when issuing Alt/Az pointings.",
        "az": "Azimuth coordinate when issuing Alt/Az pointings.",
        "filter": "Filter name to load before imaging.",
        "focus_shift": "Focus offset relative to the stored best focus.",
        "focus_position": "Absolute focus position override.",
        "n": "Number of exposures in the sequence.",
        "guiding": "Start autoguiding with Donuts before imaging.",
        "pointing": "Perform pointing correction with twirl before imaging.",
        "bin": "Camera binning factor.",
        "dir": "Directory path for saving images.",
        "execute_parallel": "Execute exposure steps concurrently when supported.",
        "disable_telescope_movement": "Prevent any telescope motion during the sequence.",
        "reset_guiding_reference": "Acquire a fresh guiding reference frame at the start.",
        "subframe_width": "Width of the requested subframe in binned pixels.",
        "subframe_height": "Height of the requested subframe in binned pixels.",
        "subframe_center_x": "Horizontal location of the subframe center (0=left, 1=right).",
        "subframe_center_y": "Vertical location of the subframe center (0=top, 1=bottom).",
    }

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_type": "object",
        "action_value": {
            "object": "M42",
            "exptime": 60.0,
            "ra": 83.82208,
            "dec": -5.39111,
            "filter": "V",
            "n": 3,
            "guiding": True,
            "pointing": True,
        },
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def validate(self):
        missing = []
        for f in self.__dataclass_fields__.values():
            if f.metadata.get("required") and getattr(self, f.name) is None:
                missing.append(f.name)
        if missing:
            raise ValueError(f"Missing required fields: {missing}")

        # Coordinate system validation
        has_radec = self.ra is not None or self.dec is not None
        has_altaz = self.alt is not None or self.az is not None

        # Can't mix coordinate systems
        if has_radec and has_altaz:
            raise ValueError(
                "Cannot specify both RA/Dec and Alt/Az coordinates. "
                "Use either 'ra' and 'dec' OR 'alt' and 'az', not both."
            )

        # Must provide complete coordinate pairs
        if (self.ra is not None and self.dec is None) or (
            self.ra is None and self.dec is not None
        ):
            raise ValueError(
                f"Both 'ra' and 'dec' must be provided together. Got: ra={self.ra}, dec={self.dec}"
            )

        if (self.alt is not None and self.az is None) or (
            self.alt is None and self.az is not None
        ):
            raise ValueError(
                f"Both 'alt' and 'az' must be provided together. Got: alt={self.alt}, az={self.az}"
            )

        # Subframe validation
        self.validate_subframe()

    def validate_visibility(
        self,
        start_time: Time,
        end_time: Time,
        observatory_location: EarthLocation,
        min_altitude: float = 0.0,
    ) -> None:
        """Validate that the target is visible during the scheduled observation window.

        Checks target visibility at the beginning, middle, and end of the planned
        observation to ensure the target remains observable throughout.

        Args:
            start_time: Observation start time as astropy Time object
            end_time: Observation end time as astropy Time object
            observatory_location: Observatory location as EarthLocation object
            min_altitude: Minimum altitude in degrees for target to be considered visible (default: 0°)

        Raises:
            ValueError: If RA/Dec are not provided or if target is below minimum altitude
                at any of the three check points (start, middle, end)

        Note:
            Only checks visibility when RA and Dec coordinates are provided.
            Alt/Az coordinates are not checked as they are position-specific.
        """
        # Only check visibility if RA/Dec are provided
        if self.ra is None or self.dec is None:
            return

        # Create target coordinate
        target = SkyCoord(
            ra=u.Quantity(self.ra, "deg"),
            dec=u.Quantity(self.dec, "deg"),
            frame="icrs",
        )

        # Check times: start, middle, end
        mid_time = Time(
            (start_time.unix + end_time.unix) / 2,
            format="unix",
        )
        check_times = [
            ("start", start_time),
            ("middle", mid_time),
            ("end", end_time),
        ]

        visibility_issues = []

        for label, check_time in check_times:
            # Transform to horizontal coordinates
            altaz_frame = AltAz(obstime=check_time, location=observatory_location)
            target_altaz = target.transform_to(altaz_frame)

            altitude = target_altaz.alt.deg  # type: ignore

            if altitude < min_altitude:  # type: ignore
                visibility_issues.append(
                    f"{label}: altitude {altitude:.1f}° (below {min_altitude:.1f}° limit)"
                )

        if visibility_issues:
            raise ValueError(
                f"Target '{self.object}' at RA={self.ra:.2f}°, Dec={self.dec:.2f}° "
                f"is not visible during observation window:\n  "
                + "\n  ".join(visibility_issues)
            )


@dataclass
class CalibrationActionConfig(BaseActionConfig):
    """Capture a sequence of calibration images (bias/dark)."""

    exptime: List[float] = field(default_factory=list, metadata={"required": True})
    n: List[int] = field(default_factory=list, metadata={"required": True})
    filter: Optional[str] = None
    dir: Optional[str] = None
    bin: int = 1
    execute_parallel: bool = False
    subframe_width: Optional[int] = None
    subframe_height: Optional[int] = None
    subframe_center_x: float = 0.5
    subframe_center_y: float = 0.5

    FIELD_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "exptime": "Exposure times (seconds) to iterate.",
        "n": "Exposure counts aligned with each exposure time.",
        "filter": "Filter specification to use while capturing calibration frames.",
        "dir": "Directory path for saving images.",
        "bin": "Camera binning factor.",
        "execute_parallel": "Execute the sequence in parallel mode when supported.",
        "subframe_width": "Width of the requested subframe in binned pixels.",
        "subframe_height": "Height of the requested subframe in binned pixels.",
        "subframe_center_x": "Horizontal subframe center (0=left, 1=right).",
        "subframe_center_y": "Vertical subframe center (0=top, 1=bottom).",
    }

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_type": "calibration",
        "action_value": {"exptime": [0.0, 5.0, 30.0], "n": [10, 5, 3]},
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def validate(self):
        missing = []
        for f in self.__dataclass_fields__.values():
            if f.metadata.get("required") and (
                getattr(self, f.name) is None or getattr(self, f.name) == []
            ):
                missing.append(f.name)
        if missing:
            raise ValueError(f"Missing required fields: {missing}")

        # ensure exptime and n have the same length
        if len(self.exptime) != len(self.n):
            raise ValueError(
                f"'exptime' and 'n' must have the same length. Got: exptime={self.exptime}, n={self.n}"
            )

        # Subframe validation
        self.validate_subframe()


@dataclass
class FlatsActionConfig(BaseActionConfig):
    """Capture a sequence of sky flats as the sky brightness evolves.

    Steps:
        1. Wait for Sun altitude between -1° and -12°
        2. Point to a near-uniform patch of sky opposite the Sun
        3. Capture exposures and re-position between frames
        4. Iterate through requested filters while adjusting exposure times
    """

    filter: List[str] = field(default_factory=list, metadata={"required": True})
    n: List[int] = field(default_factory=list, metadata={"required": True})
    dir: Optional[str] = None
    bin: int = 1
    execute_parallel: bool = False
    disable_telescope_movement: bool = False
    subframe_width: Optional[int] = None
    subframe_height: Optional[int] = None
    subframe_center_x: float = 0.5
    subframe_center_y: float = 0.5

    FIELD_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "filter": "Filters to iterate while capturing flats.",
        "n": "Number of flats to capture per filter.",
        "dir": "Directory path for saving images.",
        "bin": "Camera binning factor.",
        "execute_parallel": "Execute exposures in parallel when supported.",
        "disable_telescope_movement": "Prevent telescope motion during the sequence.",
        "subframe_width": "Width of the requested subframe in binned pixels.",
        "subframe_height": "Height of the requested subframe in binned pixels.",
        "subframe_center_x": "Horizontal subframe center (0=left, 1=right).",
        "subframe_center_y": "Vertical subframe center (0=top, 1=bottom).",
    }

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_type": "flats",
        "action_value": {"filter": ["V", "R"], "n": [10, 10]},
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def validate(self):
        missing = []
        for f in self.__dataclass_fields__.values():
            if f.metadata.get("required") and (
                getattr(self, f.name) is None or getattr(self, f.name) == []
            ):
                missing.append(f.name)
        if missing:
            raise ValueError(f"Missing required fields: {missing}")

        # ensure filter and n have the same length
        if len(self.filter) != len(self.n):
            raise ValueError(
                f"'filter' and 'n' must have the same length. Got: filter={self.filter}, n={self.n}"
            )

        # Subframe validation
        self.validate_subframe()


@dataclass
class CalibrateGuidingActionConfig(BaseActionConfig):
    """Calibrate guiding parameters using timed guide pulses."""

    filter: Optional[str] = None
    pulse_time: float = 5000.0
    exptime: float = 5.0
    settle_time: float = 10.0
    number_of_cycles: int = 10
    focus_shift: Optional[float] = None
    focus_position: Optional[float] = None
    bin: int = 1
    subframe_width: Optional[int] = None
    subframe_height: Optional[int] = None
    subframe_center_x: float = 0.5
    subframe_center_y: float = 0.5

    FIELD_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "filter": "Filter to use during calibration.",
        "pulse_time": "Duration of guide pulses in milliseconds.",
        "exptime": "Exposure time for calibration images.",
        "settle_time": "Wait time after pulses before exposing.",
        "number_of_cycles": "How many calibration cycles to run.",
        "focus_shift": "Focus offset relative to best focus.",
        "focus_position": "Absolute focus position override.",
        "bin": "Camera binning factor.",
        "subframe_width": "Width of the requested subframe in binned pixels.",
        "subframe_height": "Height of the requested subframe in binned pixels.",
        "subframe_center_x": "Horizontal subframe center (0=left, 1=right).",
        "subframe_center_y": "Vertical subframe center (0=top, 1=bottom).",
    }

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_type": "calibrate_guiding",
        "action_value": {},
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def validate(self):
        self.validate_subframe()


@dataclass
class PointingModelActionConfig(BaseActionConfig):
    """Generate sync points to build a telescope pointing model.

    Captures a spiral of points from zenith down to 30° altitude while
    avoiding positions within 20° of the Moon.
    """

    n: int = 100
    exptime: float = 1.0
    dark_subtraction: bool = False
    object: str = "Pointing Model"
    ra: Optional[float] = None
    dec: Optional[float] = None
    filter: Optional[str] = None
    focus_shift: Optional[float] = None
    focus_position: Optional[float] = None
    bin: int = 1
    dir: Optional[str] = None
    subframe_width: Optional[int] = None
    subframe_height: Optional[int] = None
    subframe_center_x: float = 0.5
    subframe_center_y: float = 0.5

    FIELD_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "n": "Number of points to include in the model.",
        "exptime": "Exposure time for each pointing image.",
        "dark_subtraction": "Enable dark subtraction using matching calibration frames.",
        "object": "Descriptive label for the pointing run.",
        "ra": "Right Ascension for starting point when overriding automatic selection.",
        "dec": "Declination for starting point when overriding automatic selection.",
        "filter": "Filter to use for exposures.",
        "focus_shift": "Focus offset relative to best focus.",
        "focus_position": "Absolute focus position override.",
        "bin": "Camera binning factor.",
        "dir": "Directory path for saving images.",
        "subframe_width": "Width of the requested subframe in binned pixels.",
        "subframe_height": "Height of the requested subframe in binned pixels.",
        "subframe_center_x": "Horizontal subframe center (0=left, 1=right).",
        "subframe_center_y": "Vertical subframe center (0=top, 1=bottom).",
    }

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_type": "pointing_model",
        "action_value": {},
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def validate(self):
        self.validate_subframe()


class SelectionMethod(Enum):
    SINGLE = "single"
    MAXIMAL = "maximal"
    ANY = "any"

    @classmethod
    def from_string(cls, key: str, logger=None) -> "SelectionMethod":
        key = key.upper()
        if key in cls.__members__:
            return cls[key]

        if logger is not None:
            logger.warning(f"Unknown selection_method: {key}. Fall back to 'SINGLE'.")

        return cls.SINGLE


@dataclass
class AutofocusCalibrationFieldConfig(BaseActionConfig):
    """Configuration for automated autofocus calibration field selection."""

    maximal_zenith_angle: Optional[float | int | Angle] = None
    airmass_threshold: float = 1.01
    g_mag_range: List[float | int] = field(default_factory=lambda: [0, 10])
    j_mag_range: List[float | int] = field(default_factory=lambda: [0, 10])
    fov_height: float | int = 0
    fov_width: float | int = 0
    selection_method: SelectionMethod | str = "single"
    use_gaia: bool = True
    observation_time: Optional[Time] = None
    maximal_number_of_stars: int = 100_000
    ra: Optional[float | int] = None
    dec: Optional[float | int] = None
    _coordinates: Optional[SkyCoord] = None

    FIELD_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "maximal_zenith_angle": "Maximum zenith angle allowed when selecting autofocus fields.",
        "airmass_threshold": "Highest acceptable airmass for autofocus candidates.",
        "g_mag_range": "Inclusive Gaia G magnitude range to consider.",
        "j_mag_range": "Inclusive 2MASS J magnitude range to consider.",
        "fov_height": "Height of the field of view in degrees.",
        "fov_width": "Width of the field of view in degrees.",
        "selection_method": "Strategy for selecting stars (single, maximal, any).",
        "use_gaia": "Whether to rely on Gaia catalog sources.",
        "observation_time": "Observation time used when evaluating constraints.",
        "maximal_number_of_stars": "Maximum number of stars to query or consider.",
        "ra": "Fixed Right Ascension used to bypass automatic selection.",
        "dec": "Fixed Declination used to bypass automatic selection.",
    }

    def __post_init__(self):
        from astrafocus.targeting import find_airmass_threshold_crossover

        if self.maximal_zenith_angle is None:
            self.maximal_zenith_angle = Angle(
                find_airmass_threshold_crossover(
                    airmass_threshold=self.airmass_threshold
                )
                * 180
                / np.pi,
                unit=u.deg,
            )
        elif isinstance(self.maximal_zenith_angle, (float, int)):
            self.maximal_zenith_angle = Angle(self.maximal_zenith_angle, unit=u.deg)
        elif isinstance(self.maximal_zenith_angle, Angle):
            pass
        else:
            raise ValueError("maximal_zenith_angle must be of type float, int.")

        if not isinstance(self.selection_method, SelectionMethod):
            self.selection_method = SelectionMethod.from_string(self.selection_method)

        self.validate()

    @property
    def coordinates(self) -> SkyCoord:
        if self._coordinates is not None:
            return self._coordinates

        raise ValueError("Calibration coordinates have not been set.")

    @coordinates.setter
    def coordinates(self, value: SkyCoord) -> None:
        self._coordinates = value

    @classmethod
    def from_dict(
        cls, config_dict: dict, logger=None, default_dict: dict = {}
    ) -> "AutofocusCalibrationFieldConfig":
        kwargs = cls.merge_config_dicts(config_dict, default_dict)
        if "selection_method" in kwargs and not isinstance(
            kwargs["selection_method"], SelectionMethod
        ):
            kwargs["selection_method"] = SelectionMethod.from_string(
                kwargs["selection_method"], logger=logger
            )

        return cls(**kwargs)


@dataclass
class AutofocusConfig(BaseActionConfig):
    """Perform an autofocus sweep to determine the optimal focus position.

    Steps:
        1. Select a suitable autofocus field (or use provided coordinates)
        2. Move the telescope if needed
        3. Capture images at different focus positions
        4. Measure star sharpness in each image
        5. Fit a curve to determine optimal focus
        6. Save plots/results and optionally persist the best focus position
    """

    exptime: float | int = field(default=3.0, metadata={"required": True})
    reduce_exposure_time: bool = False
    search_range: Optional[List[int] | int] = None
    search_range_is_relative: bool = False
    n_steps: List[int] = field(default_factory=lambda: [30, 20])
    n_exposures: List[int] | int = 1
    decrease_search_range: bool = True
    star_find_threshold: float | int = 5.0
    fwhm: int = 8
    percent_to_cut: int = 60
    focus_measure_operator: str = "HFR"
    save: bool = True
    extremum_estimator: str = "LOWESS"
    extremum_estimator_kwargs: dict[str, Any] = field(default_factory=dict)
    secondary_focus_measure_operators: List[str] = field(
        default_factory=lambda: [
            "fft",
            "normalized_variance",
            "tenengrad",
        ]
    )
    calibration_field: AutofocusCalibrationFieldConfig = field(
        default_factory=AutofocusCalibrationFieldConfig,
        metadata={"required": True, "flatten": True},
    )
    save_path: Optional[Path] = None
    subframe_width: Optional[int] = None
    subframe_height: Optional[int] = None
    subframe_center_x: float = 0.5
    subframe_center_y: float = 0.5
    _focus_measure_operator = None
    _secondary_focus_measure_operators = {}

    FIELD_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "exptime": "Exposure time for focus frames in seconds.",
        "search_range": "Range of focus positions to search. Accepts a single width or explicit bounds.",
        "search_range_is_relative": "Interpret search_range relative to the current focus position.",
        "n_steps": "Number of steps for each sweep.",
        "n_exposures": (
            "Number of exposures at each focus position or an array specifying exposures for each sweep. "
            "If an integer is given, the same number of exposures is used for each sweep. "
            "If an array is given, the length of the array must match the number of sweeps. "
        ),
        "decrease_search_range": "Reduce the search range after each sweep.",
        "star_find_threshold": "DAOStarFinder threshold for star detection.",
        "fwhm": "DAOStarFinder FWHM of the Gaussian kernel in pixels.",
        "percent_to_cut": "Percentage of worst-performing focus samples to drop when shrinking the range.",
        "focus_measure_operator": "Focus metric to optimize (e.g., hfr, gauss, tenengrad, fft, normalized_variance).",
        "reduce_exposure_time": "Automatically shorten exposures to prevent saturation.",
        "save": "Persist the optimal focus position back into observatory configuration.",
        "extremum_estimator": "Curve-fitting method used to determine the minimum (LOWESS, medianfilter, spline, rbf).",
        "extremum_estimator_kwargs": "Additional keyword overrides for the extremum estimator.",
        "secondary_focus_measure_operators": "Additional focus metrics to compute for diagnostics.",
        "save_path": "Directory override for saving autofocus results.",
        "subframe_width": "Width of the requested subframe in binned pixels.",
        "subframe_height": "Height of the requested subframe in binned pixels.",
        "subframe_center_x": "Horizontal subframe center (0=left, 1=right).",
        "subframe_center_y": "Vertical subframe center (0=top, 1=bottom).",
    }

    EXAMPLE_SCHEDULE: ClassVar[dict] = {
        "device_name": "camera_name",
        "action_type": "autofocus",
        "action_value": {
            "exptime": 3.0,
            "n_steps": [30, 20],
        },
        "start_time": "2025-01-01 00:00:00.000",
        "end_time": "2025-02-01 00:00:00.000",
    }

    def __post_init__(self) -> None:
        from astrafocus import FocusMeasureOperatorRegistry

        # Store operator classes, not instances, to avoid premature initialization
        self._secondary_focus_measure_operators = {
            FocusMeasureOperatorRegistry.get(
                key
            ).name: FocusMeasureOperatorRegistry.get(key)
            for key in self.secondary_focus_measure_operators
            if key in FocusMeasureOperatorRegistry.list()
        }
        self._focus_measure_operator = FocusMeasureOperatorRegistry.from_name(
            self.focus_measure_operator
        )
        self.validate()
        # Validate subframe after base validation
        self.validate_subframe()

    @classmethod
    def from_dict(
        cls, config_dict: dict, logger=None, default_dict: dict = {}
    ) -> "AutofocusConfig":
        autofocus_calibration_field = AutofocusCalibrationFieldConfig.from_dict(
            config_dict,
            logger=logger,
            default_dict=default_dict,
        )
        kwargs = cls.merge_config_dicts(config_dict, default_dict)
        kwargs["calibration_field"] = autofocus_calibration_field

        if len(kwargs["n_exposures"]) != len(kwargs["n_steps"]):
            if logger is not None:
                logger.warning(
                    "'n_exposures' length does not match 'n_steps' length. "
                    "Defaulting to 1 exposure per step."
                )
            kwargs["n_exposures"] = [1] * len(kwargs["n_steps"])

        return cls(**kwargs)

    @property
    def focus_measure_operator_kwargs(self) -> dict:
        return {
            "star_find_threshold": self.star_find_threshold,
            "fwhm": self.fwhm,
        }

    @property
    def focus_measure_operator_name(self) -> str:
        return (
            self._focus_measure_operator.name
            if self._focus_measure_operator
            else "Unknown"
        )


ACTION_CONFIGS = {
    "object": ObjectActionConfig,
    "calibration": CalibrationActionConfig,
    "flats": FlatsActionConfig,
    "calibrate_guiding": CalibrateGuidingActionConfig,
    "autofocus": AutofocusConfig,
    "pointing_model": PointingModelActionConfig,
    "open": OpenActionConfig,
    "close": CloseActionConfig,
    "cool_camera": CoolCameraActionConfig,
    "complete_headers": CompleteHeadersActionConfig,
}
