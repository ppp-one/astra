"""Action configuration dataclasses for observatory operations.

Key capabilities:
    - Define structured configurations for various observatory actions
    - Validate required fields and types for action parameters
    - Provide defaults from observatory configuration
    - Support dictionary-like access to action configuration fields
"""

import logging
import typing
from dataclasses import MISSING, dataclass, field
from dataclasses import fields as dataclass_fields
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Union

import astropy.units as u
import numpy as np
from astropy.coordinates import AltAz, Angle, EarthLocation, SkyCoord
from astropy.time import Time

from astra.config import Config, ObservatoryConfig
from astra.utils.ephemeris import (
    NotMovingBodyError,
    get_body_coordinates,
    precompute_ephemeris,
)

logger = logging.getLogger(__name__)

# Cadence of the altitude check across an observation window, and the largest
# number of samples it may take. Three samples at the start, middle and end are
# enough for a star, whose altitude changes monotonically between them, but not
# for a satellite: the ISS rises and sets several times in a long window, so it
# can be above the horizon at all three moments and below it for most of the
# sequence. Sampling every 30 seconds finds any excursion that lasts longer than
# that, and 2000 samples of it cost about a tenth of a second.
_VISIBILITY_SAMPLE_INTERVAL_S = 30.0
_VISIBILITY_MAX_SAMPLES = 2000

# Ephemeris computed past the end of an action, so a sequence that overruns still
# reads an interpolated position rather than an extrapolated one. A quarter of the
# window, held between five minutes and half an hour. A satellite pass that lasts
# a minute is sampled every few seconds, so half an hour of it would be thousands
# of points, and fetching them delays the schedule load for no benefit.
_EPHEMERIS_PAD_FRACTION = 0.25
_EPHEMERIS_PAD_MIN_HOURS = 5.0 / 60.0
_EPHEMERIS_PAD_MAX_HOURS = 0.5


def ephemeris_window_hours(start_time: Time, end_time: Time) -> float:
    """Return how many hours of ephemeris an action needs.

    The window covers the action itself and a margin past its end.

    Args:
        start_time: Start of the action.
        end_time: End of the action.

    Returns:
        float: Length of the ephemeris window in hours.
    """
    span_hours = max((end_time - start_time).to_value("hr"), 0.0)
    pad_hours = min(
        max(span_hours * _EPHEMERIS_PAD_FRACTION, _EPHEMERIS_PAD_MIN_HOURS),
        _EPHEMERIS_PAD_MAX_HOURS,
    )
    return span_hours + pad_hours


def public_fields(config) -> list:
    """Return the fields of an action config that belong in an action value.

    Class variables such as FIELD_DESCRIPTIONS and EXAMPLE_SCHEDULE, private
    attributes, and fields that are not constructor arguments are left out.

    Args:
        config: An action config class or instance.

    Returns:
        list: The dataclass fields that a user can set in an action value.
    """
    return [
        f for f in dataclass_fields(config) if f.init and not f.name.startswith("_")
    ]


def field_default(config, name: str):
    """Return the declared default of an action config field.

    Args:
        config: An action config class or instance.
        name (str): Name of the field.

    Returns:
        The default value, or None if the field has no default.
    """
    f = config.__dataclass_fields__[name]
    if f.default is not MISSING:
        return f.default
    if f.default_factory is not MISSING:
        return f.default_factory()
    return None


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
            raise ValueError(
                f"Missing required fields: {missing} in {self.__class__.__name__}"
            )
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
        nonsidereal_supported: bool | None = None,
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
        keys = {f.name for f in public_fields(cls)}
        return {k: v for k, v in default_dict.items() if k in keys} | {
            k: v for k, v in config_dict.items() if k in keys
        }

    def to_jsonable(self) -> dict:
        """Return this config as a JSON-serializable action value.

        Only the fields a user can set are included. A nested config marked
        with ``flatten`` metadata is merged into the parent dict, because that
        is the layout `from_dict` reads back.

        Returns:
            dict: The action value for this config.
        """

        def convert(val):
            if isinstance(val, Angle):
                return val.deg
            elif isinstance(val, SkyCoord):
                return {"ra": val.ra.deg, "dec": val.dec.deg}  # type: ignore
            elif isinstance(val, Time):
                return val.isot
            elif isinstance(val, Enum):
                return val.value
            elif isinstance(val, Path):
                return str(val)
            elif isinstance(val, dict):
                return {k: convert(v) for k, v in val.items()}
            elif isinstance(val, list):
                return [convert(v) for v in val]
            elif hasattr(val, "__dataclass_fields__"):
                return convert_config(val)
            else:
                return val

        def convert_config(config) -> dict:
            selected = public_fields(config)
            own_names = {f.name for f in selected if not f.metadata.get("flatten")}
            out = {}
            for f in selected:
                value = convert(getattr(config, f.name))
                if f.metadata.get("flatten") and isinstance(value, dict):
                    out.update({k: v for k, v in value.items() if k not in own_names})
                else:
                    out[f.name] = value
            return out

        return convert_config(self)


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

    Uses paired device polled data to fill in FITS header fields that were unavailable
    at exposure time. Automatically executed at the end of every schedule.
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
    and timeout from observatory configuration.
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
        1. Pre-sequence setup (pointing, filters, focus, binning, sub-framing, headers)
            - Observatory opens if not already done by a prior action if coordinates specified
        2. Capture exposures in succession
        3. Perform pointing correction if ``pointing=true``
        4. Start autoguiding if ``guiding=true``
        5. Stop exposures, guiding, and tracking at completion

    Non-sidereal tracking:
        Differential tracking is enabled implicitly: whenever ``lookup_name`` is
        given in place of a fixed ``ra``/``dec`` and resolves to a moving body, the
        sequence is tracked non-sidereally. Names resolved against Astropy's
        built-in ephemeris (the planets, the Moon and the Sun) or against JPL
        Horizons (asteroids and comets) are treated as moving, while names resolved
        as stars or deep-sky objects are tracked sidereally.
        ``nonsidereal_recenter_interval`` governs only how often the mount re-slews
        once tracking is under way. Autoguiding is incompatible with non-sidereal
        tracking and is disabled automatically. For Earth-orbiting objects, supply
        ``tle`` and set ``lookup_name`` to "TLE".

        The mount must report the ASCOM capabilities ``CanSetRightAscensionRate``
        and ``CanSetDeclinationRate``. Where ``lookup_name`` resolves to a moving
        body and no telescope in the observatory reports both, the schedule is
        rejected as it is loaded rather than run sidereally.

        **Schedule example for tracking Saturn**::

            {
                "device_name": "camera_name",
                "action_type": "object",
                "action_value": {
                    "object": "Saturn",
                    "lookup_name": "saturn",
                    "exptime": 30,
                    "filter": "Clear",
                    "nonsidereal_recenter_interval": 300,
                },
                "start_time":"2025-01-01 00:00:00.000",
                "end_time":"2025-01-01 01:00:00.000",
            }

    """

    object: str = field(metadata={"required": True})
    exptime: float = field(metadata={"required": True})
    ra: Optional[float] = None
    dec: Optional[float] = None
    alt: Optional[float] = None
    az: Optional[float] = None
    lookup_name: Optional[str] = None
    tle: Optional[str] = None
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
    nonsidereal_recenter_interval: int = 0
    nonsidereal_start_lead_time_seconds: float = 0.0
    nonsidereal_rate_update_interval: Optional[float] = None
    _nonsidereal: bool = field(default=False, init=False, repr=False)
    _ra_interp: Any = field(default=None, init=False, repr=False)
    _dec_interp: Any = field(default=None, init=False, repr=False)
    _ra_rate_interp: Any = field(default=None, init=False, repr=False)
    _dec_rate_interp: Any = field(default=None, init=False, repr=False)
    # Epoch the interpolators above are keyed to (their t=0). Kept so a sequence
    # can verify it is starting at the time the ephemeris was computed for, rather
    # than trusting that nothing reshuffled the schedule in between.
    _ephemeris_epoch: Any = field(default=None, init=False, repr=False)

    FIELD_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "object": "Target name.",
        "exptime": "Exposure time per frame in seconds.",
        "ra": "Right Ascension to slew to",
        "dec": "Declination to slew to",
        "alt": "Altitude coordinate when issuing Alt/Az pointings.",
        "az": "Azimuth coordinate when issuing Alt/Az pointings.",
        "lookup_name": "Instead of specifying ra/dec or alt/az, use SIMBAD/Astropy to look up coordinates for celestial body to observe (e.g., 'mars', 'M31').",
        "tle": "Two-line element set for an Earth-orbiting object, given as the two element lines separated by a newline. Set lookup_name to 'TLE' when this is supplied. Requires a mount that can set differential tracking rates.",
        "filter": "Filter name to load before imaging.",
        "focus_shift": "Focus offset relative to the stored best focus.",
        "focus_position": "Absolute focus position override.",
        "n": "Number of exposures in the sequence. If not specified, defaults to infinite exposures until end_time.",
        "guiding": "Start autoguiding with Donuts before imaging. Should be False for solar system objects using non-sidereal tracking, as the star field drifts relative to the guide reference.",
        "nonsidereal_recenter_interval": "Interval in seconds at which the mount re-slews to the target's current ephemeris position, refreshing the tracking rates as it does so. Setting it to 0 suppresses the re-slews and leaves the differential rates to work alone, which does not disable non-sidereal tracking. Has no effect on fixed targets.",
        "nonsidereal_start_lead_time_seconds": "Lead time in seconds for the initial slew, for targets too fast to slew to directly. The mount is sent to the position the target will occupy at start_time plus this value, idles until that moment, and only then begins exposing. Set it to at least the slew and settling time. The default of 0 slews straight at the target, which is accurate to well under an arcsecond for a planet or comet; only satellites, which cross degrees during a slew, need a lead time.",
        "nonsidereal_rate_update_interval": "Minimum interval in seconds between differential rate commands sent to the mount. Astra will not issue a new rate more often than this, however rapidly the ephemeris changes. Lengthen it for mounts that stutter when a rate is applied; shorten it for targets whose rate changes over seconds, such as satellites in low orbit. Defaults to 10 seconds.",
        "pointing": "Perform pointing correction with twirl before imaging.",
        "bin": "Camera binning factor.",
        "dir": "Base directory path for saving images.",
        "execute_parallel": "Execute action in parallel mode when supported.",
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
            raise ValueError(
                f"Missing required fields: {missing} in {self.__class__.__name__}"
            )

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

        if self.nonsidereal_start_lead_time_seconds < 0:
            raise ValueError(
                "nonsidereal_start_lead_time_seconds must be >= 0, "
                f"got {self.nonsidereal_start_lead_time_seconds}"
            )

        if (
            self.nonsidereal_rate_update_interval is not None
            and self.nonsidereal_rate_update_interval < 0
        ):
            raise ValueError(
                "nonsidereal_rate_update_interval must be >= 0, "
                f"got {self.nonsidereal_rate_update_interval}"
            )

        # A TLE and lookup_name 'TLE' only make sense together. Catching this here
        # gives a clear message. Left unchecked, the name 'TLE' would fall through to
        # a SIMBAD lookup and fail with an unrelated name-resolution error.
        is_tle_name = self.lookup_name is not None and self.lookup_name.upper() == "TLE"
        if is_tle_name and self.tle is None:
            raise ValueError(
                "lookup_name is 'TLE' but no 'tle' was given. Supply the two element "
                "lines in 'tle', separated by a newline."
            )
        if self.tle is not None and not is_tle_name:
            raise ValueError(
                f"'tle' was given but lookup_name is {self.lookup_name!r}. "
                "Set lookup_name to 'TLE' to track a target from its element set."
            )

        # A satellite is somewhere different every second, so a fixed coordinate
        # cannot describe where to point at it. Given both, the mount would be sent
        # to the TLE position and the fixed one silently ignored.
        if self.tle is not None and (has_radec or has_altaz):
            raise ValueError(
                "A 'tle' cannot be combined with fixed 'ra'/'dec' or 'alt'/'az' "
                "coordinates. A satellite has no fixed position, so give the "
                "element set alone."
            )

    def _resolve_lookup_name(
        self,
        start_time: Time,
        end_time: Time,
        observatory_location: EarthLocation,
        nonsidereal_supported: bool | None = None,
        coordinates_needed: bool = True,
    ) -> tuple[float | None, float | None]:
        """Resolve lookup_name to (ra_deg, dec_deg) at start_time.

        For solar system bodies and minor bodies (comets/asteroids), pre-computes
        the ephemeris interpolators and sets _nonsidereal=True as a side effect.
        For fixed targets (stars, DSOs), falls back to a name resolver and sets
        _nonsidereal=False.

        Args:
            nonsidereal_supported: Whether any mount in the observatory can accept
                differential tracking rates. ``None`` means unknown (no connected
                devices) and is treated as supported.
            coordinates_needed: Whether the caller wants a position back. A caller
                that already has fixed coordinates only needs to know whether the
                name moves, so the name resolver is not called for a fixed target.

        Returns:
            (ra_deg, dec_deg) at start_time, or (None, None) for a fixed target
            when coordinates_needed is False.

        Raises:
            ValueError: If the name resolves to a moving body but no mount supports
                differential rates.
        """
        duration_hours = ephemeris_window_hours(start_time, end_time)
        try:
            (
                self._ra_interp,
                self._dec_interp,
                self._ra_rate_interp,
                self._dec_rate_interp,
            ) = precompute_ephemeris(
                self.lookup_name,
                start_time,
                duration_hours,
                observatory_location,
                # Let the sampling interval follow the target's own sky motion. A
                # planet needs a sample a minute and a satellite one every few
                # seconds, and too coarse a grid leaves the interpolated position
                # degrees out. nonsidereal_recenter_interval controls only when the
                # telescope physically re-slews, and must not be conflated with the
                # ephemeris resolution.
                None,
                self.tle,
                return_rates=True,
            )
            self._nonsidereal = True
            self._ephemeris_epoch = start_time
            ra = float(self._ra_interp(0.0)) % 360.0
            dec = float(self._dec_interp(0.0))
        except NotMovingBodyError as e:
            self._nonsidereal = False
            self._ra_rate_interp = None
            self._dec_rate_interp = None
            self._ephemeris_epoch = None

            # Say why the name was not treated as a moving body. An ambiguous
            # Horizons match, for example, would otherwise be resolved as a star
            # without any trace of the Horizons message.
            logger.info(
                f"'{self.lookup_name}' is not a moving body, resolving it as a fixed "
                f"target: {e}"
            )

            if coordinates_needed:
                target_coord = get_body_coordinates(
                    body_name=self.lookup_name,
                    obs_time=start_time,
                    obs_location=observatory_location,
                )
                ra = target_coord.ra.deg
                dec = target_coord.dec.deg
            else:
                ra = dec = None

        if self._nonsidereal and nonsidereal_supported is False:
            # The resolver says this body moves, so tracking it sidereally would
            # trail it across the exposure. Refuse the schedule instead of quietly
            # producing smeared frames all night.
            raise ValueError(
                f"Target '{self.object}' (lookup_name='{self.lookup_name}') resolves "
                "to a moving body and needs non-sidereal tracking, but no telescope "
                "in this observatory supports differential tracking rates "
                "(CanSetRightAscensionRate / CanSetDeclinationRate are False)."
            )

        return ra, dec

    def validate_visibility(
        self,
        start_time: Time,
        end_time: Time,
        observatory_location: EarthLocation,
        min_altitude: float = 0.0,
        nonsidereal_supported: bool | None = None,
    ) -> None:
        """Validate that the target is visible during the scheduled observation window.

        Samples the target's altitude across the window, starting at the exact
        start time and ending at the exact end time, and reports every sample below
        the limit. A moving target is followed through its ephemeris, so the
        altitude is that of the body itself at each moment, not of its start-time
        position.

        Args:
            start_time: Observation start time as astropy Time object
            end_time: Observation end time as astropy Time object
            observatory_location: Observatory location as EarthLocation object
            min_altitude: Minimum altitude in degrees for target to be considered visible (default: 0°)
            nonsidereal_supported: Whether any telescope in the observatory can accept
                differential tracking rates (ASCOM CanSetRightAscensionRate and
                CanSetDeclinationRate). ``None`` means unknown -- no devices are
                connected -- and leaves non-sidereal resolution enabled.

        Raises:
            ValueError: If the target is below the minimum altitude at any sampled
                time, if lookup_name resolves to a moving body that no mount can
                track, or if lookup_name resolves to a moving body while fixed
                coordinates are also given.

        Note:
            If RA/Dec are not provided, attempts to resolve them from 'lookup_name'
            or 'alt'/'az' parameters using the start time.
        """
        ra = self.ra
        dec = self.dec
        has_fixed_coords = (ra is not None and dec is not None) or (
            self.alt is not None and self.az is not None
        )

        if self.lookup_name is not None:
            # Resolve the name even when fixed coordinates are also given. Only the
            # resolution says whether the target moves, and a moving target cannot
            # be described by a fixed coordinate.
            resolved_ra, resolved_dec = self._resolve_lookup_name(
                start_time,
                end_time,
                observatory_location,
                nonsidereal_supported=nonsidereal_supported,
                coordinates_needed=ra is None or dec is None,
            )

            if self._nonsidereal and has_fixed_coords:
                # The mount would be driven from the ephemeris and the fixed
                # coordinate ignored, so the check would report on a position the
                # telescope never visits.
                raise ValueError(
                    f"Target '{self.object}' has both lookup_name="
                    f"'{self.lookup_name}', which resolves to a moving body, and "
                    "fixed 'ra'/'dec' or 'alt'/'az' coordinates. A moving target is "
                    "tracked from its ephemeris, so remove the fixed coordinates or "
                    "remove lookup_name."
                )

            if ra is None or dec is None:
                ra, dec = resolved_ra, resolved_dec

        elif (
            (ra is None or dec is None) and self.alt is not None and self.az is not None
        ):
            # Convert Alt/Az to RA/Dec for proper visibility checking over time
            # We assume the telescope will track the RA/Dec coordinate corresponding
            # to this Alt/Az at the start time.
            altaz_coord = SkyCoord(
                alt=u.Quantity(self.alt, u.deg),
                az=u.Quantity(self.az, u.deg),
                frame=AltAz(obstime=start_time, location=observatory_location),
            )
            radec_coord = altaz_coord.transform_to("icrs")
            ra = radec_coord.ra.deg
            dec = radec_coord.dec.deg

        # Only check visibility if we have valid coordinates
        if ra is None or dec is None:
            return

        elapsed_s, check_times = self._visibility_sample_times(start_time, end_time)

        # For non-sidereal targets, read the ephemeris interpolators at every sample
        # so that the object's actual position is used rather than its start-time
        # coordinates. A fixed target keeps one position, which broadcasts against
        # the sampled times.
        if (
            self._nonsidereal
            and self._ra_interp is not None
            and self._dec_interp is not None
        ):
            target = SkyCoord(
                ra=u.Quantity(np.asarray(self._ra_interp(elapsed_s)) % 360.0, "deg"),
                dec=u.Quantity(np.asarray(self._dec_interp(elapsed_s)), "deg"),
                frame="icrs",
            )
        else:
            target = SkyCoord(
                ra=u.Quantity(ra, "deg"),
                dec=u.Quantity(dec, "deg"),
                frame="icrs",
            )

        altaz_frame = AltAz(obstime=check_times, location=observatory_location)
        altitudes = np.atleast_1d(
            np.asarray(target.transform_to(altaz_frame).alt.deg, dtype=float)  # type: ignore
        )
        below = altitudes < min_altitude

        visibility_issues = []
        if below[0]:
            visibility_issues.append(
                f"start: altitude {altitudes[0]:.1f}° (below {min_altitude:.1f}° limit)"
            )
        if below[-1]:
            visibility_issues.append(
                f"end: altitude {altitudes[-1]:.1f}° (below {min_altitude:.1f}° limit)"
            )
        if below.any():
            worst = int(np.argmin(altitudes))
            first = int(np.argmax(below))
            visibility_issues.append(
                f"below the limit at {int(below.sum())} of {below.size} sampled "
                f"times, first at {check_times[first].iso}, worst "
                f"{altitudes[worst]:.1f}° at {check_times[worst].iso}"
            )

        if visibility_issues:
            coord_str = (
                f"(non-sidereal, lookup_name='{self.lookup_name}')"
                if self._nonsidereal
                else f"at RA={ra:.2f}°, Dec={dec:.2f}°"
            )
            raise ValueError(
                f"Target '{self.object}' {coord_str} "
                f"is not visible during observation window:\n  "
                + "\n  ".join(visibility_issues)
            )

    @staticmethod
    def _visibility_sample_times(
        start_time: Time, end_time: Time
    ) -> tuple[np.ndarray, Time]:
        """Return the times the altitude check tests, over the whole window.

        The first and last samples are the exact start and end of the window, so
        those two moments are always reported on their own. The samples in between
        follow a fixed cadence, up to a ceiling on their number.

        Args:
            start_time: Start of the observation window.
            end_time: End of the observation window.

        Returns:
            (elapsed_seconds, times): Offsets from start_time in seconds, and the
            matching astropy Time array.
        """
        span_s = max((end_time - start_time).to_value(u.s), 0.0)
        # Round rather than truncate: a two hour window measured through astropy
        # comes back a fraction of a microsecond short of 7200 s, which would
        # otherwise drop a sample and stretch the cadence past the interval.
        n_samples = round(span_s / _VISIBILITY_SAMPLE_INTERVAL_S) + 1
        n_samples = min(max(n_samples, 2), _VISIBILITY_MAX_SAMPLES)
        elapsed_s = np.linspace(0.0, span_s, n_samples)
        return elapsed_s, start_time + u.Quantity(elapsed_s, u.s)


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
        "filter": "Filter name to load before imaging.",
        "dir": "Base directory path for saving images.",
        "bin": "Camera binning factor.",
        "execute_parallel": "Execute action in parallel mode when supported.",
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
            raise ValueError(
                f"Missing required fields: {missing} in {self.__class__.__name__}"
            )

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
            - Opens observatory if not already done by a prior action
        3. Capture exposures and re-position between frames
        4. Iterate through requested filters while auto-adjusting exposure times
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
        "dir": "Base directory path for saving images.",
        "bin": "Camera binning factor.",
        "execute_parallel": "Execute action in parallel mode when supported.",
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
            raise ValueError(
                f"Missing required fields: {missing} in {self.__class__.__name__}"
            )

        # ensure filter and n have the same length
        if len(self.filter) != len(self.n):
            raise ValueError(
                f"'filter' and 'n' must have the same length. Got: filter={self.filter}, n={self.n}"
            )

        # Subframe validation
        self.validate_subframe()


@dataclass
class CalibrateGuidingActionConfig(BaseActionConfig):
    """Calibrate guiding parameters using timed guide pulses.

    Steps:
        1. Slews telescope to RA = LST - 1 hour, Dec = 0° at the start of sequence
            - Opens observatory if not already done by a prior action
        2. Issues a series of guide pulses in each cardinal direction with specified duration and settling time
        3. Captures exposures after each pulse and measures star shifts to determine pixel-to-time scales and camera orientation relative to mount axes
        4. Averages results over specified number of cycles
        5. Saves calibration parameters in the observatory configuration for use in guiding

    """

    filter: Optional[str] = None
    pulse_time: int = 5000
    exptime: float = 1.0
    settle_time: float = 1.0
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
        "number_of_cycles": "How many calibration cycles to take average over.",
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
    """Aid building a telescope pointing model. Astra itself does not build or maintain
    a pointing model.

    Captures a spiral of points from zenith down to 30° altitude while
    avoiding positions within 20° of the Moon.

    Plate solves each pointing and sends SyncToCoordinates commands to the mount. The
    receipt of these commands can be used to build a pointing model in the mount
    control software. The action can be configured to use the local star catalog for
    plate solving to speed up the process if the online Gaia catalog is unavailable or
    slow.
    """

    n: int = 50
    exptime: float = 3.0
    dark_subtraction: bool = False
    object: str = "Pointing Model"
    use_local_db: bool = False
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
        "dark_subtraction": "Enable dark subtraction using previously taken calibration frames of same exposure time in the same date folder.",
        "object": "Descriptive label for the pointing run.",
        "use_local_db": "Use local star catalog database for plate solving (faster).",
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
            - Opens observatory if not already done by a prior action
        2. Move the telescope if needed
        3. Capture images at different focus positions
        4. Measure star sharpness in each image
        5. Fit a curve to determine optimal focus
        6. Save plots/results and save the best focus position in the observatory configuration

    Note:
        Coarse searches (for example `fft`, `normalized_variance`) use non-parametric
        focus measures that characterise overall frame sharpness and are well suited
        for very broad search ranges where stars appear as large, defocused "donuts".
        Analytic response-function autofocusers (for example `HFR`/StarSize), which fit
        a V-curve to measured star sizes, are better for fine-tuning near the focus
        peak but can give incorrect results if applied over an excessively large range
        because the assumed response model may not fit across the whole span.
    """

    exptime: float | int = field(default=3.0)
    filter: Optional[str] = None
    bin: int = 1
    reduce_exposure_time: bool = False
    search_range: Optional[List[int] | int] = None
    search_range_is_relative: bool = False
    n_steps: List[int] = field(default_factory=lambda: [30, 20])
    n_exposures: List[int] | int = field(default_factory=lambda: [1, 1])
    decrease_search_range: bool = True
    star_find_threshold: float | int = 5.0
    fwhm: Optional[int] = None
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
        "filter": "Filter to use during autofocus procedure.",
        "bin": "Camera binning factor.",
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
        "fwhm": (
            "DAOStarFinder FWHM of the Gaussian kernel in pixels. If not set, derived "
            'from the camera/telescope plate scale assuming ~2" seeing.'
        ),
        "percent_to_cut": "Percentage of worst-performing focus samples to drop when shrinking the range.",
        "focus_measure_operator": "Focus metric to optimize (e.g., hfr, gauss, tenengrad, fft, normalized_variance).",
        "focus_measure_operator_note": (
            "Prefer non-parametric metrics (e.g., 'fft', 'normalized_variance') for coarse/broad searches; "
            "use analytic measures (e.g., 'HFR') for fine tuning near the focus peak."
        ),
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
            "exptime": 1.0,
            "filter": "V",
            "focus_measure_operator": "HFR",
            "search_range_is_relative": True,
            "search_range": 1000,
            "n_steps": [30, 20],
            "n_exposures": [1, 1],
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

        n_steps = kwargs.get("n_steps")
        if n_steps is None:
            n_steps = field_default(cls, "n_steps")
        n_exposures = kwargs.get("n_exposures")
        if n_exposures is None:
            n_exposures = field_default(cls, "n_exposures")

        if isinstance(n_exposures, int):
            # A single value means the same number of exposures for each sweep.
            n_exposures = [n_exposures] * len(n_steps)
        elif len(n_exposures) != len(n_steps):
            if logger is not None:
                logger.warning(
                    "'n_exposures' length does not match 'n_steps' length. "
                    "Defaulting to 1 exposure per step."
                )
            n_exposures = [1] * len(n_steps)

        kwargs["n_steps"] = n_steps
        kwargs["n_exposures"] = n_exposures

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
