import typing
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional, Union

import astropy.units as u
import numpy as np
from astropy.coordinates import Angle, SkyCoord
from astropy.time import Time

from astra.config import Config


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

    def __post_init__(self):
        self.validate()

    @classmethod
    def from_dict(cls, config_dict: dict, logger=None, default_dict: dict = {}):
        kwargs = cls.merge_config_dicts(config_dict, default_dict)

        if logger is not None:
            logger.debug(f"Extracting action values {kwargs} for {cls.__name__}")

        return cls(**kwargs)

    def validate(self):
        missing = []
        type_errors = []
        for f in self.__dataclass_fields__.values():
            val = getattr(self, f.name)
            # Check required fields
            if f.metadata.get("required") and val is None:
                missing.append(f.name)
            # Type validation for all fields
            err = self.validate_type(f)
            if err:
                type_errors.append(err)
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
        if type_errors:
            raise TypeError(f"Type errors in fields: {type_errors}")

    def validate_type(self, f):
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
        return getattr(self, key, default)

    def __getitem__(self, key: str):
        return getattr(self, key)

    def __setitem__(self, key: str, value):
        return setattr(self, key, value)

    def __contains__(self, key: str):
        return hasattr(self, key)

    def keys(self):
        return [
            item
            for item in self.__dataclass_fields__.keys()
            if not item.startswith("_")
        ]

    def __iter__(self):
        return iter(self.keys())

    def __len__(self):
        return len(self.keys())

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
    def validate(self):
        pass


@dataclass
class CloseActionConfig(BaseActionConfig):
    def validate(self):
        pass


@dataclass
class CompleteHeadersActionConfig(BaseActionConfig):
    def validate(self):
        pass


@dataclass
class CoolCameraActionConfig(BaseActionConfig):
    def validate(self):
        pass


@dataclass
class ObjectActionConfig(BaseActionConfig):
    object: str = field(metadata={"required": True})
    exptime: float = field(metadata={"required": True})
    ra: Optional[float] = None
    dec: Optional[float] = None
    filter: Optional[str] = None
    focus_shift: Optional[float] = None
    focus_position: Optional[float] = None
    n: Optional[int] = None
    guiding: bool = False
    pointing: bool = False
    bin: int = 1
    dir: Optional[str] = None

    def validate(self):
        missing = []
        for f in self.__dataclass_fields__.values():
            if f.metadata.get("required") and getattr(self, f.name) is None:
                missing.append(f.name)
        if missing:
            raise ValueError(f"Missing required fields: {missing}")


@dataclass
class CalibrationActionConfig(BaseActionConfig):
    exptime: List[float] = field(default_factory=list, metadata={"required": True})
    n: List[int] = field(default_factory=list, metadata={"required": True})
    filter: Optional[str] = None
    dir: Optional[str] = None
    bin: int = 1

    def validate(self):
        missing = []
        for f in self.__dataclass_fields__.values():
            if f.metadata.get("required") and (
                getattr(self, f.name) is None or getattr(self, f.name) == []
            ):
                missing.append(f.name)
        if missing:
            raise ValueError(f"Missing required fields: {missing}")


@dataclass
class FlatsActionConfig(BaseActionConfig):
    filter: List[str] = field(default_factory=list, metadata={"required": True})
    n: List[int] = field(default_factory=list, metadata={"required": True})
    dir: Optional[str] = None
    bin: int = 1

    def validate(self):
        missing = []
        for f in self.__dataclass_fields__.values():
            if f.metadata.get("required") and (
                getattr(self, f.name) is None or getattr(self, f.name) == []
            ):
                missing.append(f.name)
        if missing:
            raise ValueError(f"Missing required fields: {missing}")


@dataclass
class CalibrateGuidingActionConfig(BaseActionConfig):
    filter: Optional[str] = None
    pulse_time: float = 5000.0
    exptime: float = 5.0
    settle_time: float = 10.0
    number_of_cycles: int = 10
    focus_shift: Optional[float] = None
    focus_position: Optional[float] = None
    bin: int = 1

    def validate(self):
        pass


@dataclass
class PointingModelActionConfig(BaseActionConfig):
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

    def validate(self):
        pass


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
    fov_height: float | int = 11.666666 / 60
    fov_width: float | int = 11.666666 / 60
    selection_method: SelectionMethod = SelectionMethod.SINGLE
    use_gaia: bool = True
    observation_time: Optional[Time] = None
    maximal_number_of_stars: int = 100_000
    ra: Optional[float | int] = None
    dec: Optional[float | int] = None
    _coordinates: Optional[SkyCoord] = None

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
    exptime: float | int = field(default=3.0, metadata={"required": True})
    reduce_exposure_time: bool = False
    search_range: Optional[List[int] | int] = None
    search_range_is_relative: bool = False
    n_steps: List[int] = field(default_factory=lambda: [30, 20])
    n_exposures: List[int] = field(default_factory=lambda: [1, 1])
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
        default_factory=AutofocusCalibrationFieldConfig, metadata={"required": True}
    )
    _save_path: Optional[Path] = None
    _focus_measure_operator = None
    _secondary_focus_measure_operators = {}

    def __post_init__(self) -> None:
        from astrafocus import FocusMeasureOperatorRegistry

        self._secondary_focus_measure_operators = {
            operator.name: operator
            for operator in [
                FocusMeasureOperatorRegistry.get(key)()
                for key in self.secondary_focus_measure_operators
                if key in FocusMeasureOperatorRegistry.list()
            ]
        }
        self._focus_measure_operator = FocusMeasureOperatorRegistry.from_name(
            self.focus_measure_operator
        )
        self.validate()

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

        return cls(**kwargs)

    @property
    def save_path(self) -> Path:
        if self._save_path is None:
            date = datetime.now().strftime("%Y%m%d")
            self._save_path = Config().paths.images / "autofocus_ref" / date
            self._save_path.mkdir(exist_ok=True, parents=True)

        return self._save_path

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
    "object": OpenActionConfig,
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
