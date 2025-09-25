"""
Astronomical image processing and FITS file management utilities.

This module provides functions for handling astronomical images captured from
observatory cameras. It manages image directory creation, data type conversion,
and FITS file saving with proper headers and metadata.

Key features:
- Automatic directory creation with date-based naming
- Image data type conversion and array reshaping for FITS compatibility
- FITS file saving with comprehensive metadata and WCS support
- Intelligent filename generation based on observation parameters

The module handles various image types including light frames, bias frames,
dark frames, and calibration images, ensuring proper metadata preservation
and file organization for astronomical data processing pipelines.

"""

import datetime
from dataclasses import dataclass, field

from jinja2 import Template

__all__ = ["FilenameTemplates", "JinjaFilenameTemplates"]


@dataclass
class FilenameTemplates:
    """Filename templates using Python str.format() syntax.

    The templates can be customised by passing a dictionary to
    `FilenameTemplates.from_dict()`, which is the constructor used in astra.
    If the templates contain Jinja2 syntax, the `JinjaFilenameTemplates` class will
    be used instead, which allows more advanced logic (see examples below).

    Examples:

    >>> from astra.image_handler import FilenameTemplates

    Default templates
    >>> templates = FilenameTemplates()
    >>> templates.render_filename(
    ... **templates.TEST_KWARGS | {"action_type": "object", "imagetype": "light"}
    ... )
    '20240101/TestCamera_TestFilter_TestObject_300.123_2025-01-01_00-00-00.fits'

    Lets create a template with more advanced logic using using Jinja2
    >>> flat_template = (
    ...     # use subdirs
    ...     "{{ imagetype.split('_')[0].upper() }}/{{ device }}_"
    ...     # customise timestamp format
    ...     + "{{ datetime_timestamp.strftime('%Y%m%d_%H%M%S.%f')[:-5] }}_"
    ...     # Add custom logic
    ...     + "{{ 'Dusk' if (datetime_timestamp + datetime.timedelta(hours=5)).hour > 12 else 'Dawn' }}"
    ...     + "_sequence_{{ '%03d'|format(sequence_counter) }}"
    ...     + ".fits"
    ... )
    >>> filename_templates = FilenameTemplates.from_dict(
    ...     {"flats": flat_template}
    ... )
    >>> filename_templates.render_filename(
    ...     **filename_templates.TEST_KWARGS | {
    ...         "action_type": "flats", "imagetype": "Flat Frame"
    ...     }
    ... )
    'FLAT/TestCamera_20250101_000000.0_Dawn_sequence_000.fits'

    """

    object: str = "{action_date}/{device}_{filter_name}_{object_name}_{exptime:.3f}_{timestamp}.fits"
    calibration: str = (
        "{action_date}/{device}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
    )
    flats: str = "{action_date}/{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
    autofocus: str = "autofocus/{action_date}/{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
    calibrate_guiding: str = "calibrate_guiding/{action_date}/{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
    pointing_model: str = "pointing_model/{action_date}/{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
    default: str = "{action_date}/{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"

    TEST_KWARGS = {
        "action_type": "object",
        "device": "TestCamera",
        "filter_name": "TestFilter",
        "object_name": "TestObject",
        "imagetype": "light",
        "exptime": 300.123456,
        "timestamp": "2025-01-01_00-00-00",
        "datetime_timestamp": datetime.datetime(2025, 1, 1, 0, 0, 0, 0),
        "action_date": "20240101",
        "action_datetime": datetime.datetime(2024, 1, 1, 0, 0, 0, 0),
        "datetime": datetime,
        "sequence_counter": 0,
    }
    SUPPORTED_ACTION_TYPES = [
        "object",
        "calibration",
        "flats",
        "autofocus",
        "calibrate_guiding",
        "pointing_model",
        "default",
    ]
    SUPPORTED_IMAGETYPES = ["light", "bias", "dark", "flat", "default"]

    @property
    def SUPPORTED_ARGS(self) -> set[str]:
        return set(self.TEST_KWARGS.keys())

    def __post_init__(self):
        if self._has_jinja_templates(
            [getattr(self, key) for key in self.SUPPORTED_ACTION_TYPES]
        ):
            raise ValueError(
                "FilenameTemplates contains Jinja2 syntax. "
                "Please use JinjaFilenameTemplates class instead."
            )
        self._validate()

    @classmethod
    def from_dict(cls, template_dict: dict[str, str]) -> "FilenameTemplates":
        valid_keywords = {
            key: value
            for key, value in template_dict.items()
            if key in cls.SUPPORTED_ACTION_TYPES
        }

        if cls._has_jinja_templates(list(valid_keywords.values())):
            return JinjaFilenameTemplates(**valid_keywords)  # type: ignore

        return cls(**valid_keywords)

    def render_filename(self, action_type, **kwargs) -> str:
        imagetype_standardised = self._get_imagetype(kwargs.pop("imagetype"))

        return getattr(self, action_type).format(
            imagetype=imagetype_standardised, **kwargs
        )

    def _get_imagetype(self, imagetype: str) -> str:
        imagetype_lower = imagetype.lower()
        for name in self.SUPPORTED_IMAGETYPES:
            if name in imagetype_lower:
                return name
        return "default"

    def _validate(self):
        for action_type in self.SUPPORTED_ACTION_TYPES:
            try:
                self.render_filename(**self.TEST_KWARGS | {"action_type": action_type})
            except Exception as e:
                raise ValueError(
                    f"Error rendering template for '{action_type}'. "
                    f"Template: '{getattr(self, action_type)}'. Exception: {e}."
                )

    @staticmethod
    def _has_jinja_templates(templates: list) -> bool:
        return any(["{{" in item and "}}" in item for item in templates])


@dataclass
class JinjaFilenameTemplates(FilenameTemplates):
    """Filename templates using Jinja2 syntax.

    Examples:

    Lets create a template with more advanced logic using using Jinja2
    >>> from astra.image_handler import JinjaFilenameTemplates
    >>> flat_template = (
    ...     # use subdirs
    ...     "{{ imagetype.split('_')[0].upper() }}/{{ device }}_"
    ...     # customise timestamp format
    ...     + "{{ datetime_timestamp.strftime('%Y%m%d_%H%M%S.%f')[:-5] }}_"
    ...     # Add custom logic
    ...     + "{{ 'Dusk' if (datetime_timestamp + datetime.timedelta(hours=5)).hour > 12 else 'Dawn' }}"
    ...     + "_sequence_{{ '%03d'|format(sequence_counter) }}"
    ...     + ".fits"
    ... )
    >>> filename_templates = FilenameTemplates.from_dict(
    ...     {"flats": flat_template}
    ... )
    >>> filename_templates.render_filename(
    ...     **filename_templates.TEST_KWARGS | {
    ...         "action_type": "flats", "imagetype": "Flat Frame"
    ...     }
    ... )
    'FLAT/TestCamera_20250101_000000.0_Dawn_sequence_000.fits'

    """

    object: str = "{{ action_date }}/{{ device }}_{{ filter_name }}_{{ object_name }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
    calibration: str = "{{ action_date }}/{{ device }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
    flats: str = "{{ action_date }}/{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
    autofocus: str = "autofocus/{{ action_date }}/{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
    calibrate_guiding: str = "calibrate_guiding/{{ action_date }}/{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
    pointing_model: str = "pointing_model/{{ action_date }}/{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
    default: str = "{{ action_date }}/{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"

    _compiled_templates: dict[str, Template] = field(default_factory=dict)

    def __post_init__(self):
        self._validate_templates()
        self._compiled_templates = {}

        for name in self.SUPPORTED_ACTION_TYPES:
            template_str = getattr(self, name)
            self._compiled_templates[name] = Template(template_str)

        self._validate()

    def render_filename(self, action_type, **kwargs) -> str:
        imagetype_standardised = self._get_imagetype(kwargs.pop("imagetype"))

        return self._compiled_templates[action_type].render(
            imagetype=imagetype_standardised, **kwargs
        )

    def _validate_templates(self):
        import re

        pattern = re.compile(r"{{\s*([\w]+)[^}]*}}")
        for name in self.SUPPORTED_ACTION_TYPES:
            template = getattr(self, name)
            if not isinstance(template, str):
                continue
            for match in pattern.findall(template):
                if match not in self.SUPPORTED_ARGS:
                    raise ValueError(
                        f"Template '{name}' uses unsupported argument: {{{{{match}}}}}."
                    )
