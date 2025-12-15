"""
This module provides two primary classes for generating filenames used by
ASTRA when writing image files:

- ``FilenameTemplates`` — simple templates using Python ``str.format``.
- ``JinjaFilenameTemplates`` — richer templates powered by Jinja2 when templates include
  template logic (``{{ ... }}``) at the cost of a slightly slower template rendering.

Key behavior
------------

- Normalises ``imagetype`` values to a standard set
  (``light``, ``bias``, ``dark``, ``flat``, ``default``).
- Validates templates against a set of test keyword arguments to catch formatting errors
  early.
- Automatically selects the Jinja2-based implementation when input templates contain
  Jinja2 markers.

Quick example
-------------
>>> from astra.image_handler import FilenameTemplates
>>> templates = FilenameTemplates()
>>> templates.render_filename(**templates.TEST_KWARGS)
'20240101/TestCamera_TestFilter_TestObject_300.123_2025-01-01_00-00-00.fits'

For details and advanced examples see the class docstrings for
``FilenameTemplates`` and ``JinjaFilenameTemplates``.

Example configurations
----------------------

Example str.format() configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following is the default used by ASTRA in `observatory_config.yaml`.

.. code-block:: yaml

    filename_templates:
      object: "{action_date}/{device}_{filter_name}_{object_name}_{exptime:.3f}_{timestamp}.fits"
      calibration: "{action_date}/{device}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
      flats: "{action_date}/{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
      autofocus: "autofocus/{action_date}/{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
      calibrate_guiding: "calibrate_guiding/{action_date}/{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
      pointing_model: "pointing_model/{action_date}/{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"
      default: "{action_date}/{device}_{filter_name}_{imagetype}_{exptime:.3f}_{timestamp}.fits"

Example Jinja2 configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following code would be identical to the default used by ASTRA in
`observatory_config.yaml`, but using Jinja2 syntax. See the example in
:class:`JinjaFilenameTemplates` for more advanced examples using Jinja2 logic.

.. code-block:: yaml

    filename_templates:
      object: "{{ action_date }}/{{ device }}_{{ filter_name }}_{{ object_name }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
      calibration: "{{ action_date }}/{{ device }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
      flats: "{{ action_date }}/{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
      autofocus: "autofocus/{{ action_date }}/{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
      calibrate_guiding: "calibrate_guiding/{{ action_date }}/{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
      pointing_model: "pointing_model/{{ action_date }}/{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"
      default: "{{ action_date }}/{{ device }}_{{ filter_name }}_{{ imagetype }}_{{ '%.3f'|format(exptime) }}_{{ timestamp }}.fits"

"""

import datetime
from dataclasses import dataclass, field

from jinja2 import Template

__all__ = ["FilenameTemplates", "JinjaFilenameTemplates"]


@dataclass
class FilenameTemplates:
    """Filename templates using Python :py:meth:`str.format` syntax.

    The templates can be customised by passing a dictionary to
    :meth:`FilenameTemplates.from_dict()`, which is the constructor used in astra.
    If the templates contain Jinja2 syntax, the :py:class:`jinja2.Template` class will
    be used instead, which allows more advanced logic.

    Examples:

        >>> from astra.image_handler import FilenameTemplates

        Default templates

        >>> templates = FilenameTemplates()
        >>> templates.render_filename(
        ... **templates.TEST_KWARGS | {"action_type": "object", "imagetype": "light"}
        ... )
        '20240101/TestCamera_TestFilter_TestObject_300.123_2025-01-01_00-00-00.fits'

        Let's create a template with more advanced logic using :py:class:`jinja2.Template` syntax.
        As the following example illustrates, :py:class:`jinja2.Template` supports more complex logic,
        than :py:meth:`str.format` syntax, at the cost of a slightly slower template
        rendering performance.

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

    See Also:
        :class:`JinjaFilenameTemplates` for more advanced template logic using :py:class:`jinja2.Template`.

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
        """Create FilenameTemplates from a dictionary.

        If the templates contain Jinja2 syntax, the JinjaFilenameTemplates class will
        be used instead.

        Examples:

            Basic Example using :py:meth:`str.format` syntax:

            >>> from astra.image_handler import FilenameTemplates
            >>> templates = FilenameTemplates.from_dict(
            ...     {
            ...         "object": "{device}_{object_name}_{timestamp}.fits",
            ...         "flats": "{device}_FLAT_{timestamp}.fits"
            ...     }
            ... )
            >>> type(templates)
            <class 'astra.filename_templates.FilenameTemplates'>
            >>> templates.render_filename(
            ...     **templates.TEST_KWARGS | {"action_type": "object", "imagetype": "light"}
            ... )
            'TestCamera_TestObject_2025-01-01_00-00-00.fits'
            >>> templates.render_filename(
            ...     **templates.TEST_KWARGS | {"action_type": "flats", "imagetype": "Flat Frame"}
            ... )
            'TestCamera_FLAT_2025-01-01_00-00-00.fits'


            Example using :py:class:`jinja2.Template` syntax:

            >>> from astra.image_handler import FilenameTemplates
            >>> templates = FilenameTemplates.from_dict(
            ...     {
            ...         "object": "{{ device }}_{{ object_name }}_{{ timestamp }}.fits",
            ...         "flats": "{{ device }}_FLAT_{{ timestamp }}.fits"
            ...     }
            ... )
            >>> type(templates)
            <class 'astra.filename_templates.JinjaFilenameTemplates'>
            >>> templates.render_filename(
            ...     **templates.TEST_KWARGS | {"action_type": "object", "imagetype": "light"}
            ... )
            'TestCamera_TestObject_2025-01-01_00-00-00.fits'
            >>> templates.render_filename(
            ...     **templates.TEST_KWARGS | {"action_type": "flats", "imagetype": "Flat Frame"}
            ... )
            'TestCamera_FLAT_2025-01-01_00-00-00.fits'

        """
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
    """Filename templates using :py:class:`jinja2.Template` syntax.

    Examples:

        Let's create a template with more advanced logic using :py:class:`jinja2.Template` syntax.

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
