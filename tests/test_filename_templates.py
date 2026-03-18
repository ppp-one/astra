"""Tests for filename templates."""

import datetime

import pytest

import doctest
import astra.filename_templates

from astra.filename_templates import FilenameTemplates, JinjaFilenameTemplates


class TestFilenameTemplate:
    def test_doctest_filename_templates(self):
        failures, _ = doctest.testmod(astra.filename_templates, raise_on_error=False)
        assert failures == 0, "Doctest failures in filename_templates.py"

    def test_filename_templates(self):
        templates = FilenameTemplates()
        jinja_templates = JinjaFilenameTemplates()
        test_types = ["light", "bias", "dark", "flat", "default"]
        action_types = {
            "light": "object",
            "bias": "calibration",
            "dark": "calibration",
            "flat": "flats",
            "default": "default",
        }
        expected = {
            "light": "20240101/TestCamera_TestFilter_TestObject_300.123_2025-01-01_00-00-00.fits",
            "bias": "20240101/TestCamera_bias_300.123_2025-01-01_00-00-00.fits",
            "dark": "20240101/TestCamera_dark_300.123_2025-01-01_00-00-00.fits",
            "flat": "20240101/TestCamera_TestFilter_flat_300.123_2025-01-01_00-00-00.fits",
            "default": "20240101/TestCamera_TestFilter_default_300.123_2025-01-01_00-00-00.fits",
        }
        for imagetype in test_types:
            action_type = action_types[imagetype]
            filename = templates.render_filename(
                **templates.TEST_KWARGS
                | {"imagetype": imagetype, "action_type": action_type}
            )

            assert filename == expected[imagetype], (
                f"For {imagetype}, got {filename}, expected {expected[imagetype]}"
            )

            assert filename == jinja_templates.render_filename(
                **jinja_templates.TEST_KWARGS
                | {"imagetype": imagetype, "action_type": action_type}
            ), (
                "JinjaFilenameTemplates template does not match standard template. "
                f"For {imagetype}, got {filename}, expected {expected[imagetype]}"
            )

    def test_filename_template_with_subdir(self):
        templates = JinjaFilenameTemplates(
            calibration="{{ imagetype.split(' ')[0].upper() }}/"
            + JinjaFilenameTemplates.calibration
        )
        filename = templates.render_filename(
            **templates.TEST_KWARGS
            | {"imagetype": "dark", "action_type": "calibration"}
        )
        expected = "DARK/20240101/TestCamera_dark_300.123_2025-01-01_00-00-00.fits"
        assert filename == expected, f"Got {filename}, expected {expected}"

    def test_filename_templates_invalid_action_type(self):
        templates = FilenameTemplates()
        with pytest.raises(ValueError, match="Invalid action_type"):
            templates.render_filename(action_type="invalid", imagetype="light")

    def test_invalid_action_type_consistent_for_standard_and_jinja(self):
        templates = FilenameTemplates()
        jinja_templates = JinjaFilenameTemplates()

        with pytest.raises(ValueError, match="Invalid action_type"):
            templates.render_filename(action_type="invalid", imagetype="light")

        with pytest.raises(ValueError, match="Invalid action_type"):
            jinja_templates.render_filename(action_type="invalid", imagetype="light")

    def test_from_dict_dispatches_by_template_syntax(self):
        plain_templates = FilenameTemplates.from_dict(
            {"object": "{device}_{object_name}_{timestamp}.fits"}
        )
        assert isinstance(plain_templates, FilenameTemplates)
        assert not isinstance(plain_templates, JinjaFilenameTemplates)

        jinja_templates = FilenameTemplates.from_dict(
            {"object": "{{ device }}_{{ object_name }}_{{ timestamp }}.fits"}
        )
        assert isinstance(jinja_templates, JinjaFilenameTemplates)

    def test_from_dict_ignores_unknown_keys(self):
        templates = FilenameTemplates.from_dict(
            {
                "object": "{device}_{object_name}_{timestamp}.fits",
                "not_a_supported_action": "{{ this_should_be_ignored }}",
            }
        )

        assert isinstance(templates, FilenameTemplates)
        assert (
            templates.render_filename(
                **templates.TEST_KWARGS
                | {"action_type": "object", "imagetype": "light"}
            )
            == "TestCamera_TestObject_2025-01-01_00-00-00.fits"
        )

    def test_explicit_supported_action_type_templates(self):
        expected = {
            "autofocus": "autofocus/20240101/TestCamera_TestFilter_light_300.123_2025-01-01_00-00-00.fits",
            "calibrate_guiding": "calibrate_guiding/20240101/TestCamera_TestFilter_light_300.123_2025-01-01_00-00-00.fits",
            "pointing_model": "pointing_model/20240101/TestCamera_TestFilter_light_300.123_2025-01-01_00-00-00.fits",
            "default": "20240101/TestCamera_TestFilter_light_300.123_2025-01-01_00-00-00.fits",
        }

        for templates in (FilenameTemplates(), JinjaFilenameTemplates()):
            for action_type, expected_filename in expected.items():
                filename = templates.render_filename(
                    **templates.TEST_KWARGS
                    | {"action_type": action_type, "imagetype": "light"}
                )
                assert filename == expected_filename

    def test_saintex_filename_template(self):
        templates = JinjaFilenameTemplates(
            object="{{ action_date }}/Raw/{{ datetime_timestamp.strftime('%Y%m%d') }}-{{ datetime_timestamp.strftime('%H%M%S') }}-{{ object_name }}-S001-R001-C{{ '%03d'|format(sequence_counter) }}-{{ filter_name }}.fts",
            calibration="{{ action_date }}/{{ imagetype.capitalize() }}/{{ datetime_timestamp.strftime('%Y%m%d') }}-{{ datetime_timestamp.strftime('%H%M%S') }}-{{ imagetype.capitalize() }}-S001-R001-C{{'%03d'|format(sequence_counter)}}-NoFilt.fts",
            flats="{{ action_date }}/Flat/{{ datetime_timestamp.strftime('%Y%m%d') }}-{{ datetime_timestamp.strftime('%H%M%S') }}-{{  'Dusk' if (datetime_timestamp + datetime.timedelta(hours=-8)).hour > 12 else 'Dawn' }}-{{ filter_name }}-Bin1-Temp_-60-{{ '%03d'|format(sequence_counter) }}.fts",
        )

        cases = [
            (
                {
                    "imagetype": "dark",
                    "action_type": "calibration",
                    "action_date": "20260317",
                    "datetime_timestamp": datetime.datetime(
                        2026, 3, 17, 23, 45, 1, 123
                    ),
                    "sequence_counter": 42,
                },
                "20260317/Dark/20260317-234501-Dark-S001-R001-C042-NoFilt.fts",
            ),
            (
                {
                    "imagetype": "bias",
                    "action_type": "calibration",
                    "action_date": "20260317",
                    "datetime_timestamp": datetime.datetime(
                        2026, 3, 17, 23, 45, 1, 123
                    ),
                    "sequence_counter": 42,
                },
                "20260317/Bias/20260317-234501-Bias-S001-R001-C042-NoFilt.fts",
            ),
            (
                {
                    "imagetype": "light",
                    "action_type": "object",
                    "object_name": "M31",
                    "action_date": "20260317",
                    "filter_name": "I+z",
                    "datetime_timestamp": datetime.datetime(
                        2026, 3, 17, 23, 45, 1, 123
                    ),
                    "sequence_counter": 42,
                },
                "20260317/Raw/20260317-234501-M31-S001-R001-C042-I+z.fts",
            ),
            (
                {
                    "imagetype": "flat",
                    "action_type": "flats",
                    "action_date": "20260202",
                    "filter_name": "I+z",
                    "datetime_timestamp": datetime.datetime(2026, 2, 3, 1, 41, 8, 123),
                    "sequence_counter": 42,
                },
                "20260202/Flat/20260203-014108-Dusk-I+z-Bin1-Temp_-60-042.fts",
            ),
            (
                {
                    "imagetype": "flat",
                    "action_type": "flats",
                    "action_date": "20260202",
                    "filter_name": "I+z",
                    "datetime_timestamp": datetime.datetime(
                        2026, 2, 3, 14, 13, 34, 123
                    ),
                    "sequence_counter": 42,
                },
                "20260202/Flat/20260203-141334-Dawn-I+z-Bin1-Temp_-60-042.fts",
            ),
        ]

        for case_kwargs, expected in cases:
            filename = templates.render_filename(**templates.TEST_KWARGS | case_kwargs)
            assert filename == expected, (
                f"Got {filename}, expected {expected} for kwargs {case_kwargs}"
            )
