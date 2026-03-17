"""Unit tests for image_handler module."""

import pytest

from astra.filename_templates import FilenameTemplates, JinjaFilenameTemplates
import datetime


class TestFilenameTemplate:
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
        with pytest.raises(KeyError):  # render_filename expects valid action_type
            templates.render_filename(action_type="invalid", imagetype="light")

    def test_jinja_filename_templates_custom_logic(self):
        templates = JinjaFilenameTemplates(
            flats="{{ 'Flat_' + imagetype + '_' + timestamp if exptime > 10 else 'ShortFlat_' + imagetype }}"
        )
        filename = templates.render_filename(
            **templates.TEST_KWARGS
            | {"action_type": "flats", "imagetype": "Flat Frame", "exptime": 20}
        )
        assert "Flat_Flat Frame_" in filename
        filename_short = templates.render_filename(
            **templates.TEST_KWARGS
            | {"action_type": "flats", "imagetype": "Flat Frame", "exptime": 5}
        )
        assert "ShortFlat_Flat Frame" in filename_short

    def test_jinja_stx(self):
        templates = JinjaFilenameTemplates(
            object="{{ action_date }}/Raw/{{ datetime_timestamp.strftime('%Y%m%d') }}-{{ datetime_timestamp.strftime('%H%M%S') }}-{{ object_name }}-S001-R001-C{{ '%03d'|format(sequence_counter) }}-{{ filter_name }}.fts",
            calibration="{{ action_date }}/{{ imagetype.capitalize() }}/{{ datetime_timestamp.strftime('%Y%m%d') }}-{{ datetime_timestamp.strftime('%H%M%S') }}-{{ imagetype.capitalize() }}-S001-R001-C{{'%03d'|format(sequence_counter)}}-NoFilt.fts",
            flats="{{ action_date }}/Flat/{{ datetime_timestamp.strftime('%Y%m%d') }}-{{ datetime_timestamp.strftime('%H%M%S') }}-{{  'Dusk' if (datetime_timestamp + datetime.timedelta(hours=-8)).hour > 12 else 'Dawn' }}-{{ filter_name }}-Bin1-Temp_-60-{{ '%03d'|format(sequence_counter) }}.fts",
        )
        filename = templates.render_filename(
            **templates.TEST_KWARGS
            | {
                "imagetype": "dark",
                "action_type": "calibration",
                "action_date": "20260317",
                "datetime_timestamp": datetime.datetime(2026, 3, 17, 23, 45, 1, 123),
                "sequence_counter": 42,
            }
        )
        expected = "20260317/Dark/20260317-234501-Dark-S001-R001-C042-NoFilt.fts"
        assert filename == expected, f"Got {filename}, expected {expected}"
        filename = templates.render_filename(
            **templates.TEST_KWARGS
            | {
                "imagetype": "bias",
                "action_type": "calibration",
                "action_date": "20260317",
                "datetime_timestamp": datetime.datetime(2026, 3, 17, 23, 45, 1, 123),
                "sequence_counter": 42,
            }
        )
        expected = "20260317/Bias/20260317-234501-Bias-S001-R001-C042-NoFilt.fts"
        assert filename == expected, f"Got {filename}, expected {expected}"
        filename = templates.render_filename(
            **templates.TEST_KWARGS
            | {
                "imagetype": "light",
                "action_type": "object",
                "object_name": "M31",
                "action_date": "20260317",
                "filter_name": "I+z",
                "datetime_timestamp": datetime.datetime(2026, 3, 17, 23, 45, 1, 123),
                "sequence_counter": 42,
            }
        )
        expected = "20260317/Raw/20260317-234501-M31-S001-R001-C042-I+z.fts"
        assert filename == expected, f"Got {filename}, expected {expected}"

        filename = templates.render_filename(
            **templates.TEST_KWARGS
            | {
                "imagetype": "flat",
                "action_type": "flats",
                "action_date": "20260202",
                "filter_name": "I+z",
                "datetime_timestamp": datetime.datetime(2026, 2, 3, 1, 41, 8, 123),
                "sequence_counter": 42,
            }
        )
        expected = "20260202/Flat/20260203-014108-Dusk-I+z-Bin1-Temp_-60-042.fts"
        assert filename == expected, f"Got {filename}, expected {expected}"

        filename = templates.render_filename(
            **templates.TEST_KWARGS
            | {
                "imagetype": "flat",
                "action_type": "flats",
                "action_date": "20260202",
                "filter_name": "I+z",
                "datetime_timestamp": datetime.datetime(2026, 2, 3, 14, 13, 34, 123),
                "sequence_counter": 42,
            }
        )
        expected = "20260202/Flat/20260203-141334-Dawn-I+z-Bin1-Temp_-60-042.fts"
        assert filename == expected, f"Got {filename}, expected {expected}"
