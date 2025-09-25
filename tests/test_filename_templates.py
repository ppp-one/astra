"""Unit tests for image_handler module."""

import pytest

from astra.filename_templates import FilenameTemplates, JinjaFilenameTemplates


class FilenameTemplateTests:
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
