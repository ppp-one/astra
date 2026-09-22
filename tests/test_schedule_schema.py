"""Unit tests for the schedule editor's action value schema and validation.

Covers:
    - `action_value_schema` describes every settable field of an action config,
      with the type, required flag, default and description the editor needs.
    - A nested config marked `flatten` is expanded in place, and the parent's
      own fields win a name clash.
    - `json_type` maps annotations onto the JSON type names the editor uses.
    - `schedule_template_loader` still produces the defaults it always did.
    - `validate_schedule_items` rejects the rows the scheduler would reject.
"""

import json
from unittest.mock import MagicMock

import pytest

from astra.action_configs import (
    ACTION_CONFIGS,
    AutofocusCalibrationFieldConfig,
    AutofocusConfig,
    CalibrationActionConfig,
    FlatsActionConfig,
    ObjectActionConfig,
    action_value_schema,
    json_type,
    public_fields,
)


def schema_by_name(config) -> dict:
    return {entry["name"]: entry for entry in action_value_schema(config)}


class TestJsonType:
    @pytest.mark.parametrize(
        "annotation,expected",
        [
            (str, "string"),
            (int, "integer"),
            (float, "number"),
            (bool, "boolean"),
            (list[int], "array"),
            (dict[str, int], "object"),
            (str | None, "string"),
            (float | None, "number"),
            # int and float accept the same input, so they collapse
            (float | int, "number"),
            # a genuine either/or stays open rather than pretending to be one type
            (list[int] | int | None, "any"),
        ],
    )
    def test_maps_annotation_to_json_type(self, annotation, expected):
        assert json_type(annotation)[0] == expected

    def test_bool_does_not_fall_through_to_integer(self):
        assert json_type(bool)[0] == "boolean"

    def test_enum_reports_its_values_as_choices(self):
        field_type, choices = json_type(
            AutofocusCalibrationFieldConfig.__annotations__["selection_method"]
        )
        assert field_type == "string"
        assert choices == ["single", "maximal", "any"]

    def test_unannotated_field_is_open(self):
        assert json_type(None)[0] == "any"


class TestActionValueSchema:
    def test_describes_every_settable_field(self):
        names = {entry["name"] for entry in action_value_schema(ObjectActionConfig)}
        assert names == {f.name for f in public_fields(ObjectActionConfig)}

    def test_private_fields_are_left_out(self):
        names = {entry["name"] for entry in action_value_schema(ObjectActionConfig)}
        assert not any(name.startswith("_") for name in names)
        assert "_nonsidereal" not in names

    def test_required_fields_are_marked(self):
        schema = schema_by_name(ObjectActionConfig)
        assert schema["object"]["required"] is True
        assert schema["exptime"]["required"] is True
        assert schema["filter"]["required"] is False

    def test_reports_declared_defaults(self):
        schema = schema_by_name(ObjectActionConfig)
        assert schema["bin"]["default"] == 1
        assert schema["guiding"]["default"] is False
        assert schema["subframe_center_x"]["default"] == 0.5
        assert schema["ra"]["default"] is None

    def test_carries_the_field_descriptions(self):
        schema = schema_by_name(ObjectActionConfig)
        assert (
            schema["exptime"]["description"]
            == ObjectActionConfig.FIELD_DESCRIPTIONS["exptime"]
        )

    def test_types_follow_the_annotations(self):
        schema = schema_by_name(ObjectActionConfig)
        assert schema["object"]["type"] == "string"
        assert schema["exptime"]["type"] == "number"
        assert schema["n"]["type"] == "integer"
        assert schema["guiding"]["type"] == "boolean"
        assert schema["metadata"]["type"] == "object"

    def test_flats_takes_lists_where_object_takes_scalars(self):
        # The editor relies on this to drop a value that no longer fits when the
        # action type changes.
        assert schema_by_name(ObjectActionConfig)["filter"]["type"] == "string"
        assert schema_by_name(FlatsActionConfig)["filter"]["type"] == "array"

    def test_actions_without_settings_have_an_empty_schema(self):
        for action_type in ("open", "close", "cool_camera", "complete_headers"):
            assert action_value_schema(ACTION_CONFIGS[action_type]) == []

    def test_every_action_type_produces_a_serialisable_schema(self):
        for action_type, config_cls in ACTION_CONFIGS.items():
            schema = action_value_schema(config_cls)
            json.dumps(schema)  # must survive the trip to the template
            for entry in schema:
                assert set(entry) == {
                    "name",
                    "type",
                    "required",
                    "default",
                    "description",
                    "choices",
                    "example",
                    "group",
                }, action_type


class TestFlattenedConfig:
    def test_nested_fields_are_expanded_in_place(self):
        schema = schema_by_name(AutofocusConfig)
        # calibration_field is flattened away, its fields take its place
        assert "calibration_field" not in schema
        assert "airmass_threshold" in schema
        assert "selection_method" in schema

    def test_nested_fields_name_their_group(self):
        schema = schema_by_name(AutofocusConfig)
        assert schema["airmass_threshold"]["group"] == "calibration_field"
        assert schema["exptime"]["group"] is None

    def test_parent_field_wins_a_name_clash(self):
        # Both configs declare a filter; the parent's must be the one described.
        schema = schema_by_name(AutofocusConfig)
        entries = [
            e for e in action_value_schema(AutofocusConfig) if e["name"] == "filter"
        ]
        assert len(entries) == 1
        assert schema["filter"]["group"] is None

    def test_nested_default_comes_from_the_built_instance(self):
        # AutofocusCalibrationFieldConfig fills maximal_zenith_angle in
        # __post_init__, so the declared default of None is not what a user gets.
        schema = schema_by_name(AutofocusConfig)
        assert schema["maximal_zenith_angle"]["default"] is not None
        assert isinstance(schema["maximal_zenith_angle"]["default"], float)

    def test_enum_default_is_reported_as_its_value(self):
        schema = schema_by_name(AutofocusConfig)
        assert schema["selection_method"]["default"] == "single"


class TestScheduleTemplateLoader:
    def test_template_holds_one_key_per_schema_field(self):
        from astra.main import schedule_schema_loader, schedule_template_loader

        schemas = schedule_schema_loader()
        templates = schedule_template_loader(schemas)

        for action_type, schema in schemas.items():
            assert set(templates[action_type]) == {e["name"] for e in schema}

    def test_templates_are_json_serialisable(self):
        from astra.main import schedule_template_loader

        json.dumps(schedule_template_loader())

    def test_template_values_match_the_config_defaults(self):
        from astra.main import schedule_template_loader

        templates = schedule_template_loader()
        assert templates["object"]["bin"] == 1
        assert templates["object"]["guiding"] is False
        assert templates["autofocus"]["focus_measure_operator"] == "HFR"


class TestValidateScheduleItems:
    @pytest.fixture(autouse=True)
    def observatory(self, monkeypatch):
        import astra.main as main

        obs = MagicMock()
        obs.config = None
        monkeypatch.setattr(main, "OBSERVATORY", obs)
        return obs

    def validate(self, item):
        from astra.main import validate_schedule_items

        return validate_schedule_items([item])

    def test_accepts_a_valid_object_row(self):
        assert (
            self.validate(
                {
                    "device_name": "cam1",
                    "action_type": "object",
                    "action_value": {"object": "M42", "exptime": 60.0},
                }
            )
            == []
        )

    def test_accepts_an_action_that_takes_no_settings(self):
        assert (
            self.validate(
                {"device_name": "cam1", "action_type": "open", "action_value": {}}
            )
            == []
        )

    def test_rejects_a_missing_required_field(self):
        errors = self.validate(
            {
                "device_name": "cam1",
                "action_type": "object",
                "action_value": {"object": "M42"},
            }
        )
        assert len(errors) == 1
        assert "exptime" in errors[0]["message"]

    def test_rejects_half_a_coordinate_pair(self):
        errors = self.validate(
            {
                "device_name": "cam1",
                "action_type": "object",
                "action_value": {"object": "M42", "exptime": 1.0, "ra": 83.8},
            }
        )
        assert len(errors) == 1
        assert "dec" in errors[0]["message"]

    def test_rejects_mixed_coordinate_systems(self):
        errors = self.validate(
            {
                "device_name": "cam1",
                "action_type": "object",
                "action_value": {
                    "object": "M42",
                    "exptime": 1.0,
                    "ra": 83.8,
                    "dec": -5.4,
                    "alt": 45.0,
                    "az": 90.0,
                },
            }
        )
        assert len(errors) == 1
        assert "Alt/Az" in errors[0]["message"]

    def test_rejects_an_unknown_action_type(self):
        errors = self.validate(
            {"device_name": "cam1", "action_type": "nonsense", "action_value": {}}
        )
        assert len(errors) == 1
        assert "nonsense" in errors[0]["message"]

    def test_rejects_a_row_without_a_device(self):
        errors = self.validate(
            {"device_name": "", "action_type": "open", "action_value": {}}
        )
        assert len(errors) == 1
        assert "device" in errors[0]["message"].lower()

    def test_rejects_mismatched_flats_lengths(self):
        errors = self.validate(
            {
                "device_name": "cam1",
                "action_type": "flats",
                "action_value": {"filter": ["V", "B"], "n": [3]},
            }
        )
        assert len(errors) == 1
        assert "same length" in errors[0]["message"]

    def test_reports_the_row_index_of_each_bad_row(self):
        from astra.main import validate_schedule_items

        errors = validate_schedule_items(
            [
                {"device_name": "cam1", "action_type": "open", "action_value": {}},
                {"device_name": "cam1", "action_type": "object", "action_value": {}},
                {"device_name": "cam1", "action_type": "close", "action_value": {}},
                {"device_name": "cam1", "action_type": "bogus", "action_value": {}},
            ]
        )
        assert [e["row"] for e in errors] == [1, 3]

    def test_a_missing_action_value_is_treated_as_empty(self):
        assert self.validate({"device_name": "cam1", "action_type": "open"}) == []


class TestParseScheduleJsonl:
    def test_parses_one_row_per_line(self):
        from astra.main import parse_schedule_jsonl

        rows = parse_schedule_jsonl('{"a": 1}\n{"a": 2}\n')
        assert rows == [{"a": 1}, {"a": 2}]

    def test_ignores_blank_lines(self):
        from astra.main import parse_schedule_jsonl

        assert parse_schedule_jsonl('\n{"a": 1}\n\n') == [{"a": 1}]

    def test_raises_on_bad_json(self):
        from astra.main import parse_schedule_jsonl

        with pytest.raises(json.JSONDecodeError):
            parse_schedule_jsonl('{"a": 1,}')


class TestCalibrationSchema:
    def test_calibration_takes_lists(self):
        schema = schema_by_name(CalibrationActionConfig)
        assert schema["exptime"]["type"] == "array"
        assert schema["n"]["type"] == "array"


class TestFiltersByCamera:
    def test_maps_each_camera_to_its_own_wheel(self, monkeypatch):
        import astra.main as main

        obs = MagicMock()
        obs.devices = {}
        obs.config = None
        monkeypatch.setattr(main, "OBSERVATORY", obs)
        monkeypatch.setattr(main, "FWS", {"fw1": ["U", "B", "V"], "fw2": ["Ha"]})

        def fake_pairing(camera_name, devices=None, observatory_config=None):
            return {"cam1": {"FilterWheel": "fw1"}, "cam2": {"FilterWheel": "fw2"}}[
                camera_name
            ]

        monkeypatch.setattr(
            main.PairedDevices, "from_camera_name", staticmethod(fake_pairing)
        )

        result = main.filters_by_camera(["cam1", "cam2"])
        assert result["cam1"] == ["U", "B", "V"]
        assert result["cam2"] == ["Ha"]

    def test_camera_without_a_wheel_gets_an_empty_list(self, monkeypatch):
        import astra.main as main

        obs = MagicMock()
        obs.devices = {}
        obs.config = None
        monkeypatch.setattr(main, "OBSERVATORY", obs)
        monkeypatch.setattr(main, "FWS", {"fw1": ["U"]})
        monkeypatch.setattr(
            main.PairedDevices, "from_camera_name", staticmethod(lambda **kw: {})
        )

        assert main.filters_by_camera(["cam1"])["cam1"] == []

    def test_a_pairing_error_is_not_fatal(self, monkeypatch):
        import astra.main as main

        obs = MagicMock()
        obs.devices = {}
        obs.config = None
        monkeypatch.setattr(main, "OBSERVATORY", obs)
        monkeypatch.setattr(main, "FWS", {"fw1": ["U"]})

        def boom(**kwargs):
            raise ValueError("camera not in observatory config")

        monkeypatch.setattr(main.PairedDevices, "from_camera_name", staticmethod(boom))

        result = main.filters_by_camera(["cam1"])
        assert result["cam1"] == []
        assert result["__all__"] == ["U"]

    def test_all_key_holds_every_filter_sorted(self, monkeypatch):
        import astra.main as main

        obs = MagicMock()
        obs.devices = {}
        obs.config = None
        monkeypatch.setattr(main, "OBSERVATORY", obs)
        monkeypatch.setattr(main, "FWS", {"fw1": ["V", "B"], "fw2": ["Ha", "B"]})
        monkeypatch.setattr(
            main.PairedDevices, "from_camera_name", staticmethod(lambda **kw: {})
        )

        assert main.filters_by_camera([])["__all__"] == ["B", "Ha", "V"]

    def test_no_filter_wheels_at_all(self, monkeypatch):
        import astra.main as main

        obs = MagicMock()
        obs.devices = {}
        obs.config = None
        monkeypatch.setattr(main, "OBSERVATORY", obs)
        monkeypatch.setattr(main, "FWS", {})
        monkeypatch.setattr(
            main.PairedDevices, "from_camera_name", staticmethod(lambda **kw: {})
        )

        result = main.filters_by_camera(["cam1"])
        assert result == {"cam1": [], "__all__": []}


class TestAutofocusValidationRegression:
    """BaseActionConfig.from_dict takes (config_dict, default_dict, logger) but
    AutofocusConfig takes (config_dict, logger, default_dict). A positional call
    hands the defaults to the autofocus configs as their logger, which crashes
    the moment one of them logs a warning."""

    @pytest.fixture(autouse=True)
    def observatory(self, monkeypatch):
        import astra.main as main

        obs = MagicMock()
        obs.config = None
        monkeypatch.setattr(main, "OBSERVATORY", obs)

    def test_autofocus_row_that_warns_is_not_reported_as_broken(self):
        from astra.main import validate_schedule_items

        # n_exposures longer than n_steps makes AutofocusConfig log a warning
        errors = validate_schedule_items(
            [
                {
                    "device_name": "cam1",
                    "action_type": "autofocus",
                    "action_value": {
                        "exptime": 1.0,
                        "n_steps": [30, 20],
                        "n_exposures": [1, 1, 1],
                    },
                }
            ]
        )
        assert errors == [], errors

    def test_a_plain_autofocus_row_validates(self):
        from astra.main import validate_schedule_items

        assert (
            validate_schedule_items(
                [
                    {
                        "device_name": "cam1",
                        "action_type": "autofocus",
                        "action_value": {"exptime": 1.0, "filter": "V"},
                    }
                ]
            )
            == []
        )

    def test_defaults_reach_the_autofocus_config(self, monkeypatch):
        # Passing by keyword is what makes this work for both signatures.
        from astra.action_configs import AutofocusConfig

        config = AutofocusConfig.from_dict({}, default_dict={"exptime": 7.5})
        assert config.exptime == 7.5


class TestFieldExamples:
    """A single hardcoded placeholder was wrong for most JSON-typed fields."""

    def test_a_curated_example_is_preferred(self):
        schema = schema_by_name(FlatsActionConfig)
        # flats takes filter names, not numbers
        assert schema["filter"]["example"] == ["V", "R"]
        assert schema["n"]["example"] == [10, 10]

    def test_a_mapping_field_gets_a_mapping_example(self):
        assert isinstance(
            schema_by_name(ObjectActionConfig)["metadata"]["example"], dict
        )

    def test_a_mapping_with_no_curated_example_still_gets_one(self):
        schema = schema_by_name(CalibrationActionConfig)
        assert schema["metadata"]["example"] == {"key": "value"}

    def test_a_non_empty_default_is_used_when_there_is_no_curated_example(self):
        schema = schema_by_name(AutofocusConfig)
        assert schema["secondary_focus_measure_operators"]["example"] == [
            "fft",
            "normalized_variance",
            "tenengrad",
        ]
        assert schema["g_mag_range"]["example"] == [0, 10]

    def test_a_scalar_or_list_field_shows_the_curated_scalar(self):
        assert schema_by_name(AutofocusConfig)["search_range"]["example"] == 1000

    def test_every_json_typed_field_has_a_usable_example(self):
        for action_type, config_cls in ACTION_CONFIGS.items():
            for entry in action_value_schema(config_cls):
                if entry["type"] not in ("array", "object", "any"):
                    continue
                example = entry["example"]
                assert example is not None, (action_type, entry["name"])
                # and it matches the shape the field declares
                if entry["type"] == "array":
                    assert isinstance(example, list), (action_type, entry["name"])
                elif entry["type"] == "object":
                    assert isinstance(example, dict), (action_type, entry["name"])

    def test_an_empty_default_is_not_offered_as_an_example(self):
        from astra.action_configs import example_value

        assert example_value(list[int], [], None) == [1, 2]
        assert example_value(dict[str, int], {}, None) == {"key": "value"}

    def test_a_list_of_strings_is_not_shown_as_numbers(self):
        from astra.action_configs import example_value

        assert example_value(list[str], [], None) == ["a", "b"]

    def test_every_example_survives_json(self):
        from astra.main import schedule_schema_loader

        json.dumps(schedule_schema_loader())
