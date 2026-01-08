from dataclasses import MISSING, Field, fields, is_dataclass
import inspect
import types
from typing import Any, cast, get_args, get_origin, get_type_hints
import typing

from docutils import nodes
from docutils.parsers.rst import directives
import json
import html
from docutils.statemachine import StringList
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import nested_parse_with_titles


def _is_primitive(val):
    return isinstance(val, (str, int, float, bool)) or val is None


def pretty_json(obj, indent=4, inline_list_max=5, current_indent=0):
    """Render JSON with small primitive lists kept on one line.

    - `inline_list_max` controls the maximum length of a primitive-only list
      to render inline (e.g., `[1, 2]` instead of each on its own line).
    """
    space = " " * current_indent
    next_indent = current_indent + indent

    if isinstance(obj, dict):
        if not obj:
            return "{}"
        pieces = []
        for k, v in obj.items():
            key = json.dumps(k, ensure_ascii=False)
            rendered = pretty_json(
                v,
                indent=indent,
                inline_list_max=inline_list_max,
                current_indent=next_indent,
            )
            pieces.append(f"{' ' * next_indent}{key}: {rendered}")
        inner = ",\n".join(pieces)
        return "{\n" + inner + "\n" + space + "}"

    if isinstance(obj, list):
        if not obj:
            return "[]"
        # keep short primitive lists inline
        if len(obj) <= inline_list_max and all(_is_primitive(x) for x in obj):
            return json.dumps(obj, ensure_ascii=False)
        pieces = []
        for v in obj:
            rendered = pretty_json(
                v,
                indent=indent,
                inline_list_max=inline_list_max,
                current_indent=next_indent,
            )
            pieces.append(f"{' ' * next_indent}{rendered}")
        inner = ",\n".join(pieces)
        return "[\n" + inner + "\n" + space + "]"

    # primitives
    return json.dumps(obj, ensure_ascii=False)


class AutoScheduleActions(SphinxDirective):
    """Directive that emits readable schedule action docs straight from configs."""

    has_content = False
    option_spec = {
        "format": directives.unchanged,
    }

    def run(self):
        try:
            from astra.action_configs import ACTION_CONFIGS
        except Exception as exc:  # pragma: no cover - import-time Sphinx errors
            warning = self.state.document.reporter.warning(
                f"autoscheduleactions: could not import ACTION_CONFIGS: {exc}",
                line=self.lineno,
            )
            return [warning]

        sections: list[nodes.section] = []
        for key, cls in ACTION_CONFIGS.items():
            sections.append(self._build_action_section(key, cls))

        return sections

    def _build_action_section(self, key: str, cls: type) -> nodes.section:
        section_id = nodes.make_id(f"{key}-action")
        section = nodes.section(ids=[section_id])

        title = nodes.title()
        title += nodes.literal(text=key)
        section += title

        self._append_description(section, cls)

        # If the config class provides an `EXAMPLE_SCHEDULE`, render it as
        # a JSON code block between the description and the parameter list.
        example = getattr(cls, "EXAMPLE_SCHEDULE", None)
        if example is not None:
            try:
                example_text = pretty_json(example, indent=4)
            except Exception:
                try:
                    example_text = json.dumps(example, indent=4, ensure_ascii=False)
                except Exception:
                    example_text = repr(example)

            ex_heading = nodes.paragraph()
            ex_heading += nodes.strong(text="Minimal schedule example")
            section += ex_heading
            code_block = nodes.literal_block(example_text, example_text)
            code_block["language"] = "json"
            section += code_block

        render_format = self.options.get("format", "table").lower()
        parameters = self._build_parameters_node(cls, render_format)
        if parameters is not None:
            heading = nodes.paragraph()
            heading += nodes.strong(text="Action values")
            section += heading
            section += parameters
        else:
            pass
            # section += nodes.paragraph(
            #     text="This action has no configurable parameters."
            # )

        return section

    def _append_description(self, section: nodes.section, cls: type) -> None:
        description = inspect.getdoc(cls)
        if not description:
            section += nodes.paragraph(text="No description available.")
            return

        doc_lines = StringList(description.splitlines(), source="")
        nested_parse_with_titles(self.state, doc_lines, section)

    def _build_parameters_node(self, cls: type, render_format: str):
        documented_fields = list(self._iter_documented_fields(cls))
        if not documented_fields:
            return None
        # support a literal/boxed format that emits a single pre/code block
        # where parts of each line are wrapped in spans with CSS classes
        if render_format in ("literal", "boxed"):
            return self._build_parameters_literal_box(documented_fields)

        if render_format == "bullet":
            return self._build_parameters_bullet_list(documented_fields)

        if render_format not in ("table", "bullet", "literal", "boxed"):
            self.state.document.reporter.warning(
                f"autoscheduleactions: unknown format '{render_format}', falling back to table",
                line=self.lineno,
            )

        return self._build_parameters_table(documented_fields)

    def _build_parameters_table(
        self, documented_fields: list[tuple[str, Field[Any], Any, str | None]]
    ) -> nodes.table | None:
        rows: list[tuple[nodes.Node, nodes.Node, nodes.Node, nodes.Node]] = []
        for name, field_obj, annotation, description in documented_fields:
            rows.append(
                (
                    nodes.literal(text=name),
                    nodes.paragraph(text=description or "No description provided."),
                    self._make_literal(self._format_type(annotation) or "–"),
                    self._format_requirement_cell(field_obj),
                )
            )
        return self._render_table(rows)

    def _build_parameters_bullet_list(
        self, documented_fields: list[tuple[str, Field[Any], Any, str | None]]
    ) -> nodes.bullet_list:
        bullet_list = nodes.bullet_list()

        for name, field_obj, annotation, description in documented_fields:
            item = nodes.list_item()

            header = nodes.paragraph()

            # header += nodes.literal(name)

            literal_text = f"{name}: {self._format_type(annotation) or '–'}"

            # Append required/default info inline. When required, avoid using '='
            if not field_obj.metadata.get("required"):
                default_text = self._format_default(field_obj)
                literal_text += (
                    f" = {default_text}" if default_text is not None else " (optional)"
                )
                header += self._make_literal(literal_text)
            else:
                header += self._make_literal(literal_text)
                header += nodes.Text(" ")
                header += nodes.strong(text="Required")

            # Put the description on the same line, separated by an em dash
            desc = description or "No description provided."
            header += nodes.Text(" — ")
            header += nodes.Text(desc)

            item += header

            bullet_list += item

        return bullet_list

    def _build_parameters_literal_box(
        self, documented_fields: list[tuple[str, Field[Any], Any, str | None]]
    ) -> nodes.raw:
        """Emit a single HTML <pre> block with spans for name/type/default/required.

        This provides a single literal-looking box where individual parts can be
        colored via CSS targeting the emitted classes.
        """
        lines: list[str] = []
        for name, field_obj, annotation, description in documented_fields:
            name_html = f'<span class="param-name">{html.escape(name)}</span>'
            type_text = self._format_type(annotation) or "–"
            type_html = f'<span class="param-type">{html.escape(type_text)}</span>'

            if field_obj.metadata.get("required"):
                default_html = ""
                req_html = ' <span class="param-required">Required</span>'
            else:
                default_text = self._format_default(field_obj)
                if default_text is not None:
                    default_html = f' <span class="param-default">= {html.escape(default_text)}</span>'
                else:
                    default_html = ' <span class="param-default">(optional)</span>'
                req_html = ""

            desc = description or "No description provided."
            desc_html = html.escape(desc)

            line = f"{name_html}: {type_html}{default_html}{req_html} — {desc_html}"
            lines.append(line)

        html_block = '<pre class="autoschedule-params">' + "\n".join(lines) + "</pre>"
        return nodes.raw(html_block, html_block, format="html")

    def _iter_documented_fields(
        self, cls: type
    ) -> typing.Iterable[tuple[str, Field[Any], Any, str | None]]:
        type_hints = self._safe_type_hints(cls)
        descriptions = getattr(cls, "FIELD_DESCRIPTIONS", {})

        for f in fields(cls):
            if not f.init or f.name.startswith("_"):
                continue

            annotation = type_hints.get(f.name)
            if f.metadata.get("flatten") and self._is_dataclass_type(annotation):
                yield from self._iter_documented_fields(cast(type, annotation))
                continue

            yield (f.name, f, annotation, descriptions.get(f.name))

    @staticmethod
    def _is_dataclass_type(annotation: Any) -> bool:
        if annotation is None:
            return False
        if isinstance(annotation, str):
            return False
        origin = get_origin(annotation)
        if origin is not None:
            return False
        return is_dataclass(annotation)

    @staticmethod
    def _render_table(
        rows: list[tuple[nodes.Node, nodes.Node, nodes.Node, nodes.Node]],
    ) -> nodes.table:
        table = nodes.table()
        tgroup = nodes.tgroup(cols=4)
        table += tgroup

        for width in (20, 40, 20, 20):
            tgroup += nodes.colspec(colwidth=width)

        thead = nodes.thead()
        tgroup += thead
        header_row = nodes.row()
        for title in ("Field", "Description", "Type", "Default"):
            entry = nodes.entry()
            entry += nodes.paragraph(text=title)
            header_row += entry
        thead += header_row

        tbody = nodes.tbody()
        tgroup += tbody
        for field_cell, description_cell, type_cell, default_cell in rows:
            row = nodes.row()

            entry = nodes.entry()
            entry += field_cell
            row += entry

            entry = nodes.entry()
            entry += description_cell
            row += entry

            entry = nodes.entry()
            entry += type_cell
            row += entry

            entry = nodes.entry()
            entry += default_cell
            row += entry

            tbody += row

        return table

    def _format_requirement_cell(self, field: Field[Any]) -> nodes.Node:
        if field.metadata.get("required"):
            return nodes.strong(text="Required")

        default_text = self._format_default(field)
        if default_text is not None:
            return self._make_literal(default_text)

        return nodes.Text("Optional")

    @staticmethod
    def _make_literal(text: str) -> nodes.literal:
        return nodes.literal(text=text)

    @staticmethod
    def _safe_type_hints(action_cls: type) -> dict[str, Any]:
        try:
            return get_type_hints(action_cls, include_extras=True)
        except Exception:
            return getattr(action_cls, "__annotations__", {}) or {}

    @staticmethod
    def _format_type(annotation: Any) -> str:
        if annotation is None:
            return ""

        if annotation is type(None):  # noqa: E721 - explicit NoneType check
            return "None"

        origin = get_origin(annotation)
        args = get_args(annotation)

        if origin is None:
            if isinstance(annotation, type):
                return "None" if annotation is type(None) else annotation.__name__
            return str(annotation).replace("typing.", "")

        if origin in {list, set}:
            inner = (
                ", ".join(
                    filter(
                        None, (AutoScheduleActions._format_type(arg) for arg in args)
                    )
                )
                or "Any"
            )
            name = origin.__name__
            return f"{name.title()}[{inner}]"

        if origin is tuple:
            if args and args[-1] is Ellipsis:
                inner = AutoScheduleActions._format_type(args[0])
                return f"Tuple[{inner}, ...]"
            inner = ", ".join(
                filter(None, (AutoScheduleActions._format_type(arg) for arg in args))
            )
            return f"Tuple[{inner}]"

        if origin is dict:
            key = AutoScheduleActions._format_type(args[0]) if args else "Any"
            value = (
                AutoScheduleActions._format_type(args[1]) if len(args) > 1 else "Any"
            )
            return f"Dict[{key}, {value}]"

        if origin in {typing.Union, types.UnionType}:  # type: ignore[attr-defined]
            formatted = [AutoScheduleActions._format_type(arg) for arg in args]
            filtered = [f or "None" for f in formatted]
            return " | ".join(filtered)

        return str(annotation).replace("typing.", "")

    @staticmethod
    def _format_default(f: Field[Any]) -> str | None:
        if f.default is not MISSING:
            return AutoScheduleActions._stringify_default(f.default)
        factory = getattr(f, "default_factory", MISSING)
        if factory is not MISSING:
            try:
                produced = factory()
            except Exception:
                produced = None

            if produced is not None:
                if hasattr(produced, "__dataclass_fields__"):
                    return f"{produced.__class__.__name__}()"
                return AutoScheduleActions._stringify_default(produced)

            name = getattr(factory, "__name__", repr(factory))
            return f"{name}()"
        return None

    @staticmethod
    def _stringify_default(value) -> str:
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, str):
            return f"'{value}'"
        if value is None:
            return "None"
        return repr(value)


def setup(app):
    app.add_directive("autoscheduleactions", AutoScheduleActions)
    return {"version": "0.1", "parallel_read_safe": True}
