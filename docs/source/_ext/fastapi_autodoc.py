"""Custom Sphinx extension to auto-document FastAPI endpoints."""

import inspect
import re
from typing import Any

from docutils import nodes
from docutils.parsers.rst import directives
from docutils.statemachine import StringList
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import nested_parse_with_titles


class AutoFastAPIEndpoints(SphinxDirective):
    """Directive that generates documentation for FastAPI endpoints."""

    has_content = False
    option_spec = {
        "app": directives.unchanged_required,  # e.g., "astra.frontend.api.app"
    }

    def run(self):
        app_path = self.options.get("app", "astra.frontend.api.app")

        try:
            # Import the FastAPI app
            module_path, app_name = app_path.rsplit(".", 1)
            module = __import__(module_path, fromlist=[app_name])
            app = getattr(module, app_name)
        except Exception as exc:
            warning = self.state.document.reporter.warning(
                f"autofastapi: could not import FastAPI app from {app_path}: {exc}",
                line=self.lineno,
            )
            return [warning]

        # Group routes by category
        rest_routes = []
        websocket_routes = []
        page_routes = []

        for route in app.routes:
            if not hasattr(route, "methods") or not hasattr(route, "path"):
                continue

            # Categorize routes
            if hasattr(route, "endpoint"):
                endpoint_name = getattr(route.endpoint, "__name__", "")

                # Skip routes with include_in_schema=False
                if hasattr(route, "include_in_schema") and not route.include_in_schema:
                    continue

                # Detect WebSocket routes
                if "websocket" in endpoint_name.lower() or any(
                    "websocket" in str(m).lower() for m in getattr(route, "methods", [])
                ):
                    websocket_routes.append(route)
                # Detect HTML page routes (typically GET routes without /api/ prefix)
                elif (
                    "GET" in getattr(route, "methods", [])
                    and not route.path.startswith("/api/")
                    and endpoint_name.endswith("_page")
                ):
                    page_routes.append(route)
                else:
                    rest_routes.append(route)

        sections = []

        # REST API endpoints section
        if rest_routes:
            rest_section = self._build_rest_api_section(rest_routes)
            sections.append(rest_section)

        # WebSocket endpoints section
        if websocket_routes:
            ws_section = self._build_websocket_section(websocket_routes)
            sections.append(ws_section)

        # Page routes section (optional, can be excluded if not relevant)
        if page_routes:
            page_section = self._build_page_routes_section(page_routes)
            sections.append(page_section)

        return sections

    def _build_rest_api_section(self, routes: list) -> nodes.section:
        """Build documentation section for REST API endpoints."""
        section = nodes.section(ids=["rest-api-endpoints"])

        title = nodes.title(text="REST API Endpoints")
        section += title

        intro = nodes.paragraph(
            text="The following REST API endpoints are available for interacting with the Astra observatory system."
        )
        section += intro

        # Sort routes by path for better organization
        sorted_routes = sorted(routes, key=lambda r: (r.path, str(r.methods)))

        for route in sorted_routes:
            endpoint_section = self._build_endpoint_section(route)
            section += endpoint_section

        return section

    def _build_websocket_section(self, routes: list) -> nodes.section:
        """Build documentation section for WebSocket endpoints."""
        section = nodes.section(ids=["websocket-endpoints"])

        title = nodes.title(text="WebSocket Endpoints")
        section += title

        intro = nodes.paragraph(
            text="WebSocket endpoints provide real-time bidirectional communication."
        )
        section += intro

        for route in routes:
            endpoint_section = self._build_websocket_endpoint_section(route)
            section += endpoint_section

        return section

    def _build_page_routes_section(self, routes: list) -> nodes.section:
        """Build documentation section for HTML page routes."""
        section = nodes.section(ids=["page-routes"])

        title = nodes.title(text="Page Routes")
        section += title

        intro = nodes.paragraph(text="Frontend HTML pages served by the application.")
        section += intro

        for route in sorted(routes, key=lambda r: r.path):
            endpoint_section = self._build_page_endpoint_section(route)
            section += endpoint_section

        return section

    def _build_endpoint_section(self, route: Any) -> nodes.section:
        """Build documentation for a single REST endpoint."""
        # Create section ID from path and method
        methods = [m for m in route.methods if m not in ["HEAD", "OPTIONS"]]
        method_str = "-".join(sorted(methods)).lower()
        path_id = route.path.replace("/", "-").replace("{", "").replace("}", "")
        section_id = f"{method_str}{path_id}"

        section = nodes.section(ids=[section_id])

        # Title: METHOD /path
        # title_text = f"{', '.join(sorted(methods))} {route.path}"
        title = nodes.title()

        # Add method badges
        for method in sorted(methods):
            method_node = nodes.inline(
                text=method, classes=["http-method", f"http-{method.lower()}"]
            )
            title += method_node
            title += nodes.Text(" ")

        # Add path
        path_node = nodes.literal(text=route.path, classes=["endpoint-path"])
        title += path_node
        section += title

        # Description from docstring
        if hasattr(route, "endpoint") and route.endpoint:
            docstring = inspect.getdoc(route.endpoint)
            if docstring:
                # Parse the docstring as RST to support formatting
                doc_lines = StringList(docstring.splitlines(), source="")
                desc_container = nodes.container()
                nested_parse_with_titles(self.state, doc_lines, desc_container)
                section += desc_container

        # Path parameters
        path_params = re.findall(r"\{(\w+)\}", route.path)
        if path_params:
            params_heading = nodes.paragraph()
            params_heading += nodes.strong(text="Path Parameters:")
            section += params_heading

            param_list = nodes.bullet_list()
            for param in path_params:
                list_item = nodes.list_item()
                param_para = nodes.paragraph()
                param_para += nodes.literal(text=param)
                param_para += nodes.Text(" – Path parameter")
                list_item += param_para
                param_list += list_item
            section += param_list

        # Try to extract parameter information from function signature
        if hasattr(route, "endpoint") and route.endpoint:
            self._add_function_parameters(section, route.endpoint)

        return section

    def _build_websocket_endpoint_section(self, route: Any) -> nodes.section:
        """Build documentation for a WebSocket endpoint."""
        path_id = route.path.replace("/", "-")
        section_id = f"websocket{path_id}"

        section = nodes.section(ids=[section_id])

        title = nodes.title()
        ws_badge = nodes.inline(
            text="WebSocket", classes=["http-method", "http-websocket"]
        )
        title += ws_badge
        title += nodes.Text(" ")
        path_node = nodes.literal(text=route.path, classes=["endpoint-path"])
        title += path_node
        section += title

        if hasattr(route, "endpoint") and route.endpoint:
            docstring = inspect.getdoc(route.endpoint)
            if docstring:
                doc_lines = StringList(docstring.splitlines(), source="")
                desc_container = nodes.container()
                nested_parse_with_titles(self.state, doc_lines, desc_container)
                section += desc_container

        return section

    def _build_page_endpoint_section(self, route: Any) -> nodes.section:
        """Build documentation for an HTML page route."""
        path_id = route.path.replace("/", "-") or "root"
        section_id = f"page{path_id}"

        section = nodes.section(ids=[section_id])

        title = nodes.title()
        page_badge = nodes.inline(text="PAGE", classes=["http-method", "http-page"])
        title += page_badge
        title += nodes.Text(" ")
        path_node = nodes.literal(text=route.path or "/", classes=["endpoint-path"])
        title += path_node
        section += title

        if hasattr(route, "endpoint") and route.endpoint:
            docstring = inspect.getdoc(route.endpoint)
            if docstring:
                para = nodes.paragraph(text=docstring)
                section += para

        return section

    def _add_function_parameters(self, section: nodes.section, func: Any) -> None:
        """Extract and document function parameters (query params, body, etc.)."""
        try:
            sig = inspect.signature(func)

            # Filter out Request, WebSocket, and other framework parameters
            skip_types = {"Request", "WebSocket", "BackgroundTasks"}
            params_to_doc = []

            for param_name, param in sig.parameters.items():
                # Skip self, cls, and common framework parameters
                if param_name in {
                    "self",
                    "cls",
                    "request",
                    "websocket",
                    "background_tasks",
                }:
                    continue

                # Check if type is in skip_types
                if param.annotation != inspect.Parameter.empty:
                    type_name = getattr(
                        param.annotation, "__name__", str(param.annotation)
                    )
                    if type_name in skip_types:
                        continue

                params_to_doc.append((param_name, param))

            if params_to_doc:
                params_heading = nodes.paragraph()
                params_heading += nodes.strong(text="Parameters:")
                section += params_heading

                param_list = nodes.bullet_list()
                for param_name, param in params_to_doc:
                    list_item = nodes.list_item()
                    param_para = nodes.paragraph()

                    # Parameter name
                    param_para += nodes.literal(text=param_name)

                    # Type annotation
                    if param.annotation != inspect.Parameter.empty:
                        type_str = self._format_type_annotation(param.annotation)
                        param_para += nodes.Text(f" ({type_str})")

                    # Default value
                    if param.default != inspect.Parameter.empty:
                        default_str = repr(param.default)
                        if len(default_str) > 50:
                            default_str = default_str[:47] + "..."
                        param_para += nodes.Text(f" = {default_str}")

                    list_item += param_para
                    param_list += list_item

                section += param_list

        except Exception:
            # If we can't inspect the signature, just skip this section
            pass

    def _format_type_annotation(self, annotation: Any) -> str:
        """Format a type annotation as a string."""
        try:
            if hasattr(annotation, "__name__"):
                return annotation.__name__
            return str(annotation).replace("typing.", "")
        except Exception:
            return str(annotation)


def setup(app):
    """Register the directive with Sphinx."""
    app.add_directive("autofastapi", AutoFastAPIEndpoints)

    return {
        "version": "0.1",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
