import importlib.metadata
import os
import sys

# Add the project source directory to the path so that autodoc can find the modules
sys.path.insert(0, os.path.abspath("../../src"))

# Project information
project = "Astra"
copyright = "2025, Peter Pedersen"
author = "Peter Pedersen"

# The full version, including alpha/beta/rc tags
version = importlib.metadata.version("astra")

# General configuration
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosummary",
    "sphinx_copybutton",
    "myst_nb",
]

# Add mappings for intersphinx
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "scipy": ("https://docs.scipy.org/doc/scipy/", None),
    "matplotlib": ("https://matplotlib.org/stable/", None),
    "astropy": ("https://docs.astropy.org/en/stable/", None),
    "jinja2": ("https://jinja.palletsprojects.com/en/stable/", None),
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# HTML output options
html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_css_files = ["style.css"]
html_short_title = "Astra"
html_title = f"{html_short_title}"
# html_logo = "../../astra-art.png"
html_favicon = "../../astra-art.png"

html_context = {
    "default_mode": "light",
}

html_theme_options = {
    "repository_url": "https://github.com/ppp-one/astra",
    "use_repository_button": True,
    "use_fullscreen_button": False,
    "use_download_button": False,
    "home_page_in_toc": False,
    "show_navbar_depth": 1,
    "collapse_navigation": True,
    "navbar_end": ["navbar-icon-links"],
}


# Auto-generate API documentation
autodoc_member_order = "bysource"
autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
    "undoc-members": True,
}
autosummary_generate = True
autodoc_preserve_defaults = True  # Prevents evaluation of default values
autoclass_content = "both"

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = False
napoleon_use_rtype = False
