# Contributing

```{image} _static/contributing-banner.svg
:class: responsive-banner
:align: center
:alt: banner
```

Thank you for your interest in contributing to _Astra_! This section describes how to set up your development environment and contribute to the project.

## Development Setup

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/ppp-one/astra.git
   cd astra
   ```
3. Set up a reproducible development environment with [_uv_](https://docs.astral.sh/uv/):
   See the [_uv_ documentation](https://docs.astral.sh/uv/getting-started/installation/) for installation instructions.

   Then, run the following commands to sync dependencies and set up pre-commit hooks:

   ```bash
   # Sync development dependencies and create a reproducible environment
   uv sync --dev

   # Activate the virtual environment created or updated by `uv sync`
   source .venv/bin/activate

   # (Optional) Generate or update the lockfile when dependencies change
   uv lock

   # Install pre-commit hooks using the uv environment
   uv run pre-commit install

   # Run pre-commit hooks on all files to ensure code style compliance
   uv run pre-commit run --all-files
   ```

## Code Style

_Astra_ follows these coding conventions:

- We use [_Ruff_](https://docs.astral.sh/ruff/) for linting and code formatting
- Maximum line length is 88 characters
- Docstrings follow the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html#s3.8-comments-and-docstrings)

## Pull Requests

Before submitting a pull request:

1. Make sure all tests pass
2. Update documentation if you've changed functionality
3. If you've added functionality, add tests for it

## Documentation

Documentation is written using Sphinx. To build the documentation locally:

```bash
# Install documentation dependencies
uv sync --group docs

# Build documentation
uv run make -C docs html
```

The built documentation will be in `docs/build/html`.

## Running Tests

To run the test suite:

```bash
uv run --dev pytest
```

You can also run a subset of tests that do not run test schedules and therefore complete
more quickly by using the following `pytest` tag:

```bash
uv run --dev pytest -m "not slow"
```

## Versioning

_Astra_ follows semantic versioning. Version numbers follow the format `MAJOR.MINOR.PATCH`:

- `MAJOR`: incompatible API changes
- `MINOR`: new functionality in a backwards-compatible manner
- `PATCH`: backwards-compatible bug fixes

## Release Process

1. Update version number in `pyproject.toml`
2. Create a git tag for the release
3. Push the tag to GitHub
