Contributing
===========

Thank you for your interest in contributing to Astra! This section describes how to set up your development environment and contribute to the project.

Development Setup
---------------

1. Fork the repository on GitHub
2. Clone your fork locally:

   .. code-block:: bash

      git clone https://github.com/ppp-one/astra.git
      cd astra

3. Create a virtual environment and install development dependencies:

   .. code-block:: bash

      python -m venv venv
      source venv/bin/activate  # On Windows: venv\Scripts\activate
      pip install -e ".[dev, test]"
      
      pre-commit install  # Set up pre-commit hooks


Code Style
---------

Astra follows these coding conventions:

- We use Ruff for linting and code formatting
- Maximum line length is 88 characters
- Docstrings follow the NumPy style

Pull Requests
-----------

Before submitting a pull request:

1. Make sure all tests pass
2. Update documentation if you've changed functionality
3. If you've added functionality, add tests for it

Documentation
-----------

Documentation is written using Sphinx. To build the documentation locally:

.. code-block:: bash

   # Install documentation dependencies
   pip install -e ".[docs]"
   
   # Build documentation
   cd docs
   make html

The built documentation will be in ``docs/build/html``.

Running Tests
-----------

To run the test suite:

.. code-block:: bash

   sh ./run_tests_individually.sh

Versioning
---------

Astra follows semantic versioning. Version numbers follow the format MAJOR.MINOR.PATCH:

- MAJOR: incompatible API changes
- MINOR: new functionality in a backwards-compatible manner
- PATCH: backwards-compatible bug fixes

Release Process
-------------

1. Update CHANGELOG.md
2. Update version number in pyproject.toml
3. Create a git tag for the release
4. Push the tag to GitHub