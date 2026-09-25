# Installation

```{image} _static/installation-banner.svg
:class: responsive-banner
:align: center
:alt: banner
```

## Prerequisites

Before installing _Astra_, please ensure you have the following prerequisites:

- [_uv_](https://docs.astral.sh/uv/), [_conda_](https://docs.conda.io/projects/conda/), or some [Python](https://www.python.org/downloads/) 3.11 environment
- ASCOM Alpaca-compatible devices or [simulators](https://github.com/ppp-one/alpaca-simulators)
- Optional: [Git](https://git-scm.com/install) (for installation from source)

## Installation Steps

### 1. Clone the _Astra_ repository

```bash
git clone https://github.com/ppp-one/astra.git
cd astra
```

Or, download the ZIP archive from the [GitHub repository](https://github.com/ppp-one/astra/releases) and extract it.

### 2. Set up a Python environment using _uv_ or _conda_

#### Using _uv_ (recommended)

We recommend using [_uv_](https://docs.astral.sh/uv/) because it provides consistent, reproducible dependency management. See the [_uv_ documentation](https://docs.astral.sh/uv/getting-started/installation/) for installation instructions.

Using your terminal, navigate to the _astra_ directory and run:

```bash
# Create a new uv environment, install Astra and its dependencies
uv sync
```

#### Or, using _conda_

Alternatively, you can use [_conda_](https://docs.conda.io/projects/conda/) to create a virtual environment.

Like above, using your terminal, navigate to the _astra_ directory and run:

```bash
# Create a new conda environment
conda create -n astra_env python=3.11

# Activate the environment
conda activate astra_env

# Install Astra in local mode
pip install -e .
```
