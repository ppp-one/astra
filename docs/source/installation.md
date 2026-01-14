# Installation

## Prerequisites

Before installing *Astra*, please ensure you have the following prerequisites:

- [*uv*](https://docs.astral.sh/uv/), [*conda*](https://docs.conda.io/projects/conda/), or some Python 3.11 environment
- ASCOM Alpaca-compatible devices or [simulators](https://github.com/ppp-one/alpaca-simulators)
- Optional: [Git](https://git-scm.com/install) (for installation from source)

## Installation Steps

### 1. Clone the *Astra* repository

```bash
git clone https://github.com/ppp-one/astra.git
cd astra
```

Or, download the ZIP archive from the [GitHub repository](https://github.com/ppp-one/astra.git) and extract it.

### 2. Set up a Python environment using *uv* or *conda*

#### Using *uv* (recommended)

We recommend using [*uv*](https://docs.astral.sh/uv/) because it provides consistent, reproducible dependency management. See the [*uv* documentation](https://docs.astral.sh/uv/getting-started/installation/) for installation instructions.

Using your terminal, navigate to the *astra* directory and run:

```bash
# Create a new uv environment
uv sync
```

#### Or, using *conda*

Alternatively, you can use [*conda*](https://docs.conda.io/projects/conda/) to create a virtual environment.

Like above, using your terminal, navigate to the *astra* directory and run:

```bash
# Create a new conda environment
conda create -n astra_env python=3.11

# Activate the environment
conda activate astra_env

# Install Astra in local mode
pip install -e .
```
