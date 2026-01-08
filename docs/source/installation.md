# Installation

## Prerequisites

Before installing *Astra*, please ensure you have the following prerequisites:

- [*uv*](https://docs.astral.sh/uv/), [*conda*](https://docs.conda.io/projects/conda/), or some Python 3.11 environment
- ASCOM Alpaca-compatible devices or [simulators](https://github.com/ppp-one/alpaca-simulators)
- [Git](https://git-scm.com/install) (for installation from source)
- *Optional*: [Gaia-2MASS sqlite catalogue](https://drive.google.com/file/d/1xg23KtKkl_0b0zLuDpouUjTh3klyae2c/view) (18 GB)
  - Catalogue of 300M Gaia stars cross matched with 2MASS, proper motion included (see [here](https://github.com/ppp-one/gaia-tmass-sqlite) for details)
  - This is required for offline plate solving and autofocus field selection features.
  - Please place it somewhere accessible, you'll require its path during *Astra*'s first start up.

## Installation Steps

### 1. Clone the *Astra* repository

```bash
git clone https://github.com/ppp-one/astra.git
cd astra
```

### 2. Set up a Python environment using *uv* or *conda*

#### Using *uv* (recommended)

We recommend using [*uv*](https://docs.astral.sh/uv/) because it provides consistent, reproducible dependency management. See the [*uv* documentation](https://docs.astral.sh/uv/getting-started/installation/) for installation instructions.

```bash
# Install dependencies and set up virtual environment
uv sync

# Activate the virtual environment
source .venv/bin/activate
```

<!-- ````{dropdown} Environment Activation
:icon: info
:color: info
:open:

Always remember to activate your virtual environment before running astra.

Alternatively, you can run
~~~bash
uv run astra
~~~
in the directory where astra is installed, which will automatically use `.venv`.


```{dropdown} Automatic Environment Detection
:icon: info
:color: info

If installed, you can also consider using `direnv` to auto-activate the python
environment in the directory where *Astra* is installed using
~~~bash
echo 'source .venv/bin/activate' > .envrc
direnv allow
~~~
```

```` -->

#### Or, using *conda*

Alternatively, you can use [*conda*](https://docs.conda.io/projects/conda/) to create a virtual environment:

```bash
# Create new conda environment
conda create -n astra_env python=3.11

# Activate the environment
conda activate astra_env

# Install Astra in local mode
pip install -e .
```
