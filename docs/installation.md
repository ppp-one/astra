# Installation

Clone the Astra Github repository with

```bash
git clone https://github.com/ppp-one/astra.git
```

then, depending on your use case, install Astra with the following methods.

## pip
Astra can be installed as a local python package with

```bash
pip install -e {path_to_astra}
```

It is highly recommended to install Astra in a virtual/conda environment. For example:
```bash
conda create -n astra python=3.11
```

## conda
Astra dependencies can be installed in a fresh conda environment with

```bash
conda env create -f {path_to_astra}/{operating system}-environment.yml
```

## Testing installation 
> only if installed as a local python package with pip

Once installed, activate your environment (highly recommended) and run

```bash
conda activate astra
python -c "from astra import Astra"
```

If no error is raised you're all set!