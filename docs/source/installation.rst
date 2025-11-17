Installation
============

Prerequisites
------------

Before installing Astra, ensure you have the following prerequisites:

* Python 3.11 or higher
* ASCOM Alpaca-compatible devices or `simulators <https://github.com/ppp-one/alpaca-simulators>`_ 
* Git (for installation from source)
* *Optional*: `Gaia-2MASS sqlite catalogue <https://drive.google.com/file/d/1xg23KtKkl_0b0zLuDpouUjTh3klyae2c/view>`_ (18 GB)
   * Catalogue of 300M Gaia stars cross matched with 2MASS, proper motion included (see `here <https://github.com/ppp-one/gaia-tmass-sqlite>`_ for details)
   * This is required for plate solving and autofocus field selection features.
   * Please place it somewhere accessible, you'll require its path during Astra's first start up.

Installation Steps
-----------------

1. Clone the Astra repository:

   .. code-block:: bash

       git clone https://github.com/ppp-one/astra.git
       cd astra

2. Create a virtual environment with conda or venv:

   * Using conda:

     .. code-block:: bash

       conda create -n astra_env python=3.11
       conda activate astra_env

   * Using venv:
   
     .. code-block:: bash

       python -m venv astra_env
       source astra_env/bin/activate  # On Windows use: astra_env\Scripts\activate

3. Install Astra in local mode:

   .. code-block:: bash

        pip install -e .

   To include optional dependencies (e.g., for development or documentation), append extras like `"[dev]"`, `"[docs]"`, or `"[test]"`.

This will install Astra and all its python dependencies.