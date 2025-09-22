Utils Module
===========

.. automodule:: astra.utils
   :members:
   :undoc-members:
   :show-inheritance:

Overview
--------

The ``utils`` module provides utility functions for handling time conversions, astronomical calculations, database queries, and other helper functions required for observatory operations.

Key Components
-------------

Time Conversions
~~~~~~~~~~~~~~~

Functions for converting between different time standards used in astronomy:

- ``to_jd``: Convert datetime to Julian Date
- ``time_conversion``: Convert between different time standards

Database Operations
~~~~~~~~~~~~~~~~~

- ``db_query``: Query a federated database for astronomical data within a specified range

Astronomical Calculations
~~~~~~~~~~~~~~~~~~~~~~~

- ``is_sun_rising``: Determine if the sun is rising and when flat fielding is possible
- ``interpolate_dfs``: Interpolate pandas dataframes

Telescope Error Handling
~~~~~~~~~~~~~~~~~~~~~~

- ``check_astelos_error``: Check for telescope errors
- ``ack_astelos_error``: Acknowledge telescope errors