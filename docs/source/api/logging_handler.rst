Logging Handler Module
===================

.. automodule:: astra.logging_handler
   :members:
   :undoc-members:
   :show-inheritance:

Overview
--------

The ``logging_handler`` module provides customized logging facilities for the Astra system. It enables logging to both the console and the database.

Key Components
-------------

- ``DatabaseLoggingHandler``: Custom logging handler that stores log records in database
- ``emit``: Method that handles emitting log records to the appropriate destination
- Automatic log level detection and formatting