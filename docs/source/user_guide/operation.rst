Operation Guide
================

Observatory operation with Astra is designed to be as automated and safe as possible, with a focus on robotic observing. This guide covers the key aspects of operating Astra, including startup, web interface,watchdog functionality, weather safety, core logic, and troubleshooting.

Startup
-------

Following :doc:`../getting_started`, `astra` has a few optional startup options:

.. code-block:: text

    usage: astra [-h] [--debug] [--port PORT] [--truncate TRUNCATE] [--observatory {speculoos}] [--reset]

    Run Astra

    options:
        -h, --help            show this help message and exit
        --debug               run in debug mode (default: false)
        --port PORT           port to run the server on (default: 8000)
        --truncate TRUNCATE   truncate schedule by factor and reset time start time to now (default: None)
        --observatory {speculoos}
                                        specify observatory name (default: None)
        --reset               reset the Astra's base config

In most cases, unless you're SPECULOOS or developing Astra, you will run without any options.

.. note:: SPECULOOS requires certain workarounds due to the quirks of its ASTELCO made observatory -- namely, custom error handling and some of its ASCOM methods not conforming asynchronous standards.


.. logic, best practices, safety no. 1

When Astra starts, it goes through three main phases: initialization, device connection, and web interface.

**1. Initialization**  
    - **Database**: Creates (if it doesn't exist) a local SQLite database to store polled device data and logs.  
    - **Configuration**: Loads both observatory and FITS header configuration.
    - **Queue**: Starts a shared queue for managing communication between device processes.  
    - **Flags**: Initializes status flags for running the watchdog, schedule, weather safety, and error-free state.  
    - **Schedule**: Checks for and loads an observation schedule, if available.  
    - **Devices**: Creates independent processes for each configured device.  

**2. Device Connection**  
    - **Connect Devices**: Each device process attempts to connect to its hardware.
    - **Polling**: Starts automatic polling of device properties (as dictated by the FITS header configuration).
    - **Safety System**: Watchdog starts monitoring weather, device process health, and system status.  

**3. Web Interface**  
    - **FastAPI**: jinja2 delivered user interface and API are initialized.


Web Interface
-----------------

If you're interested in jumping straight into Astra, the web interface is where you'll spend most of your time. Otherwise, please continue reading for more context on how Astra operates.

.. figure:: ../_static/ui-robotic-switch-screenshot.jpg
   :width: 80%
   :align: center
   :alt: Top portion of Astra's web interface

   Top portion of Astra's web interface

Astra's web interface is built with FastAPI and jinja2, with its API documentation available at http://localhost:8000/docs after startup.

At the top of the web interface, you will find key status indicators:
    - Observatory's name (turns red if any errors are present)
    - UTC time
    - Watchdog status (green=running, red=stopped)
    - Weather safety status (green=safe, red=unsafe)
    - Schedule running status (green=on, gray=off)
    - Robotic toggle switch (green=on, gray=off)

.. warning:: Toggling the robotic switch **on** will begin any loaded schedule.

Astra's web interface is divided into four main sections:
    - **Summary**: Displays real-time status of connected devices, including key properties and error states.
    - **Logs**: Provides access to system and device logs for monitoring and troubleshooting. It also displays the currently loaded schedule and its status.
    - **Weather**: Shows current weather conditions, graphs, and the respective safety limits set in the observatory configuration.
    - **Controls**: Some basic observatory controls, such as closing the observatory.


Watchdog
---------

The watchdog serves as the backbone of Astra's operational safety, where it continously monitors:

- **SafetyMonitor and ObservingConditions devices**: If weather conditions are unsafe, the observatory will close
- **Device Health**: Communication status and responsiveness of all connected devices
- **Error Management**: System errors and device failures, the observatory will close if critical errors are detected
- **Schedule Coordination**: If robotic switch is enabled and a valid schedule exists, the scheduler will be started
- **Health Reporting**: Updates a heartbeat dictionary of system status and polled values from devices for external heartbeat monitoring
- **Logs Backup**: Performs daily backups of logs

Once the watchdog is running, enabling the robotic switch will start the scheduler if a valid schedule is loaded. The scheduler will then execute actions based on the schedule and current conditions.


Weather Safety
---------------

Astra continuously monitors weather conditions using the SafetyMonitor device and the internal safety monitor using the parameters from observatory configuration. 
The scheduler handles different action types based on weather dependency:

**Weather-dependent actions** (require safe conditions):
    - ``open``, ``object``, ``autofocus``, ``calibrate_guiding``, ``pointing_model``

**Weather-independent actions** (can run in unsafe weather):
    - ``calibration``, ``close``, ``cool_camera``, ``complete_headers``

If weather becomes unsafe during execution, weather-dependent actions will stop, while weather-independent actions continue. In either case, the observatory will close safely if needed.  The scheduler will also attempt to resume operations once conditions are safe again.


Core Logic
-----------------

Astra is built around a multi-process architecture, where each device runs in its own process. This design ensures that issues with one device do not affect the overall system's stability. Communication between the main process and device processes is managed through a shared queue.

.. figure:: ../_static/core-logic.svg
   :width: 80%
   :align: center
   :alt: Inter-process communication in Astra with two Alpaca devices for illustration.

   Inter-process communication in Astra with two Alpaca devices for illustration.

An SQLite database is used for storing polled device data and logs. However, since SQLite does not support concurrent writes, Astra employs a `database worker <https://github.com/dashawn888/sqlite3worker>`_ to manage database access. 

Each device process sends its polled data to the main process via the shared queue, which is then managed by the database worker that handles all database writes. This approach prevents database locks and ensures data integrity.

The watchdog reads from SQLite database as part of the weather safety logic, monitoring the history of the SafetyMonitor and ObservingConditions.

Pipes are used for direct communication between the main process and device processes, allowing for efficient command execution and status updates.

Astra uses several open-source libraries for its core logic, namely astropy, alpyca, sqlite3worker, fastapi, pandas, twirl, photutils, donuts, psutil, pyyaml, and astrafocus. Please refer to the source code for further implementation details.


Troubleshooting
--------------

**Schedule not starting:**
    - Check that watchdog is running
    - Verify robotic switch is enabled
    - Ensure schedule end time is in the future
    - Confirm schedule file format is valid JSONL
    - Verify camera device name exists in configuration

**Actions skipping:**
    - Check weather conditions for weather-dependent actions
    - Verify camera device name matches configuration exactly
    - Review action parameters for correct format
    - Check for timing conflicts or overlaps
    - Ensure camera has required paired devices configured

**Incomplete sequences:**
    - Monitor error logs for device communication issues
    - Verify safety conditions throughout sequence
    - Check for sufficient time allocation between actions

**Invalid action parameters:**
    - Validate JSON syntax in action_value fields
    - Ensure required parameters are present
    - Check coordinate ranges and filter names