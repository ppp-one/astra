# Operation Guide

Observatory operation with _Astra_ is designed to be as automated and safe as possible, with a focus on robotic observing. This guide covers the key aspects of operating _Astra_, including startup, first operations, web interface, watchdog functionality, weather safety, core logic, and troubleshooting.

## First Operations

After starting _Astra_, we assume the following:

- **Independent Safety:** An independent safety system is in place to monitor the observatory and weather (exposing its state via an ASCOM SafetyMonitor).
  - This system must be capable of independently closing the observatory should the computer running _Astra_ fail.
  - **Recommendation:** Configure this system with weather thresholds that are _less conservative_ than _Astra_'s internal safety monitor. This ensures _Astra_ attempts a graceful closing first, with the independent system acting as a redundant fail-safe.

- **Hardware Status:** All devices are connected, aligned, and the dome is slaved (if applicable).
  - Ensure the `close_dome_on_telescope_error` flag is set correctly for your needs in the [observatory configuration](observatory_configuration), default is `false`.

- **Focus:** The focus position is roughly set in the [observatory configuration](observatory_configuration).

### 1. Create a Commissioning Schedule

Create a schedule file containing the following sequence:

**A. Open and Cool**
First, trigger the open action. This opens the observatory and automatically cools the camera to the temperature defined in your configuration.

```json
// Open observatory and cool
{
  "device_name": "camera_main",
  "action_type": "open",
  "action_value": {},
  "start_time": "2025-08-23 22:30:00.000",
  "end_time": "2025-08-24 10:00:00.000"
}
```

**B. Autofocus**
Run the autofocus routine. Ensure the `filter` matches one installed in your wheel and `exptime` is sufficient for star detection.

```json
// Autofocus
{
  "device_name": "camera_main",
  "action_type": "autofocus",
  "action_value": {
    "exptime": 3,
    "filter": "V",
    "search_range_is_relative": true,
    "search_range": 1000,
    "n_steps": [30, 20],
    "n_exposures": [1, 1]
  },
  "start_time": "2025-08-23 22:35:00.000",
  "end_time": "2025-08-23 22:50:00.000"
}
```

In successful completion, this will set the new focus position in the observatory configuration. Images from the autofocus process are saved in the images/autofocus/ directory in addition to a V-curve plot.

**C. Calibrate guiding and Pointing**
Once focused, calibrate the autoguider and (optionally) build a pointing model.

```json
// Calibrate Guiding
{
  "device_name":"camera_main",
  "action_type":"calibrate_guiding",
  "action_value":{},
  "start_time":"2025-08-23 22:50:00.000",
  "end_time":"2025-08-23 23:10:00.000"
}

// Optional: Pointing Model
{
  "device_name":"camera_main",
  "action_type":"pointing_model",
  "action_value":{},
  "start_time":"2025-08-23 23:10:00.000",
  "end_time":"2025-08-24 00:10:00.000"
}

```

## Startup

Following [Quickstart](../quickstart), `astra` has a few optional startup options:

```{eval-rst}
.. argparse::
   :module: astra.main
   :func: get_parser
   :prog: astra
   :nodescription: true
```

In most cases you will run `astra` without any additional options.

## Web Interface

If you're interested in jumping straight into _Astra_, the web interface is where you'll spend most of your time. Otherwise, please continue reading for more context on how _Astra_ operates.

```{figure} ../_static/ui-robotic-switch-screenshot.jpg
:width: 80%
:align: center
:alt: Top portion of _Astra_'s web interface

Top portion of _Astra_'s web interface
```

_Astra_'s web interface is built with FastAPI and jinja2, with its API documentation available at [http://localhost:8000/docs](http://localhost:8000/docs) after startup.

At the top of the web interface, you will find key status indicators:

- Observatory's name (turns red if any errors are present)
- UTC time
- Watchdog status (green=running, red=stopped)
- Weather safety status (green=safe, red=unsafe)
- Schedule running status (green=on, gray=off)
- Robotic toggle switch (green=on, gray=off)

```{warning}
Toggling the robotic switch **on** will begin any loaded schedule.
```

_Astra_'s web interface is divided into four main sections:

- **Summary**: Displays real-time status of connected devices, including key properties and error states. Latest FITS images, and optionally live webcam feed + all-sky camera, are also shown here.
- **Logs**: Provides access to system and device logs for monitoring and troubleshooting. It also displays the currently loaded schedule and its status.
- **Weather**: Shows current weather conditions, graphs, and the respective safety limits set in the observatory configuration.
- **Controls**: Sky map showing current telescope position. Some basic observatory controls, such as closing the observatory.

## Watchdog

The watchdog serves as the backbone of _Astra_'s operational safety, where it continously monitors:

- **SafetyMonitor and ObservingConditions devices**: If weather conditions are unsafe, the observatory will close.
- **Device Health**: Communication status and responsiveness of all connected devices.
- **Error Management**: System errors and device failures, the observatory will close if critical errors are detected.
- **Schedule Coordination**: If robotic switch is enabled and a valid schedule exists, the scheduler will be initiated.
- **Health Reporting**: Updates a heartbeat dictionary of system status and polled values from devices to permit external heartbeat monitoring via _Astra_'s API.
- **Logs Backup**: Performs daily backups of logs into CSV files of the past 24 hours, purges data older than 3 days from the database to manage size.

Once the watchdog is running, enabling the robotic switch will start the scheduler if a valid schedule is loaded. The scheduler will then execute actions based on the schedule and current conditions.

## Weather Safety

_Astra_ continuously monitors weather conditions using the SafetyMonitor device and the internal safety monitor using the parameters from observatory configuration.
The scheduler handles different action types based on weather dependency:

- **Weather-dependent actions** (require safe conditions): `open`, `object`, `autofocus`, `calibrate_guiding`, `pointing_model`
- **Weather-independent actions** (can run in unsafe weather): `calibration`, `close`, `cool_camera`, `complete_headers`

If weather becomes unsafe during execution, weather-dependent actions will stop, while weather-independent actions continue. In either case, the observatory will close safely if needed. The scheduler will also attempt to resume operations once conditions are safe again (determined by the `max_safe_duration` in the [observatory configuration](observatory_configuration)) and within the schedule's time frame.

## Core Logic

When _Astra_ starts, it goes through three main phases: initialization, device connection, and web interface.

1. **Initialization**
   - **Database**: Creates (if it doesn't exist) a local SQLite database to store polled device data and logs.
   - **Configuration**: Loads both observatory and FITS header configuration.
   - **Queue**: Starts a shared queue for managing communication between device processes.
   - **Flags**: Initializes status flags for running the watchdog, schedule, weather safety, and error-free state.
   - **Schedule**: Checks for and loads an observation schedule, if available.
   - **Devices**: Creates independent processes for each configured device.
2. **Device Connection**
   - **Connect Devices**: Each device process attempts to connect to its hardware.
   - **Polling**: Starts automatic polling of device properties (as dictated by the FITS header configuration).
   - **Safety System**: Watchdog starts monitoring weather, device process health, and system status.
3. **Web Interface**
   - **FastAPI**: jinja2 delivered user interface and API are initialized.

_Astra_ is built around a multi-process architecture, where each device runs in its own process. This design ensures that issues with one device do not affect the overall system's stability. Communication between the main process and device processes is managed through a shared queue.

```{figure} ../_static/core-logic.svg
:width: 80%
:align: center
:alt: Inter-process communication in *Astra* with two Alpaca devices for illustration.

Inter-process communication in *Astra* with two Alpaca devices for illustration.
```

An SQLite database is used for storing polled device data and logs. However, since SQLite does not support concurrent writes, _Astra_ employs a [database worker](https://github.com/dashawn888/sqlite3worker) to manage database access.

Each device process sends its polled data to the main process via the shared queue, which is then managed by the database worker that handles all database writes. This approach prevents database locks and ensures data integrity.

The watchdog reads from SQLite database as part of the weather safety logic, monitoring the history of the SafetyMonitor and ObservingConditions.

Pipes are used for direct communication between the main process and device processes, allowing for efficient command execution and status updates.

_Astra_ uses several open-source libraries for its core logic, namely _alpyca, astrafocus, astropy, cabaret, donuts, fastapi, jinja2, matplotlib, pandas, photutils, psutil, pyyaml, ruamel, yaml, scipy, sqlite3worker_, and _twirl_. Please refer to the source code for further implementation details.

## Troubleshooting

- **Schedule not starting:**
  - Check that watchdog is running
  - Verify robotic switch is enabled
  - Ensure schedule end time is in the future
  - Confirm schedule file format is valid JSONL
  - Verify camera device name exists in configuration

- **Actions skipping:**
  - Check weather conditions for weather-dependent actions
  - Verify camera device name matches configuration exactly
  - Review action parameters for correct format
  - Check for timing conflicts or overlaps
  - Ensure camera has required paired devices configured

- **Incomplete sequences:**
  - Monitor error logs for device communication issues
  - Verify safety conditions throughout sequence
  - Check for sufficient time allocation between actions

- **Invalid action parameters:**
  - Validate JSON syntax in action_value fields
  - Ensure required parameters are present
  - Check coordinate ranges and filter names
