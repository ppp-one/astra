# Operation Guide

```{image} ../_static/undraw_adventure_9my9.svg
:class: responsive-banner
:align: center
:alt: banner
```

_Astra_ prioritizes safe, fully automated robotic observing. This guide details operational workflows including startup, initial calibration, the web interface, watchdog mechanisms, weather safety, core logic, and troubleshooting.

## Prerequisites

For reliable and safe operation, ensure the following prerequisites are met:

- **Independent Safety:** An independent safety system is in place to monitor the observatory and weather (exposing its state as an ASCOM SafetyMonitor).
  - This system must be capable of independently closing the observatory should the computer running _Astra_ fail.
  - **Recommendation:** Configure the independent safety monitor with slightly relaxed weather thresholds compared to _Astra_. This ensures _Astra_ triggers closure first, reserving the independent system as a true fail-safe.

- **Network Stability:** ASCOM Alpaca devices are reachable over the network and properly configured in the observatory configuration file.

- **Properly Configured Hardware:** All device hardware is connected, aligned, and the dome is slaved (if not roll-off). In the [observatory configuration](observatory_configuration), please ensure:
  - `close_dome_on_telescope_error` flag is set correctly for your needs, default is `false`.
  - The `focus_position` is set to a known good value.
  - The target camera `temperature` is defined; _Astra_ uses this value during cooling sequences.

- **Accurate Timekeeping:** The system clock is set to UTC and is accurate (e.g., via NTP).

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

## First Schedule - Autofocus & Calibrate Guiding

Create a schedule file containing the following sequence (adjust times as needed):

**1. Open**
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

**2. Autofocus**
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

Upon successful completion, the optimized focus position is updated in the configuration. Autofocus images and V-curve plot are saved to the `images/autofocus` directory.

**3. Calibrate guiding and Pointing**
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

## Web Interface

```{figure} ../_static/ui-robotic-switch-screenshot.jpg
:width: 80%
:align: center
:alt: Top portion of _Astra_'s web interface

Top portion of _Astra_'s web interface
```

_Astra_ provides a modern web interface for monitoring and control. API documentation is available at [http://localhost:8000/docs](http://localhost:8000/docs) after startup.

The header bar displays critical system status:

- **Observatory Name**: Turns red if system errors are present.
- **UTC Time**: Current universal time.
- **Watchdog Status**: Green when the watchdog is running, red if stopped.
- **Weather Status**: Green indicates safe conditions, red indicates unsafe - dictated by the SafetyMonitor and internal safety monitor logic.
- **Schedule Status**: Green when a schedule is active, gray when idle.
- **Robotic Operations Switch**: The master switch for automated control (green=enabled, gray=disabled).

```{warning}
Enabling the **Robotic Operations Switch** will immediately start processing any valid, loaded schedule.
```

The interface is organized into four main operational views:

- **Summary**: Real-time dashboard of device status, error states, and telemetry. Also displays the latest scientific images and, if configured, live webcam or all-sky feeds.
- **Logs**: Centralized view for system and device logs for diagnostics. The currently loaded schedule and line-by-line execution status are also displayed here.
- **Weather**: Detailed environmental monitoring including 3-day graph history and current values against safety thresholds defined in the observatory configuration.
- **Controls**: Interactive sky map showing telescope position and some manual override controls.

## Watchdog

The watchdog is the backbone of _Astra_'s operational safety and automation. It continuously monitors the system state and manages:

- **Weather Safety**: Automatically closes the observatory if the SafetyMonitor or ObservingConditions report unsafe parameters.
- **Device Health**: Tracks connectivity and responsiveness of all hardware.
- **Error Management**: In the event of system errors or critical device failures, the observatory is automatically safely closed.
- **Schedule Execution**: Triggers the scheduler when the Robotic Operations Switch is enabled and a valid schedule is authorized.
- **System Heartbeat**: Maintains a real-time status object (accessible via API) for external monitoring services.
- **Data Retention**: Archives the last 24 hours of logs to CSV format daily and purges database records older than 3 days to maintain performance.

When the watchdog is active and the Robotic Operations Switch is enabled, the system enters autonomous mode and begins executing the schedule. If a new schedule is loaded while running, the robotic switch needs to be toggled off and on again to unload the previous schedule and start the new one.

## Weather Safety

_Astra_ ensures observatory equipment safety by monitoring conditions via the configured ASCOM SafetyMonitor and its internal logic.

The scheduler distinguishes between two types of actions:

- **Weather-dependent**: Cannot run in unsafe conditions (e.g., `open`, `object`, `autofocus`, `calibrate_guiding`, `pointing_model`).
- **Weather-independent**: Safe to run regardless of weather (e.g., `calibration`, `close`, `cool_camera`, `complete_headers`).

If weather becomes unsafe, the system immediately suspends weather-dependent actions and closes the observatory. Weather-independent actions (like taking dark frames) may continue.

**Automatic Resumption**: Operations resume automatically when conditions return to safe levels for a period defined by the `max_safe_duration` setting, provided the schedule is still valid for the current time.

## Core Logic

_Astra_'s startup sequence consists of three phases:

1. **Initialization**
   - **System Setup**: Initializes the SQLite database, shared communication queues, and global state flags.
   - **Configuration**: Loads observatory settings and FITS header templates.
   - **Process Creation**: Spawns isolated processes for each configured device (Camera, Telescope, etc.).
2. **Device Connection**
   - **Handshake**: Device processes connect to their respective hardware drivers.
   - **Telemetry**: Devices begin polling properties defined in the FITS header configuration.
   - **Safety**: The Watchdog begins monitoring device health and weather conditions.
3. **Interface Launch**
   - **API & UI**: The web server and user interface come online.

### System Architecture

_Astra_ utilizes a **multi-process architecture** for stability. Each hardware device runs in its own isolated process, ensuring that a single driver failure does not crash the entire observatory.

```{figure} ../_static/core-logic.svg
:width: 80%
:align: center
:alt: Inter-process communication diagram

Inter-process communication in *Astra* using separate device processes.
```

**Data Management**
All telemetry and logs are stored in a local SQLite database. To handle high-concurrency writes from multiple device processes without locking issues, _Astra_ uses a dedicated **Database Worker**.

- Devices send data to a shared queue.
- The Database Worker consumes the queue and handles all write operations.
- The Watchdog reads historical weather data from the DB to make informed safety decisions.

**Communication**

- **Queues**: Used for transferring bulk data (logs, telemetry) to the database.
- **Pipes**: Used for direct, low-latency command execution between the main controller and device processes.

For a specific list of dependencies (including _astropy, fastapi, photutils_, etc.), please refer to the `pyproject.toml` file or the source code.

## Troubleshooting

**Schedule Not Starting?**

- **Watchdog**: Ensure the Watchdog is running.
- **Mode**: Verify the Robotic Operations Switch is enabled.
- **Time**: Confirm the schedule's `start_time` and `end_time` are valid for the current time.
- **Syntax**: Validate the schedule file is valid JSONL format.
- **Config**: Ensure referenced device names match your configuration exactly.

**Actions Skipping?**

- **Weather**: Weather-dependent actions skip automatically during unsafe conditions.
- **Consistency**: Check that device names in the schedule match `observatory.yaml`.
- **Parameters**: specific formatting errors in `action_value` can cause skipping.
- **Conflicts**: Check for timing overlaps or impossible constraints.
- **Dependencies**: Ensure required paired devices (e.g., Focuser for Autofocus) are configured.

**Incomplete Sequences?**

- **Logs**: Check "Error" logs for device timeouts or disconnects.
- **Safety**: Intermittent weather issues can abort a running sequence.
- **Timing**: Ensure sufficient duration was allocated for the action to complete.

**Invalid Parameters?**

- **JSON**: Validate JSON syntax in `action_value` fields.
- **Required Fields**: Ensure no mandatory parameters are missing.
- **Ranges**: Check that coordinates (RA/Dec) and filter names are valid.
