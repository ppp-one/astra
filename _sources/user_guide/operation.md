# Operation Guide

```{image} ../_static/operation-banner.svg
:class: responsive-banner
:align: center
:alt: banner
```

_Astra_ prioritizes safe, fully automated robotic observing. This guide details operational workflows including first schedule, the web interface, startup options, and troubleshooting.

## Prerequisites

For reliable and safe operation, please ensure the following prerequisites are met:

- **Independent Safety:** An independent safety system is in place to monitor the observatory and weather (exposing its state as an ASCOM SafetyMonitor).
  - This system must be capable of independently closing the observatory should the computer running _Astra_ fail.
  - **Recommendation:** Configure the independent safety monitor with slightly relaxed weather thresholds compared to _Astra_ (e.g., safety monitor set to trigger at 10% higher wind speeds than _Astra_). This ensures _Astra_ triggers closure first, reserving the independent system as a fail-safe.

- **Properly Configured Hardware:** All device hardware is connected, aligned, and the dome is slaved (if not roll-off). In the [observatory configuration](observatory_configuration), please ensure:
  - `close_dome_on_telescope_error` flag is set correctly for your needs, default is `false`. Independent to this, the observatory will always close if a non-telescope/dome device encounters an error.
  - The `focus_position` is set to a known good value.
  - The target camera `temperature` is defined; _Astra_ uses this value during cooling sequences.

- **Accurate Timekeeping:** The system and telescope clocks are accurate (e.g., via NTP or GPS).

## First Schedule - Autofocus & Calibrate Guiding

On first schedule run, you should autofocus and calibrate guiding to ensure optimal image quality and guiding performance for subsequent schedules.

Create a schedule file containing the following sequence (adjust times as needed) and then turn the robotic switch on (see [Web Interface](#web-interface) below):

**1. Open**
First, trigger the [open action](scheduling.md#open-action). This opens the observatory and automatically cools the camera to the temperature defined in your configuration.

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
Run the [autofocus action](scheduling.md#autofocus-action). Ensure the `filter` matches one installed in your wheel.

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

<!-- **3. Optional: Pointing model**
Once focused, build a pointing model. Each pointing is plate solved and sends SyncToCoordinates commands to the mount. The receipt of these commands can be used to build a pointing model in the mount control software. _Astra_ itself does not build or maintain a pointing model.

```json
// Optional: Pointing Model
{
  "device_name": "camera_main",
  "action_type": "pointing_model",
  "action_value": {},
  "start_time": "2025-08-23 23:10:00.000",
  "end_time": "2025-08-24 00:10:00.000"
}
``` -->

**3. Calibrate guiding**
Finally, [calibrate the autoguider parameters](scheduling.md#calibrate-guiding-action). The schedule measures the response of the PulseGuide command and the orientation of the camera on the sky.

```json
// Calibrate Guiding
{
  "device_name": "camera_main",
  "action_type": "calibrate_guiding",
  "action_value": {},
  "start_time": "2025-08-23 22:50:00.000",
  "end_time": "2025-08-23 23:10:00.000"
}
```

After this, you can run science observation schedules with guiding enabled.

## Web Interface

```{figure} ../_static/ui-robotic-switch-screenshot.jpg
:width: 80%
:align: center
:alt: Top portion of _Astra_'s web interface

Top portion of _Astra_'s web interface
```

_Astra_ provides a modern web interface for monitoring and control. If needed, API documentation is available at [http://localhost:8000/docs](http://localhost:8000/docs) after startup or here at [API Endpoints](../api/endpoints) -- but most users will interact with the web interface for operation.

The header bar displays critical system status:

- **Observatory Name**: Turns red if system errors are present.
- **UTC Time**: Current universal time.
- **Watchdog Status**: Green when the watchdog is running, red if stopped. It should always be running during operation to ensure safety mechanisms are active. If stopped, the system will not execute any schedule actions and will ignore weather and device statuses.
- **Weather Status**: Green indicates safe conditions, red indicates unsafe - dictated by the SafetyMonitor and internal safety monitor logic.
- **Schedule Status**: Green when a schedule is active, gray when idle.
- **Robotic Operations Switch**: The master switch for automated control (green=enabled, gray=disabled).

```{warning}
Enabling the **Robotic Operations Switch** will immediately start processing any valid, loaded schedule.

To reload a new schedule while running, you must first disable the robotic switch, then enable it again after the schedule has changed. This is to ensure the previous schedule is fully unloaded before starting the new one.
```

The interface is organized into four main operational views:

- **Summary**: Real-time dashboard of device status, error states, and polled data. Also displays the latest scientific images and, if configured, live webcam or all-sky feeds.
- **Logs**: Centralized view for system and device logs. It also displays the currently loaded schedule and line-by-line execution status, and guiding graphs.
- **Weather**: Detailed environmental monitoring including 3-day graph history, current values, and safety limit thresholds used by the internal safety monitor logic (defined in the [observatory configuration](observatory_configuration.md#observingconditions-configuration)).
- **Controls**: Sky map showing telescope position and some manual override controls.

## Startup Options

Following [Quickstart](../quickstart), `astra` has a few optional startup options:

```{eval-rst}
.. argparse::
   :module: astra.main
   :func: get_parser
   :prog: astra
   :nodescription: true
```

```{important}
Each _Astra_ instance runs exactly one observatory at a time, determined by the `observatory_name` field in your base configuration file (`~/.astra/astra_config.yml`).
```

**Key Options:**

- **`--config`**: Specify a different base configuration file. Useful for running multiple observatories (each needs different `--config` and `--port`).
- **`--observatory`**: Select a custom Python subclass for site-specific behavior (see [Custom Observatories](custom_observatories)). Optional - only needed if you've created custom subclasses.

In most cases you will run `astra` without any additional options.

## Troubleshooting

**Schedule Not Starting?**

- **Watchdog**: Ensure the Watchdog is running.
- **Robotic Mode**: Verify the Robotic Operations Switch is enabled.
- **Time**: Confirm the schedule's `start_time` and `end_time` are valid for the current time.
- **Syntax**: Validate the schedule file is valid JSONL format.
- **Config**: Ensure referenced device names match your configuration exactly.

**Actions Skipping?**

- **Weather**: Weather-dependent actions do not run during unsafe conditions.
- **Consistency**: Check that device names in the schedule match `observatory.yaml`.
- **Conflicts**: Check for timing overlaps or impossible constraints.
- **Dependencies**: Ensure required paired devices are configured.

**Incomplete Sequences?**

- **Safety**: Intermittent weather issues can abort a running sequence.
- **Timing**: Ensure sufficient duration was allocated for the action to complete.
