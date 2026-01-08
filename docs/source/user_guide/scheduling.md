# Scheduling Syntax

Astra uses a flexible scheduling system to automate observatory operations. Schedules are defined using JSONL files (JSON Lines format), where each line represents a scheduled action with these fields:

- `device_name`: Name of the camera device (the primary instrument that coordinates all operations)
- `action_type`: Type of action to perform
- `action_value`: Parameters for the action
- `start_time`: When the action should start (UTC ISO format: YYYY-MM-DD HH:MM:SS.sss)
- `end_time`: Latest when the action should end (UTC ISO format: YYYY-MM-DD HH:MM:SS.sss)

```{admonition} Instrument-Centric Design
All scheduled actions specify a camera as the `device_name`. The camera acts as the primary instrument that coordinates operations with its paired devices (telescope, dome, filter wheel, focuser, etc.). This design ensures all devices work together as a cohesive system.
```

## Example Schedule

```json
// open observatory
{"device_name":"camera_main","action_type":"open","action_value":{},"start_time":"2025-08-23 22:38:25.210","end_time":"2025-08-24 10:49:15.363"}
// dusk sky flats
{"device_name":"camera_main","action_type":"flats","action_value":{"filter":["r'", "g'"],"n":[20, 20]},"start_time":"2025-08-23 22:39:25.210","end_time":"2025-08-23 23:16:00.018"}
// science observations
{"device_name":"camera_main","action_type":"object","action_value":{"object":"Kepler-1","filter":"r'","ra":286.808542,"dec":49.316422,"exptime":8,"guiding":true,"pointing":true},"start_time":"2025-08-23 23:17:00.018","end_time":"2025-08-24 04:43:40.018"}
// dawn sky flats
{"device_name":"camera_main","action_type":"flats","action_value":{"filter":["r'", "g'"],"n":[20, 20]},"start_time":"2025-08-24 10:24:40.018","end_time":"2025-08-24 10:49:15.363"}
// close observatory
{"device_name":"camera_main","action_type":"close","action_value":{},"start_time":"2025-08-24 10:49:15.363","end_time":"2025-08-24 11:49:15.363"}
// calibration frames, biases and darks
{"device_name":"camera_main","action_type":"calibration","action_value":{"exptime":[0,10,15,30,38,60,120],"n":[10,10,10,10,10,10,10]},"start_time":"2025-08-24 10:55:15.363","end_time":"2025-08-24 11:49:15.363"}
```

```{admonition} JSONL Comments
Astra's JSONL files support comments using lines that start with `//`:
```

## Schedule File Location

Place your schedule file in the observatory schedules directory with a `.jsonl` extension. For example:

- `~/Documents/Astra/schedules/observatory_name.jsonl`

Astra will automatically detect and load the JSONL schedule file, with the specified name pattern, if modified.

## Supported Action Types

Astra supports the following action types for observatory automation, organized by function:

- `open`: Open observatory
- `close`: Close observatory
- `cool_camera`: Activate camera cooling
- `object`: Capture light frames with optional pointing correction/autoguiding
- `calibration`: Capture dark and bias frames
- `flats`: Capture sky flat field frames
- `autofocus`: Autofocus
- `calibrate_guiding`: Calibrate guiding parameters
- `pointing_model`: Help build a telescope pointing model
- `complete_headers`: Complete FITS headers of all images captured

```{note}
The `complete_headers` action automatically runs at the end of every schedule execution to ensure complete metadata in all FITS files.
```

## Action Value Parameters

Each action type requires specific parameters in the `action_value` field.
The sections below are generated automatically from the action configuration
dataclasses to ensure the documentation always matches the implementation.

```{eval-rst}
.. autoscheduleactions::
   :format: literal
```
