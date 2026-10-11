# Scheduling Syntax

```{image} ../_static/scheduling-banner.svg
:class: responsive-banner
:align: center
:alt: banner
```

_Astra_ uses a schedule to operate the observatory automatically. A schedule is a
JSONL file (JSON Lines format). Each line of the file is one JSON object, and each
object is one action. An action has these fields:

- `device_name`: The name of the camera. The camera tells _Astra_ which other
  devices the action uses through its configured paired devices.
- `action_type`: The type of the action.
- `action_value`: The parameters of the action.
- `start_time`: The first time at which the action can start. Use the UTC ISO
  format YYYY-MM-DD HH:MM:SS.sss.
- `end_time`: The last time at which the action can run. Use the UTC ISO format
  YYYY-MM-DD HH:MM:SS.sss.

```{admonition} Instrument-Centric Design
Each action gives a camera as the `device_name`. The camera does not control the
other devices. It tells _Astra_ which devices the action uses. The observatory
configuration gives the `paired_devices` of each camera: the telescope, the dome,
the filter wheel and the focuser. _Astra_ operates these devices together for the
action.
```

```{admonition} Timing and Execution Flow
The `start_time` and `end_time` fields give the window in which the action is
valid. They do not give a fixed duration.

* **Early completion**: If an action is completed correctly before its `end_time`,
  _Astra_ does **not** wait. It starts the next action immediately. If the next
  action cannot start before its `start_time`, _Astra_ waits until that time.
* **Order**: _Astra_ does the actions in sequence, in the order of their start
  times. The next action does not start until the current action is completed.
  This is also true when the `start_time` of the next action is in the past. To
  let actions run at the same time, set `execute_parallel` to true.
```

## Example Schedule

```json
// open observatory
{
   "device_name":"camera_main",
   "action_type":"open",
   "action_value":{},
   "start_time":"2025-08-23 22:38:25.210",
   "end_time":"2025-08-24 10:49:15.363"
}
// dusk sky flats
{
   "device_name":"camera_main",
   "action_type":"flats",
   "action_value":{"filter":["r'", "g'"],"n":[20, 20]},
   "start_time":"2025-08-23 22:39:25.210",
   "end_time":"2025-08-23 23:16:00.018"
}
// science observations
{
   "device_name":"camera_main",
   "action_type":"object",
   "action_value":{"object":"Kepler-1","filter":"r'","ra":286.808542,"dec":49.316422,"exptime":8,"guiding":true,"pointing":true},
   "start_time":"2025-08-23 23:17:00.018",
   "end_time":"2025-08-24 04:43:40.018"
}
// dawn sky flats
{
   "device_name":"camera_main",
   "action_type":"flats",
   "action_value":{"filter":["r'", "g'"],"n":[20, 20]},
   "start_time":"2025-08-24 10:24:40.018",
   "end_time":"2025-08-24 10:49:15.363"
}
// close observatory
{
   "device_name":"camera_main",
   "action_type":"close",
   "action_value":{},
   "start_time":"2025-08-24 10:49:15.363",
   "end_time":"2025-08-24 11:49:15.363"
}
// calibration frames, biases and darks
{
   "device_name":"camera_main",
   "action_type":"calibration",
   "action_value":{"exptime":[0,10,15,30,38,60,120],"n":[10,10,10,10,10,10,10]},
   "start_time":"2025-08-24 10:55:15.363",
   "end_time":"2025-08-24 11:49:15.363"
}
```

```{admonition} JSONL Comments
In a JSONL file, a line that starts with `//` is a comment. _Astra_ ignores it.
```

## Schedule File Location

Put the schedule file in the schedules directory of the observatory. The file must
have the extension `.jsonl`. For example:

- `~/Documents/Astra/schedules/{observatory_name}.jsonl`

_Astra_ finds a file with this name and loads it. If you change the file, _Astra_
loads it again.

## Supported Action Types

_Astra_ has these action types:

- `open`: Open the observatory.
- `close`: Close the observatory.
- `cool_camera`: Start the camera cooling.
- `object`: Record light frames. Pointing correction and autoguiding are optional.
- `calibration`: Record dark frames and bias frames.
- `flats`: Record sky flat frames.
- `autofocus`: Focus the telescope.
- `calibrate_guiding`: Calibrate the guiding parameters.
- `pointing_model`: Collect data for a telescope pointing model.
- `complete_headers`: Complete the FITS headers of all the images.

```{note}
_Astra_ runs the `complete_headers` action at the end of each schedule. This makes
sure that all the FITS files have the full metadata.
```

```{note}
Each action runs `cool_camera` first. This makes sure that the camera is at the
correct temperature before an exposure starts. The `open` and `close` actions run
`cool_camera` after they are completed.
```

## Tracking Moving Targets

Solar system bodies and artificial satellites move against the background stars.
You can see this movement in one exposure. To correct for it, _Astra_ commands
differential tracking rates in right ascension and declination.

To observe such a target, give a `lookup_name` in the `object` action. Give the
name alone, because _Astra_ points the mount from the ephemeris of the target. A
fixed position cannot describe a moving target. If you also give `ra` and `dec`,
or `alt` and `az`, _Astra_ rejects the schedule. _Astra_ rejects a `tle` with one
of these pairs in the same way.

You do not have to give `object` when you give `lookup_name`. _Astra_ then uses
`lookup_name` as the target name in the FITS `OBJECT` header and in the file name.
For a `tle`, the default name is the NORAD catalog number from line 1, for example
`NORAD 25544`. Give `object` if you want a different name.

_Astra_ finds the position for `lookup_name` when it loads the schedule. The source
of the position sets the type of tracking:

| Source of the position | Example | Tracking |
| --- | --- | --- |
| Astropy's built-in ephemeris | `"mars"`, `"moon"` | Non-sidereal |
| JPL Horizons small-body search | `"C/2023 A3"`, `"Ceres"` | Non-sidereal |
| A two-line element set that you give in `tle` | The ISS | Non-sidereal |
| SIMBAD (stars and deep-sky objects) | `"M31"`, `"Vega"` | Sidereal |

### Sequence of operations

1. **Pointing.** The mount slews to the calculated position of the target at
   `start_time`. A planet or a comet moves less than one arcsecond during a normal
   slew. Thus the mount points at it correctly. A satellite moves much more
   quickly. For example, the ISS moves about 20 degrees in 30 seconds. The mount
   cannot slew directly to a target that moves this quickly.

   For a satellite, set `nonsidereal_start_lead_time_seconds`. Make it equal to or
   more than the slew time and the settling time of the mount. The mount then
   slews to the position of the target at `start_time` plus this number of
   seconds. The mount waits there for the target.

   The exposures can start later than planned. The target can then be more than
   one arcminute from the position of the mount. If this occurs, _Astra_ re-centers
   the mount before the exposures start. This limit is an angle, not a time. Thus
   the delay that is possible changes with the speed of the target. For Mars, the
   delay must be about 45 minutes before a re-center is necessary. For the ISS, a
   delay of a few milliseconds is enough.

2. **Tracking.** _Astra_ applies the differential rates and refreshes them during
   the full sequence. This includes the exposures and the time when _Astra_ writes
   each frame to the disk. The re-center interval starts when _Astra_ applies the
   rates for the first time.
3. **Re-centering.** At each interval of `nonsidereal_recenter_interval` seconds,
   the mount slews to the current ephemeris position of the target. If you set the
   interval to zero, the mount does not do these slews. Only the rates then keep
   the target in the field. This does not stop non-sidereal tracking.
4. **Reset.** _Astra_ sets the rates to zero when the sequence ends, and also when
   an error occurs. _Astra_ sets the rates to zero before each slew. So a sequence
   that stopped incorrectly cannot leave old rates on the mount.

### Why periodic re-centering is necessary

Differential rates are an open-loop control of speed. They tell the mount how
quickly to move, but not where to point. So they remove the apparent movement of
the target. They do not correct a position error that already occurred.

_Astra_ stops autoguiding during non-sidereal tracking. So re-centering is the
only feedback of position. Without it, three errors increase continuously.

The first pointing error continues. The initial slew leaves an offset. The offset
comes from the pointing model, from an old ephemeris, or from the movement of the
target during the slew. The rates keep this offset.

Rate errors become position errors. No mount applies a commanded rate exactly, and
the correct rate changes between the updates.

_Astra_ does not correct mechanical defects. Periodic error, flexure and polar
misalignment have the same effect during non-sidereal tracking as during sidereal
tracking.

The interval is a balance between drift and dead time. Each re-center needs a slew
and a settling time. If a fast target moves more than one arcsecond during the
first slew, the mount does a second slew to correct this. For a comet or an
asteroid, start with an interval of a few minutes. A target that moves more quickly
needs a shorter interval.

### Requirements and limitations

- **The mount must have differential rates.** It must report the ASCOM
  capabilities `CanSetRightAscensionRate` and `CanSetDeclinationRate`. If no
  telescope in the observatory reports the two capabilities, _Astra_ rejects the
  schedule when it loads it.
- **_Astra_ stops autoguiding.** The guide stars move across the field when the
  mount tracks a moving target. So the guider operates against the tracking rates.
  If you set `guiding` to true for a moving target, _Astra_ gives a warning and
  ignores the field.
- **_Astra_ sends few rate commands.** Some mounts stop the tracking for a short
  time when they receive a new rate. So _Astra_ sends a rate only when the current
  rate causes the target to trail. It does not send a rate more frequently than
  `nonsidereal_rate_update_interval` seconds. The default value is 10 seconds.
  Decrease the value for a target whose rate changes quickly. Increase it for a
  mount that is sensitive to rate commands.
- **All positions are ICRS (J2000) before they go to the mount.** The planet
  positions from Astropy, the Horizons positions and the SIMBAD positions are all
  astrometric ICRS. So they agree to better than one arcsecond. If the mount
  reports that it needs JNow coordinates, _Astra_ converts each slew position to
  the apparent frame of the date. Refer to `equatorial_system` in the
  [telescope configuration](observatory_configuration.md#telescope-configuration).
- **The ephemeris interval agrees with the movement of the target.** One sample
  each minute gives the position of a planet to better than one arcsecond. In one
  minute, the ISS moves 30 degrees. _Astra_ cannot interpolate an arc that it did
  not sample, and the error becomes some degrees. So _Astra_ uses an interval of a
  few seconds for a satellite, and one minute for a planet, a comet or an
  asteroid. It then measures the speed of the target in the ephemeris and changes
  the interval if the first value was not correct. A very fast target in a long
  window gets to the maximum number of samples. _Astra_ then gives a warning.
- **Minor bodies and TLEs need a network connection when the schedule loads.**
  _Astra_ sends a request to JPL Horizons when it reads the schedule. It
  calculates an ephemeris and interpolates it during the night. Usually one
  request is enough. _Astra_ sends a second request only when the interval
  does not agree with the speed of the target. So a network failure during the
  observations has no effect. But a network failure when the
  schedule loads causes _Astra_ to reject the schedule. _Astra_ calculates the
  positions of the planets and the Moon with the built-in ephemeris of Astropy.
  These positions need no network.

### The target must be above the horizon

_Astra_ does this check for each `object` action, for a moving target and for a
fixed target. It calculates the altitude of the target every 30 seconds, from
`start_time` to `end_time`. If the altitude at one of these times is less than 0
degrees, _Astra_ rejects the schedule. For a moving target, _Astra_ uses the
ephemeris. So the altitude is the altitude of the body at each time. The error
message gives the altitude at the start and at the end when these are below the
limit. It also gives the number of times below the limit, the first of them, and
the lowest altitude.

This is most important for a satellite. A satellite rises and sets more than one
time in a long window. Its altitude at the start, at the middle and at the end
tells you little about the other times. Make the window no longer than one pass.

### Examples

This action tracks Saturn. The mount re-centers every five minutes:

```json
{
   "device_name":"camera_main",
   "action_type":"object",
   "action_value":{"object":"Saturn","lookup_name":"saturn","filter":"r'","exptime":30,"nonsidereal_recenter_interval":300},
   "start_time":"2025-08-23 23:17:00.018",
   "end_time":"2025-08-24 00:17:00.018"
}
```

This action tracks a satellite with its two-line element set. Give the two element
lines in `tle`, with a `\n` between them. You do not need `lookup_name`. If you give
it, it must be `"TLE"`. The lead
time gives the mount 45 seconds to move to the position of the satellite. The rate
update interval is short because the rates of a low orbit change quickly:

```json
{
   "device_name":"camera_main",
   "action_type":"object",
   "action_value":{"object":"ISS","tle":"1 25544U 98067A   26084.45430866  .00012951  00000-0  24673-3 0  9999\n2 25544  51.6344 354.4276 0006215 231.1671 128.8763 15.48531543558777","filter":"Clear","exptime":2,"nonsidereal_recenter_interval":60,"nonsidereal_rate_update_interval":1,"nonsidereal_start_lead_time_seconds":45},
   "start_time":"2025-08-23 23:17:00.018",
   "end_time":"2025-08-23 23:27:00.018"
}
```

```{warning}
A two-line element set becomes less accurate quickly. If a set is more than a few
days old, it cannot give the position of a satellite in low Earth orbit with
enough accuracy. The satellite is then not on the detector.
```

## Action Value Parameters

Each action type needs its own parameters in the `action_value` field. _Astra_
generates the sections that follow from the action configuration dataclasses. So
the documentation always agrees with the software.

```{eval-rst}
.. autoscheduleactions::
   :format: literal
```
