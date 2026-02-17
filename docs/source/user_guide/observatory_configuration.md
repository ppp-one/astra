# Observatory Configuration

```{image} ../_static/observatory_configuration-banner.svg
:class: responsive-banner
:align: center
:alt: banner
```

_Astra_ requires an observatory configuration file in YAML format that defines all devices, their settings, safety limits, and how they interact with each other.

```{important}
Each _Astra_ instance runs exactly one observatory at a time, specified by the `observatory_name` field in your base configuration file (`~/.astra/astra_config.yml`).

This name determines which configuration files are loaded: `{observatory_name}_config.yml` and `{observatory_name}_fits_header_config.csv`.
```

You'll need to configure two files for your observatory:

- **`{observatory_name}_config.yml`** (this page) - Device definitions, safety limits, and settings
- **`{observatory_name}_fits_header_config.csv`** ([FITS Header Configuration](fits_header_configuration)) - FITS header keyword mappings

```{literalinclude} ../../../src/astra/config/templates/observatory_config.yml
:language: yaml
:caption: Example Observatory Configuration Template
:class: scrollable-code
```

This example configuration shows all supported device types and their parameters. You can customize this template for your specific observatory setup.

## Device Types and Configuration

_Astra_ supports the following device types, all conforming to the ASCOM Alpaca standard:

- **Telescope**
- **Focuser**
- **Camera**
- **Dome**
- **FilterWheel**
- **ObservingConditions**
- **SafetyMonitor**
- **CoverCalibrator**
- **Rotator**
- **Switch**

Each device type has specific configuration parameters detailed below.

## Common Device Parameters

All devices share these required parameters:

- `device_name`: Unique identifier for the device (string, e.g., "camera_main")
- `ip`: Network address of the Alpaca device (string, format: "hostname:port")
- `device_number`: ASCOM Alpaca device number (integer)
- `polling_interval`: How often to poll ASCOM properties (set by [FITS header configuration](fits_header_configuration)), in seconds (integer, optional, default: 5)
- `connectable`: Whether to attempt connection at startup (boolean, optional, default: true)

## Telescope Configuration

Additional parameters for telescope mounts:

- `pointing_threshold`: Maximum acceptable pointing error in arcminutes before pointing correction is applied (float, default: 0.1)
- `settle_factor`: Exposure multiplier for calculated settle time after pointing - useful for continuous acquisition type cameras (float, default: 0.0)
- `meridian_flip`: Enable automated meridian flips (boolean, default: false)
- `meridian_flip_min`: Buffer time in minutes past meridian to trigger flip (float, default: 5)
- `guider`: Autoguider calibration settings (dict, populated automatically by the [calibrate_guiding](scheduling.md#calibrate-guiding-action) sequence)

## Focuser Configuration

Focuser-specific parameters:

- `focus_position`: Best known absolute focus position (integer)
- `settle_time`: Time in seconds to wait after focus position move (integer)

## Camera Configuration

Camera-specific parameters for cooling and imaging:

**Cooling Parameters:**

- `temperature`: Target cooling temperature in Celsius (float)
- `temperature_tolerance`: Acceptable temperature tolerance from target in Celsius (float)
- `cooling_timeout`: Time in minutes to wait to reach target temperature before timing out (int, default: 30)

**Sky Flat Parameters:**

- `flats`: Configuration for automated flat field acquisition (dict)
  - `target_adu`: Target median ADU value for sky flat frame (int)
  - `target_adu_tolerance`: Acceptable tolerance around target ADU (int)
  - `bias_offset`: Median bias level offset in ADU for exposure time calculations (int)
  - `lower_exptime_limit`: Minimum allowed exposure time in seconds (int)
  - `upper_exptime_limit`: Maximum allowed exposure time in seconds (int)

**Device Associations:**

- `paired_devices`: Links to other devices for FITS headers and sequence coordination (dict)
  - `{device_type}`: `{device_name}` which must match the name used in device configuration (string)

## Dome Configuration

Dome-specific parameters:

- `close_dome_on_telescope_error`: Flag to close the dome in case of a telescope error. (boolean, default: false)
- `telescopes`: List of associated telescope(s) within the dome (list)

## ObservingConditions Configuration

Weather monitoring and safety parameters:

- `closing_limits`: Weather safety thresholds that trigger observatory closure (dict)
  - `{parameter}`: Weather parameter name (e.g., `Humidity`, `WindSpeed`, `Temperature`)
  - `{parameter}[i].upper/lower`: Threshold value - `upper` for maximum safe value, `lower` for minimum (float)
  - `{parameter}[i].max_safe_duration`: Time in minutes the parameter must stay within safe limits before `weather_safe` is set to `True` (int)

**Supported Parameters:**

- Standard ASCOM: `CloudCover`, `DewPoint`, `Humidity`, `Pressure`, `RainRate`, `SkyBrightness`, `SkyQuality`, `SkyTemperature`, `StarFWHM`, `Temperature`, `WindDirection`, `WindGust`, `WindSpeed`
- Custom: `RelativeSkyTemp` (sky temperature minus ambient temperature; requires both `SkyTemperature` and `Temperature` to be available from your weather station via its ASCOM Alpaca ObservingConditions driver)

```{important}
A weather parameter is only used for safety evaluation when **both** conditions are met:

1. It is exposed by your ASCOM Alpaca **ObservingConditions** driver.
2. It is defined in your [FITS header configuration](fits_header_configuration) such that _Astra_ can retrieve historical values from the local SQLite database for safety monitoring its `max_safe_duration`.
```

## SafetyMonitor Configuration

Safety system monitoring parameters:

- `max_safe_duration`: Time in minutes the `IsSafe` property must remain `True` continuously before the observatory is considered `weather_safe` (int)

## Misc Options

Optional observatory-wide settings:

- `backup_time`: UTC time of day to perform automatic daily backups of polled data and logs on the SQLite database (string, format: "HH:MM")
- `Webcam`: Webcam feed configuration. The URL is embedded in an iframe element in the frontend. Can be:
  - Single URL string for one webcam (e.g., `Webcam: http://localhost:8888/inside`)
  - Array of objects for multiple webcams, each with `name` and `url` properties
  - Any iframe-compatible video source (e.g., [mediamtx](https://github.com/bluenviron/mediamtx))
- `AllSky`: All-sky camera configuration. Images are fetched via `/api/allsky/latest` endpoint and automatically refreshed every 60 seconds. Can be:
  - Single path string for one camera (e.g., `AllSky: /path/to/allsky.jpg`)
  - Array of objects for multiple cameras, each with `name` and `path` properties
  - Supports JPEG and PNG formats
- `filename_templates`: Customize how FITS files are named and organized (dict). See {py:mod}`astra.filename_templates` for more details.
