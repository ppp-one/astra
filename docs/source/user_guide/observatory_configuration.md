# Observatory Configuration

*Astra* requires a configuration file in YAML format for each observatory you want to operate. This file defines all devices, their settings, and how they interact with each other.

Before setting up your observatory, you'll need to configure two main components:

1. **Observatory Configuration** - Defines your hardware devices and their settings
2. **FITS Header Configuration** - Maps device properties to FITS header keywords

Let's start with the observatory configuration.

```{literalinclude} ../../../src/astra/config/templates/observatory_config.yml
:language: yaml
:caption: Example Observatory Configuration Template
:class: scrollable-code
```

This example configuration shows all supported device types and their parameters. You can customize this template for your specific observatory setup.

## Device Types and Configuration

*Astra* supports the following device types, all conforming to the ASCOM Alpaca standard:

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

- `device_name`: Unique identifier for the device (string)
- `ip`: Network address of the Alpaca device (string, format: "hostname:port")
- `device_number`: ASCOM Alpaca device number (integer)
- `polling_interval`: How often to poll ASCOM properties (set by [FITS header configuration](fits_header_configuration)), in seconds (integer, optional, default: 5)
- `connectable`: Whether to attempt connection at startup (boolean, optional, default: true)

## Telescope Configuration

Additional parameters for telescope mounts:

- `pointing_threshold`: Maximum acceptable pointing error in arcminutes if pointing correction enabled (float)
- `settle_factor`: Exposure multiplier for calculated settle time after pointing (float)
- `guider`: Autoguider calibration settings (dict, populated automatically by {py:meth}`astra.observatory.Observatory.guiding_calibration_sequence`

## Focuser Configuration

Focuser-specific parameters:

- `focus_position`: Best known absolute focus position (integer)
- `settle_time`: Time in seconds to wait after move (integer)

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
  
  - `<device_type>`: Must match the `device_name` used in device configuration (string)

**Autofocus Parameters:**

- `autofocus`: Configuration for automated focusing (dict)

  - `airmass_threshold`: Maximum airmass to query local star catalogue (float)
  - `exptime`: Exposure time for autofocus frames in seconds (float)
  - `g_mag_range`: Range of G magnitudes for star selection [min, max] (list of float)

## Dome Configuration

Dome-specific parameters:

- `close_dome_on_telescope_error`: Flag to close the dome in case of a telescope error. (boolean, default: false)
- `telescopes`: List of associated telescope(s) within the dome (list)

## ObservingConditions Configuration

Weather monitoring and safety parameters:

- `closing_limits`: Weather safety thresholds that trigger observatory closure (dict)
  
  - `<parameter>`: Weather parameter name (e.g., `Humidity`, `WindSpeed`, `Temperature`)
  - `<parameter>[i].upper/lower`: Threshold value - `upper` for maximum safe value, `lower` for minimum (float)
  - `<parameter>[i].max_safe_duration`: Time in minutes the parameter must stay within safe limits before `weather_safe` is set to `True` (int)

**Supported Parameters:**

- Standard ASCOM: `CloudCover`, `DewPoint`, `Humidity`, `Pressure`, `RainRate`, `SkyBrightness`, `SkyQuality`, `SkyTemperature`, `StarFWHM`, `Temperature`, `WindDirection`, `WindGust`, `WindSpeed`
- Custom: `RelativeSkyTemp` (sky temperature minus ambient temperature, requires both `SkyTemperature` and `Temperature`)

## SafetyMonitor Configuration

Safety system monitoring parameters:

- `max_safe_duration`: Time in minutes the `IsSafe` property must remain `True` continuously before the observatory is considered `weather_safe` (int)

## Misc Options

Optional observatory-wide settings:

- `webcam`: URL for webcam feed (string, currently supports HLS stream format)
- `backup_time`: UTC time of day to perform automatic backups of polled data and logs (string, format: "HH:MM")
- `filename_templates`: Customize how FITS files are named and organized (dict). See {py:mod}`astra.filename_templates` for more details.
