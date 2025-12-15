# FITS Header Configuration

*Astra* automatically generates comprehensive FITS headers for all captured images using
metadata from your observatory devices.
This system ensures every image contains the scientific metadata required for analysis.

The FITS header system uses a CSV configuration file that maps device properties and observatory parameters to standard FITS keywords. This approach provides flexibility while maintaining consistency across observations.

## Configuration File Format

The CSV configuration file defines how FITS headers are constructed:

- `header`: FITS header keyword
- `dtype`: Data type (string, integer, float, boolean)
- `fixed`: Whether the value is set at the beginning of an imaging sequence (`true`) or filled later from polling (`false`)
- `device_type`: Source device type or data source
- `device_command`: ASCOM command or data source identifier
- `comment`: Description of the header keyword

## Example Configuration

```{csv-table} Example FITS Header Configuration
  :file: ../../../src/astra/config/templates/observatory_fits_header_config.csv
  :header-rows: 1
  :class: scrollable-table
```

## How FITS Headers Are Created

*Astra* creates FITS headers in two stages to ensure complete and accurate metadata:

### 1. Base Headers (at the beginning of an imaging sequence)

When an imaging sequence begins, *Astra* immediately creates an initial FITS header containing (if `fixed=true` in the configuration):

- **Observatory information:** Site name, coordinates, altitude
- **Optical system:** Aperture diameter, area, focal length  
- **Instrument details:** Camera settings, filter name, pixel scale
- **Observation metadata:** Object name, exposure time, image type placeholders
- **Software information:** *Astra* version number
- **Fixed device values:** Properties marked as `fixed=True` in the configuration

TODO: note about `IMAGETYP` `EXPTIME`

### 2. Final Headers (after exposure)

After an imaging sequence, *Astra* completes any missing header values:

- Identifies images with incomplete headers
- Retrieves device polling data from ±10 seconds around each exposure time (stored in a local sqlite database)
- Interpolates device readings (temperature, pointing, focus position, etc.) to match exposure timestamps
- Updates FITS files in place while preserving original header structure
- Adds calculated values such as:
  
  - Various time scales and reference frames
  - Derived parameters like airmass

This two-stage approach ensures headers contain accurate, time-synchronized metadata, while minimizing time loss between exposures.

## Customizing FITS Headers

You can customize the FITS headers by modifying the CSV configuration file:

- **Adding headers:** Add new rows to include additional metadata
- **Removing headers:** Delete rows to exclude certain keywords  
- **Timing control:** Set `fixed=True` for immediate values, `fixed=False` for post-exposure interpolated values
