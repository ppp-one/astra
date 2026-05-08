# FITS Header Configuration

```{image} ../_static/fits_header_configuration-banner.svg
:class: responsive-banner
:align: center
:alt: banner
```

_Astra_ automatically generates comprehensive FITS headers for all captured images using
metadata from your observatory devices.
This system ensures every image contains the scientific metadata required for analysis.

The FITS header system uses a CSV configuration file that maps device properties and observatory parameters to standard FITS keywords. This approach provides flexibility while maintaining consistency across observations.

## Configuration File Format

The CSV configuration file defines how FITS headers are constructed:

- `header`: FITS header keyword
- `dtype`: Data type (string, integer, float, boolean)
- `fixed`: Whether the value is set at the beginning of an imaging sequence (`true`) or filled later from polled device properties stored in the SQLite database (`false`)
- `device_type`: Source ([ASCOM Alpaca device](https://ascom-standards.org/alpyca/alpacaclasses.html), `static` for unchanging values, `astra` for _Astra_ calculated/derived values, `astropy_default` for standard headers filled by Astropy)
- `device_command`: ASCOM device property, a static value, or empty.
- `comment`: Description of the header keyword

## Example Configuration

```{csv-table} Example FITS Header Configuration
  :file: ../../../src/astra/config/templates/observatory_fits_header_config.csv
  :header-rows: 1
  :class: scrollable-table
```

## How FITS Headers Are Created

_Astra_ creates FITS headers in two stages to ensure complete and accurate metadata:

### 1. Base Headers (at the beginning of an imaging sequence)

When an imaging sequence begins, _Astra_ immediately creates an initial FITS header containing (if `fixed=true` in the FITS configuration), such as:

- **Observatory information:** Site name, coordinates
- **Telescope properties:** Aperture diameter, area, focal length
- **Observation metadata:** Object name, exposure time, image type placeholders
- **Software information:** _Astra_ version number

`IMAGETYP` changes dynamically based on the imaging sequence (e.g., 'Light Frame', 'Dark Frame', 'Bias Frame', 'Flat Frame').

### 2. Final Headers (after schedule ends)

After an imaging sequence, _Astra_ completes any missing header values:

- Identifies images with incomplete headers
- Retrieves device polling data from ±10 seconds around each exposure time (stored in the local SQLite database)
- Interpolates device readings (e.g. ambient temperature, focus position, etc.) to match exposure timestamps
- Updates FITS files in place while preserving original header structure
- Adds calculated values such as:
  - Various time scales and reference frames
  - Derived parameters like airmass
  - Checksum and datasum values

This two-stage approach ensures headers contain accurate, time-synchronized metadata, while minimising time loss between exposures.

## Customizing FITS Headers

You can customize the FITS headers by modifying the CSV configuration file:

- **Adding headers:** Add new rows to include additional metadata
- **Removing headers:** Delete rows to exclude certain keywords
- **Timing control:** Set `fixed=True` for immediate values, `fixed=False` for post-exposure interpolated values
