Configuration
=============

Observatory Configuration
------------------------

Astra requires a configuration file in YAML format for each observatory you want to operate. This file defines all the devices, their settings, and how they interact with each other.

Device Types
~~~~~~~~~~~

Astra supports the following device types, all conforming to the ASCOM Alpaca standard:

- Camera
- CoverCalibrator
- Dome
- FilterWheel
- Focuser
- ObservingConditions
- Rotator
- SafetyMonitor
- Switch
- Telescope

Device Configuration
~~~~~~~~~~~~~~~~~~~

Each device requires specific configuration parameters:

Common Parameters
^^^^^^^^^^^^^^

Parameters required for all devices:

- ``device_name``: Unique name for the device (string)
- ``device_type``: Type of device (string, must match section name)
- ``device_number``: Alpaca device number (integer)
- ``address``: Address of the Alpaca device (string, format: "hostname:port")

Telescope Parameters
^^^^^^^^^^^^^^^

- ``pointing_threshold``: Threshold for pointing corrections in arcseconds (float)
- ``guider``: Configuration for the autoguider (dictionary)

Camera Parameters
^^^^^^^^^^^^^

- ``temperature``: Target cooling temperature in Celsius (float)
- ``temperature_tolerance``: Acceptable temperature variation (float)
- ``paired_devices``: Dictionary of devices to be used with this camera
- ``flats``: Configuration for flat field acquisition

FilterWheel Parameters
^^^^^^^^^^^^^^^^^

No additional parameters required beyond the common parameters.

Focuser Parameters
^^^^^^^^^^^^^^^

No additional parameters required beyond the common parameters.

Dome Parameters
^^^^^^^^^^^

No additional parameters required beyond the common parameters.

SafetyMonitor Parameters
^^^^^^^^^^^^^^^^^^

- ``max_safe_duration``: Maximum time in minutes before considering outdated data unsafe (integer)

ObservingConditions Parameters
^^^^^^^^^^^^^^^^^^^^^^^

- ``closing_limits``: Threshold values for when to close the observatory (dictionary)

Example Configuration
~~~~~~~~~~~~~~~~~~~

Here's a complete example of an observatory configuration:

.. code-block:: yaml

    Misc:
      backup_time: "03:00"
    
    Telescope:
      - device_name: "Main Telescope"
        device_type: "Telescope"
        device_number: 0
        address: "localhost:11111"
        pointing_threshold: 30
        guider:
          guiding_interval: 10
          guiding_max_correction: 5
    
    Camera:
      - device_name: "Main Camera"
        device_type: "Camera"
        device_number: 0
        address: "localhost:11112"
        temperature: -20
        temperature_tolerance: 1
        paired_devices:
          Telescope: "Main Telescope"
          FilterWheel: "Filter Wheel"
          Focuser: "Main Focuser"
        flats:
          target_adu: 30000
          bias_offset: 1000
          lower_exptime_limit: 0.1
          upper_exptime_limit: 15
    
    FilterWheel:
      - device_name: "Filter Wheel"
        device_type: "FilterWheel"
        device_number: 0
        address: "localhost:11113"
    
    Focuser:
      - device_name: "Main Focuser"
        device_type: "Focuser"
        device_number: 0
        address: "localhost:11114"
        temperature_compensation: false
    
    Dome:
      - device_name: "Observatory Dome"
        device_type: "Dome"
        device_number: 0
        address: "localhost:11115"
        slaved: true
    
    SafetyMonitor:
      - device_name: "Weather Monitor"
        device_type: "SafetyMonitor"
        device_number: 0
        address: "localhost:11116"
        max_safe_duration: 30
    
    ObservingConditions:
      - device_name: "Weather Station"
        device_type: "ObservingConditions"
        device_number: 0
        address: "localhost:11117"
        closing_limits:
          Humidity:
            - limit: 85
              duration: 10
          WindSpeed:
            - limit: 40
              duration: 5

FITS Header Configuration
------------------------

Astra uses a CSV file to configure what information is included in the FITS headers of captured images. This file maps device properties to standard FITS keywords.

A sample FITS header configuration file (``observatory_fits_header_config.csv``) looks like this:

.. code-block:: text

    header,comment,device_type,device_name,device_command,fixed
    TELESCOP,"Telescope name",astra_fixed,"",telescop,True
    INSTRUME,"Camera name",Camera,"","Name",True
    OBSERVER,"Observer name",astra_fixed,"","Observer Name",True
    OBJECT,"Object name",astra,"","",True
    RA,"Right Ascension (deg)",Telescope,"","RightAscension",False
    DEC,"Declination (deg)",Telescope,"","Declination",False
    EQUINOX,"Equinox",astra_fixed,"",2000.0,True
    FILTER,"Filter name",astra,"","",True
    EXPTIME,"Exposure time (s)",astra,"","",True
    IMAGETYP,"Image type",astra,"","",True
    DATE-OBS,"Date of observation start",astropy_default,"","",True
    CCD-TEMP,"CCD temperature (C)",Camera,"","CCDTemperature",False
    FOCPOS,"Focuser position",Focuser,"","Position",False