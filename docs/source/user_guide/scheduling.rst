Scheduling
==========

Astra uses a flexible scheduling system to automate observatory operations. Schedules are defined using JSONL files (JSON Lines format), where each line represents a scheduled action with these fields:

.. - ``device_type``: Type of the device (e.g., camera, telescope)
- ``device_name``: Name of the camera device (the primary instrument that coordinates all operations)
- ``action_type``: Type of action to perform
- ``action_value``: Parameters for the action (as a JSON object)
- ``start_time``: When the action should start (ISO format: YYYY-MM-DD HH:MM:SS.sss)
- ``end_time``: When the action should end (ISO format: YYYY-MM-DD HH:MM:SS.sss)

.. note::
   **Instrument-Centric Design**: All scheduled actions specify a camera as the ``device_name``. The camera acts as the primary instrument that coordinates operations with its paired devices (telescope, dome, filter wheel, focuser, etc.). This design ensures all devices work together as a cohesive system.

Example Schedule
----------------
.. code-block:: json

    {"device_name":"camera_main","action_type":"open","action_value":{},"start_time":"2025-08-23 22:38:25.210","end_time":"2025-08-24 10:49:15.363"}
    {"device_name":"camera_main","action_type":"flats","action_value":{"filter":["J", "H"],"n":[50, 50]},"start_time":"2025-08-23 22:39:25.210","end_time":"2025-08-23 23:16:00.018"}
    {"device_name":"camera_main","action_type":"object","action_value":{"object":"Sp2151-4017","filter":"J","ra":327.88132,"dec":-40.28976,"exptime":8,"guiding":true,"pointing":false},"start_time":"2025-08-23 23:17:00.018","end_time":"2025-08-24 04:43:40.018"}
    {"device_name":"camera_main","action_type":"object","action_value":{"object":"Sp2343-2906","filter":"H","ra":355.88360,"dec":-29.10759,"exptime":38,"guiding":true,"pointing":false},"start_time":"2025-08-24 04:46:40.018","end_time":"2025-08-24 10:23:40.018"}
    {"device_name":"camera_main","action_type":"flats","action_value":{"filter":["H", "J"],"n":[50, 50]},"start_time":"2025-08-24 10:24:40.018","end_time":"2025-08-24 10:49:15.363"}
    {"device_name":"camera_main","action_type":"close","action_value":{},"start_time":"2025-08-24 10:49:15.363","end_time":"2025-08-24 11:49:15.363"}
    {"device_name":"camera_main","action_type":"calibration","action_value":{"exptime":[0,10,15,30,38,60,120],"n":[10,10,10,10,10,10,10],"filter":"Dark"},"start_time":"2025-08-24 10:55:15.363","end_time":"2025-08-24 11:49:15.363"}

.. note::
    Astra's JSONL files support comments using lines that start with ``//``:


Schedule File Location
-------------------

Place your schedule file in the observatory schedules directory with a ``.jsonl`` extension. For example:

- ``~/Documents/Astra/schedules/observatory_name.jsonl``

Astra will automatically detect and load JSONL schedule files with the specified name pattern.

Supported Action Types
--------------------

Astra supports the following action types for observatory automation, organized by function:

- ``open``: Open observatory (dome, telescope, camera cooling)
- ``close``: Close observatory (park telescope, close dome)
- ``cool_camera``: Activate camera cooling
- ``object``: Capture light frames with optional pointing correction/autoguiding
- ``calibration``: Take dark and bias frames
- ``flats``: Capture sky flat field frames
- ``autofocus``: Autofocus
- ``calibrate_guiding``: Calibrate guiding parameters
- ``pointing_model``: Build telescope pointing model
- ``complete_headers``: Complete FITS headers

.. note::
   The ``complete_headers`` action automatically runs at the end of every schedule execution to ensure complete metadata in all FITS files.


Action Value Parameters
-----------------------

Each action type requires specific parameters in the ``action_value`` field. All parameters are specified as JSON objects.

``open``
^^^^^^^^

Open the observatory for observations:

1. Opens dome shutter
2. Unparks telescope

The sequence only proceeds if weather conditions are safe and no errors are present. For SPECULOOS observatories, special error handling and polling management is performed.

**Required parameters:**
    None

**Optional parameters:**
    None


``close``
^^^^^^^^

Close the observatory:

1. Stop any active guiding operations
2. Stop telescope slewing and tracking
3. Park the telescope
4. Park the dome and close shutter

For SPECULOOS observatories, includes special error handling and polling
management during the closure sequence.

**Required parameters:**
    None

**Optional parameters:**
    None

``object``
^^^^^^^^^^

Execute a sequence of light frames:

1. Pre-sequence setup (telescope pointing, setting filters, focus position, camera binning, base headers)
2. Perform pointing correction (if `pointing=true`)
3. Start guiding (if `guiding=true`)
4. Capture exposures
5. Stop guiding and telescope tracking at completion

**Required parameters:**
    - ``object``: Target name (string)
    - ``exptime``: Exposure time in seconds (float)

**Optional parameters:**
    - ``ra``: Right Ascension in degrees (float, default: current RA)
    - ``dec``: Declination in degrees (float, default: current Dec)
    - ``filter``: Filter name (string, default: current filter)
    - ``focus_shift``: Focus shift value from best focus position (float, default: None)
    - ``focus_position``: Absolute focus position value (float, default: best focus position)
    - ``n``: Number of exposures (int, default: inf)
    - ``guiding``: Enable autoguiding with `Donuts <https://donuts.readthedocs.io/en/latest/>`_ (boolean, default: false)
    - ``pointing``: Enable pointing correction with `twirl <https://twirl.readthedocs.io/en/latest/>`_ (boolean, default: false)
    - ``bin``: Binning factor (int, default: 1)
    - ``dir``: Absolute directory path for saving images (string, default: auto-generated as ~/Documents/Astra/images/YYYYMMDD where YYYYMMDD is the local night's date calculated from schedule's UTC start time plus site longitude offset in hours)


``calibration``
^^^^^^^^^^^^^^^

Execute a sequence of calibration images.

**Required parameters:**
    - ``exptime``: List of exposure times in seconds (List[float])
    - ``n``: List of number of exposures for each exposure time (List[int])

**Optional parameters:**
    - ``filter``: Filter specification (string, default: current filter)
    - ``dir``: Same as for ``object`` action type
    - ``bin``: Binning factor (int, default: 1)



``flats``
^^^^^^^^^

Execute a sequence of sky flat field frames:

1. Monitors sun altitude for optimal flat field conditions
2. Positions telescope for best uniformity on sky (180 degrees opposite the sun, 75 degrees above the horizon)
3. Calculates optimal exposure times for target ADU levels
4. Capture exposures
5. Iterates through multiple filters if specified
6. Handles exposure time adjustments as sky brightness changes


**Required parameters:**
    - ``filter``: List of filter names (List[string])
    - ``n``: Number of flats per filter (List[int])

**Optional parameters:**
    - ``dir``: Same as for ``object`` action type
    - ``bin``: Binning factor (int, default: 1)

**Configuration-based parameters:**
    The sky flat field sequence automatically uses camera-specific settings from the observatory configuration to calculate optimal exposure times based on sky brightness and target ADU levels.

``autofocus``
^^^^^^^^^^^^^
Perform autofocus sequence to achieve optimal telescope focus.

Executes an automated focusing routine that systematically tests different
focus positions to find the optimal focus setting. Uses star analysis
to measure focus quality and determine the best focus position.

Process:
    1. Prepares observatory and creates autofocus metadata
    2. Checks safety conditions before starting
    3. Executes autofocus routine using appropriate algorithm
    4. Takes test exposures at different focus positions
    5. Analyzes star quality metrics (FWHM, HFD, etc.)
    6. Determines and sets optimal focus position
    7. Returns success status

**Required parameters:**
    None

**Optional parameters:**
    - ``exptime``: Exposure time for focus frames in seconds (int/float, default: 3.0)
    - ``reduce_exposure_time``: Reduce exposure time if necessary to prevent saturation (boolean, default: false)
    - ``search_range``: Range of focus positions to search for the best focus (float, default: None)
    - ``search_range_is_relative``: Whether the search range is relative to the current focus position (boolean, default: false)
    - ``n_steps``: Number of steps for each sweep (Tuple(int,int), default: (30, 20))
    - ``n_exposures``: Number of exposures at each focus position or an array specifying exposures for each sweep (Tuple(int,int) or List[int], default: (1, 1))
    - ``decrease_search_range``: Whether to decrease the search range after each sweep (boolean, default: true)
    - ``ra``: Right Ascension of the target (float, default: from field selection)
    - ``dec``: Declination of the target (float, default: from field selection)
    - ``star_find_threshold``: DAOStarFinder threshold for star detection (float, default: 5.0)
    - ``fwhm``: DAOStarFinder full-width half-maximum (FWHM) of the major axis of the Gaussian kernel in units of pixels (float, default: 8.0)
    - ``maximal_zenith_angle``: Maximum allowed zenith angle for best autofocusing field (float, default: None)
    - ``airmass_threshold``: Maximum allowed airmass for best autofocusing field (float, default: 1.01)
    - ``percent_to_cut``: Percentage of worst-performing focus positions to exclude when updating the search range (float, default: 60)
    - ``filter``: Filter to use for focusing (string, default: current filter)
    - ``observation_time``: Observation time specified using astropy's Time (astropy.Time, default: now)
    - ``maximal_number_of_stars``: Maximum number of stars to be considered in the NeighbourhoodQuery query (int, default: 100000)
    - ``g_mag_range``: Range of G-band magnitudes for star selection (Tuple[float, float], default: (0, 10))
    - ``j_mag_range``: Range of J-band magnitudes for star selection (Tuple[float, float], default: (0, 10))
    - ``fov_height``: Height of the field of view (FOV) in degrees (int, default: 0.2)
    - ``fov_width``: Width of the field of view (FOV) in degrees (int, default: 0.2)
    - ``selection_method``: Method for selecting stars for focus measurement ("single", "maximal", "any") (string, default: "single")
    - ``focus_measure_operator``: Operator for focus measurement ("HFR", "2dgauss", "normavar") (string, default: "HFR")
    - ``extremum_estimator``: Curve fitting method for determining optimal focus ("LOWESS", "medianfilter", "spline", "rbf") (string, default: "LOWESS")
    - ``save``: Updates the observatory configuration with the optimal focus position found during autofocus operation for future use. (boolean, default: true)
    - ``bin``: Binning factor (int, default: 1)


``calibrate_guiding``
^^^^^^^^^^^^^^^^^^^^

**Required parameters:**
None

**Optional parameters:**
    - ``filter``: Filter name (string, default: current filter)
    - ``pulse_time``: Duration of guide pulses in milliseconds (float, default: 5000)
    - ``exptime``: Exposure time for calibration images (float, default: 5)
    - ``settle_time``: Wait time after pulses before exposing (float, default: 10)
    - ``number_of_cycles``: Number of calibration cycles to perform (int, default: 10)
    - ``focus_shift``: Focus shift value from best focus position (float, default: None)
    - ``focus_position``: Absolute focus position value (float, default: best focus position)
    - ``bin``: Binning factor (int, default: 1)

``pointing_model``
^^^^^^^^^^^^^^^^^^

**Required parameters:**
None

**Optional parameters:**
    - ``n``: Number of points to use for the model (int, default: 100)
    - ``exptime``: Exposure time for the model (float, default: 1)
    - ``dark_subtraction``: Apply dark subtraction (requires previously executed calibration sequence of matching dark frames) (boolean, default: false)
    - ``filter``: Filter name (string, default: current filter)
    - ``focus_shift``: Focus shift value from best focus position (float, default: None)
    - ``focus_position``: Absolute focus position value (float, default: best focus position)
    - ``bin``: Binning factor (int, default: 1)

``complete_headers``
^^^^^^^^^^^^^^^^^^^^

**Required parameters:**
None

**Optional parameters:**
None


``cool_camera``
^^^^^^^^^^^^^^^

**Required parameters:**
None

**Optional parameters:**
None



Safety and Monitoring
-------------------

Weather Conditions
~~~~~~~~~~~~~~~~~

Astra continuously monitors weather conditions using the SafetyMonitor device. The scheduler handles different action types based on weather dependency:

**Weather-dependent actions** (require safe conditions):
    - ``open``, ``object``, ``autofocus``, ``calibrate_guiding``, ``pointing_model``

**Weather-independent actions** (can run in unsafe weather):
    - ``calibration``, ``close``, ``cool_camera``, ``complete_headers``

If weather becomes unsafe during execution, weather-dependent actions will stop, while weather-independent actions continue. In either case, the observatory will close safely if needed.  The scheduler will also attempt to resume operations once conditions are safe again.


Troubleshooting
--------------

Common Issues
~~~~~~~~~~~

**Schedule not starting:**
    - Check that watchdog is running
    - Verify robotic switch is enabled
    - Ensure schedule end time is in the future
    - Confirm schedule file format is valid JSONL
    - **Verify camera device name exists in configuration**

**Actions skipping:**
    - Check weather conditions for weather-dependent actions
    - **Verify camera device name matches configuration exactly**
    - Review action parameters for correct format
    - Check for timing conflicts or overlaps
    - **Ensure camera has required paired devices configured**

**Incomplete sequences:**
    - Monitor error logs for device communication issues
    - Verify safety conditions throughout sequence
    - Check for sufficient time allocation between actions

**Invalid action parameters:**
    - Validate JSON syntax in action_value fields
    - Ensure required parameters are present
    - Check coordinate ranges and filter names