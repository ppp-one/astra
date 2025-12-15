Scheduling Syntax
=================

Astra uses a flexible scheduling system to automate observatory operations. Schedules are defined using JSONL files (JSON Lines format), where each line represents a scheduled action with these fields:

.. - ``device_type``: Type of the device (e.g., camera, telescope)
- ``device_name``: Name of the camera device (the primary instrument that coordinates all operations)
- ``action_type``: Type of action to perform
- ``action_value``: Parameters for the action
- ``start_time``: When the action should start (UTC ISO format: YYYY-MM-DD HH:MM:SS.sss)
- ``end_time``: Latest when the action should end (UTC ISO format: YYYY-MM-DD HH:MM:SS.sss)

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
----------------------

Place your schedule file in the observatory schedules directory with a ``.jsonl`` extension. For example:

- ``~/Documents/Astra/schedules/observatory_name.jsonl``

Astra will automatically detect and load the JSONL schedule file, with the specified name pattern, if modified.

Supported Action Types
----------------------

Astra supports the following action types for observatory automation, organized by function:

- ``open``: Open observatory
- ``close``: Close observatory
- ``cool_camera``: Activate camera cooling
- ``object``: Capture light frames with optional pointing correction/autoguiding
- ``calibration``: Capture dark and bias frames
- ``flats``: Capture sky flat field frames
- ``autofocus``: Autofocus
- ``calibrate_guiding``: Calibrate guiding parameters
- ``pointing_model``: Help build a telescope pointing model
- ``complete_headers``: Complete FITS headers of all images captured

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

**Required parameters:**
    None

**Optional parameters:**
    None


``close``
^^^^^^^^^

Close the observatory:

1. Stop any active guiding operations
2. Stop telescope slewing and tracking
3. Park the telescope
4. Park the dome and close shutter


**Required parameters:**
    None

**Optional parameters:**
    None

``object``
^^^^^^^^^^

Capture a sequence of light frames:

1. Pre-sequence setup (telescope pointing, setting filters, focus position, camera binning, base headers)
2. Capture exposures
3. Perform pointing correction (if `pointing=true`)
4. Start guiding (if `guiding=true`)
5. Stop exposures, guiding, and telescope tracking at completion

**Required parameters:**
    - ``object``: Target name (string)
    - ``exptime``: Exposure time in seconds (float)

**Optional parameters:**
    - ``ra``: Right Ascension in degrees (float, default: current RA)
    - ``dec``: Declination in degrees (float, default: current Dec)
    - ``alt``: Altitude in degrees (float, default: current altitude)
    - ``az``: Azimuth in degrees (float, default: current azimuth)
    - ``filter``: Filter name (string, default: current filter)
    - ``focus_shift``: Focus shift value from best focus position (float, default: None)
    - ``focus_position``: Absolute focus position value (float, default: best focus position)
    - ``n``: Number of exposures (int, default: inf)
    - ``guiding``: Enable autoguiding with `Donuts <https://donuts.readthedocs.io/en/latest/>`_ (boolean, default: false)
    - ``pointing``: Enable pointing correction with `twirl <https://twirl.readthedocs.io/en/latest/>`_ (boolean, default: false)
    - ``bin``: Binning factor (int, default: 1)
    - ``dir``: Absolute directory path for saving images (string, default: auto-generated as ~/Documents/Astra/images/YYYYMMDD where YYYYMMDD is the local night's date calculated from schedule's UTC start time plus site longitude offset in hours)
    - ``execute_parallel``: Whether to execute the sequence in parallel mode (boolean, default: false)
    - ``disable_telescope_movement``: Whether to disable telescope movement during the sequence (boolean, default: false)
    - ``reset_guiding_reference``: Reset the guiding reference frame at the start of the sequence (boolean, default: true)
    - ``subframe_width``: Width of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_height``: Height of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_center_x``: Horizontal center position of subframe, 0.0=left, 0.5=center, 1.0=right (float, default: 0.5)
    - ``subframe_center_y``: Vertical center position of subframe, 0.0=top, 0.5=center, 1.0=bottom (float, default: 0.5)


``calibration``
^^^^^^^^^^^^^^^

Capture a sequence of calibration images.

**Required parameters:**
    - ``exptime``: List of exposure times in seconds (List[float])
    - ``n``: List of number of exposures for each exposure time (List[int])

**Optional parameters:**
    - ``filter``: Filter specification (string, default: current filter)
    - ``bin``: Binning factor (int, default: 1)
    - ``dir``: Same as for ``object`` action type
    - ``execute_parallel``: Whether to execute the sequence in parallel mode (boolean, default: false)
    - ``subframe_width``: Width of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_height``: Height of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_center_x``: Horizontal center position of subframe (float, default: 0.5)
    - ``subframe_center_y``: Vertical center position of subframe (float, default: 0.5)



``flats``
^^^^^^^^^

Capture a sequence of sky flat field frames:

1. Waits for the Sun's altitude to be between -1 and -12 degrees
2. Positions telescope to `near-uniform portion of the sky <https://arxiv.org/pdf/1407.8283.pdf>`_, 180 degrees opposite the Sun in azimuth, 75 degrees above the horizon in altitude
3. Capture exposures and re-positions telescope between exposures
4. Iterates through filters
5. Handles exposure time adjustments as sky brightness changes

**Required parameters:**
    - ``filter``: List of filter names (List[string])
    - ``n``: Number of flats per filter (List[int])

**Optional parameters:**
    - ``dir``: Same as for ``object`` action type
    - ``bin``: Binning factor (int, default: 1)
    - ``execute_parallel``: Whether to execute the sequence in parallel mode (boolean, default: false)
    - ``disable_telescope_movement``: Whether to disable telescope movement during the sequence (boolean, default: false)
    - ``subframe_width``: Width of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_height``: Height of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_center_x``: Horizontal center position of subframe (float, default: 0.5)
    - ``subframe_center_y``: Vertical center position of subframe (float, default: 0.5)

**Configuration-based parameters:**
    The sky flat field sequence automatically uses camera-specific settings from the observatory configuration to calculate optimal exposure times based on sky brightness and target ADU levels.

``autofocus``
^^^^^^^^^^^^^

Perform autofocus sequence to achieve optimal telescope focus:

1. If no RA/DEC specified, use the local star database to identify a suitable field with enough stars
2. Move telescope
3. Takes images at different focus positions
4. Measures star sharpness in each image
5. Fits a curve to determine optimal focus
6. Generates plots and saves focus results

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
    - ``subframe_width``: Width of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_height``: Height of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_center_x``: Horizontal center position of subframe (float, default: 0.5)
    - ``subframe_center_y``: Vertical center position of subframe (float, default: 0.5)


``calibrate_guiding``
^^^^^^^^^^^^^^^^^^^^^

Calibrate guiding parameters by measuring pixel-to-time scales of pulse guiding commands and determining camera orientation relative to telescope mount axes.

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
    - ``subframe_width``: Width of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_height``: Height of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_center_x``: Horizontal center position of subframe (float, default: 0.5)
    - ``subframe_center_y``: Vertical center position of subframe (float, default: 0.5)

``pointing_model``
^^^^^^^^^^^^^^^^^^

Generate a series of pointings and capture an image at each
location, plate solve, and send SyncToCoordinates to the telescope. The sequence creates
a spiral pattern of points from zenith down to a 30 degree altitude, avoiding
positions less than 20 degrees to the Moon.

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
    - ``subframe_width``: Width of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_height``: Height of subframe region in binned pixels (int, default: None for full frame)
    - ``subframe_center_x``: Horizontal center position of subframe (float, default: 0.5)
    - ``subframe_center_y``: Vertical center position of subframe (float, default: 0.5)

``complete_headers``
^^^^^^^^^^^^^^^^^^^^

Post-processes captured images by adding dynamic FITS header information that
wasn't available at exposure time. Uses polled device data from paired devices to interpolate
accurate values for each image timestamp.

**Required parameters:**
    None

**Optional parameters:**
    None


``cool_camera``
^^^^^^^^^^^^^^^

Activates the camera cooler and sets the target temperature with specified tolerance 
from observatory configuration with a 30 minute timeout.

**Required parameters:**
    None

**Optional parameters:**
    None
