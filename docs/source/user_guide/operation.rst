Operation Guide
================

logic, best practices, safety no. 1

should add feature to close obs outside of speculoos

Safety and Monitoring
-------------------

Weather Conditions
~~~~~~~~~~~~~~~~~

Astra continuously monitors weather conditions using the SafetyMonitor device and the internal safety monitor using the parameters from observatory configuration. 
The scheduler handles different action types based on weather dependency:

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