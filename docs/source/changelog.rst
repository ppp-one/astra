Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.


0.3.1 - 2025-08-21
------------------

Added
^^^^^
- Tracking stop at the end of object plans (thanks @Sebastián Zúñiga-Fernández)


0.3.0 - 2025-07-03
------------------

Added
^^^^^
- Defocus/refocus within plans (thanks David Degen)
- Improved autofocusing stability 
- Relative autofocus range search
- Camera binning
- Relative sky temperature for better cloud detection
- German equatorial mount guiding support, including pier flip handling.


0.2.5 - 2024-05-03
------------------

Fixed
^^^^^
- fix:Astelos acknowledgment by @dsagred in #51


0.2.4 - 2024-03-20
------------------

Added
^^^^^
- ``speculoos`` property to Astra class to permit easier use of Astra outside of speculoos
- time_to_safe variable to permit the user to set the time to safe from config, instead of fixed 30 mins (in future, planned to add smarter logic for different weather conditions)
- Basic test framework for Astra class
- Simple script to remove guiding reference image row from sqlite database.

Fixed
^^^^^
- AstelOS error handling for malformed GPS


Removed
^^^^^^^
- Removed unused files for ascom devices -- sticking to alpaca.


0.2.3 - 2024-01-19
------------------

Added
^^^^^
- Added 'Malformed telegram from GPS' to AstelOS error handling
- Made the AstelOS acknowledgement verbose the messages in the logs and better error handling
- Changelog



0.2.2 - 2024-01-11
------------------

Removed
^^^^^^^
- Removed subframing from flats as camera was crashing due to this -- unknown why, it started happening only recently.


0.2.1 - 2024-01-08
------------------

Added
^^^^^
- 0.1s sleep to camera acquistion sequence
- 0.5s (from 0.1s) to monitor status function


0.2.0 - 2023-12-17
------------------

Added
^^^^^
- Working version after implementation at SSO
  - SPECULOOS specific hardware handling
- Flats sequence
- Guiding logic from DONUTS
- Pointing logic (only working in theory since AsTelOS does not accept sync commands?)


Changed
^^^^^^^
- Moved alpaca devices to their own process as a single device would previously lock all others if polling request hung
- UI improvements
- Error handling logic



0.1.0 - 2023-05-23
------------------

Added
^^^^^
- Initial version
