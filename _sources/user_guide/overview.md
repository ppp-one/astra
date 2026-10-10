# Overview

```{image} /_static/overview-banner.svg
:class: responsive-banner
:align: center
:alt: banner
```

This User Guide assists users in configuring and operating observatories with _Astra_.
It covers the main topics required to get started:

- **[Observatory Configuration](observatory_configuration)**:

  Configure your observatory's hardware and safety limits.

- **[FITS Header Configuration](fits_header_configuration)**:

  Manage FITS headers, linking device methods to FITS keywords.

- **[Scheduling](scheduling)**:

  Develop automated observing plans using JSON Lines (`.jsonl`) files.

- **[Operation](operation)**:

  Learn how to safely operate your observatory, including startup, calibration, and safety mechanisms.

- **[Core Logic](core_logic)**:

  Understand the internal workings of _Astra_, including process management, device communication, and error handling.

- **[Customising Observatories by Subclassing](custom_observatories)**:

  Learn how to create and load `Observatory` subclasses to adapt site-specific
  behaviour — for example custom startup/shutdown sequences — without modifying the
  core source.
