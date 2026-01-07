# Overview

The following User Guide should help users configure and operate observatories with *Astra*.
It covers the following main topics needed to get started:

- **[Observatory Configuration](observatory_configuration)**:
  Learn how to register devices, specify site metadata, configure properties, and
  manage observatory profiles.
- **[FITS Header Configuration](fits_header_configuration)**:
  Understand how to customize FITS headers, including standard keywords and
  metadata inclusion.
- **[Scheduling](scheduling)**: Explore *Astra*'s scheduling capabilities,
  including defining observing blocks and simulating runs based on conditions.
- **[Operation](operation)**: Discover the daily operations of *Astra*, from starting
  the program, running schedules and monitoring to error recovery techniques.
- **[Customising Observatories by Subclassing](custom_observatories)**:
  Learn how to create and load `Observatory` subclasses to adapt site-specific
  behaviour — for example custom startup/shutdown sequences, device-specific
  error handling, and non-standard device integration — without modifying the
  core source.

