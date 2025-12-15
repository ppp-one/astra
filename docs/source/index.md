# *Astra*

<div style="margin: 60px"></div>

```{image} _static/astra-banner.jpg
:width: 100%
:align: center
:alt: Astra logo
```

**Astra** (Automated Survey observaTory Robotised with Alpaca) is an open-source,
cross-platform Python system for the sustained, fully autonomous operation of
astronomical observatories.

*Astra* controls telescopes, cameras, domes, focusers, filter wheels, safety monitors,
and weather sensors via the **ASCOM Alpaca protocol** and executes prescheduled observatory
actions under continuous safety supervision, including advanced multi-device actions,
such as plate-solve-based pointing correction using an offline Gaia–2MASS catalogue,
PID-controlled autoguiding, and autofocusing. A FastAPI web interface provides a
browser UI, REST and WebSocket APIs for real-time status, image previews, SQLite-backed
telemetry and logs.

## Used By

Currently, *Astra* is deployed at multiple professional observatories
delivering reliable, unattended survey operations, including

- SPECULOOS-South: Paranal, Chile
- Saint-Ex: San Pedro Mártir, Mexico
- ETH Observatory: Zurich, Switzerland
- SPECULOOS-North: Teide Observatory, Tenerife, Spain ... soon

```{note}
This documentation is a work in progress. We are continuously updating and improving it.
If you have any questions or suggestions, please feel free to reach out to us.
We appreciate your feedback and contributions to make this documentation better.
```

---

```{toctree}
:hidden:

motivation
```

```{toctree}
:maxdepth: 2
:caption: Getting Started
:hidden:

installation
quickstart
user_guide/index
```

```{toctree}
:maxdepth: 2
:caption: Developer Documentation
:hidden:

api/index
contributing
changelog
```
