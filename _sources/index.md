# Astra Documentation

```{image} _static/astra-banner.jpg
:width: 100%
:align: center
:alt: Astra banner
```

**_Astra_** (Automated Survey observaTory Robotised with Alpaca) is an open-source,
cross-platform Python system for the sustained, fully autonomous operation of
astronomical observatories.

_Astra_ controls observatory devices via the **[ASCOM Alpaca protocol](https://ascom-standards.org/api/)**. It can execute prescheduled observatory actions under continuous weather safety supervision,
such as object observations with [plate-solve](https://doi.org/10.1093/mnras/stab3113)-based pointing correction using an [offline Gaia–2MASS catalogue](https://doi.org/10.5281/zenodo.18214671), [PID-controlled autoguiding](https://doi.org/10.1086/670940), [sky-flats](https://doi.org/10.1117/12.2055459), and [autofocusing](https://github.com/dgegen/astrafocus).

A [FastAPI](https://fastapi.tiangolo.com/) web interface provides a browser UI, alongside REST and WebSocket APIs for real-time status monitoring, image previews, and interaction with the [SQLite](https://sqlite.org/index.html)-backed database.

<iframe
  src="https://www.youtube.com/embed/QIElFSS1hkA?rel=0"
  style="width:100%;aspect-ratio:16/9;border:0;"
  allow="fullscreen"
  loading="lazy"
  frameborder="0"
  referrerpolicy="strict-origin-when-cross-origin">
</iframe>

## Used By

Currently, _Astra_ is deployed at multiple professional observatories
delivering reliable, unattended survey operations, including:

- [SPECULOOS-South (4x 1 m class): ESO Paranal, Chile](https://www.eso.org/public/teles-instr/paranal-observatory/speculoos/)
- [Saint-Ex (1 m class): San Pedro Mártir, Mexico](https://www.saintex.unibe.ch/saint_ex/description/)
- [ETH Observatory (0.5 m class): Zurich, Switzerland](https://mira.ethz.ch/)

## Screenshots

<table>
  <tr>
    <td width="24%">
      <img src="_static/ui-summary-tab.png" alt="Observatory overview"/>
      <p align="center"><em>Observatory overview</em></p>
    </td>
    <td width="24%">
      <img src="_static/ui-log-tab.png" alt="System logs"/>
      <p align="center"><em>System logs</em></p>
    </td>
    <td width="24%">
      <img src="_static/ui-weather-tab.png" alt="Weather monitoring"/>
      <p align="center"><em>Weather monitoring</em></p>
    </td>
    <td width="24%">
      <img src="_static/ui-controls-tab.png" alt="Controls tab"/>
      <p align="center"><em>Controls tab</em></p>
    </td>
  </tr>
</table>

<table style="margin-top: 20px;">
  <tr>
    <td width="45%">
      <img src="_static/ui-schedule-editor.png" alt="Astra schedule editor"/>
      <p align="center"><em>Schedule editor</em></p>
    </td>
    <td width="45%">
      <img src="_static/ui-fits-viewer.jpg" alt="Astra fits viewer"/>
      <p align="center"><em>FITS viewer</em></p>
    </td>
  </tr>
</table>

## Developed by

_Astra_ is developed by a team of astronomers and software engineers at [Queloz Group, ETH Zürich, Switzerland](https://queloz-group.ethz.ch/), in collaboration with the [SPECULOOS consortium](https://www.speculoos.uliege.be/cms/c_4259452/en/speculoos).

```{note}
This documentation is a work in progress. We are continuously updating and improving it. If you have any questions or suggestions, please feel free to reach out to us via [GitHub Issues](https://github.com/ppp-one/astra/issues).

We appreciate your feedback and contributions to make this software and documentation better.
```

---

<!-- ```{toctree}
:hidden:

motivation
``` -->

```{toctree}
:maxdepth: 2
:caption: Getting Started
:hidden:

installation
quickstart
```

```{toctree}
:maxdepth: 2
:caption: User Guide
:hidden:

user_guide/overview
user_guide/observatory_configuration
user_guide/fits_header_configuration
user_guide/scheduling
user_guide/operation
user_guide/core_logic
user_guide/custom_observatories
```

```{toctree}
:maxdepth: 2
:caption: Developer Documentation
:hidden:

contributing
api/index
api/endpoints
```
