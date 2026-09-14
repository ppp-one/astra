# Customising Observatories by Subclassing

```{image} ../_static/custom_observatories-banner.svg
:class: responsive-banner
:align: center
:alt: banner
```

You can create custom `Observatory` subclasses to adapt _Astra_ to site-specific requirements without modifying the source code:

- Add observatory-specific shutdown/opening sequences
- Override polling or error-acknowledgement for non-standard devices
- Encapsulate organization-specific safeguards and logging

## How It Works

_Astra_ uses an `ObservatoryLoader` that searches your `custom_observatories` directory for Python files containing `Observatory` subclasses. When you use the `--observatory` flag, the loader searches for a matching subclass (case-insensitive, including `OBSERVATORY_ALIASES` class attribute). If no match is found, the default `Observatory` class is used.

```bash
# Load SPECULOOS custom subclass (if it exists)
astra --observatory SPECULOOS
```

```{note}
The `--observatory` flag is optional and only needed if you've created custom subclasses. It selects which Python subclass to use. Not to be mistaken with which observatory configuration to run -- that's determined by `observatory_name` in your base configuration file (`~/.astra/astra_config.yml`).
```

For background on subclassing and inheritance in Python, see e.g.
[Python inheritance tutorial](https://docs.python.org/3/tutorial/classes.html#inheritance)
in the official documentation.

## Example SPECULOOS

Being ASTELCO made observatories, SPECULOOS telescopes are subject to certain
quirks that require special handling. Specifically, they need custom error
handling and some of its ASCOM methods not conforming asynchronous standards.

In the following you can have a look at the subclass used by the SPECULOOS observatories.

```{literalinclude} ../../../example_observatories/speculoos/speculoos.py
:language: python
```
