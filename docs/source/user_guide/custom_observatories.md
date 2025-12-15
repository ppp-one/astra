# Customising Observatories by Subclassing

There are many reasons why you might consider creating a custom `Observatory` subclass
to adapt to site-specific behaviour without changing the source code, such as

- Add observatory-specific shutdown/opening sequences.
- Override polling or error-acknowledgement for non-standard devices.
- Encapsulate organisation-specific safeguards and logging.

`astra` / `main.py` uses an `ObservatoryLoader` to discover and load custom
observatory subclasses at runtime. The loader searches the directory configured
by `Config().paths.custom_observatories` for `*.py` files, imports them using
`importlib.util.spec_from_file_location`, and inspects each module for classes
that are subclasses of `Observatory` (excluding `Observatory` itself).

Matching is case-insensitive: the loader compares the requested observatory
name (lowercased) against the lowercased class name and any names listed in
the class attribute `OBSERVATORY_ALIASES`. When a match is found the loader
returns that subclass; if no match is found it returns the base
`Observatory` class.

Because of this fallback behaviour, `main.py` and the default runtime continue
to work unchanged if no custom subclass is present or matched.

For background on subclassing and inheritance in Python, see e.g.
[Python inheritance tutorial](https://docs.python.org/3/tutorial/classes.html#inheritance)
in the official documentation.

## Example SPECULOOS

Being ASTELCO made observatories, SPECULOOS telescopes are subject to certain
quirks that require special handling. Specifically, they need custom error
handling and some of its ASCOM methods not conforming asynchronous standards.

In the following you can have a look at the subclass used by the SPECULOOS observatoris.

```{literalinclude} ../../../example_observatories/speculoos/speculoos.py
:language: python
```
