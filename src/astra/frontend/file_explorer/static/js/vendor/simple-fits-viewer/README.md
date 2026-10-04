# simple-fits-viewer (vendored)

FITS viewer from https://github.com/ppp-one/simple-fits-viewer (GPL-3.0, like Astra).
Copied here so the file explorer also works without internet access.

- Base: commit 8c3c5f9 (`lib/*.js` and `style.css`).
- Plus the `feat/embeddable` changes, not yet merged upstream: the viewer sizes
  itself to its container, `loadImageData()` with `binning` for downsampled
  previews, `setHeaderData()`, optional keyboard shortcuts, scoped CSS, touch
  support (long press, compact layout, bottom-sheet panels), FWHM accuracy fixes,
  SIP fixes, and listener cleanup in `destroy()`.

To update: copy `lib/fits-viewer.js`, `lib/utils.js`, `lib/fwhm.js`, `lib/wcs.js`
and `style.css` from the library into this folder. It needs D3 v7 as `window.d3`.
