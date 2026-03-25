(function initAstraFileTypes() {
    const FITS_EXTENSIONS = ['.fits', '.fit', '.fts'];
    const PNG_EXTENSIONS = ['.png'];

    function toLower(value) {
        return String(value || '').toLowerCase();
    }

    function hasExtension(name, extensions) {
        const lower = toLower(name);
        return extensions.some((ext) => lower.endsWith(ext));
    }

    function isFitsFile(name) {
        return hasExtension(name, FITS_EXTENSIONS);
    }

    function isPngFile(name) {
        return hasExtension(name, PNG_EXTENSIONS);
    }

    function isSupportedPreviewFile(name) {
        return isFitsFile(name) || isPngFile(name);
    }

    window.__astraFileTypes = {
        FITS_EXTENSIONS,
        PNG_EXTENSIONS,
        isFitsFile,
        isPngFile,
        isSupportedPreviewFile,
    };
})();
