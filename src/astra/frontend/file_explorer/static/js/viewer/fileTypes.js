function fallbackCheck(path, ext) {
    return String(path || '').toLowerCase().endsWith(ext);
}

function resolveHelpers() {
    if (typeof window === 'undefined') {
        return null;
    }
    const helpers = window.__astraFileTypes;
    if (helpers && typeof helpers.isPngFile === 'function') {
        return helpers;
    }
    // Safety net for non-standard runtime contexts (tests/partial DOM boot).
    // In the normal browser path, shared/fileTypes.js should be loaded first.
    return null;
}

const helpers = resolveHelpers();

export function isPngFile(path) {
    return helpers ? !!helpers.isPngFile(path) : fallbackCheck(path, '.png');
}

export function isFitsFile(path) {
    return helpers
        ? !!helpers.isFitsFile(path)
        : fallbackCheck(path, '.fits') || fallbackCheck(path, '.fit') || fallbackCheck(path, '.fts');
}
