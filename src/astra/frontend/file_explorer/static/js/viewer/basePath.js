const GLOBAL_KEY = '__ASTRA_FITS_BASE_PATH';

function normalizeBase(path) {
    if (!path || typeof path !== 'string') {
        return '/';
    }
    let normalized = path.trim();
    if (!normalized.startsWith('/')) {
        normalized = `/${normalized}`;
    }
    if (!normalized.endsWith('/')) {
        normalized = `${normalized}/`;
    }
    return normalized;
}

function resolveBasePathInternal() {
    if (typeof window === 'undefined') {
        return '/';
    }
    const provided = window[GLOBAL_KEY];
    if (provided && typeof provided === 'string') {
        return normalizeBase(provided);
    }
    const pathname = window.location?.pathname || '/';
    if (pathname.endsWith('/')) {
        return normalizeBase(pathname);
    }
    const lastSlash = pathname.lastIndexOf('/');
    if (lastSlash >= 0) {
        return normalizeBase(pathname.slice(0, lastSlash + 1));
    }
    return '/';
}

const BASE_PATH = resolveBasePathInternal();

export function getBasePath() {
    return BASE_PATH;
}

export function withBase(relative = '') {
    const trimmed = relative.startsWith('/') ? relative.slice(1) : relative;
    if (!trimmed) {
        return BASE_PATH;
    }
    if (BASE_PATH === '/') {
        return `/${trimmed}`;
    }
    return `${BASE_PATH}${trimmed}`;
}

export function staticUrl(subPath = '') {
    const trimmed = subPath.startsWith('/') ? subPath.slice(1) : subPath;
    return withBase(`static/${trimmed}`);
}
