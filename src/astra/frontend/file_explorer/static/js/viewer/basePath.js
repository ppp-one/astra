const GLOBAL_KEY = '__ASTRA_FITS_BASE_PATH';
const GLOBAL_FILES_KEY = '__ASTRA_FITS_FILES_BASE_PATH';

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

function resolveFilesBasePathInternal() {
    if (typeof window === 'undefined') {
        return '/fits/';
    }
    const provided = window[GLOBAL_FILES_KEY];
    if (provided && typeof provided === 'string') {
        return normalizeBase(provided);
    }
    return '/fits/';
}

const FILES_BASE_PATH = resolveFilesBasePathInternal();

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

export function getFilesBasePath() {
    return FILES_BASE_PATH;
}

export function encodePathSegments(filePath = '') {
    return String(filePath || '')
        .split('/')
        .map((segment) => encodeURIComponent(segment))
        .join('/');
}

export function rawFitsUrl(filePath = '') {
    const trimmed = encodePathSegments(filePath);
    if (!trimmed) {
        return FILES_BASE_PATH;
    }
    if (FILES_BASE_PATH === '/') {
        return `/${trimmed}`;
    }
    return `${FILES_BASE_PATH}${trimmed}`;
}

export function staticUrl(subPath = '') {
    const trimmed = subPath.startsWith('/') ? subPath.slice(1) : subPath;
    return withBase(`static/${trimmed}`);
}
