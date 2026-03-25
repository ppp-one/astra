import { withBase, rawFitsUrl, encodePathSegments } from './basePath.js';

async function fetchOrThrow(url, options = {}) {
    const response = await fetch(url, options);
    if (!response.ok) {
        const detail = await response.text().catch(() => response.statusText);
        throw new Error(`Request failed (${response.status}): ${detail}`);
    }
    return response;
}

export async function fetchPreviewFITS(
    filePath,
    { hdu = null, maxDim = 512, signal } = {}
) {
    const encoded = encodePathSegments(filePath);
    const params = new URLSearchParams();
    if (hdu !== null && hdu !== undefined) params.set('hdu', hdu);
    if (maxDim) params.set('max_dim', maxDim);
    const query = params.toString();
    const url = withBase(`preview/${encoded}${query ? `?${query}` : ''}`);
    const response = await fetchOrThrow(url, { signal });
    return response.arrayBuffer();
}

export async function fetchFullFITS(filePath, { signal } = {}) {
    const url = rawFitsUrl(filePath);
    const response = await fetchOrThrow(url, { signal });
    return response.arrayBuffer();
}

export async function fetchHeaderData(filePath, { hdu = null, signal } = {}) {
    const encoded = encodePathSegments(filePath);
    const params = new URLSearchParams();
    if (hdu !== null && hdu !== undefined) params.set('hdu', hdu);
    const query = params.toString();
    const url = withBase(`header/${encoded}${query ? `?${query}` : ''}`);
    const response = await fetchOrThrow(url, { signal });
    return response.json();
}

export async function fetchHduList(filePath, { signal } = {}) {
    const encoded = encodePathSegments(filePath);
    const url = withBase(`hdu_list/${encoded}`);
    const response = await fetchOrThrow(url, { signal });
    return response.json();
}
