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
    // Source pixels per preview pixel, so the viewer knows the data is binned
    const stride = Number(response.headers.get('X-Astra-Preview-Stride')) || 1;
    return { arrayBuffer: await response.arrayBuffer(), stride };
}

export async function fetchFullFITS(filePath, { signal, onProgress } = {}) {
    const url = rawFitsUrl(filePath);
    const response = await fetchOrThrow(url, { signal });
    if (!onProgress || !response.body) {
        return response.arrayBuffer();
    }

    // With gzip, Content-Length (if any) is the compressed size, not what the
    // stream yields, so only trust it for unencoded responses.
    const encoded = response.headers.get('Content-Encoding');
    const total = encoded ? 0 : Number(response.headers.get('Content-Length')) || 0;
    const reader = response.body.getReader();
    const chunks = [];
    let received = 0;
    onProgress(received, total);
    for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
        received += value.byteLength;
        onProgress(received, total);
    }

    const result = new Uint8Array(received);
    let offset = 0;
    for (const chunk of chunks) {
        result.set(chunk, offset);
        offset += chunk.byteLength;
    }
    return result.buffer;
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
