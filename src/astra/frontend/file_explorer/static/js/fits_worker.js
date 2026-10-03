// WebWorker to parse FITS files off the main thread
// Expects to receive: {type: 'parse', arrayBuffer: ArrayBuffer}
// Replies with: {type: 'result', header, width, height, imageData, dataMin, dataMax}
// The imageData buffer is transferred, not copied.

const BLOCK_SIZE = 2880;
const CARD_SIZE = 80;
const HOST_IS_LITTLE_ENDIAN = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;

self.addEventListener('message', (e) => {
    const msg = e.data;
    if (msg && msg.type === 'parse') {
        try {
            const result = parseFITSImage(msg.arrayBuffer);
            self.postMessage({ type: 'result', ...result }, [result.imageData.buffer]);
        } catch (err) {
            self.postMessage({ type: 'error', message: String(err) });
        }
    }
});

function parseHeader(arrayBuffer) {
    const decoder = new TextDecoder('ascii');
    const header = {};
    let offset = 0;
    while (offset + BLOCK_SIZE <= arrayBuffer.byteLength) {
        const block = decoder.decode(new Uint8Array(arrayBuffer, offset, BLOCK_SIZE));
        offset += BLOCK_SIZE;
        for (let i = 0; i < BLOCK_SIZE; i += CARD_SIZE) {
            const keyword = block.substring(i, i + 8).trim();
            if (keyword === 'END') return { header, dataOffset: offset };
            if (keyword) header[keyword] = block.substring(i + 10, i + CARD_SIZE).trim();
        }
    }
    throw new Error('FITS header has no END card');
}

// FITS data is big-endian. Swap it in place to host order so typed arrays can
// read it directly. The header length is a multiple of 2880, so the data is
// aligned for every word size.
function swapToHostOrder(arrayBuffer, offset, count, wordSize) {
    if (!HOST_IS_LITTLE_ENDIAN || wordSize === 1) return;
    if (wordSize === 2) {
        const a = new Uint16Array(arrayBuffer, offset, count);
        for (let i = 0; i < count; i++) {
            const x = a[i];
            a[i] = (x >>> 8) | (x << 8);
        }
        return;
    }
    const a = new Uint32Array(arrayBuffer, offset, count * (wordSize / 4));
    const swap32 = (x) => (x >>> 24) | ((x >>> 8) & 0xff00) | ((x & 0xff00) << 8) | (x << 24);
    if (wordSize === 4) {
        for (let i = 0; i < a.length; i++) a[i] = swap32(a[i]);
    } else {
        for (let i = 0; i < a.length; i += 2) {
            const hi = a[i];
            a[i] = swap32(a[i + 1]);
            a[i + 1] = swap32(hi);
        }
    }
}

function parseFITSImage(arrayBuffer) {
    const { header, dataOffset } = parseHeader(arrayBuffer);

    const width = parseInt(header['NAXIS1'], 10);
    const height = parseInt(header['NAXIS2'], 10) || 1;
    const bitpix = parseInt(header['BITPIX'], 10);
    const bscale = parseFloat(header['BSCALE']) || 1;
    const bzero = parseFloat(header['BZERO']) || 0;
    const count = width * height;
    if (!(count > 0)) throw new Error('FITS HDU has no image data');

    const RawArray = { 8: Uint8Array, 16: Int16Array, 32: Int32Array, '-32': Float32Array, '-64': Float64Array }[bitpix];
    if (!RawArray) throw new Error(`Unsupported BITPIX: ${bitpix}`);
    const wordSize = RawArray.BYTES_PER_ELEMENT;
    if (dataOffset + count * wordSize > arrayBuffer.byteLength) {
        throw new Error('FITS file is truncated');
    }

    swapToHostOrder(arrayBuffer, dataOffset, count, wordSize);
    const raw = new RawArray(arrayBuffer, dataOffset, count);

    let data = raw;
    if (bscale !== 1 || bzero !== 0) {
        // Floats are scaled in place; scaled integers need a wider array
        if (bitpix > 0) {
            data = Number.isInteger(bscale) && Number.isInteger(bzero) && bitpix <= 16
                ? new Int32Array(count) // e.g. uint16 stored with BZERO=32768
                : new Float64Array(count); // fractional scaling or 32-bit unsigned
        }
        for (let i = 0; i < count; i++) data[i] = raw[i] * bscale + bzero;
    }

    let dataMin = Infinity;
    let dataMax = -Infinity;
    for (let i = 0; i < count; i++) {
        const v = data[i];
        // NaN fails both comparisons; the extra checks drop +/-Infinity
        if (v < dataMin && v !== -Infinity) dataMin = v;
        if (v > dataMax && v !== Infinity) dataMax = v;
    }

    return { header, width, height, imageData: data, dataMin, dataMax };
}
