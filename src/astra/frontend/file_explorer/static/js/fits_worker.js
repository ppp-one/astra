// WebWorker to parse FITS files off the main thread
// Expects to receive: {type: 'parse', arrayBuffer: ArrayBuffer}
// Replies with: {type: 'result', header, normalizedData, width, height, imageData}

self.addEventListener('message', (e) => {
    const msg = e.data;
    if (msg && msg.type === 'parse') {
        try {
            const arrayBuffer = msg.arrayBuffer;
            const view = new DataView(arrayBuffer);
            const result = parseFITSImage(arrayBuffer, view);
            // Transfer the normalizedData buffer back if possible
            // If normalizedData is a Float32Array, its buffer can be transferred
            const [header, normalizedData, width, height, imageData] = result;
            let transfer = [];
            if (normalizedData && normalizedData.buffer) transfer.push(normalizedData.buffer);
            self.postMessage({type: 'result', header, normalizedData, width, height, imageData}, transfer);
        } catch (err) {
            self.postMessage({type: 'error', message: String(err)});
        }
    }
});

// Copy of parseFITSImage (kept minimal and self-contained)
function parseFITSImage(arrayBuffer, dataView) {
    // Very basic FITS header parsing
    let headerText = "";
    let offset = 0;
    const headerSize = 2880;
    while (true) {
        const block = new TextDecoder().decode(
            arrayBuffer.slice(offset, offset + headerSize)
        );
        headerText += block;
        offset += headerSize;
        if (block.trim().endsWith("END")) break;
    }

    const headerLines = headerText.match(/.{1,80}/g); // Split into 80-char lines
    const header = {};
    for (const line of headerLines) {
        const keyword = line.substring(0, 8).trim();
        const value = line.substring(10, 80).trim();
        if (keyword === "END") break;
        header[keyword] = value;
    }

    const width = parseInt(header["NAXIS1"], 10);
    const height = parseInt(header["NAXIS2"], 10);
    const bitpix = parseInt(header["BITPIX"], 10);
    const bscale = parseFloat(header["BSCALE"]) || 1;
    const bzero = parseFloat(header["BZERO"]) || 0;

    const dataSize = width * height;
    const bytesPerPixel = Math.abs(bitpix) / 8;

    let data;
    if (bitpix === 8 || bitpix === 16 || bitpix === 32) {
        data = new Int32Array(dataSize);
    } else if (bitpix === -32) {
        data = new Float32Array(dataSize);
    } else if (bitpix === -64) {
        data = new Float64Array(dataSize);
    } else {
        throw new Error(`Unsupported BITPIX: ${bitpix}`);
    }

    for (let i = 0; i < dataSize; i++) {
        if (bitpix === 8) {
            data[i] = dataView.getUint8(offset) * bscale + bzero;
        } else if (bitpix === 16) {
            data[i] = dataView.getInt16(offset, false) * bscale + bzero;
        } else if (bitpix === 32) {
            data[i] = dataView.getInt32(offset, false) * bscale + bzero;
        } else if (bitpix === -32) {
            data[i] = dataView.getFloat32(offset, false) * bscale + bzero;
        } else if (bitpix === -64) {
            data[i] = dataView.getFloat64(offset, false) * bscale + bzero;
        }
        offset += bytesPerPixel;
    }

    // Normalize Data (simple min/max)
    let vmin = Infinity;
    let vmax = -Infinity;
    for (let i = 0; i < data.length; i++) {
        const v = data[i];
        if (v < vmin) vmin = v;
        if (v > vmax) vmax = v;
    }
    const scale = 255 / (vmax - vmin || 1);
    const normalizedData = new Float32Array(data.length);
    for (let i = 0; i < data.length; i++) {
        normalizedData[i] = (data[i] - vmin) * scale;
    }

    return [header, normalizedData, width, height, data];
}
