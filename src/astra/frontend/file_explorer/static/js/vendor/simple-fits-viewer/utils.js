export function parseFITSImage(arrayBuffer, dataView) {

    console.time("parseFITSImage");

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

    // Parse Header Keywords
    const headerLines = headerText.match(/.{1,80}/g); // Split into 80-char lines
    const header = {};
    for (const line of headerLines) {
        let keyword = line.substring(0, 8).trim();
        let value = line.substring(10, 80).trim();
        if (keyword === "END") break;
        // ESO HIERARCH convention: "HIERARCH LONG KEY NAME = value / comment"
        if (keyword === "HIERARCH") {
            const eq = line.indexOf("=");
            if (eq > 8) {
                keyword = line.substring(8, eq).trim();
                value = line.substring(eq + 1).trim();
            }
        }
        if (keyword) header[keyword] = value;
    }
    console.timeLog("parseFITSImage", "parseFITSHeader");

    const width = parseInt(header["NAXIS1"], 10);
    const height = parseInt(header["NAXIS2"], 10);
    const bitpix = parseInt(header["BITPIX"], 10);
    const bscale = parseFloat(header["BSCALE"]) || 1;
    const bzero = parseFloat(header["BZERO"]) || 0;

    // Parse Image Data — using byte-swap + typed array for speed
    const dataSize = width * height;
    const bytesPerPixel = Math.abs(bitpix) / 8;
    const totalBytes = dataSize * bytesPerPixel;
    const src = new Uint8Array(arrayBuffer, offset, totalBytes);

    let data;
    let dataMin = Infinity, dataMax = -Infinity;
    // Integer arrays only hold integer results; fractional BSCALE/BZERO needs floats
    const integerScale = Number.isInteger(bscale) && Number.isInteger(bzero);

    if (bitpix === 8) {
        // 8-bit: no byte-swap needed
        data = integerScale ? new Int32Array(dataSize) : new Float32Array(dataSize);
        for (let i = 0; i < dataSize; i++) {
            const v = src[i] * bscale + bzero;
            data[i] = v;
            if (v < dataMin) dataMin = v;
            if (v > dataMax) dataMax = v;
        }
    } else if (bitpix === 16) {
        // 16-bit big-endian → single pass: read big-endian int16 directly from src,
        // sign-extend with (<< 16 >> 16), apply scale, write to Int32Array.
        data = integerScale ? new Int32Array(dataSize) : new Float32Array(dataSize);
        if (bscale === 1 && bzero === 0) {
            for (let i = 0; i < dataSize; i++) {
                const v = (src[i * 2] << 8 | src[i * 2 + 1]) << 16 >> 16;
                data[i] = v;
                if (v < dataMin) dataMin = v;
                if (v > dataMax) dataMax = v;
            }
        } else {
            for (let i = 0; i < dataSize; i++) {
                const v = ((src[i * 2] << 8 | src[i * 2 + 1]) << 16 >> 16) * bscale + bzero;
                data[i] = v;
                if (v < dataMin) dataMin = v;
                if (v > dataMax) dataMax = v;
            }
        }
    } else if (bitpix === 32 || bitpix === -32 || bitpix === -64) {
        // Multi-byte big-endian: byte-swap into a native typed array, then apply scale.
        const bytesPerSample = Math.abs(bitpix) / 8;
        const buf = new ArrayBuffer(totalBytes);
        const dst = new Uint8Array(buf);
        for (let i = 0; i < totalBytes; i += bytesPerSample) {
            for (let b = 0; b < bytesPerSample; b++) {
                dst[i + b] = src[i + (bytesPerSample - 1 - b)];
            }
        }
        const TypedArray = bitpix === 32 ? Int32Array : bitpix === -32 ? Float32Array : Float64Array;
        const raw = new TypedArray(buf);
        // Always scan for min/max (needed by buildHistogram) and apply bzero/bscale.
        // For integer data with identity scale the values are unchanged but we still
        // need the range, so we always do the pass. Scaled 32-bit integers (e.g.
        // unsigned, BZERO = 2^31) do not fit an Int32Array, so they go to Float64Array.
        const needsScale = bscale !== 1 || bzero !== 0;
        data = needsScale && bitpix === 32 ? new Float64Array(dataSize) : raw;
        for (let i = 0; i < dataSize; i++) {
            const v = needsScale ? raw[i] * bscale + bzero : raw[i];
            if (needsScale) data[i] = v;
            // NaN fails both tests; the extra checks drop +/-Infinity
            if (v < dataMin && v !== -Infinity) dataMin = v;
            if (v > dataMax && v !== Infinity) dataMax = v;
        }
    } else {
        throw new Error(`Unsupported BITPIX: ${bitpix}`);
    }
    offset += totalBytes;
    console.timeLog("parseFITSImage", "parseFITSImageData");
    console.timeEnd("parseFITSImage");

    return [header, width, height, data, dataMin, dataMax];
}

export function normalizeData(data, vmin, vmax) {
    // Normalize Data for Display
    console.time("normalizeData");
    const scale = 255 / (vmax - vmin);
    const _offset = -vmin * scale;
    const normalizedData = new Float32Array(data.length);

    for (let i = 0; i < data.length; i++) {
        normalizedData[i] = data[i] * scale + _offset;
    }
    console.timeEnd("normalizeData");

    return normalizedData;
}

// Same algorithm as astropy.visualization.ZScaleInterval
export function zscale(
    values,
    histogram,
    autoZscale,
    n_samples = 1000,
    contrast = 0.25,
    max_reject = 0.5,
    min_npixels = 5,
    krej = 2.5,
    max_iterations = 5
) {
    console.time("zscale");

    if (!autoZscale) {
        const vmin = histogram.min;
        const vmax = histogram.max;
        console.timeEnd("zscale");
        return { vmin, vmax };
    }

    // Sample the image
    const stride = Math.max(1, Math.floor(values.length / n_samples));
    const samples = [];
    for (let i = 0; i < values.length && samples.length < n_samples; i += stride) {
        if (Number.isFinite(values[i])) samples.push(values[i]);
    }
    samples.sort((a, b) => a - b);

    const npix = samples.length;
    let vmin = samples[0];
    let vmax = samples[npix - 1];

    const x = new Array(npix);
    for (let i = 0; i < npix; i++) {
        x[i] = i;
    }

    let ngoodpix = npix;
    let last_ngoodpix = ngoodpix + 1;
    let badpix = new Uint8Array(npix);
    const minpix = Math.max(min_npixels, Math.floor(npix * max_reject));
    const ngrow = Math.max(1, Math.floor(npix * 0.01));
    let fit = { slope: 0, intercept: 0 };

    for (let iter = 0; iter < max_iterations; iter++) {
        if (ngoodpix >= last_ngoodpix || ngoodpix < minpix) break;

        fit = linearFit(x, samples, badpix);
        const flat = new Array(npix);
        for (let i = 0; i < npix; i++) {
            flat[i] = samples[i] - (fit.slope * x[i] + fit.intercept);
        }

        // k-sigma clipping; rejections accumulate and grow into their neighbours
        const goodPixels = [];
        for (let i = 0; i < npix; i++) {
            if (!badpix[i]) goodPixels.push(flat[i]);
        }
        const threshold = krej * std(goodPixels);
        for (let i = 0; i < npix; i++) {
            if (Math.abs(flat[i]) > threshold) badpix[i] = 1;
        }
        badpix = growMask(badpix, ngrow);

        last_ngoodpix = ngoodpix;
        ngoodpix = 0;
        for (let i = 0; i < npix; i++) {
            if (!badpix[i]) ngoodpix++;
        }
    }

    if (ngoodpix >= minpix) {
        let slope = fit.slope;
        if (contrast > 0) {
            slope = slope / contrast;
        }
        const center_pixel = Math.floor((npix - 1) / 2);
        const mid = Math.floor(npix / 2);
        const median = npix % 2 ? samples[mid] : (samples[mid - 1] + samples[mid]) / 2;
        vmin = Math.max(vmin, median - (center_pixel - 1) * slope);
        vmax = Math.min(vmax, median + (npix - center_pixel) * slope);
    }
    console.timeEnd("zscale");

    return { vmin, vmax };
}

// Dilate a bad-pixel mask like np.convolve(mask, ones(size), mode="same")
function growMask(mask, size) {
    const before = Math.floor((size - 1) / 2);
    const after = size - 1 - before;
    const grown = new Uint8Array(mask.length);
    for (let i = 0; i < mask.length; i++) {
        if (!mask[i]) continue;
        const end = Math.min(mask.length - 1, i + after);
        for (let j = Math.max(0, i - before); j <= end; j++) grown[j] = 1;
    }
    return grown;
}

function linearFit(x, y, badpix) {
    // Optimized linear fit using loops
    let sumX = 0,
        sumY = 0,
        sumXY = 0,
        sumX2 = 0,
        n = 0;
    for (let i = 0; i < x.length; i++) {
        if (!badpix[i]) {
            const xi = x[i];
            const yi = y[i];
            sumX += xi;
            sumY += yi;
            sumXY += xi * yi;
            sumX2 += xi * xi;
            n++;
        }
    }
    const denominator = n * sumX2 - sumX * sumX;
    const slope = (n * sumXY - sumX * sumY) / denominator;
    const intercept = (sumY - slope * sumX) / n;
    return { slope, intercept };
}

function std(arr) {
    // Optimized standard deviation calculation
    let mean = 0;
    for (let i = 0; i < arr.length; i++) {
        mean += arr[i];
    }
    mean /= arr.length;
    let variance = 0;
    for (let i = 0; i < arr.length; i++) {
        const diff = arr[i] - mean;
        variance += diff * diff;
    }
    variance /= arr.length;
    return Math.sqrt(variance);
}

export function formatNumber(num, precision) {
    if (Math.floor(num) === num) {
        return num; // return as is, when it's an integer
    } else {
        return num.toFixed(precision); // use toFixed when there are decimals
    }
}

// Histogram / percentile helpers (computed once per image)
// knownMin/knownMax may be supplied by parseFITSImage to skip the first pass.
export function buildHistogram(values, nbins = 4096, knownMin, knownMax) {
    const isIntegerArray = values instanceof Int32Array;

    // Adaptive stride: sample large images so the binning pass visits fewer
    // pixels. min/max always come from the full parse so no values clip.
    // 4096 bins with 500 k+ samples gives >100 samples/bin — enough for
    // accurate percentile lookups.
    const stride = values.length > 2_000_000 ? 4
        : values.length > 500_000 ? 2
            : 1;

    let min, max, n;
    if (knownMin !== undefined && knownMin !== Infinity) {
        // min/max already found during parsing — skip the first scan entirely.
        min = knownMin;
        max = knownMax;
        n = Math.ceil(values.length / stride);
    } else {
        // Fallback: find min/max with the same stride.
        min = Infinity; max = -Infinity; n = 0;
        if (isIntegerArray) {
            for (let i = 0; i < values.length; i += stride) {
                const v = values[i];
                if (v < min) min = v;
                if (v > max) max = v;
            }
            n = Math.ceil(values.length / stride);
        } else {
            for (let i = 0; i < values.length; i += stride) {
                const v = values[i];
                if (!Number.isFinite(v)) continue;
                if (v < min) min = v;
                if (v > max) max = v;
                n++;
            }
        }
    }

    if (n === 0) return null;
    if (min === max) max = min + 1;

    const counts = new Uint32Array(nbins);
    const binWidth = (max - min) / nbins;
    const invBinWidth = 1 / binWidth;
    if (isIntegerArray) {
        // Values are guaranteed >= min so idx >= 0; use | 0 instead of Math.floor.
        for (let i = 0; i < values.length; i += stride) {
            let idx = ((values[i] - min) * invBinWidth) | 0;
            if (idx >= nbins) idx = nbins - 1;
            counts[idx]++;
        }
    } else {
        for (let i = 0; i < values.length; i += stride) {
            const v = values[i];
            if (!Number.isFinite(v)) continue;
            let idx = ((v - min) * invBinWidth) | 0;
            if (idx < 0) idx = 0;
            else if (idx >= nbins) idx = nbins - 1;
            counts[idx]++;
        }
    }
    // cumulative
    const cumsum = new Float64Array(nbins);
    let acc = 0;
    for (let i = 0; i < nbins; i++) {
        acc += counts[i];
        cumsum[i] = acc;
    }
    return { min, max, nbins, binWidth, counts, cumsum, total: acc };
}

// Convert a percentile (0–100) to a data value using the histogram.
export function valueForPercentile(histogram, p) {
    if (!histogram) return 0;
    const target = (p / 100) * (histogram.total - 1);
    const c = histogram.cumsum;
    // binary search for bin
    let lo = 0,
        hi = c.length - 1;
    while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (c[mid] >= target) hi = mid;
        else lo = mid + 1;
    }
    const binIdx = lo;
    const binStartCount = binIdx === 0 ? 0 : c[binIdx - 1];
    const binCount = histogram.counts[binIdx];
    const frac = binCount === 0 ? 0 : (target - binStartCount) / binCount;
    const value = histogram.min + (binIdx + frac) * histogram.binWidth;
    return value;
}

// Convert a data value to a percentile (0–100) using the histogram.
export function percentileForValue(histogram, v) {
    if (!histogram) return 0;
    if (!Number.isFinite(v)) return 0;
    let idx = Math.floor((v - histogram.min) / histogram.binWidth);
    if (idx < 0) idx = 0;
    if (idx >= histogram.nbins) idx = histogram.nbins - 1;
    const countBefore = idx === 0 ? 0 : histogram.cumsum[idx - 1];
    const within = histogram.counts[idx];
    const frac =
        within === 0
            ? 0
            : (v - (histogram.min + idx * histogram.binWidth)) /
            histogram.binWidth;
    const rank = countBefore + frac * within;
    return (rank / (histogram.total - 1)) * 100;
}

// Split a raw card value ("'a/b' / comment", "1.5 / comment") into value and
// comment. A "/" inside a quoted string belongs to the value; quotes are removed.
// Objects {value, comment} (from a host) pass through.
export function splitHeaderValue(raw) {
    if (raw && typeof raw === 'object') {
        return { value: String(raw.value ?? ''), comment: String(raw.comment ?? '') };
    }
    const s = String(raw ?? '');
    let inQuote = false;
    let cut = s.length;
    for (let i = 0; i < s.length; i++) {
        if (s[i] === "'") {
            if (inQuote && s[i + 1] === "'") { i++; continue; } // '' is an escaped quote
            inQuote = !inQuote;
        } else if (s[i] === '/' && !inQuote) {
            cut = i;
            break;
        }
    }
    let value = s.slice(0, cut).trim();
    if (value.length >= 2 && value.startsWith("'") && value.endsWith("'")) {
        value = value.slice(1, -1).replace(/''/g, "'").trimEnd();
    }
    return { value, comment: cut < s.length ? s.slice(cut + 1).trim() : '' };
}

// Plate scale in arcsec/pixel from XPIXSZ (microns) and FOCALLEN, or null.
// FOCALLEN has no standard unit (some software writes mm, some m), so the unit
// must be in its comment; a result outside 0.01-100 "/px is treated as wrong.
export function plateScaleFromPixelSize(header) {
    const pixel = parseFloat(header['XPIXSZ']);
    const focal = parseFloat(header['FOCALLEN']);
    if (!(pixel > 0 && focal > 0)) return null;
    const comment = splitHeaderValue(header['FOCALLEN']).comment.toLowerCase();
    let focalMetres = null;
    if (/\bmm\b|millimet/.test(comment)) focalMetres = focal / 1000;
    else if (/\[\s*m\s*\]|\bmet(er|re)s?\b/.test(comment)) focalMetres = focal;
    if (!focalMetres) return null;
    const scale = Math.atan((pixel * 1e-6) / focalMetres) * (180 / Math.PI) * 3600;
    return scale > 0.01 && scale < 100 ? scale : null;
}

// Lookup table from normalised value (index / (size - 1)) to an opaque grey RGBA
// pixel, so log and gamma are computed once per entry instead of once per pixel.
export function buildStretchLut(range, gamma, useLog, size = 65536) {
    const lut = new Uint32Array(size);
    const littleEndian = new Uint8Array(new Uint16Array([1]).buffer)[0] === 1;
    const logDenom = Math.log10(1 + range);
    for (let i = 0; i < size; i++) {
        let norm = i / (size - 1);
        if (useLog) norm = Math.log10(1 + norm * range) / logDenom;
        const c = Math.round(Math.pow(norm, 1 / gamma) * 255);
        lut[i] = littleEndian
            ? 0xff000000 | (c << 16) | (c << 8) | c
            : (c << 24) | (c << 16) | (c << 8) | 0xff;
    }
    return lut;
}
