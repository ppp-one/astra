document.addEventListener('DOMContentLoaded', function() {
    // Make headerGridContainer visible from the start
    const headerGridContainer = document.getElementById("headerGridContainer");
    headerGridContainer.style.display = "grid";
});

document.addEventListener('keydown', function(event) {
    const fitsSelect = document.getElementById("fits-select");
    const selectedIndex = fitsSelect.selectedIndex;

    if (event.key === 'ArrowRight') {
        // Move to the next file in the list
        if (selectedIndex < fitsSelect.options.length - 1) {
            fitsSelect.selectedIndex = selectedIndex + 1;
            loadFits();
        }
    } else if (event.key === 'ArrowLeft') {
        // Move to the previous file in the list
        if (selectedIndex > 0) {
            fitsSelect.selectedIndex = selectedIndex - 1;
            loadFits();
        }
    }
});


function selectFile(file) {
    // URL-encode each path segment so that nested directories (/) are preserved
    const encoded = file.split('/').map(encodeURIComponent).join('/');
    renderMonochromeImage(`/fits/${encoded}`);
}

function handleFileUpload(event) {
    const file = event.target.files[0];
    if (file) {
        const fileUrl = URL.createObjectURL(file);
        renderMonochromeImage(fileUrl);
    }
}


function parseFITSImage(arrayBuffer, dataView) {

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
        const keyword = line.substring(0, 8).trim();
        const value = line.substring(10, 80).trim();
        if (keyword === "END") break;
        header[keyword] = value;
    }
    console.timeLog("parseFITSImage", "parseFITSHeader");

    const width = parseInt(header["NAXIS1"], 10);
    const height = parseInt(header["NAXIS2"], 10);
    const bitpix = parseInt(header["BITPIX"], 10);
    const bscale = parseFloat(header["BSCALE"]) || 1;
    const bzero = parseFloat(header["BZERO"]) || 0;

    // Parse Image Data
    const dataSize = width * height;
    const bytesPerPixel = Math.abs(bitpix) / 8;

    // Use a typed array for image data
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
    console.timeLog("parseFITSImage", "parseFITSImageData");

    // Normalize Data for Display
    const { vmin, vmax } = zscale(data);
    console.timeLog("parseFITSImage", "zscale");
    // const normalizedData = data.map(
    //     (value) => ((value - vmin) / (vmax - vmin)) * 255
    // );
    const scale = 255 / (vmax - vmin);
    const _offset = -vmin * scale;
    const normalizedData = new Float32Array(data.length);

    for (let i = 0; i < data.length; i++) {
        normalizedData[i] = data[i] * scale + _offset;
    }
    console.timeLog("parseFITSImage", "normalizeData");

    console.timeEnd("parseFITSImage", "parseFITSImage done");

    // console.log(header, normalizedData);
    return [header, normalizedData, width, height, data];
}


function zscale(
    values,
    n_samples = 1000,
    contrast = 0.25,
    max_reject = 0.5,
    min_npixels = 5,
    krej = 2.5,
    max_iterations = 5
) {
    console.time("zscale");

    // Sample the image
    const stride = Math.max(1, Math.floor(values.length / n_samples));
    const samples = [];
    for (let i = 0; i < values.length && samples.length < n_samples; i += stride) {
        samples.push(values[i]);
    }
    console.timeLog("zscale", "sampleImage");

    // Sort in-place to avoid extra memory usage
    samples.sort((a, b) => a - b);
    console.timeLog("zscale", "sortSamples");

    const npix = samples.length;
    let vmin = samples[0];
    let vmax = samples[npix - 1];

    // Precompute x values
    const x = new Array(npix);
    for (let i = 0; i < npix; i++) {
        x[i] = i;
    }
    console.timeLog("zscale", "precomputeX");

    let ngoodpix = npix;
    let last_ngoodpix = ngoodpix + 1;

    // Initialize bad pixels mask
    const badpix = new Array(npix).fill(false);

    const minpix = Math.max(min_npixels, Math.floor(npix * max_reject));
    let fit = { slope: 0, intercept: 0 };
    console.timeLog("zscale", "initializeBadPixelsMask");

    for (let iter = 0; iter < max_iterations; iter++) {
        if (ngoodpix >= last_ngoodpix || ngoodpix < minpix) break;

        fit = linearFit(x, samples, badpix);
        // Compute fitted values and residuals using loops
        const fitted = new Array(npix);
        const flat = new Array(npix);
        for (let i = 0; i < npix; i++) {
            fitted[i] = fit.slope * x[i] + fit.intercept;
            flat[i] = samples[i] - fitted[i];
        }

        // Compute threshold for k-sigma clipping
        const goodPixels = [];
        for (let i = 0; i < npix; i++) {
            if (!badpix[i]) goodPixels.push(flat[i]);
        }
        const sigma = std(goodPixels);
        const threshold = krej * sigma;

        // Update badpix mask
        ngoodpix = 0;
        for (let i = 0; i < npix; i++) {
            if (Math.abs(flat[i]) > threshold) {
                badpix[i] = true;
            } else {
                badpix[i] = false;
                ngoodpix++;
            }
        }

        last_ngoodpix = ngoodpix;
    }
    console.timeLog("zscale", "kSigmaClipping");

    if (ngoodpix >= minpix) {
        let slope = fit.slope;
        if (contrast > 0) {
            slope = slope / contrast;
        }
        const center_pixel = Math.floor((npix - 1) / 2);
        const median = medianValue(samples);
        vmin = Math.max(vmin, median - (center_pixel - 1) * slope);
        vmax = Math.min(vmax, median + (npix - center_pixel) * slope);
    }
    console.timeLog("zscale", "updateMinMax");

    return { vmin, vmax };
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

function medianValue(arr) {
    // Optimized median calculation using Quickselect algorithm
    const n = arr.length;
    const k = Math.floor(n / 2);
    return quickSelect(arr, k);
}

function quickSelect(arr, k) {
    // In-place Quickselect algorithm
    let left = 0;
    let right = arr.length - 1;
    while (left <= right) {
        const pivotIndex = partition(arr, left, right);
        if (pivotIndex === k) {
            return arr[k];
        } else if (pivotIndex < k) {
            left = pivotIndex + 1;
        } else {
            right = pivotIndex - 1;
        }
    }
}

function partition(arr, left, right) {
    const pivotValue = arr[right];
    let pivotIndex = left;
    for (let i = left; i < right; i++) {
        if (arr[i] < pivotValue) {
            [arr[i], arr[pivotIndex]] = [arr[pivotIndex], arr[i]];
            pivotIndex++;
        }
    }
    [arr[right], arr[pivotIndex]] = [arr[pivotIndex], arr[right]];
    return pivotIndex;
}

function convolve(arr, kernel) {
    // Optimized convolution using loops
    const result = new Array(arr.length).fill(false);
    const kernelLength = kernel.length;
    for (let i = 0; i < arr.length; i++) {
        if (arr[i]) {
            for (let j = 0; j < kernelLength; j++) {
                const idx = i + j;
                if (idx < arr.length) {
                    result[idx] = true;
                }
            }
        }
    }
    return result;
}

function formatNumber(num, precision) {
    if (Math.floor(num) === num) {
        return num; // return as is, when it's an integer
    } else {
        return num.toFixed(precision); // use toFixed when there are decimals
    }
}
