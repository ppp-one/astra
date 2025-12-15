const spinner = document.getElementById("spinner");

const canvas = document.getElementById("loadedImage");
const ctx = canvas.getContext("2d");

const xProfileCanvas = document.getElementById("xProfile");
const xProfileCtx = xProfileCanvas.getContext("2d");
const yProfileCanvas = document.getElementById("yProfile");
const yProfileCtx = yProfileCanvas.getContext("2d");

let offscreenCanvas, offscreenCtx;
let imageWidth, imageHeight;
let currentTransform = d3.zoomIdentity;
let imageData = null;
let imageDataT = null;
let normalizedData = null;
let scaleFactor = 1;
let rect = null;
let scaleX = null;
let scaleY = null;
let headerData = {};
let profileColor = "#ffffff";
let gridColor = "#e0e0e0";

const mainContainer = document.querySelector(".mainContainer");
const headerTab = document.getElementById("headerTab");
const headerGridContainer = document.getElementById(
    "headerGridContainer"
);
const imageGridContainer = document.getElementById("imageGridContainer");
const headerTable = document.getElementById("headerTable");
const searchInput = document.getElementById("searchInput");
const resetButton = document.getElementById("resetButton");
const returnButton = document.getElementById("returnButton");

function resolveExplorerBase() {
    const globalBase = window.__ASTRA_FITS_BASE_PATH;
    if (globalBase && typeof globalBase === 'string') {
        return globalBase.endsWith('/') ? globalBase : `${globalBase}/`;
    }
    const pathname = window.location?.pathname || '/';
    if (pathname.endsWith('/')) {
        return pathname;
    }
    const idx = pathname.lastIndexOf('/');
    return idx >= 0 ? pathname.slice(0, idx + 1) : '/';
}

function staticUrl(path = '') {
    const base = resolveExplorerBase();
    const trimmed = path.startsWith('/') ? path.slice(1) : path;
    if (!trimmed) {
        return base;
    }
    return base === '/' ? `/${trimmed}` : `${base}${trimmed}`;
}

// Create a WebWorker for parsing FITS off the main thread
let fitsWorker = null;
if (window.Worker) {
    try {
        fitsWorker = new Worker(staticUrl('static/js/fits_worker.js'));
    } catch (err) {
        console.warn('Failed to create Worker', err);
        fitsWorker = null;
    }
}

console.log("searchInput:", searchInput);
console.log("resetButton:", resetButton);
console.log("returnButton:", returnButton);



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

        // use shared linearFit implementation
        const au = (typeof window !== 'undefined' && window.arrayUtils) ? window.arrayUtils : null;
        if (!au) throw new Error('arrayUtils is required by fits_preview.js but not available. Ensure shared/array_utils.js is loaded before this script.');
        fit = au.linearFit(x, samples, badpix);
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
        const sigma = au.std(goodPixels);
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
        const median = au.medianValue(samples);
        vmin = Math.max(vmin, median - (center_pixel - 1) * slope);
        vmax = Math.min(vmax, median + (npix - center_pixel) * slope);
    }
    console.timeLog("zscale", "updateMinMax");

    return { vmin, vmax };
}

function linearFit(x, y, badpix) {
    const au = (typeof window !== 'undefined' && window.arrayUtils) ? window.arrayUtils : null;
    if (!au) throw new Error('arrayUtils is required by fits_preview.js but not available.');
    return au.linearFit(x, y, badpix);
}

function std(arr) {
    const au = (typeof window !== 'undefined' && window.arrayUtils) ? window.arrayUtils : null;
    if (!au) throw new Error('arrayUtils is required by fits_preview.js but not available.');
    return au.std(arr);
}

function medianValue(arr) {
    const au = (typeof window !== 'undefined' && window.arrayUtils) ? window.arrayUtils : null;
    if (!au) throw new Error('arrayUtils is required by fits_preview.js but not available.');
    return au.medianValue(arr);
}

function quickSelect(arr, k) {
    const au = (typeof window !== 'undefined' && window.arrayUtils) ? window.arrayUtils : null;
    if (!au) throw new Error('arrayUtils is required by fits_preview.js but not available.');
    return au.quickSelect(arr, k);
}

function partition(arr, left, right) {
    const au = (typeof window !== 'undefined' && window.arrayUtils) ? window.arrayUtils : null;
    if (!au) throw new Error('arrayUtils is required by fits_preview.js but not available.');
    return au.partition(arr, left, right);
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


if (headerTab) {
    headerTab.addEventListener("click", () => {
        // Check the current display state of the header
        if (headerGridContainer.style.display === "grid") {
            // If it's currently displayed, hide it and change button text
            headerGridContainer.style.display = "none";
            headerTab.textContent = "Show Header"; // Change button text
        } else {
            // If it's currently hidden, show it and change button text
            headerGridContainer.style.display = "grid";
            headerTab.textContent = "Hide Header"; // Change button text
        }
        
        // Ensure the image grid container is displayed
        imageGridContainer.style.display = "grid";
    });
}

if (returnButton) {
    returnButton.addEventListener("click", () => {
        headerGridContainer.style.display = "grid";
        imageGridContainer.style.display = "grid";
    });
}

if (searchInput) {
    searchInput.addEventListener("input", () => {
        console.log("Search input changed to:", searchInput.value);
        const query = searchInput.value.toLowerCase();
        const filteredData = Object.fromEntries(
            Object.entries(headerData).filter(([key, value]) => {
                let valStr = "";
                let commentStr = "";
                // support new object shape {value, comment}
                if (value && typeof value === 'object' && ('value' in value || 'comment' in value)) {
                    valStr = value.value ? String(value.value).toLowerCase() : "";
                    commentStr = value.comment ? String(value.comment).toLowerCase() : "";
                } else {
                    // legacy: 'value/comment' string
                    const valueStr = value === null || value === undefined ? "" : String(value);
                    const parts = valueStr.split('/').map(s => s && s.trim().toLowerCase());
                    valStr = parts[0] || "";
                    commentStr = parts.slice(1).join('/') || "";
                }
                return (
                    key.toLowerCase().includes(query) ||
                    (valStr && valStr.includes(query)) ||
                    (commentStr && commentStr.includes(query))
                );
            })
        );
        displayHeaderTable(filteredData);
    });
}

if (resetButton) {
    resetButton.addEventListener("click", () => {
        searchInput.value = "";
        displayHeaderTable(headerData);
    });
}

const mutationObserver = new MutationObserver((mutationsList, observer) => {
    updateColor();
});

mutationObserver.observe(document.body, { childList: false, attributes: true })

function loadFits() {
    const filename = document.getElementById("fits-select").value;
    // URL-encode each path segment so nested directories (/) are preserved
    const encoded = filename.split('/').map(encodeURIComponent).join('/');
    const fitsUrl = `/fits/${encoded}`;  // Works for nested files
    renderMonochromeImage(fitsUrl);
}

function drawLineProfile(profileCtx, profileData, isHorizontal, offset) {
    // set canvas size
    profileCtx.clearRect(
        0,
        0,
        profileCtx.canvas.width,
        profileCtx.canvas.height
    );

    // Find max value for scaling
    const maxVal = Math.max(...profileData);

    // Draw background grid
    profileCtx.strokeStyle = gridColor;
    profileCtx.beginPath();
    if (isHorizontal) {
        for (let i = 0; i <= 5; i++) {
        const y = profileCtx.canvas.height * (1 - i / 5);
        profileCtx.moveTo(0, y);
        profileCtx.lineTo(profileCtx.canvas.width, y);
        }
    } else {
        for (let i = 0; i <= 5; i++) {
        const x = profileCtx.canvas.width * (1 - i / 5);
        profileCtx.moveTo(x, 0);
        profileCtx.lineTo(x, profileCtx.canvas.height);
        }
    }
    profileCtx.stroke();

    // Draw profile line
    profileCtx.strokeStyle = profileColor;
    profileCtx.beginPath();
    if (isHorizontal) {
        profileData.forEach((val, index) => {
        const x =
            ((index - offset) / (imageWidth / currentTransform.k)) *
            profileCtx.canvas.width;
        const y = profileCtx.canvas.height * (1 - val / maxVal);
        if (index === 0) {
            profileCtx.moveTo(x, y);
        } else {
            const prevY =
            profileCtx.canvas.height *
            (1 - profileData[index - 1] / maxVal);
            profileCtx.lineTo(x, prevY);
            profileCtx.lineTo(x, y);
        }
        
        // Draw the last point off-screen
        if (index == profileData.length - 1) {
            const nextX =
            ((index + 1 - offset) / (imageWidth / currentTransform.k)) *
            profileCtx.canvas.width;
            profileCtx.lineTo(nextX, y); 
        }
        });
    } else {
        profileData.forEach((val, index) => {
        const x = profileCtx.canvas.width * (1 - val / maxVal);
        const y =
            ((index - offset) / (imageHeight / currentTransform.k)) *
            profileCtx.canvas.height;
        if (index === 0) {
            profileCtx.moveTo(x, y);
        } else {
            const prevX =
            profileCtx.canvas.width * (1 - profileData[index - 1] / maxVal);
            profileCtx.lineTo(prevX, y);
            profileCtx.lineTo(x, y);
        }

        // Draw the last point off-screen
        if (index == profileData.length - 1) {
            const nextY =
            ((index + 1 - offset) / (imageHeight / currentTransform.k)) *
            profileCtx.canvas.height;
            profileCtx.lineTo(x, nextY); 
        }
        });
    }
    profileCtx.stroke();
    }

function displayHeaderTable(data) {
    headerTable.innerHTML = "";
    // table header
    const headerRow = document.createElement("tr");
    const keyHeader = document.createElement("th");
    const valueHeader = document.createElement("th");
    const commentHeader = document.createElement("th");
    keyHeader.textContent = "Key";
    valueHeader.textContent = "Value";
    commentHeader.textContent = "Comment";
    headerRow.appendChild(keyHeader);
    headerRow.appendChild(valueHeader);
    headerRow.appendChild(commentHeader);
    headerTable.appendChild(headerRow);

    for (const [key, value] of Object.entries(data)) {
        const row = document.createElement("tr");
        const keyCell = document.createElement("td");
        const valueCell = document.createElement("td");
        const commentCell = document.createElement("td");
        keyCell.textContent = key;
        // Support new object shape {value, comment} while remaining compatible with legacy strings
        if (value && typeof value === 'object' && ('value' in value || 'comment' in value)) {
            valueCell.textContent = value.value || "";
            commentCell.textContent = value.comment || "";
        } else {
            const valueStr = value === null || value === undefined ? "" : String(value);
            const parts = valueStr.split("/");
            valueCell.textContent = parts[0] || "";
            commentCell.textContent = parts.slice(1).join("/") || "";
        }
        row.appendChild(keyCell);
        row.appendChild(valueCell);
        row.appendChild(commentCell);
        headerTable.appendChild(row);
    }
    }

function renderFits(imageData, width, height) {
    const canvas = document.createElement("canvas");
    const ctx = canvas.getContext("2d");

    canvas.width = width;
    canvas.height = height;

    const imgData = ctx.createImageData(canvas.width, canvas.height);
    const data = imgData.data;

    for (let i = 0; i < imageData.length; i++) {
        const pixel = Math.round(imageData[i]); // Ensure pixel values are integers
        data[i * 4] = pixel; // Red
        data[i * 4 + 1] = pixel; // Green
        data[i * 4 + 2] = pixel; // Blue
        data[i * 4 + 3] = 255; // Alpha
    }

    ctx.putImageData(imgData, 0, 0);

    const viewer = document.getElementById("fits-viewer");
    viewer.innerHTML = "";
    viewer.appendChild(canvas);
}

function imageInteractionHandler(event, width, height) {
    const x = Math.floor((event.clientX - rect.left) * scaleX);
    const y = Math.floor((event.clientY - rect.top) * scaleY);

    // Apply the current transform to get the actual pixel coordinates
    const transformedX = Math.floor(
        (x - currentTransform.x * scaleX) / currentTransform.k
    );
    const transformedY = Math.floor(
        (y - currentTransform.y * scaleY) / currentTransform.k
    );

    // current x width and y height in terms of pixels in the image
    const xWidth = Math.ceil(imageWidth / currentTransform.k);
    const yHeight = Math.ceil(imageHeight / currentTransform.k);

    // left and top of the image in terms of pixels in the image
    const left = Math.floor(
        (-currentTransform.x * scaleX) / currentTransform.k
    );
    const top = Math.floor(
        (-currentTransform.y * scaleY) / currentTransform.k
    );

    if (
        transformedX >= 0 &&
        transformedX < width &&
        transformedY >= 0 &&
        transformedY < height
    ) {
        // Extract X and Y line profiles of region shown in the canvas
        const xProfile = imageData.slice(
        transformedY * width + left,
        transformedY * width + left + xWidth + 1
        );
        const yProfile = imageDataT[transformedX].slice(top, top + yHeight + 1);

        // Draw line profiles
        drawLineProfile(
        xProfileCtx,
        xProfile,
        true,
        (-currentTransform.x * scaleX) / currentTransform.k - left
        );
        drawLineProfile(
        yProfileCtx,
        yProfile,
        false,
        (-currentTransform.y * scaleY) / currentTransform.k - top
        );

        // Show pixel value
        const pixelValue = formatNumber(
        imageData[transformedY * width + transformedX],
        2
        );
        document.getElementById("pixelValue").innerText = `${pixelValue}`;
        document.getElementById(
        "pixelPosition"
        ).innerText = `${transformedX}, ${transformedY}`;
    }
    }        

/* Responsive resizing for FITS image and histogram panels */
window.addEventListener("resize", resizeImageAndProfiles);

function resizeImageAndProfiles() {
    // if (!imageWidth || !imageHeight) return;
    // // Calculate new scale factor based on container or window size
    // const container = document.getElementById("imageGridContainer");
    // const containerWidth = container.offsetWidth;
    // const containerHeight = container.offsetHeight;
    // // Leave some margin for profiles and info
    // const availableWidth = containerWidth - 120; // adjust as needed
    // const availableHeight = containerHeight - 120; // adjust as needed
    // scaleFactor = Math.min(
    //     availableWidth / imageWidth,
    //     availableHeight / imageHeight,
    //     1 // don't upscale beyond 1:1
    // );
    // canvas.style.width = `${imageWidth * scaleFactor}px`;
    // canvas.style.height = `${imageHeight * scaleFactor}px`;
    // // Set xProfile width to match image width, yProfile height to match image height
    // xProfileCanvas.width = imageWidth * scaleFactor;
    // xProfileCanvas.height = 100; // or a fixed height
    // yProfileCanvas.width = 100; // or a fixed width
    // yProfileCanvas.height = imageHeight * scaleFactor;
    // // Update rect and scales
    // rect = canvas.getBoundingClientRect();
    // scaleX = canvas.width / rect.width;
    // scaleY = canvas.height / rect.height;

    // xProfileCanvas.width = rect.width;
    // yProfileCanvas.height = rect.height;
}

async function renderMonochromeImage(fileUri) {
    // Clear previous error messages
    clearMessage();
    console.time("renderMonochromeImage");

    // Show spinner immediately
    if (spinner) spinner.style.display = 'grid';

    // Try loading a small thumbnail first for a quick preview
    try {
        const parts = fileUri.split('/').slice(2); // remove leading '' and 'fits'
        const path = parts.map(encodeURIComponent).join('/');
        const thumbResp = await fetch(`/thumbnail/${path}`);
        if (thumbResp.ok) {
            const blob = await thumbResp.blob();
            const img = new Image();
            img.src = URL.createObjectURL(blob);
            img.onload = () => {
                // draw thumbnail centered
                canvas.width = img.width;
                canvas.height = img.height;
                ctx.clearRect(0, 0, canvas.width, canvas.height);
                ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
            };
        }
    } catch (err) {
        console.warn('Thumbnail fetch failed', err);
    }

    // Step 1: Read the full FITS file with abort and timeout
    const controller = new AbortController();
    const timeoutMs = 20000; // 20s
    const timeout = setTimeout(() => {
        controller.abort();
    }, timeoutMs);

    let arrayBuffer;
    try {
        const response = await fetch(fileUri, { signal: controller.signal });
        clearTimeout(timeout);
        if (!response.ok) throw new Error(`Failed to load FITS: ${response.status}`);
        arrayBuffer = await response.arrayBuffer();
        console.timeLog("renderMonochromeImage", "FITS file loaded");
    } catch (err) {
        clearTimeout(timeout);
        console.error('Error loading FITS', err);
        showMessage('Failed to load FITS file (timeout or network error).');
        if (spinner) spinner.style.display = 'none';
        return;
    }

    // Step 2: Parse the FITS header and data (use worker if available)
    console.timeLog("renderMonochromeImage", "Data ready, parsing");
    if (fitsWorker) {
        const parsed = await new Promise((resolve, reject) => {
            const onMessage = (ev) => {
                const d = ev.data;
                if (d.type === 'result') {
                    fitsWorker.removeEventListener('message', onMessage);
                    resolve(d);
                } else if (d.type === 'error') {
                    fitsWorker.removeEventListener('message', onMessage);
                    reject(new Error(d.message));
                }
            };
            fitsWorker.addEventListener('message', onMessage);
            // Transfer the arrayBuffer to the worker to avoid copy
            try {
                fitsWorker.postMessage({type: 'parse', arrayBuffer: arrayBuffer}, [arrayBuffer]);
            } catch (err) {
                // If transfer fails, send without transfer
                fitsWorker.postMessage({type: 'parse', arrayBuffer: arrayBuffer});
            }
            // Timeout if worker doesn't respond
            setTimeout(() => reject(new Error('Worker parse timeout')), 15000);
        });

        headerData = parsed.header;
        normalizedData = parsed.normalizedData;
        imageWidth = parsed.width;
        imageHeight = parsed.height;
        imageData = parsed.imageData;
    } else {
        const dataView = new DataView(arrayBuffer);
        [headerData, normalizedData, imageWidth, imageHeight, imageData] = parseFITSImage(arrayBuffer, dataView);
    }

    displayHeaderTable(headerData);

    console.timeLog("renderMonochromeImage", "FITS header and data parsed");

    // Step 4: Precompute the transposed data for vertical profiles
    imageDataT = new Array(imageWidth);
    for (let x = 0; x < imageWidth; x++) {
        imageDataT[x] = new Array(imageHeight);
        for (let y = 0; y < imageHeight; y++) {
            imageDataT[x][y] = imageData[y * imageWidth + x];
        }
    }

    console.timeLog("renderMonochromeImage", "Data transposed");

    // Step 5: Compute the ImageData object for rendering
    const canvasData = new ImageData(imageWidth, imageHeight);
    const data = canvasData.data;
    for (let i = 0; i < normalizedData.length; i++) {
        const pixelValue = normalizedData[i];
        const index = i * 4;
        data[index] = data[index + 1] = data[index + 2] = pixelValue;
        data[index + 3] = 255;
    }
    console.timeLog("renderMonochromeImage", "ImageData object created");

    // Step 6: Render the image on the canvas
    canvas.width = imageWidth;
    canvas.height = imageHeight;

    offscreenCanvas = document.createElement("canvas");
    offscreenCanvas.width = imageWidth;
    offscreenCanvas.height = imageHeight;
    offscreenCtx = offscreenCanvas.getContext("2d");
    offscreenCtx.putImageData(canvasData, 0, 0);

    ctx.drawImage(offscreenCanvas, 0, 0);
    ctx.webkitImageSmoothingEnabled = false;
    ctx.mozImageSmoothingEnabled = false;
    ctx.imageSmoothingEnabled = false;

    // Responsive resize
    resizeImageAndProfiles();

    console.timeLog("renderMonochromeImage", "Image rendered");

    // rescale the canvas to fit the window
    scaleFactor = Math.min(
        window.innerWidth / imageWidth,
        window.innerHeight / imageHeight
    );
    canvas.style.width = `${imageWidth * scaleFactor - 100}px`;
    canvas.style.height = `${imageHeight * scaleFactor - 100}px`;

    spinner.style.display = "grid";
    mainContainer.style.display = "grid";

    rect = canvas.getBoundingClientRect();
    scaleX = canvas.width / rect.width;
    scaleY = canvas.height / rect.height;

    // set width and height of line profile canvases
    xProfileCanvas.width = rect.width;
    yProfileCanvas.height = rect.height;

    // set width of header-container to match the canvas width
    // document.querySelector(
    //   ".header-container"
    // ).style.width = `${rect.width}px`;

    // Add mousemove event listener to show pixel value and line profiles
    canvas.addEventListener("mousemove", (event) => {
        imageInteractionHandler(event, imageWidth, imageHeight);
    });

    console.timeEnd("renderMonochromeImage", "Image rendered finished");

    if (spinner) spinner.style.display = 'none';

}

function showMessage(msg) {
    let el = document.getElementById('fe-message');
    if (!el) {
        el = document.createElement('div');
        el.id = 'fe-message';
        el.style.position = 'absolute';
        el.style.top = '10px';
        el.style.left = '50%';
        el.style.transform = 'translateX(-50%)';
        el.style.background = 'rgba(0,0,0,0.7)';
        el.style.color = '#fff';
        el.style.padding = '8px 12px';
        el.style.borderRadius = '6px';
        el.style.zIndex = '2000';
        document.body.appendChild(el);
    }
    el.textContent = msg;
}

function clearMessage() {
    const el = document.getElementById('fe-message');
    if (el) el.remove();
}

console.time("loadData");
window.addEventListener("message", (event) => {
    const message = event.data;
    if (message.command === "loadData") {
    renderMonochromeImage(message.fileUri);
    setupZoom(); // Initialize zoom after rendering
    updateColor();
    console.timeEnd("loadData");
    }
});

// Load preview (thumbnail + header) and show a button to load full FITS
async function loadPreview(filePath) {
    // Show modal elements
    const modal = document.getElementById('fits-modal');
    const mainContainer = document.querySelector('.mainContainer');
    if (modal) {
        modal.classList.add('is-visible');
        modal.setAttribute('aria-hidden', 'false');
    }
    document.body.classList.add('fe-modal-open');
    if (mainContainer) {
        mainContainer.style.display = 'grid';
    }

    // Clear existing header table
    displayHeaderTable({});

    // Fetch header
    try {
        const encoded = filePath.split('/').map(encodeURIComponent).join('/');
        const hdrResp = await fetch(`/header/${encoded}`);
        if (hdrResp.ok) {
            const hdr = await hdrResp.json();
            displayHeaderTable(hdr);
        }
    } catch (err) {
        console.warn('Header fetch failed', err);
    }

    // Fetch thumbnail and draw it into canvas
    try {
        const encoded = filePath.split('/').map(encodeURIComponent).join('/');
        const tResp = await fetch(`/thumbnail/${encoded}`);
        if (tResp.ok) {
            const blob = await tResp.blob();
            const img = new Image();
            img.src = URL.createObjectURL(blob);
            img.onload = () => {
                // Fit thumbnail to canvas area
                canvas.width = img.width;
                canvas.height = img.height;
                ctx.clearRect(0, 0, canvas.width, canvas.height);
                ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
            };
        }
    } catch (err) {
        console.warn('Thumbnail fetch failed', err);
    }

    // Add a 'Load full FITS' button if not present
    let btn = document.getElementById('load-full-fits');
    if (!btn) {
        btn = document.createElement('button');
        btn.id = 'load-full-fits';
        btn.textContent = 'Load full FITS';
        btn.style.position = 'absolute';
        btn.style.top = '10px';
        btn.style.right = '10px';
        btn.style.zIndex = 1500;
        btn.className = 'px-3 py-2 bg-blue-600 rounded';
        document.body.appendChild(btn);
    }

    btn.onclick = () => {
        const fitsUrl = `/fits/${filePath.split('/').map(encodeURIComponent).join('/')}`;
        renderMonochromeImage(fitsUrl);
        // remove button after loading full
        btn.remove();
    };
}
