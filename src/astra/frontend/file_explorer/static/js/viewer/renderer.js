import { staticUrl } from './basePath.js';

let spinner;
let canvas;
let ctx;
let xProfileCanvas;
let xProfileCtx;
let yProfileCanvas;
let yProfileCtx;
let mainContainer;
let pixelValueEl;
let pixelPositionEl;
let viewerToolbar;

let offscreenCanvas;
let offscreenCtx;
let imageWidth;
let imageHeight;
let currentTransform = d3.zoomIdentity;
let imageData = null;
let imageDataT = null;
let resizeObserver = null;
let imageGridContainer = null;
let rect = null;
let scaleX = null;
let scaleY = null;
let profileColor = '#ffffff';
let gridColor = '#e0e0e0';

let fitsWorker = null;
let zoomInstance = null;
let themeObserver = null;
let mouseMoveAttached = false;
let stateRef = null;
let resizeDebounceTimer = null;
let resizePending = false;

let stretchSettings = {
    min: null,
    max: null,
    gamma: 1.0,
    mode: 'linear',
};

let stretchDefaults = {
    min: null,
    max: null,
};

let currentStats = null;
let latestCanvasData = null;
let xPixSize = 1;
let yPixSize = 1;
let physicalAspect = 1; // (physical height / physical width)
const PROFILE_HIDE_THRESHOLD = 600; // px, container width below which profiles auto-hide

export function initRenderer(domRefs, state) {
    spinner = domRefs.spinner;
    canvas = domRefs.canvas;
    ctx = canvas.getContext('2d');
    xProfileCanvas = domRefs.xProfileCanvas;
    xProfileCtx = xProfileCanvas.getContext('2d');
    yProfileCanvas = domRefs.yProfileCanvas;
    yProfileCtx = yProfileCanvas.getContext('2d');
    mainContainer = domRefs.mainContainer;
    pixelValueEl = domRefs.pixelValueEl;
    pixelPositionEl = domRefs.pixelPositionEl;
    viewerToolbar = domRefs.viewerToolbar;
    imageGridContainer = domRefs.imageGridContainer;
    stateRef = state;

    fitsWorker = initWorker();
    observeTheme();
    window.addEventListener('resize', resizeImageAndProfiles);
    if (imageGridContainer && !resizeObserver) {
        resizeObserver = new ResizeObserver(() => {
            // Debounce to prevent cascading resize loops
            if (resizeDebounceTimer) clearTimeout(resizeDebounceTimer);
            resizeDebounceTimer = setTimeout(() => {
                resizeImageAndProfiles();
            }, 16); // ~one frame delay
        });
        resizeObserver.observe(imageGridContainer);
    }

    return {
        async renderFromArrayBuffer(arrayBuffer, { source = 'preview' } = {}) {
            clearMessage();
            toggleSpinner(true);
            try {
                await renderArrayBuffer(arrayBuffer, { source });
            } finally {
                toggleSpinner(false);
            }
        },
        updateStretchSettings(partial) {
            updateStretch(partial);
        },
        updateTheme: updateColor,
        showMessage,
        clearMessage,
        forceResize(retries = 4) {
            const attempt = (remaining) => {
                const success = resizeImageAndProfiles();
                if (!success && remaining > 0) {
                    window.requestAnimationFrame(() => attempt(remaining - 1));
                }
            };
            window.requestAnimationFrame(() => attempt(retries));
        },
    };
}

function initWorker() {
    if (!window.Worker) {
        console.warn('WebWorker unsupported; falling back to main thread parsing.');
        return null;
    }
    try {
        return new Worker(staticUrl('js/fits_worker.js'));
    } catch (err) {
        console.warn('Failed to initialize worker', err);
        return null;
    }
}

async function renderArrayBuffer(arrayBuffer, { source }) {
    mainContainer.style.display = 'grid';

    // Explicitly clear canvas to remove previous image immediately
    if (canvas && ctx) {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
    }

    const parsed = fitsWorker
        ? await parseWithWorker(arrayBuffer)
        : parseOnMainThread(arrayBuffer);

    imageWidth = parsed.width;
    imageHeight = parsed.height;
    imageData = parsed.imageData;
    // Pixel size extraction for physical aspect ratio (fallback 1)
    if (parsed.header) {
        const xp = parseFloat(parsed.header['XPIXSZ']);
        const yp = parseFloat(parsed.header['YPIXSZ']);
        if (Number.isFinite(xp) && xp > 0) xPixSize = xp; else xPixSize = 1;
        if (Number.isFinite(yp) && yp > 0) yPixSize = yp; else yPixSize = 1;
    }
    physicalAspect = (imageHeight * yPixSize) / (imageWidth * xPixSize);

    imageDataT = new Array(imageWidth);
    for (let x = 0; x < imageWidth; x++) {
        imageDataT[x] = new Array(imageHeight);
        for (let y = 0; y < imageHeight; y++) {
            imageDataT[x][y] = imageData[y * imageWidth + x];
        }
    }

    currentStats = computeStats(imageData);
    stretchDefaults = {
        min: currentStats.autoMin,
        max: currentStats.autoMax,
    };
    stretchSettings = {
        ...stretchSettings,
        min: stretchDefaults.min,
        max: stretchDefaults.max,
    };

    if (stateRef && typeof stateRef.setStats === 'function') {
        stateRef.setStats({
            dataMin: currentStats.dataMin,
            dataMax: currentStats.dataMax,
            defaultMin: stretchDefaults.min,
            defaultMax: stretchDefaults.max,
        });
    }

    applyStretchAndRender({ resetGeometry: true });
}

function updateStretch(partial) {
    stretchSettings = {
        ...stretchSettings,
        ...partial,
    };
    applyStretchAndRender({ resetGeometry: false });
}

function applyStretchAndRender({ resetGeometry }) {
    if (!imageData || !canvas) return;

    const { min, max } = ensureStretchBounds();
    const displayBuffer = buildDisplayBuffer(imageData, {
        min,
        max,
        gamma: stretchSettings.gamma || 1,
        mode: stretchSettings.mode || 'linear',
    });
    latestCanvasData = displayBuffer;

    if (resetGeometry || !offscreenCanvas) {
        canvas.width = imageWidth;
        canvas.height = imageHeight;
        offscreenCanvas = document.createElement('canvas');
        offscreenCanvas.width = imageWidth;
        offscreenCanvas.height = imageHeight;
        offscreenCtx = offscreenCanvas.getContext('2d');
    }

    const canvasData = new ImageData(imageWidth, imageHeight);
    const data = canvasData.data;
    for (let i = 0; i < displayBuffer.length; i++) {
        const pixelValue = displayBuffer[i];
        const index = i * 4;
        data[index] = data[index + 1] = data[index + 2] = pixelValue;
        data[index + 3] = 255;
    }

    offscreenCtx.putImageData(canvasData, 0, 0);
    if (zoomInstance) {
        d3.select(canvas).call(zoomInstance.transform, currentTransform);
    } else {
        ctx.drawImage(offscreenCanvas, 0, 0);
    }
    ctx.imageSmoothingEnabled = false;

    if (resetGeometry) {
        // Single deferred resize call after layout settles
        if (!resizePending) {
            resizePending = true;
            window.requestAnimationFrame(() => {
                resizeImageAndProfiles();
                resizePending = false;
            });
        }
        if (spinner) spinner.style.display = 'grid';
        rect = canvas.getBoundingClientRect();
        scaleX = canvas.width / rect.width;
        scaleY = canvas.height / rect.height;
        xProfileCanvas.width = rect.width;
        yProfileCanvas.height = rect.height;
        setupZoom();
        if (!mouseMoveAttached) {
            canvas.addEventListener('mousemove', (event) => {
                imageInteractionHandler(event, imageWidth, imageHeight);
            });
            mouseMoveAttached = true;
        }
        if (viewerToolbar) viewerToolbar.style.display = 'block';
    }
}

function ensureStretchBounds() {
    if (!currentStats) {
        return { min: 0, max: 1 };
    }
    const dataMin = currentStats.dataMin;
    const dataMax = currentStats.dataMax;
    const epsilon = Math.max((dataMax - dataMin) / 1000, 1e-6);

    let min = typeof stretchSettings.min === 'number' ? stretchSettings.min : stretchDefaults.min;
    let max = typeof stretchSettings.max === 'number' ? stretchSettings.max : stretchDefaults.max;

    if (!Number.isFinite(min)) min = dataMin;
    if (!Number.isFinite(max)) max = dataMax;

    if (min >= max - epsilon) {
        min = max - epsilon;
    }
    if (max <= min + epsilon) {
        max = min + epsilon;
    }

    stretchSettings.min = min;
    stretchSettings.max = max;
    return { min, max };
}

function buildDisplayBuffer(data, { min, max, gamma, mode }) {
    const length = data.length;
    const buffer = new Uint8ClampedArray(length);
    const range = max - min || 1;
    const invGamma = 1 / Math.max(gamma, 0.01);

    for (let i = 0; i < length; i++) {
        let norm = (data[i] - min) / range;
        norm = Math.max(0, Math.min(1, norm));
        if (mode === 'log') {
            norm = Math.log10(9 * norm + 1);
        }
        norm = Math.pow(norm, invGamma);
        buffer[i] = Math.round(norm * 255);
    }
    return buffer;
}

function computeStats(data) {
    let dataMin = Infinity;
    let dataMax = -Infinity;
    for (let i = 0; i < data.length; i++) {
        const val = data[i];
        if (!Number.isFinite(val)) continue;
        if (val < dataMin) dataMin = val;
        if (val > dataMax) dataMax = val;
    }
    if (!Number.isFinite(dataMin)) dataMin = 0;
    if (!Number.isFinite(dataMax)) dataMax = 1;
    const auto = zscale(data);
    return {
        dataMin,
        dataMax,
        autoMin: Number.isFinite(auto.vmin) ? auto.vmin : dataMin,
        autoMax: Number.isFinite(auto.vmax) ? auto.vmax : dataMax,
    };
}

function parseWithWorker(arrayBuffer) {
    return new Promise((resolve, reject) => {
        let timeoutId;
        const onMessage = (ev) => {
            const d = ev.data;
            if (d.type === 'result') {
                fitsWorker.removeEventListener('message', onMessage);
                clearTimeout(timeoutId);
                resolve(d);
            } else if (d.type === 'error') {
                fitsWorker.removeEventListener('message', onMessage);
                clearTimeout(timeoutId);
                reject(new Error(d.message));
            }
        };
        fitsWorker.addEventListener('message', onMessage);
        try {
            fitsWorker.postMessage({ type: 'parse', arrayBuffer }, [arrayBuffer]);
        } catch (err) {
            fitsWorker.postMessage({ type: 'parse', arrayBuffer });
        }
        timeoutId = setTimeout(() => {
            fitsWorker.removeEventListener('message', onMessage);
            reject(new Error('Worker parse timeout'));
        }, 15000);
    });
}

function parseOnMainThread(arrayBuffer) {
    const dataView = new DataView(arrayBuffer);
    const [header, normalized, width, height, raw] = parseFITSImage(arrayBuffer, dataView);
    return {
        header,
        normalizedData: normalized,
        width,
        height,
        imageData: raw,
    };
}

function drawLineProfile(profileCtx, profileData, isHorizontal, offset) {
    // Skip drawing if profiles hidden
    if (!profileCtx || profileCtx.canvas.style.display === 'none') return;
    profileCtx.clearRect(0, 0, profileCtx.canvas.width, profileCtx.canvas.height);
    const maxVal = Math.max(...profileData);

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

    profileCtx.strokeStyle = profileColor;
    profileCtx.beginPath();
    if (isHorizontal) {
        profileData.forEach((val, index) => {
            const x = ((index - offset) / (imageWidth / currentTransform.k)) * profileCtx.canvas.width;
            const y = profileCtx.canvas.height * (1 - val / maxVal);
            if (index === 0) {
                profileCtx.moveTo(x, y);
            } else {
                const prevY = profileCtx.canvas.height * (1 - profileData[index - 1] / maxVal);
                profileCtx.lineTo(x, prevY);
                profileCtx.lineTo(x, y);
            }
            if (index === profileData.length - 1) {
                const nextX = ((index + 1 - offset) / (imageWidth / currentTransform.k)) * profileCtx.canvas.width;
                profileCtx.lineTo(nextX, y);
            }
        });
    } else {
        profileData.forEach((val, index) => {
            const x = profileCtx.canvas.width * (1 - val / maxVal);
            const y = ((index - offset) / (imageHeight / currentTransform.k)) * profileCtx.canvas.height;
            if (index === 0) {
                profileCtx.moveTo(x, y);
            } else {
                const prevX = profileCtx.canvas.width * (1 - profileData[index - 1] / maxVal);
                profileCtx.lineTo(prevX, y);
                profileCtx.lineTo(x, y);
            }
            if (index === profileData.length - 1) {
                const nextY = ((index + 1 - offset) / (imageHeight / currentTransform.k)) * profileCtx.canvas.height;
                profileCtx.lineTo(x, nextY);
            }
        });
    }
    profileCtx.stroke();
}

function imageInteractionHandler(event, width, height) {
    if (!rect) return;
    const x = Math.floor((event.clientX - rect.left) * scaleX);
        const y = Math.floor((event.clientY - rect.top) * scaleY);

    const transformedX = Math.floor((x - currentTransform.x * scaleX) / currentTransform.k);
    const transformedY = Math.floor((y - currentTransform.y * scaleY) / currentTransform.k);

    const xWidth = Math.ceil(imageWidth / currentTransform.k);
    const yHeight = Math.ceil(imageHeight / currentTransform.k);

    const left = Math.floor((-currentTransform.x * scaleX) / currentTransform.k);
    const top = Math.floor((-currentTransform.y * scaleY) / currentTransform.k);

    if (transformedX < 0 || transformedX >= width || transformedY < 0 || transformedY >= height) {
        return;
    }

    const xProfile = imageData.slice(
        transformedY * width + left,
        transformedY * width + left + xWidth + 1
    );
    const yProfile = imageDataT[transformedX].slice(top, top + yHeight + 1);

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

    if (pixelValueEl) {
        const pixelValue = formatNumber(imageData[transformedY * width + transformedX], 2);
        pixelValueEl.innerText = `${pixelValue}`;
    }
    if (pixelPositionEl) {
        pixelPositionEl.innerText = `${transformedX}, ${transformedY}`;
    }
}

function resizeImageAndProfiles() {
    if (!canvas || !imageWidth || !imageHeight) return false;
    if (imageGridContainer) {
        // Use parent/modal container width to avoid feedback from our own CSS variables
        let containerWidth = 0;
        const modal = imageGridContainer.closest('.relative');
        if (modal && modal.clientWidth) {
            containerWidth = modal.clientWidth - 64; // Account for modal padding
        } else if (mainContainer && mainContainer.clientWidth) {
            containerWidth = mainContainer.clientWidth;
        } else {
            containerWidth = Math.max(160, (window.innerWidth || 0) - 64);
        }
        if (!containerWidth || containerWidth <= 0) {
            return false;
        }
        const profileWidth = computeProfileWidth(containerWidth);
        applyProfileWidthCSS(profileWidth);
        updateProfileVisibility(profileWidth);
        const aspect = physicalAspect; // use physical aspect (height/width)
        const viewportWidth = window.innerWidth || document.documentElement.clientWidth || containerWidth;
        const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 800;
        const widthCap = Math.max(160, viewportWidth - 80 - profileWidth);
        const baseWidth = Math.max(1, containerWidth - profileWidth);
        let displayW = Math.min(baseWidth, widthCap);
        displayW = Math.max(120, displayW);
        let displayH = displayW * aspect;
        const heightCap = Math.max(160, viewportHeight - 240);
        if (displayH > heightCap) {
            const s = heightCap / displayH;
            displayH = heightCap;
            displayW = Math.max(120, Math.round(displayW * s));
        }
        const roundedW = Math.round(displayW);
        const roundedH = Math.round(displayH);
        canvas.style.width = `${roundedW}px`;
        canvas.style.height = `${roundedH}px`;
        // Disconnect observer to prevent resize loop when updating CSS variables
        if (resizeObserver) resizeObserver.disconnect();
        imageGridContainer.style.setProperty('--image-col-width', `${roundedW}px`);
        // Reconnect observer after CSS update
        if (resizeObserver) resizeObserver.observe(imageGridContainer);
    }
    rect = canvas.getBoundingClientRect();
    scaleX = canvas.width / rect.width;
    scaleY = canvas.height / rect.height;
    // Profiles track displayed size exactly
    xProfileCanvas.style.width = `${rect.width}px`;
    xProfileCanvas.width = rect.width;
    xProfileCanvas.style.height = `${Math.max(40, Math.round(rect.height * 0.18))}px`;
    yProfileCanvas.style.height = `${rect.height}px`;
    yProfileCanvas.height = rect.height;
    yProfileCanvas.style.width = getComputedStyle(imageGridContainer).getPropertyValue('--profile-width');
    return true;
}

function computeProfileWidth(totalWidth) {
    // Dynamic profile width: 15% of container, clamped
    if (totalWidth < PROFILE_HIDE_THRESHOLD) return 0; // auto-hide below threshold
    const minW = 80;
    const maxW = 160;
    const proposed = Math.round(totalWidth * 0.15);
    return Math.max(minW, Math.min(maxW, proposed));
}

function applyProfileWidthCSS(w) {
    if (!imageGridContainer) return;
    // Disconnect observer to prevent resize loop when updating CSS variables
    if (resizeObserver) resizeObserver.disconnect();
    imageGridContainer.style.setProperty('--profile-width', `${w}px`);
    // Reconnect observer after CSS update
    if (resizeObserver) resizeObserver.observe(imageGridContainer);
}

function updateProfileVisibility(profileWidth) {
    const hide = profileWidth === 0;
    if (hide) {
        yProfileCanvas.style.display = 'none';
        xProfileCanvas.style.display = 'none';
    } else {
        yProfileCanvas.style.display = 'block';
        xProfileCanvas.style.display = 'block';
    }
}

function setupZoom() {
    if (!canvas) return;
    if (!zoomInstance) {
        zoomInstance = d3
            .zoom()
            .scaleExtent([1, 100])
            .on('zoom', (event) => {
                    let transform = event.transform;
                    // compute bounds so the image fully covers the canvas after transform
                    const k = transform.k;
                    const clamp = (v, a, b) => Math.max(a, Math.min(b, v));

                    // allowed transform.x range (so left <= 0 and right >= canvas.width)
                    let minX = (canvas.width - imageWidth * k) / scaleX; // when right edge aligns with canvas right
                    let maxX = 0; // when left edge aligns with canvas left
                    let minY = (canvas.height - imageHeight * k) / scaleY;
                    let maxY = 0;

                    // If the image is smaller than the canvas in a dimension, center it and disallow pan
                    if (imageWidth * k <= canvas.width) {
                        const centeredX = (canvas.width - imageWidth * k) / 2 / scaleX;
                        minX = maxX = centeredX;
                    }
                    if (imageHeight * k <= canvas.height) {
                        const centeredY = (canvas.height - imageHeight * k) / 2 / scaleY;
                        minY = maxY = centeredY;
                    }

                    const clampedX = clamp(transform.x, minX, maxX);
                    const clampedY = clamp(transform.y, minY, maxY);

                    if (clampedX !== transform.x || clampedY !== transform.y) {
                        // apply clamped transform back to zoom behavior (this will re-enter zoom handler once)
                        const ct = d3.zoomIdentity.translate(clampedX, clampedY).scale(k);
                        d3.select(canvas).call(zoomInstance.transform, ct);
                        return;
                    }

                    currentTransform = transform;

                    ctx.clearRect(0, 0, canvas.width, canvas.height);
                    ctx.save();
                    ctx.translate(transform.x * scaleX, transform.y * scaleY);
                    ctx.scale(transform.k, transform.k);
                    ctx.drawImage(offscreenCanvas, 0, 0, imageWidth, imageHeight);
                    ctx.restore();
                });
        d3.select(canvas).call(zoomInstance);
    }
    d3.select(canvas).call(zoomInstance.transform, d3.zoomIdentity);
    currentTransform = d3.zoomIdentity;
}

function observeTheme() {
    if (themeObserver) return;
    themeObserver = new MutationObserver(() => updateColor());
    themeObserver.observe(document.body, { childList: false, attributes: true });
    updateColor();
}

function updateColor() {
    const styles = getComputedStyle(document.documentElement);
    const editorCompositionBorder = styles.getPropertyValue('--vscode-editor-compositionBorder') || '#ffffff';
    const editorForeground = styles.getPropertyValue('--vscode-editor-foreground') || '#e0e0e0';
    profileColor = editorCompositionBorder.trim() || '#ffffff';
    gridColor = editorForeground.trim() || '#e0e0e0';
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

function toggleSpinner(isVisible) {
    if (spinner) {
        spinner.style.display = isVisible ? 'grid' : 'none';
        spinner.setAttribute('aria-hidden', (!isVisible).toString());
    }
    if (mainContainer) {
        mainContainer.setAttribute('aria-busy', isVisible ? 'true' : 'false');
    }
}

function parseFITSImage(arrayBuffer, dataView) {
    let headerText = '';
    let offset = 0;
    const headerSize = 2880;
    while (true) {
        const block = new TextDecoder().decode(
            arrayBuffer.slice(offset, offset + headerSize)
        );
        headerText += block;
        offset += headerSize;
        if (block.trim().endsWith('END')) break;
    }

    const headerLines = headerText.match(/.{1,80}/g);
    const header = {};
    for (const line of headerLines) {
        const keyword = line.substring(0, 8).trim();
        const value = line.substring(10, 80).trim();
        if (keyword === 'END') break;
        header[keyword] = value;
    }

    const width = parseInt(header['NAXIS1'], 10);
    const height = parseInt(header['NAXIS2'], 10);
    const bitpix = parseInt(header['BITPIX'], 10);
    const bscale = parseFloat(header['BSCALE']) || 1;
    const bzero = parseFloat(header['BZERO']) || 0;

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

    const { vmin, vmax } = zscale(data);
    const scale = 255 / (vmax - vmin);
    const _offset = -vmin * scale;
    const normalized = new Float32Array(data.length);
    for (let i = 0; i < data.length; i++) {
        normalized[i] = data[i] * scale + _offset;
    }

    return [header, normalized, width, height, data];
}

function zscale(values, n_samples = 1000, contrast = 0.25, max_reject = 0.5, min_npixels = 5, krej = 2.5, max_iterations = 5) {
    const stride = Math.max(1, Math.floor(values.length / n_samples));
    const samples = [];
    for (let i = 0; i < values.length && samples.length < n_samples; i += stride) {
        samples.push(values[i]);
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
    const badpix = new Array(npix).fill(false);
    const minpix = Math.max(min_npixels, Math.floor(npix * max_reject));
    let fit = { slope: 0, intercept: 0 };

    for (let iter = 0; iter < max_iterations; iter++) {
        if (ngoodpix >= last_ngoodpix || ngoodpix < minpix) break;
        const au = (typeof window !== 'undefined' && window.arrayUtils) ? window.arrayUtils : null;
        if (!au) throw new Error('arrayUtils is required by renderer.js but not available.');
        fit = au.linearFit(x, samples, badpix);
        const flat = new Array(npix);
        for (let i = 0; i < npix; i++) {
            flat[i] = samples[i] - (fit.slope * x[i] + fit.intercept);
        }
        const goodPixels = [];
        for (let i = 0; i < npix; i++) {
            if (!badpix[i]) goodPixels.push(flat[i]);
        }
        const sigma = au.std(goodPixels);
        const threshold = krej * sigma;
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

    if (ngoodpix >= minpix) {
        let slope = fit.slope;
        if (contrast > 0) {
            slope = slope / contrast;
        }
        const center_pixel = Math.floor((npix - 1) / 2);
        const au = (typeof window !== 'undefined' && window.arrayUtils) ? window.arrayUtils : null;
        const median = au ? au.medianValue(samples) : medianValue(samples);
        vmin = Math.max(vmin, median - (center_pixel - 1) * slope);
        vmax = Math.min(vmax, median + (npix - center_pixel) * slope);
    }

    return { vmin, vmax };
}

function medianValue(arr) {
    const au = (typeof window !== 'undefined' && window.arrayUtils) ? window.arrayUtils : null;
    if (!au) throw new Error('arrayUtils is required by renderer.js but not available.');
    return au.medianValue(arr);
}


function formatNumber(num, precision) {
    if (Math.floor(num) === num) {
        return num;
    }
    return num.toFixed(precision);
}
