import { FITSViewer } from '../vendor/simple-fits-viewer/fits-viewer.js';
import { ViewerState, ViewerMode } from './state.js';
import {
    fetchPreviewFITS,
    fetchFullFITS,
    fetchHeaderData,
    fetchHduList,
} from './previewLoader.js';
import { parseFITS } from './parser.js';
import { setupToolbar, syncHeaderButton } from './toolbar.js';
import { rawFitsUrl } from './basePath.js';
import { isPngFile } from './fileTypes.js';

const state = new ViewerState();

const domRefs = {
    spinner: document.getElementById('fe-spinner'),
    message: document.getElementById('fe-message'),
    stage: document.getElementById('fe-stage'),
    viewerRoot: document.getElementById('fitsViewerRoot'),
    viewerToolbar: document.getElementById('viewerToolbar'),
    hduSelect: document.getElementById('hduSelect'),
    loadFullFitsButton: document.getElementById('loadFullFitsButton'),
    stretchButton: document.getElementById('stretchButton'),
    histogramButton: document.getElementById('histogramButton'),
    headerButton: document.getElementById('headerButton'),
    resetZoomButton: document.getElementById('resetZoomButton'),
    pngImage: document.getElementById('pngImage'),
};

const viewer = new FITSViewer(domRefs.viewerRoot, {
    // The explorer uses left/right for previous/next file and Escape to close
    keyboard: false,
    profileColor: 'rgba(96, 165, 250, 0.85)',
    gridColor: 'rgba(230, 238, 248, 0.35)',
});
window.__astraViewer = viewer;

let loadGeneration = 0;
let activeControllers = [];

setupToolbar({
    state,
    viewer,
    domRefs,
    onRequestFullLoad: ({ filePath }) => loadFullFITS(filePath),
    onRequestPreview: ({ filePath, hdu }) => loadPreview(filePath, { hdu }),
});
setupSwipeNavigation();

function beginNewSession() {
    for (const controller of activeControllers) {
        try {
            controller.abort();
        } catch (err) {
            // best effort cancellation
        }
    }
    activeControllers = [];
    clearMessage();
    viewer.closePanels();
    loadGeneration += 1;
    return loadGeneration;
}

// Called when the viewer closes, so downloads stop and no progress text stays
window.cancelViewerLoads = () => {
    beginNewSession();
    setSpinnerVisible(false);
};

function createSessionController() {
    const controller = new AbortController();
    activeControllers.push(controller);
    return controller;
}

function isActiveGeneration(gen) {
    return gen === loadGeneration;
}

function isAbortError(err) {
    if (!err) return false;
    if (err.name === 'AbortError') return true;
    const message = String(err.message || err);
    return /aborted/i.test(message);
}

function showMessage(text) {
    if (!domRefs.message) return;
    domRefs.message.textContent = text;
    domRefs.message.hidden = false;
}

function clearMessage() {
    if (domRefs.message) domRefs.message.hidden = true;
}

function setSpinnerVisible(visible) {
    if (!domRefs.spinner) return;
    domRefs.spinner.hidden = !visible;
    domRefs.spinner.setAttribute('aria-hidden', visible ? 'false' : 'true');
}

// FITS-only toolbar buttons are hidden for PNG files
function setMode(mode) {
    domRefs.viewerToolbar?.classList.toggle('png-mode', mode === 'png');
    domRefs.viewerRoot.hidden = mode === 'png';
    if (domRefs.pngImage) {
        domRefs.pngImage.hidden = mode !== 'png';
        if (mode !== 'png') {
            domRefs.pngImage.onload = null;
            domRefs.pngImage.onerror = null;
            domRefs.pngImage.removeAttribute('src');
        }
    }
}

function baseName(filePath) {
    return String(filePath).split('/').pop().replace(/\.[^.]+$/, '');
}

// Parse in the worker and hand the pixels to the viewer. `binning` > 1 marks a
// downsampled preview: the viewer then shows source pixel positions and turns
// star measurement off.
async function showFits(gen, arrayBuffer, filePath, binning) {
    const image = await parseFITS(arrayBuffer);
    if (!isActiveGeneration(gen)) return false;
    await viewer.loadImageData(image, baseName(filePath), { binning });
    // The preview's own header is rewritten (binned size, BITPIX); show the original
    const header = state.getHeader();
    if (header && Object.keys(header).length) viewer.setHeaderData(header);
    syncHeaderButton(domRefs.headerButton, viewer);
    return true;
}

export async function loadPreview(filePath, { hdu = null } = {}) {
    const gen = beginNewSession();
    setMode('fits');
    setSpinnerVisible(true);
    state.setFile(filePath, hdu);
    state.setHeader({});
    state.setHduList([]);
    state.transition(ViewerMode.PREVIEW_LOADING);

    try {
        const headerP = fetchHeaderData(filePath, {
            hdu,
            signal: createSessionController().signal,
        }).catch((err) => {
            if (!isAbortError(err)) console.warn('Failed to fetch header', err);
            return null;
        });
        const hduListP = fetchHduList(filePath, {
            signal: createSessionController().signal,
        }).catch((err) => {
            if (!isAbortError(err)) console.warn('Failed to fetch HDU list', err);
            return { items: [] };
        });
        const previewP = fetchPreviewFITS(filePath, {
            hdu,
            signal: createSessionController().signal,
        });

        const [header, hduList] = await Promise.all([headerP, hduListP]);
        if (!isActiveGeneration(gen)) return;
        if (header) state.setHeader(header);
        state.setHduList(hduList.items || []);

        const { arrayBuffer, stride } = await previewP;
        if (!isActiveGeneration(gen)) return;
        if (!(await showFits(gen, arrayBuffer, filePath, stride))) return;
        setSpinnerVisible(false);
        state.transition(ViewerMode.PREVIEW_READY);
    } catch (err) {
        if (!isActiveGeneration(gen) || isAbortError(err)) return;
        setSpinnerVisible(false);
        console.error('Preview load failed', err);
        showMessage(`Failed to load preview: ${err.message || err}`);
        state.transition(ViewerMode.ERROR, { error: err });
    }
}

// Abort a full download only when no data arrives for this long, so large
// files on slow links can still finish.
const FULL_LOAD_STALL_TIMEOUT_MS = 20000;

function formatDownloadProgress(received, total) {
    const mb = (bytes) => (bytes / 1e6).toFixed(1);
    if (total > 0) {
        const percent = Math.floor((received / total) * 100);
        return `Downloading full resolution… ${percent}% (${mb(received)} / ${mb(total)} MB)`;
    }
    return `Downloading full resolution… ${mb(received)} MB`;
}

async function loadFullFITS(filePath) {
    const gen = beginNewSession();
    setMode('fits');
    state.transition(ViewerMode.FULL_LOADING);

    const controller = createSessionController();
    let stalled = false;
    let stallTimer = null;
    const resetStallTimer = () => {
        clearTimeout(stallTimer);
        stallTimer = setTimeout(() => {
            stalled = true;
            controller.abort();
        }, FULL_LOAD_STALL_TIMEOUT_MS);
    };
    resetStallTimer();

    try {
        const arrayBuffer = await fetchFullFITS(filePath, {
            signal: controller.signal,
            onProgress: (received, total) => {
                resetStallTimer();
                if (isActiveGeneration(gen)) showMessage(formatDownloadProgress(received, total));
            },
        });
        clearTimeout(stallTimer);
        if (!isActiveGeneration(gen)) return;
        showMessage('Preparing full resolution…');
        if (!(await showFits(gen, arrayBuffer, filePath, 1))) return;
        clearMessage();
        state.transition(ViewerMode.FULL_READY);
    } catch (err) {
        clearTimeout(stallTimer);
        if (!isActiveGeneration(gen)) return;
        if (isAbortError(err) && !stalled) return;
        console.error('Full FITS load failed', err);
        showMessage(stalled ? 'Full resolution download stalled' : 'Failed to load full resolution');
        state.transition(ViewerMode.ERROR, { error: err });
    }
}

async function loadPng(filePath) {
    const gen = beginNewSession();
    setMode('png');
    state.setFile(filePath, null);
    state.setHeader({});
    state.setHduList([]);
    state.transition(ViewerMode.PNG_LOADING);

    if (!domRefs.pngImage) {
        state.transition(ViewerMode.PNG_READY);
        return;
    }

    setSpinnerVisible(true);
    await new Promise((resolve) => {
        domRefs.pngImage.onload = () => {
            if (isActiveGeneration(gen)) {
                setSpinnerVisible(false);
                state.transition(ViewerMode.PNG_READY);
            }
            resolve();
        };
        domRefs.pngImage.onerror = () => {
            if (isActiveGeneration(gen)) {
                setSpinnerVisible(false);
                showMessage('Failed to load PNG');
                state.transition(ViewerMode.ERROR, { error: new Error('Failed to load PNG') });
            }
            resolve();
        };
        domRefs.pngImage.src = rawFitsUrl(filePath);
    });
}

// Touch: swipe left/right on the image for the next/previous file, when the
// image is not zoomed in (then a swipe pans instead)
function setupSwipeNavigation() {
    const stage = domRefs.stage;
    if (!stage) return;
    const touches = new Set(); // touch pointers currently down
    let start = null; // the single-finger touch that may become a swipe
    stage.addEventListener('pointerdown', (e) => {
        if (e.pointerType !== 'touch') return;
        touches.add(e.pointerId);
        start = touches.size === 1
            ? { x: e.clientX, y: e.clientY, t: performance.now(), id: e.pointerId }
            : null; // a second finger: pinch, not a swipe
    });
    // On window: the finger can lift over something else (e.g. the context menu)
    const onEnd = (e) => {
        if (e.pointerType !== 'touch') return;
        touches.delete(e.pointerId);
        const s = start;
        if (!s || e.pointerId !== s.id) return;
        start = null;
        if (e.type !== 'pointerup' || viewer.zoomLevel > 1 || viewer.headerVisible) return;
        const dx = e.clientX - s.x;
        const dy = e.clientY - s.y;
        if (Math.abs(dx) > 60 && Math.abs(dy) < 50 && performance.now() - s.t < 700) {
            window.navigateToOffset?.(dx < 0 ? 1 : -1);
        }
    };
    window.addEventListener('pointerup', onEnd);
    window.addEventListener('pointercancel', onEnd);
}

export async function openViewerFile(filePath) {
    if (isPngFile(filePath)) {
        await loadPng(filePath);
        return;
    }
    await loadPreview(filePath);
}

window.openViewerFile = openViewerFile;

window.addEventListener('message', (event) => {
    const { command, fileUri } = event.data || {};
    if (command === 'loadData' && fileUri) {
        loadPreview(fileUri);
    }
});

window.loadPreview = loadPreview;
