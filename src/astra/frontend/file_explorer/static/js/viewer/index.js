import { ViewerState, ViewerMode } from './state.js';
import { initRenderer } from './renderer.js';
import { initDualSlider } from './dualSlider.js';
import {
    fetchPreviewFITS,
    fetchFullFITS,
    fetchHeaderData,
    fetchHduList,
} from './previewLoader.js';
import { setupInteractions } from './interactions.js';
import { rawFitsUrl } from './basePath.js';
import { isPngFile } from './fileTypes.js';

const state = new ViewerState();

const domRefs = {
    spinner: document.getElementById('spinner'),
    canvas: document.getElementById('loadedImage'),
    xProfileCanvas: document.getElementById('xProfile'),
    yProfileCanvas: document.getElementById('yProfile'),
    mainContainer: document.querySelector('.mainContainer'),
    headerGridContainer: document.getElementById('headerGridContainer'),
    imageGridContainer: document.getElementById('imageGridContainer'),
    headerTable: document.getElementById('headerTable'),
    searchInput: document.getElementById('searchInput'),
    resetButton: document.getElementById('resetButton'),
    pixelValueEl: document.getElementById('pixelValue'),
    pixelPositionEl: document.getElementById('pixelPosition'),
    viewerToolbar: document.getElementById('viewerToolbar'),
    hduSelect: document.getElementById('hduSelect'),
    stretchMin: document.getElementById('stretchMin'),
    stretchMax: document.getElementById('stretchMax'),
    stretchGamma: document.getElementById('stretchGamma'),
    stretchMinValue: document.getElementById('stretchMinValue'),
    stretchMaxValue: document.getElementById('stretchMaxValue'),
    stretchGammaValue: document.getElementById('stretchGammaValue'),
    loadFullFitsButton: document.getElementById('loadFullFitsButton'),
    logToggleButton: document.getElementById('logToggleButton'),
    stretchPanelToggle: document.getElementById('stretchPanelToggle'),
    pngImage: document.getElementById('pngImage'),
};

let loadGeneration = 0;
let activeControllers = [];

const renderer = initRenderer(domRefs, state);
window.__astraRenderer = renderer;

// initialize dual-slider
try {
    const dualEl = document.getElementById('dualRange');
    const dual = initDualSlider(dualEl, domRefs.stretchMin, domRefs.stretchMax);
    if (dualEl) dualEl._dual = dual;
    if (dual && typeof dual.set === 'function') {
        dual.set(domRefs.stretchMin.value, domRefs.stretchMax.value);
    }
} catch (err) {
    console.warn('Dual slider init failed', err);
}

setupInteractions({
    state,
    renderer,
    domRefs,
    onRequestFullLoad: ({ filePath }) => loadFullFITS(filePath),
    onRequestPreview: ({ filePath, hdu }) => loadPreview(filePath, { hdu }),
});

function beginNewSession() {
    for (const controller of activeControllers) {
        try {
            controller.abort();
        } catch (err) {
            // best effort cancellation
        }
    }
    activeControllers = [];
    loadGeneration += 1;
    return loadGeneration;
}

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

function setToolbarMode(mode) {
    if (!domRefs.viewerToolbar) return;
    domRefs.viewerToolbar.classList.toggle('png-mode', mode === 'png');
}

function setFitsControlsEnabled(enabled) {
    if (domRefs.loadFullFitsButton) domRefs.loadFullFitsButton.disabled = !enabled;
    if (domRefs.logToggleButton) domRefs.logToggleButton.disabled = !enabled;
    if (domRefs.stretchPanelToggle) domRefs.stretchPanelToggle.disabled = !enabled;
    if (domRefs.stretchMin) domRefs.stretchMin.disabled = !enabled;
    if (domRefs.stretchMax) domRefs.stretchMax.disabled = !enabled;
    if (domRefs.stretchGamma) domRefs.stretchGamma.disabled = !enabled;
}

function applyFitsLayout() {
    if (window.__astraRenderer && typeof window.__astraRenderer.enableRender === 'function') {
        window.__astraRenderer.enableRender();
    }
    if (domRefs.pngImage) {
        domRefs.pngImage.onload = null;
        domRefs.pngImage.onerror = null;
        domRefs.pngImage.style.display = 'none';
        domRefs.pngImage.src = '';
    }
    if (domRefs.imageGridContainer) {
        domRefs.imageGridContainer.style.display = '';
    }
    if (domRefs.headerGridContainer) {
        domRefs.headerGridContainer.style.display = 'grid';
    }
    if (domRefs.viewerToolbar) {
        domRefs.viewerToolbar.style.display = 'block';
    }
    setToolbarMode('fits');
    if (domRefs.hduSelect) {
        domRefs.hduSelect.disabled = false;
        const block = domRefs.hduSelect.closest('.hdu-block');
        if (block) block.style.display = '';
    }
    setFitsControlsEnabled(true);
}

function applyPngLayout() {
    if (window.__astraRenderer && typeof window.__astraRenderer.cancelPending === 'function') {
        window.__astraRenderer.cancelPending();
    }
    if (domRefs.imageGridContainer) {
        domRefs.imageGridContainer.style.display = 'none';
    }
    if (domRefs.headerGridContainer) {
        domRefs.headerGridContainer.style.display = 'none';
    }
    if (domRefs.viewerToolbar) {
        domRefs.viewerToolbar.style.display = 'block';
    }
    setToolbarMode('png');
    if (domRefs.hduSelect) {
        domRefs.hduSelect.disabled = true;
        domRefs.hduSelect.innerHTML = '';
        const block = domRefs.hduSelect.closest('.hdu-block');
        if (block) block.style.display = 'none';
    }
    setFitsControlsEnabled(false);
}

function setSpinnerVisible(visible) {
    if (!domRefs.spinner) return;
    domRefs.spinner.style.display = visible ? 'grid' : 'none';
    domRefs.spinner.setAttribute('aria-hidden', visible ? 'false' : 'true');
}

export async function loadPreview(filePath, { hdu = null } = {}) {
    const gen = beginNewSession();
    applyFitsLayout();
    state.setFile(filePath, hdu);
    state.setHeader({});
    state.setHduList([]);
    state.transition(ViewerMode.PREVIEW_LOADING);

    try {
        const headerController = createSessionController();
        const hduListController = createSessionController();
        const previewController = createSessionController();

        const headerP = fetchHeaderData(filePath, {
            hdu,
            signal: headerController.signal,
        }).catch((err) => {
            if (!isAbortError(err)) {
                console.warn('Failed to fetch header', err);
            }
            return null;
        });

        const hduListP = fetchHduList(filePath, {
            signal: hduListController.signal,
        }).catch((err) => {
            if (!isAbortError(err)) {
                console.warn('Failed to fetch HDU list', err);
            }
            return { items: [] };
        });

        const previewP = fetchPreviewFITS(filePath, {
            hdu,
            signal: previewController.signal,
        });

        const header = await headerP;
        const hduList = await hduListP;

        if (!isActiveGeneration(gen)) return;

        if (header) state.setHeader(header);
        state.setHduList(hduList.items || []);

        try {
            const arrayBuffer = await previewP;
            if (!isActiveGeneration(gen)) return;
            await renderer.renderFromArrayBuffer(arrayBuffer);
            if (!isActiveGeneration(gen)) return;
            state.transition(ViewerMode.PREVIEW_READY);
            if (renderer && typeof renderer.forceResize === 'function') {
                renderer.forceResize();
            }
        } catch (imgErr) {
            if (!isActiveGeneration(gen)) return;
            if (isAbortError(imgErr)) return;
            console.error('Preview image/render failed', imgErr);
            const errEl = document.getElementById('fe-error');
            if (errEl) {
                errEl.innerText = `Preview image failed: ${imgErr.message || imgErr}`;
                errEl.style.display = 'block';
            }
            state.transition(ViewerMode.ERROR, { error: imgErr });
        }
    } catch (err) {
        if (!isActiveGeneration(gen)) return;
        if (isAbortError(err)) return;
        renderer.showMessage('Failed to load preview');
        console.error('Preview load failed', err);
        const errEl = document.getElementById('fe-error');
        if (errEl) {
            errEl.innerText = `Failed to load preview: ${err.message || err}`;
            errEl.style.display = 'block';
        }
        state.transition(ViewerMode.ERROR, { error: err });
    }
}

async function loadFullFITS(filePath) {
    const gen = beginNewSession();
    applyFitsLayout();
    state.transition(ViewerMode.FULL_LOADING);

    const controller = createSessionController();
    const timeout = setTimeout(() => controller.abort(), 20000);

    try {
        const arrayBuffer = await fetchFullFITS(filePath, {
            signal: controller.signal,
        });
        if (!isActiveGeneration(gen)) return;
        clearTimeout(timeout);
        await renderer.renderFromArrayBuffer(arrayBuffer);
        if (!isActiveGeneration(gen)) return;
        state.transition(ViewerMode.FULL_READY);
        if (renderer && typeof renderer.forceResize === 'function') {
            renderer.forceResize();
        }
    } catch (err) {
        clearTimeout(timeout);
        if (!isActiveGeneration(gen)) return;
        if (isAbortError(err)) return;
        console.error('Full FITS load failed', err);
        renderer.showMessage('Failed to load full FITS');
        state.transition(ViewerMode.ERROR, { error: err });
    }
}

async function loadPng(filePath) {
    const gen = beginNewSession();
    applyPngLayout();
    state.setFile(filePath, null);
    state.setHeader({});
    state.setHduList([]);
    state.transition(ViewerMode.PNG_LOADING);

    if (!domRefs.pngImage) {
        state.transition(ViewerMode.PNG_READY);
        return;
    }

    setSpinnerVisible(true);
    const src = rawFitsUrl(filePath);

    await new Promise((resolve) => {
        domRefs.pngImage.onload = () => {
            if (!isActiveGeneration(gen)) {
                resolve();
                return;
            }
            domRefs.pngImage.style.display = 'block';
            setSpinnerVisible(false);
            state.transition(ViewerMode.PNG_READY);
            resolve();
        };

        domRefs.pngImage.onerror = () => {
            if (!isActiveGeneration(gen)) {
                resolve();
                return;
            }
            setSpinnerVisible(false);
            state.transition(ViewerMode.ERROR, {
                error: new Error('Failed to load PNG preview'),
            });
            resolve();
        };

        domRefs.pngImage.src = src;
    });
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
