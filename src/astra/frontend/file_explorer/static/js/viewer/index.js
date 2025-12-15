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
};

const renderer = initRenderer(domRefs, state);
window.__astraRenderer = renderer;
// initialize dual-slider
try {
    const dualEl = document.getElementById('dualRange');
    const dual = initDualSlider(dualEl, domRefs.stretchMin, domRefs.stretchMax);
    // store instance on DOM element so other modules can access it
    if (dualEl) dualEl._dual = dual;
    // sync initial positions
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
    onRequestFullLoad: ({ filePath, hdu }) => loadFullFITS(filePath, hdu),
    onRequestPreview: ({ filePath, hdu }) => loadPreview(filePath, { hdu }),
});

export async function loadPreview(filePath, { hdu = null } = {}) {
    state.setFile(filePath, hdu);
    state.transition(ViewerMode.PREVIEW_LOADING);
    try {
        // Fetch header/hdu metadata first so we can show useful info even if
        // image parsing fails. Preview FITS is fetched concurrently but we
        // apply header data as soon as it arrives.
        const headerP = fetchHeaderData(filePath, { hdu });
        const hduListP = fetchHduList(filePath);
        const previewP = fetchPreviewFITS(filePath, { hdu });

        const header = await headerP.catch((err) => {
            console.warn('Failed to fetch header', err);
            return null;
        });
        const hduList = await hduListP.catch((err) => {
            console.warn('Failed to fetch HDU list', err);
            return { items: [] };
        });

        if (header) state.setHeader(header);
        state.setHduList(hduList.items || []);

        try {
            const arrayBuffer = await previewP;
            await renderer.renderFromArrayBuffer(arrayBuffer, { source: 'preview' });
            state.transition(ViewerMode.PREVIEW_READY);
            if (renderer && typeof renderer.forceResize === 'function') {
                renderer.forceResize();
            }
        } catch (imgErr) {
            // Show a non-blocking error panel but keep the header visible
            console.error('Preview image/render failed', imgErr);
            const errEl = document.getElementById('fe-error');
            if (errEl) {
                errEl.innerText = `Preview image failed: ${imgErr.message || imgErr}`;
                errEl.style.display = 'block';
            }
            state.transition(ViewerMode.ERROR, { error: imgErr });
        }
    } catch (err) {
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

async function loadFullFITS(filePath, hdu = null) {
    state.transition(ViewerMode.FULL_LOADING);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);
    try {
        const arrayBuffer = await fetchFullFITS(filePath, { signal: controller.signal });
        clearTimeout(timeout);
        await renderer.renderFromArrayBuffer(arrayBuffer, { source: 'full' });
        state.transition(ViewerMode.FULL_READY);
        if (renderer && typeof renderer.forceResize === 'function') {
            renderer.forceResize();
        }
    } catch (err) {
        clearTimeout(timeout);
        console.error('Full FITS load failed', err);
        renderer.showMessage('Failed to load full FITS');
        state.transition(ViewerMode.ERROR, { error: err });
    }
}

window.addEventListener('message', (event) => {
    const { command, fileUri } = event.data || {};
    if (command === 'loadData' && fileUri) {
        loadPreview(fileUri);
    }
});

window.loadPreview = loadPreview;
