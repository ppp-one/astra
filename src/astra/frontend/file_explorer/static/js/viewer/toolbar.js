// Viewer toolbar: file actions (HD, HDU) and buttons for the image viewer's tools.
import { ViewerMode } from './state.js';
import { isPngFile } from './fileTypes.js';

export function setupToolbar({ state, viewer, domRefs, onRequestFullLoad, onRequestPreview }) {
    const {
        hduSelect,
        loadFullFitsButton,
        stretchButton,
        histogramButton,
        headerButton,
        resetZoomButton,
    } = domRefs;

    state.addEventListener('modechange', (event) => {
        const { mode } = event.detail;
        if (!loadFullFitsButton) return;
        if (mode === ViewerMode.PREVIEW_READY) {
            setFullLoadButton(loadFullFitsButton, 'bi-badge-hd', 'Load full resolution (measure stars)', false);
        } else if (mode === ViewerMode.FULL_LOADING) {
            setFullLoadButton(loadFullFitsButton, 'bi-hourglass-split', 'Loading full resolution...', true);
        } else if (mode === ViewerMode.FULL_READY) {
            setFullLoadButton(loadFullFitsButton, 'bi-arrow-clockwise', 'Reload full resolution', false);
        } else if (mode === ViewerMode.ERROR) {
            const isPng = isPngFile(state.getFile()?.filePath || '');
            setFullLoadButton(
                loadFullFitsButton,
                'bi-exclamation-triangle',
                isPng ? 'Full resolution is only available for FITS files' : 'Retry loading full resolution',
                isPng
            );
        } else if (mode === ViewerMode.PNG_LOADING || mode === ViewerMode.PNG_READY || mode === ViewerMode.IDLE) {
            setFullLoadButton(loadFullFitsButton, 'bi-badge-hd', 'Full resolution is only available for FITS files', true);
        } else if (mode === ViewerMode.PREVIEW_LOADING) {
            setFullLoadButton(loadFullFitsButton, 'bi-badge-hd', 'Load full resolution (measure stars)', true);
        }
    });

    loadFullFitsButton?.addEventListener('click', () => {
        const { filePath, hdu } = state.getFile();
        if (!filePath) return;
        onRequestFullLoad({ filePath, hdu });
    });

    hduSelect?.addEventListener('change', () => {
        const { filePath, hdu: current } = state.getFile();
        if (!filePath) return;
        const nextHdu = hduSelect.value === '' ? null : Number(hduSelect.value);
        if (current === nextHdu) return;
        onRequestPreview({ filePath, hdu: nextHdu });
    });

    state.addEventListener('hdulistchange', (event) => {
        populateHduSelect(hduSelect, event.detail.items || [], state.getFile().hdu);
    });

    state.addEventListener('filechange', () => {
        if (hduSelect) hduSelect.disabled = true;
        updateFilePathBanner(state.getFile()?.filePath);
    });

    stretchButton?.addEventListener('click', () => viewer.toggleZScalePanel());
    histogramButton?.addEventListener('click', () => viewer.toggleHistogramPanel());
    resetZoomButton?.addEventListener('click', () => viewer.resetView());
    headerButton?.addEventListener('click', () => {
        viewer.toggleHeader();
        syncHeaderButton(headerButton, viewer);
    });
    // The viewer's own "header >" / "< image" links also switch views
    viewer.container.addEventListener('click', () => syncHeaderButton(headerButton, viewer));
}

export function syncHeaderButton(headerButton, viewer) {
    if (!headerButton) return;
    const showing = viewer.headerVisible;
    headerButton.setAttribute('aria-pressed', showing ? 'true' : 'false');
    headerButton.title = showing ? 'Show image' : 'Show FITS header';
}

function setFullLoadButton(button, icon, title, disabled) {
    button.disabled = disabled;
    button.innerHTML = `<i class="bi ${icon}" aria-hidden="true"></i>`;
    button.title = title;
    button.setAttribute('aria-label', title);
}

function populateHduSelect(hduSelect, items = [], selected) {
    if (!hduSelect) return;
    hduSelect.innerHTML = '';
    items.forEach((item) => {
        const option = document.createElement('option');
        option.value = String(item.index);
        option.textContent = formatHduLabel(item);
        if (selected === item.index) option.selected = true;
        hduSelect.appendChild(option);
    });
    hduSelect.disabled = items.length <= 1;
    // Only worth the space when there is a choice
    const block = hduSelect.closest('.hdu-block');
    if (block) block.hidden = items.length <= 1;
}

function formatHduLabel(item) {
    const parts = [`#${item.index}`];
    if (item.name) parts.push(item.name);
    if (item.shape && item.shape.length) parts.push(`(${item.shape.join('×')})`);
    return parts.join(' ');
}

function updateFilePathBanner(filePath) {
    const banner = document.getElementById('filePathBanner');
    if (!banner) return;
    if (!filePath) {
        banner.textContent = 'No file loaded';
        banner.title = 'No file loaded';
        banner.classList.add('is-empty');
        return;
    }
    banner.textContent = filePath;
    banner.title = filePath;
    banner.classList.remove('is-empty');
}
