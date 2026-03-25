export const ViewerMode = Object.freeze({
    IDLE: 'idle',
    PNG_LOADING: 'png-loading',
    PNG_READY: 'png-ready',
    PREVIEW_LOADING: 'preview-loading',
    PREVIEW_READY: 'preview-ready',
    FULL_LOADING: 'full-loading',
    FULL_READY: 'full-ready',
    ERROR: 'error'
});

export class ViewerState extends EventTarget {
    constructor() {
        super();
        this.mode = ViewerMode.IDLE;
        this.filePath = null;
        this.hdu = null;
        this.header = {};
        this.error = null;
        this.stats = null;
    this.hduList = [];
    }

    setFile(filePath, hdu = null) {
        this.filePath = filePath;
        this.hdu = hdu;
        this.dispatchEvent(
            new CustomEvent('filechange', { detail: { filePath, hdu } })
        );
    }

    setHeader(header) {
        this.header = header || {};
        this.dispatchEvent(
            new CustomEvent('headerchange', { detail: { header: this.header } })
        );
    }

    setStats(stats) {
        this.stats = stats;
        this.dispatchEvent(
            new CustomEvent('statschange', { detail: { stats } })
        );
    }

    setHduList(items = []) {
        this.hduList = items;
        this.dispatchEvent(
            new CustomEvent('hdulistchange', { detail: { items } })
        );
    }

    transition(mode, payload = {}) {
        this.mode = mode;
        if ('error' in payload) {
            this.error = payload.error;
        } else {
            this.error = null;
        }
        this.dispatchEvent(
            new CustomEvent('modechange', { detail: { mode, payload } })
        );
    }

    getHeader() {
        return this.header;
    }

    getFile() {
        return { filePath: this.filePath, hdu: this.hdu };
    }

    getStats() {
        return this.stats;
    }

    getHduList() {
        return this.hduList;
    }
}
