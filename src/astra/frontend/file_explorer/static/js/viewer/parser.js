// Parse FITS data in a web worker, so large images do not freeze the page.
// Without worker support, fall back to the viewer library's parser.
import { staticUrl } from './basePath.js';
import { parseFITSImage } from '../vendor/simple-fits-viewer/utils.js';

const PARSE_TIMEOUT_MS = 60000;

let worker = null;
let nextId = 0;

function getWorker() {
    if (worker === null) {
        try {
            worker = window.Worker ? new Worker(staticUrl('js/fits_worker.js')) : false;
        } catch (err) {
            console.warn('FITS worker unavailable; parsing on the main thread', err);
            worker = false;
        }
    }
    return worker;
}

/**
 * @param {ArrayBuffer} arrayBuffer - Transferred to the worker (unusable afterwards).
 * @returns {Promise<{header: object, width: number, height: number, data: ArrayLike<number>,
 *   dataMin: number, dataMax: number}>}
 */
export function parseFITS(arrayBuffer) {
    const w = getWorker();
    if (!w) {
        const [header, width, height, data, dataMin, dataMax] = parseFITSImage(arrayBuffer, new DataView(arrayBuffer));
        return Promise.resolve({ header, width, height, data, dataMin, dataMax });
    }
    const id = ++nextId;
    return new Promise((resolve, reject) => {
        const finish = () => {
            clearTimeout(timer);
            w.removeEventListener('message', onMessage);
        };
        const onMessage = (event) => {
            const msg = event.data || {};
            if (msg.id !== id) return;
            finish();
            if (msg.type === 'result') {
                resolve({
                    header: msg.header,
                    width: msg.width,
                    height: msg.height,
                    data: msg.imageData,
                    dataMin: msg.dataMin,
                    dataMax: msg.dataMax,
                });
            } else {
                reject(new Error(msg.message || 'FITS parse failed'));
            }
        };
        const timer = setTimeout(() => {
            finish();
            reject(new Error('FITS parse timed out'));
        }, PARSE_TIMEOUT_MS);
        w.addEventListener('message', onMessage);
        w.postMessage({ type: 'parse', id, arrayBuffer }, [arrayBuffer]);
    });
}
