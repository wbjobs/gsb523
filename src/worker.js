import { attachWorker } from './worker-server.js';

if (typeof self !== 'undefined' && typeof postMessage === 'function') {
  attachWorker(self);
}
