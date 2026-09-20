/**
 * Web Worker wire protocol. Requests and results are structured-cloneable:
 * reducers/filters cross the boundary as function SOURCE strings and are
 * compiled inside the worker (never eval'd as application statements).
 *
 * request:  { id, method, args }
 * result:   { id, result }
 * error:    { id, error: { name, message, code, detail } }
 */
export const METHODS = [
  'init',
  'registerReducer',
  'append',
  'appendStream',
  'rollback',
  'snapshot',
  'query',
  'getState',
  'getStateAt',
  'getHead',
  'listStreams',
  'verifyStream',
  'estimateQuota',
  'setQuotaMode',
  'compact',
  'get physicalSeq'
];

export function serializeError(error) {
  return {
    name: error && error.name ? error.name : 'Error',
    message: error && error.message ? error.message : String(error),
    code: error && error.code ? error.code : null,
    detail: error && error.detail ? error.detail : null
  };
}
