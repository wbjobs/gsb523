export class ConcurrencyConflictError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'ConcurrencyConflictError';
    this.code = 'CONCURRENCY_CONFLICT';
    this.details = details;
  }
}

export class QuotaExhaustedError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'QuotaExhaustedError';
    this.code = 'QUOTA_EXHAUSTED';
    this.details = details;
  }
}

export function isQuotaError(error) {
  return Boolean(
    error &&
      (error.name === 'QuotaExceededError' ||
        error.code === 22 ||
        error.code === 'QUOTA_EXHAUSTED')
  );
}

export function serializeError(error) {
  return {
    name: error?.name || 'Error',
    code: error?.code || null,
    message: error?.message || String(error),
    details: error?.details || {},
  };
}
