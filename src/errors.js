export class StoreError extends Error {
  constructor(message, code) {
    super(message);
    this.name = 'StoreError';
    this.code = code;
  }
}

export class VersionConflictError extends StoreError {
  constructor(stream, expected, actual) {
    super(
      `version conflict on stream "${stream}": expected ${expected}, actual ${actual}`,
      'VERSION_CONFLICT'
    );
    this.name = 'VersionConflictError';
    this.expected = expected;
    this.actual = actual;
  }
}

export class QuotaExceededError extends StoreError {
  constructor(message, detail) {
    super(message, 'QUOTA_EXCEEDED');
    this.name = 'QuotaExceededError';
    this.detail = detail || {};
  }
}

export class SnapshotCorruptError extends StoreError {
  constructor(snapshotId) {
    super(`snapshot ${snapshotId} failed checksum verification`, 'SNAPSHOT_CORRUPT');
    this.name = 'SnapshotCorruptError';
    this.snapshotId = snapshotId;
  }
}

export class IntegrityError extends StoreError {
  constructor(message) {
    super(message, 'INTEGRITY_FAILURE');
    this.name = 'IntegrityError';
  }
}

export class ReadOnlyError extends StoreError {
  constructor(message) {
    super(message || 'store is in read-only degraded mode', 'READ_ONLY');
    this.name = 'ReadOnlyError';
  }
}
