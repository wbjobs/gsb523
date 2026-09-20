import { EventStore } from './event-store.js';
import { IndexedDbStorage } from './indexeddb-storage.js';
import { MemoryStorage, OverflowStorage } from './memory-storage.js';

export async function createEventStore(options = {}) {
  const fallback = new MemoryStorage({
    name: 'memory-overflow',
    quotaBytes: options.memoryQuotaBytes ?? Infinity,
  });

  if (options.storage === 'memory') {
    const primary = new MemoryStorage({
      name: 'memory',
      quotaBytes: options.quotaBytes ?? Infinity,
    });
    return new EventStore(primary, options);
  }

  const primary = new IndexedDbStorage(options.dbName || 'event-sourcing-db');
  await primary.open();
  const storage = new OverflowStorage(primary, fallback);
  return new EventStore(storage, options);
}
