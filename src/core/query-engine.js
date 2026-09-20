export class QueryEngine {
  constructor(store) {
    this.store = store;
  }

  async query(request) {
    if (request.kind === 'events') return this.events(request);
    if (request.kind === 'state') return this.state(request);
    throw new Error(`Unsupported query kind: ${request.kind}`);
  }

  events(request) {
    const { kind, ...filters } = request;
    return this.store.queryEvents(filters);
  }

  async state(request) {
    const {
      streamId,
      atVersion,
      atEventId,
      selector,
      includeMeta = false,
    } = request;
    const result = await this.store.materialize(streamId, {
      atVersion,
      atEventId,
    });

    const state = applySelector(result.state, selector);
    return includeMeta
      ? { ...result, state }
      : { state, version: result.version, eventId: result.eventId };
  }
}

function applySelector(state, selector) {
  if (!selector) return state;

  if (typeof selector === 'function') return selector(state);

  if (selector.entityId) {
    return state.entities?.[selector.entityId] || null;
  }

  if (selector.path) {
    return selector.path.split('.').reduce((value, part) => value?.[part], state);
  }

  return state;
}
