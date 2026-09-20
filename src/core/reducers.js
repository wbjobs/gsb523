export function createDefaultReducer() {
  return (state = {}, event) => {
    const entityId = event.entityId;

    if (event.type === 'counter.incremented') {
      state.entities = state.entities || {};
      state.entities.counter = state.entities.counter || {
        id: 'counter',
        count: 0,
      };
      state.entities.counter.count += event.payload.delta ?? 1;
      state.entities.counter.updatedAtVersion = event.version;
      return state;
    }

    if (!entityId) return state;

    state.entities = state.entities || {};

    if (event.type === 'entity.deleted') {
      delete state.entities[entityId];
    } else {
      state.entities[entityId] = {
        ...(state.entities[entityId] || { id: entityId }),
        ...event.payload,
        id: entityId,
        updatedAtVersion: event.version,
      };
    }

    return state;
  };
}

export function resolveReducer(reducers, streamId) {
  return reducers.get(streamId) || reducers.get('*') || createDefaultReducer();
}
