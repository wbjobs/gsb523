export class KeyedMutex {
  constructor() {
    this.locks = new Map();
  }

  async run(key, task) {
    while (this.locks.has(key)) {
      await this.locks.get(key);
    }

    let release;
    const lock = new Promise((resolve) => {
      release = resolve;
    });
    this.locks.set(key, lock);

    try {
      return await task();
    } finally {
      this.locks.delete(key);
      release();
    }
  }
}
