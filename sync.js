/**
 * @typedef {import("./merge.js").Rec} Rec
 * @typedef {import("./store.js").NoteStore} NoteStore
 *
 * @typedef {object} Transport
 * @property {(since: number, limit: number) => Promise<PullResponse>} pull
 * @property {(records: Rec[]) => Promise<{ cursor: number }>} push
 *
 * @typedef {object} PullResponse
 * @property {Rec[]} records
 * @property {number} cursor
 * @property {boolean} hasMore
 * @property {boolean} [fullResync] Server has GC'd past our cursor.
 * @property {boolean} [complete] This response is a full snapshot from seq 0.
 */

/**
 * Sync client.
 *
 * Three properties, each of which exists because of a specific failure:
 *
 * **Resumable.** The cursor advances and is persisted after *every applied
 * batch*, not at the end of the run. Sync of a large mailbox on a train that
 * goes into a tunnel resumes from the last batch rather than starting over.
 * Persisting only at the end means a client on a flaky connection can make
 * literally zero forward progress, forever, and it looks like a hang.
 *
 * **Interruptible.** An AbortSignal is threaded through, and `stop()` takes
 * effect between batches. A sync loop you cannot cancel keeps a phone's radio
 * awake after the user has closed the app.
 *
 * **Idempotent.** Every operation is a merge, and merge is idempotent, so
 * at-least-once delivery is sufficient. A push that times out *after* the
 * server committed it can simply be retried — no dedupe table, no request IDs,
 * no exactly-once machinery. This falls directly out of the CRDT choice: the
 * reason the protocol can be this dumb is that the data type absorbs the
 * duplicates. If the operations were "append" or "increment", none of this
 * would work.
 *
 * Push-before-pull is deliberate. Pulling first would mean merging the server's
 * view, marking records dirty as a result, and pushing back a superset — one
 * extra round trip on every cycle.
 */
export class SyncEngine {
  /**
   * @param {object} opts
   * @param {NoteStore} opts.store
   * @param {Transport} opts.transport
   * @param {number} [opts.cursor] Restored server cursor.
   * @param {number} [opts.batchSize]
   * @param {(state: SyncState) => void} [opts.onState]
   * @param {() => void} [opts.onPersist] Called when durable state should be saved.
   */
  constructor({ store, transport, cursor = 0, batchSize = 100, onState, onPersist }) {
    this.store = store;
    this.transport = transport;
    this.cursor = cursor;
    this.batchSize = batchSize;
    this.onState = onState ?? (() => {});
    this.onPersist = onPersist ?? (() => {});

    /** @type {SyncState} */
    this.state = { phase: "idle", error: null, lastSyncAt: null, pulled: 0, pushed: 0 };
    this.running = false;
    /** @type {any} */
    this.timer = null;
    this.aborted = false;
    /** Serializes cycles so a timer tick cannot overlap a manual sync. */
    this.inFlight = null;
  }

  /** @param {Partial<SyncState>} patch */
  setState(patch) {
    this.state = { ...this.state, ...patch };
    this.onState(this.state);
  }

  /**
   * Run one full cycle. Concurrent calls share the in-flight promise rather
   * than starting a second cycle — two overlapping syncs would both push the
   * same dirty set and race on clearing it.
   */
  async syncOnce() {
    if (this.inFlight) return this.inFlight;
    this.inFlight = this._cycle().finally(() => {
      this.inFlight = null;
    });
    return this.inFlight;
  }

  async _cycle() {
    this.aborted = false;
    try {
      this.setState({ phase: "pushing", error: null });
      const pushed = await this.pushAll();

      if (this.aborted) {
        this.setState({ phase: "idle" });
        return { pushed, pulled: 0, aborted: true };
      }

      this.setState({ phase: "pulling" });
      const pulled = await this.pullAll();

      this.setState({
        phase: "idle",
        error: null,
        lastSyncAt: Date.now(),
        pushed: this.state.pushed + pushed,
        pulled: this.state.pulled + pulled,
      });
      return { pushed, pulled, aborted: this.aborted };
    } catch (err) {
      this.setState({
        phase: "error",
        error: err instanceof Error ? err.message : String(err),
      });
      throw err;
    }
  }

  /** Push every locally-modified record, in batches. */
  async pushAll() {
    let total = 0;
    for (;;) {
      if (this.aborted) break;
      const pending = this.store.pending();
      if (pending.length === 0) break;

      const batch = pending.slice(0, this.batchSize);
      await this.transport.push(batch.map((p) => p.rec));

      // Clear only the exact (id, localSeq) pairs that were sent. Anything the
      // user edited while this request was in flight has a higher localSeq and
      // stays dirty for the next round.
      this.store.clearDirty(batch.map((p) => ({ id: p.rec.id, seq: p.seq })));
      this.onPersist();
      total += batch.length;

      if (batch.length < this.batchSize) break;
    }
    return total;
  }

  /** Pull until caught up, persisting the cursor after each batch. */
  async pullAll() {
    let total = 0;
    for (;;) {
      if (this.aborted) break;
      const res = await this.transport.pull(this.cursor, this.batchSize);

      if (res.fullResync) {
        total += await this.fullResync();
        break;
      }

      if (res.records.length > 0) {
        this.store.merge(res.records, { fromServer: true });
        total += res.records.length;
      }

      // Advance and persist per batch. This is the line that makes an
      // interrupted sync resumable rather than restartable.
      this.cursor = res.cursor;
      this.onPersist();

      if (!res.hasMore) break;
    }
    return total;
  }

  /**
   * Full resync.
   *
   * Triggered when our cursor is older than the server's GC floor — meaning
   * tombstones we never saw have already been collected, so an incremental pull
   * can no longer tell us what was deleted.
   *
   * Deletion must therefore be inferred from *absence*: any record we have that
   * the server's complete snapshot does not contain, and which we know had
   * previously round-tripped through the server, was deleted while we were
   * away. Records that never reached the server are kept — they are unsynced
   * local work, and absence says nothing about them.
   *
   * The exception is dirty records. If the user edited a note while offline and
   * the server deleted it in the meantime, we keep the edit and re-push it.
   * That is a product judgement, not a correctness one: silently destroying
   * work the user did with their own hands is worse than resurrecting a note
   * they can delete again.
   */
  async fullResync() {
    let cursor = 0;
    let total = 0;
    /** @type {Set<string>} */
    const seen = new Set();

    for (;;) {
      const res = await this.transport.pull(cursor, this.batchSize);
      for (const rec of res.records) seen.add(rec.id);
      if (res.records.length > 0) {
        this.store.merge(res.records, { fromServer: true });
        total += res.records.length;
      }
      cursor = res.cursor;
      if (!res.hasMore) break;
      if (this.aborted) return total;
    }

    for (const id of [...this.store.records.keys()]) {
      if (seen.has(id)) continue;
      if (this.store.dirty.has(id)) continue; // unsynced local work: keep
      if (!this.store.syncedIds.has(id)) continue; // never reached the server: keep
      this.store.hardDelete(id);
    }

    this.cursor = cursor;
    this.onPersist();
    return total;
  }

  /** @param {number} intervalMs */
  start(intervalMs = 4000) {
    if (this.running) return;
    this.running = true;
    const tick = async () => {
      if (!this.running) return;
      try {
        await this.syncOnce();
      } catch {
        // Transport failures are expected and already surfaced via state.
        // Offline is the normal case in a local-first app, not an exception.
      }
      if (this.running) this.timer = setTimeout(tick, intervalMs);
    };
    this.timer = setTimeout(tick, 0);
  }

  stop() {
    this.running = false;
    this.aborted = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }
}

/**
 * @typedef {object} SyncState
 * @property {"idle"|"pushing"|"pulling"|"error"} phase
 * @property {string|null} error
 * @property {number|null} lastSyncAt
 * @property {number} pulled
 * @property {number} pushed
 */
