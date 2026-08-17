import { mergeRecord, recEqual, cloneRec, isDeleted } from "../core/merge.js";

/**
 * @typedef {import("../core/merge.js").Rec} Rec
 */

/**
 * Server-side store and changefeed.
 *
 * The server is *not* an authority on conflicts — it runs the identical merge
 * function the clients do, so it is really just a well-connected replica that
 * happens to have a stable address. What it uniquely provides is a **total
 * order over deliveries**: every accepted change gets a monotonically
 * increasing `seq`, which is what makes an incremental cursor possible at all.
 *
 * Note the two orderings are separate and do different jobs:
 *   - Lamport timestamps decide *which write wins* (conflict resolution).
 *   - Server seq decides *what a client still needs to fetch* (delivery).
 *
 * Conflating them is a classic mistake. Server seq cannot resolve conflicts,
 * because it reflects arrival order at the server, which for offline edits is
 * essentially arbitrary — whoever reconnects first wins, which is not a
 * defensible rule.
 */
export class ServerStore {
  /** @param {{ records?: {rec: Rec, seq: number, deletedAt?: number}[], seq?: number, devices?: Record<string, {cursor: number, seenAt: number}>, gcFloor?: number }} [init] */
  constructor(init = {}) {
    /** @type {Map<string, { rec: Rec, seq: number, deletedAt?: number }>} */
    this.records = new Map((init.records ?? []).map((e) => [e.rec.id, e]));
    this.seq = init.seq ?? 0;
    /** @type {Map<string, { cursor: number, seenAt: number }>} */
    this.devices = new Map(Object.entries(init.devices ?? {}));
    /**
     * Highest seq below which tombstones may have been collected. A client
     * pulling from a cursor below this cannot be brought up to date
     * incrementally, because the deletions it needs to learn about are gone.
     */
    this.gcFloor = init.gcFloor ?? 0;
    /** @type {(() => void)|null} */
    this.onChange = null;
  }

  /**
   * Merge a batch from a client. Returns the number of records that actually
   * changed server state.
   *
   * Records that merge to something identical to what we already hold are *not*
   * assigned a new seq. This matters more than it looks: without the check, a
   * client that re-pushes an unchanged record bumps its seq, which makes it
   * newly visible to every other client, which causes them to pull it, merge
   * it, find no change... but the seq churn alone is enough to keep every
   * device polling forever. Quiescence is a feature and it has to be defended.
   *
   * @param {Rec[]} incoming
   * @param {string} [deviceId]
   */
  apply(incoming, deviceId) {
    let changed = 0;
    const now = Date.now();

    for (const remote of incoming) {
      if (!remote?.id || !remote.f) continue;
      const existing = this.records.get(remote.id);
      const merged = mergeRecord(existing?.rec, remote);

      if (existing && recEqual(existing.rec, merged)) continue;

      this.seq += 1;
      const wasDeleted = existing ? isDeleted(existing.rec) : false;
      const nowDeleted = isDeleted(merged);

      this.records.set(remote.id, {
        rec: merged,
        seq: this.seq,
        // Stamp when the record *became* a tombstone. The GC grace period runs
        // from this, not from the record's creation.
        deletedAt: nowDeleted ? (wasDeleted ? existing?.deletedAt ?? now : now) : undefined,
      });
      changed += 1;
    }

    if (deviceId) this.touchDevice(deviceId);
    if (changed > 0) this.onChange?.();
    return { changed, cursor: this.seq };
  }

  /**
   * Changes strictly after `since`, in seq order.
   * @param {number} since
   * @param {number} limit
   */
  changesSince(since, limit = 100) {
    if (since > 0 && since < this.gcFloor) {
      // We cannot describe the deletions this client missed. It must take a
      // complete snapshot and infer deletions from absence.
      return { records: [], cursor: this.seq, hasMore: false, fullResync: true };
    }

    const all = [...this.records.values()]
      .filter((e) => e.seq > since)
      .sort((a, b) => a.seq - b.seq);

    const page = all.slice(0, limit);
    const hasMore = all.length > page.length;
    // Cursor is the last seq actually delivered, never the global head —
    // otherwise a truncated page silently skips everything after it.
    const cursor = page.length > 0 ? page[page.length - 1].seq : since;

    return {
      records: page.map((e) => cloneRec(e.rec)),
      cursor,
      hasMore,
      complete: since === 0,
    };
  }

  /**
   * Record how far a device has caught up. This is the input to GC safety, so
   * it must reflect *applied* progress, not merely what was sent.
   * @param {string} deviceId
   * @param {number} cursor
   */
  ackDevice(deviceId, cursor) {
    const prev = this.devices.get(deviceId);
    this.devices.set(deviceId, {
      cursor: Math.max(prev?.cursor ?? 0, cursor),
      seenAt: Date.now(),
    });
  }

  /** @param {string} deviceId */
  touchDevice(deviceId) {
    const prev = this.devices.get(deviceId);
    this.devices.set(deviceId, { cursor: prev?.cursor ?? 0, seenAt: Date.now() });
  }

  /**
   * Collect tombstones.
   *
   * The safety condition is the entire point, and it is not "old enough".
   *
   *   A tombstone may be dropped only once every device the server still knows
   *   about has a cursor at or beyond that tombstone's seq.
   *
   * Drop one early and here is exactly what happens: device B, which has not
   * synced since before the delete, still holds the note. It pushes. The server
   * has no tombstone, so nothing tells it this record is dead — the merge sees
   * an unknown id and accepts it as new. The note resurrects on every device,
   * and nobody can explain why. `test/gc.test.js` runs precisely this sequence
   * both ways.
   *
   * A device that never returns would block GC forever, so devices idle beyond
   * `maxIdleMs` are evicted from the safety set. That is the deliberate trade:
   * the evicted device is no longer protected, and if it ever comes back its
   * cursor will be below `gcFloor`, so it is told to full-resync and recovers
   * deletions by absence instead. Correctness is preserved; the cost is one
   * expensive sync for a device that vanished for a month.
   *
   * @param {{ graceMs?: number, maxIdleMs?: number, force?: boolean }} [opts]
   */
  gcTombstones({ graceMs = 24 * 3600_000, maxIdleMs = 30 * 24 * 3600_000, force = false } = {}) {
    const now = Date.now();

    for (const [id, dev] of this.devices) {
      if (now - dev.seenAt > maxIdleMs) this.devices.delete(id);
    }

    // With no known devices, nothing can be resurrected, so anything past the
    // grace period is collectable.
    const cursors = [...this.devices.values()].map((d) => d.cursor);
    const safeBelow = cursors.length > 0 ? Math.min(...cursors) : this.seq;

    let collected = 0;
    let floor = this.gcFloor;

    for (const [id, entry] of this.records) {
      if (!isDeleted(entry.rec)) continue;
      if (!force) {
        if (entry.seq > safeBelow) continue; // some device has not seen this delete
        if (now - (entry.deletedAt ?? now) < graceMs) continue;
      }
      this.records.delete(id);
      collected += 1;
      floor = Math.max(floor, entry.seq);
    }

    if (collected > 0) {
      this.gcFloor = floor;
      this.onChange?.();
    }
    return { collected, gcFloor: this.gcFloor, safeBelow };
  }

  stats() {
    let live = 0;
    let tombstones = 0;
    for (const e of this.records.values()) {
      if (isDeleted(e.rec)) tombstones += 1;
      else live += 1;
    }
    return {
      seq: this.seq,
      gcFloor: this.gcFloor,
      live,
      tombstones,
      devices: [...this.devices.entries()].map(([id, d]) => ({
        id,
        cursor: d.cursor,
        idleSec: Math.round((Date.now() - d.seenAt) / 1000),
      })),
    };
  }

  toJSON() {
    return {
      seq: this.seq,
      gcFloor: this.gcFloor,
      records: [...this.records.values()],
      devices: Object.fromEntries(this.devices),
    };
  }
}
