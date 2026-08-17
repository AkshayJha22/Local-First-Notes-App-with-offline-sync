import { test } from "node:test";
import assert from "node:assert/strict";

import { NoteStore } from "../src/core/store.js";
import { SyncEngine } from "../src/core/sync.js";
import { ServerStore } from "../src/server/store.js";

function makeTransport(server, deviceId) {
  return {
    async pull(since, limit) {
      const out = server.changesSince(since, limit);
      server.touchDevice(deviceId);
      server.ackDevice(deviceId, since);
      return out;
    },
    async push(records) {
      return server.apply(records, deviceId);
    },
  };
}

function makeReplica(server, id) {
  const store = new NoteStore({ nodeId: id });
  const engine = new SyncEngine({ store, transport: makeTransport(server, id), batchSize: 50 });
  return { id, store, engine };
}

/**
 * Tombstone garbage collection.
 *
 * Tombstones cannot simply accumulate — a note deleted years ago should not
 * cost storage on every device forever. But collecting them is dangerous in a
 * specific and non-obvious way, and these tests pin down both the danger and
 * the condition that removes it.
 */

test("UNSAFE GC resurrects a note on a device that missed the delete", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const b = makeReplica(server, "B");

  const id = a.store.create({ title: "important" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();
  assert.equal(b.store.list().length, 1, "B should have the note");

  // B edits it offline — this is what gives B something to push later, and it
  // is the realistic shape of the bug: someone works on a note that was deleted
  // elsewhere while they were away.
  b.store.update(id, { body: "notes from the field" });

  // A deletes it. B never learns.
  a.store.remove(id);
  await a.engine.syncOnce();
  assert.equal(server.records.get(id).rec.f.deleted.v, true);

  // Collect the tombstone ignoring the safety condition.
  const res = server.gcTombstones({ force: true });
  assert.equal(res.collected, 1);
  assert.equal(server.records.has(id), false, "tombstone is gone");

  // B comes back and pushes its stale copy. The server has nothing that says
  // this record is dead, so it accepts it as a brand new note.
  await b.engine.syncOnce();

  assert.ok(server.records.has(id), "the note came back from the dead");
  assert.equal(server.records.get(id).rec.f.deleted.v, false);

  // The blast radius is worth being precise about, because it is what makes
  // this bug so hard to diagnose from a support ticket.
  //
  // A is NOT affected: it still holds its own tombstone, whose timestamp beats
  // the stale `deleted: false` B pushed, so A's merge correctly keeps the note
  // deleted. The system therefore looks fine from the machine most likely to
  // notice.
  await a.engine.syncOnce();
  assert.equal(a.store.list().length, 0, "A is protected by its own tombstone");

  // The damage lands on the server and on every device that does not already
  // hold the tombstone — a new device, a reinstall, a full resync. They have no
  // evidence the note was ever deleted, so they render a ghost.
  const c = makeReplica(server, "C");
  await c.engine.syncOnce();
  assert.equal(c.store.list().length, 1, "a fresh device sees a deleted note");
  assert.equal(c.store.get(id).title, "important");

  // So the state is now permanently inconsistent: A and C disagree, forever,
  // and neither is doing anything wrong.
  assert.notEqual(a.store.digest(), c.store.digest());
});

test("SAFE GC refuses to collect while any device is behind the delete", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const b = makeReplica(server, "B");

  const id = a.store.create({ title: "important" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();

  a.store.remove(id);
  await a.engine.syncOnce();

  // B's cursor is behind the tombstone's seq, so it is not collectable.
  const res = server.gcTombstones({ graceMs: 0 });
  assert.equal(res.collected, 0, "GC must decline while B is behind");
  assert.ok(server.records.has(id));

  // Once B catches up, the delete is safely collectable.
  await b.engine.syncOnce();
  assert.equal(b.store.list().length, 0, "B applied the delete");

  // Every device must confirm it is caught up, the deleter included. The ack is
  // deliberately conservative — a pull acks the cursor the client *arrived*
  // with, never the one it is being handed — so a quiescent device needs one
  // further empty poll before it counts as current. Optimistic acking would
  // credit a device for a batch it might never apply, which is precisely the
  // hole GC safety exists to close.
  await b.engine.syncOnce();
  await a.engine.syncOnce();
  await a.engine.syncOnce();
  const res2 = server.gcTombstones({ graceMs: 0 });
  assert.equal(res2.collected, 1, "GC proceeds once every device has seen it");

  // And now B pushing again cannot resurrect anything, because B agrees.
  await b.engine.syncOnce();
  await a.engine.syncOnce();
  assert.equal(a.store.list().length, 0);
  assert.equal(b.store.list().length, 0);
  assert.equal(server.records.has(id), false);
});

test("the grace period holds a tombstone even when every device is caught up", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");

  const id = a.store.create({ title: "x" });
  await a.engine.syncOnce();
  a.store.remove(id);
  await a.engine.syncOnce();
  await a.engine.syncOnce();
  await a.engine.syncOnce();

  assert.equal(server.gcTombstones({ graceMs: 60_000 }).collected, 0, "grace period holds");
  assert.equal(server.gcTombstones({ graceMs: 0 }).collected, 1, "collectable once elapsed");
});

test("a device whose cursor predates the GC floor is told to full-resync", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const b = makeReplica(server, "B");

  const keep = a.store.create({ title: "keeper" });
  const doomed = a.store.create({ title: "doomed" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();
  assert.equal(b.store.list().length, 2);

  const bCursorBefore = b.engine.cursor;

  // B goes away. A deletes, and the tombstone is eventually collected because
  // B has been idle long enough to be evicted from the safety set.
  a.store.remove(doomed);
  await a.engine.syncOnce();
  await a.engine.syncOnce();

  server.devices.delete("B"); // simulate eviction after a long absence
  const gc = server.gcTombstones({ graceMs: 0 });
  assert.equal(gc.collected, 1);
  assert.ok(gc.gcFloor > bCursorBefore, "the GC floor must now exceed B's cursor");

  // B returns. An incremental pull cannot explain the deletion, so the server
  // demands a full resync and B infers the deletion from absence.
  const res = server.changesSince(b.engine.cursor, 50);
  assert.equal(res.fullResync, true);

  await b.engine.syncOnce();

  assert.equal(b.store.records.has(doomed), false, "B dropped the record it could not see");
  assert.ok(b.store.records.has(keep), "B kept everything still present");
  assert.equal(b.store.list().length, 1);

  // And B pushing after the resync cannot resurrect it.
  await b.engine.syncOnce();
  await a.engine.syncOnce();
  assert.equal(a.store.list().length, 1);
  assert.equal(server.records.has(doomed), false);
});

test("full resync preserves unsynced local work rather than deleting it", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const b = makeReplica(server, "B");

  a.store.create({ title: "shared" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();

  // B writes a note entirely offline; the server has never heard of it.
  const localOnly = b.store.create({ title: "written on a plane" });

  // Force B into the full-resync path.
  server.gcFloor = 999;

  await b.engine.syncOnce();

  assert.ok(
    b.store.records.has(localOnly),
    "a record the server never saw must not be deleted by absence",
  );
  await b.engine.syncOnce();
  assert.ok(server.records.has(localOnly), "and it should reach the server");
});

test("a locally-edited record survives full resync even if the server deleted it", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const b = makeReplica(server, "B");

  const id = a.store.create({ title: "contested" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();

  // B edits offline; meanwhile the delete happens and is collected.
  b.store.update(id, { body: "hours of work" });

  a.store.remove(id);
  await a.engine.syncOnce();
  await a.engine.syncOnce();
  server.devices.delete("B");
  server.gcTombstones({ graceMs: 0 });

  await b.engine.syncOnce();

  // Product judgement, asserted so it cannot regress silently: destroying work
  // the user did by hand is worse than resurfacing a note they can delete again.
  assert.ok(b.store.records.has(id), "unsynced edits are not discarded");
  assert.equal(b.store.get(id).body, "hours of work");
});

test("GC with no known devices is unrestricted", () => {
  const server = new ServerStore();
  const store = new NoteStore({ nodeId: "solo" });
  const id = store.create({ title: "x" });
  store.remove(id);
  server.apply([store.records.get(id)]);

  assert.equal(server.devices.size, 0);
  assert.equal(server.gcTombstones({ graceMs: 0 }).collected, 1);
});
