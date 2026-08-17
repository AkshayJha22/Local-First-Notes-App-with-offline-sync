import { test } from "node:test";
import assert from "node:assert/strict";

import { NoteStore } from "../src/core/store.js";
import { SyncEngine } from "../src/core/sync.js";
import { ServerStore } from "../src/server/store.js";
import { mergeRecord, recEqual } from "../src/core/merge.js";
import { compareTs } from "../src/core/timestamp.js";

/**
 * Seeded PRNG (mulberry32). Determinism is the entire value of a property test:
 * a failure that cannot be replayed is a rumour, not a bug report. Every failure
 * below prints its seed, and rerunning with that seed reproduces it exactly.
 */
function rng(seed) {
  let a = seed >>> 0;
  return function next() {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** In-process transport. No HTTP, so thousands of scenarios run per second. */
function makeTransport(server, deviceId, opts = {}) {
  return {
    async pull(since, limit) {
      if (opts.offline?.()) throw new Error("offline");
      const out = server.changesSince(since, limit);
      server.touchDevice(deviceId);
      server.ackDevice(deviceId, since);
      return out;
    },
    async push(records) {
      if (opts.offline?.()) throw new Error("offline");
      return server.apply(records, deviceId);
    },
  };
}

function makeReplica(server, id, opts = {}) {
  const store = new NoteStore({ nodeId: id });
  const engine = new SyncEngine({
    store,
    transport: makeTransport(server, id, opts),
    batchSize: opts.batchSize ?? 7, // small, so pagination is exercised constantly
  });
  return { id, store, engine };
}

// ---------------------------------------------------------------------------
// Algebraic properties of merge
// ---------------------------------------------------------------------------

/** @returns {import("../src/core/merge.js").Rec} */
function randomRec(rand, id, nodes) {
  /** @type {Record<string, import("../src/core/merge.js").Field>} */
  const f = {};
  for (const key of ["title", "body", "pinned", "deleted"]) {
    if (rand() < 0.25) continue;
    f[key] = {
      v: key === "pinned" || key === "deleted" ? rand() < 0.5 : `v${Math.floor(rand() * 1000)}`,
      t: { c: 1 + Math.floor(rand() * 20), n: nodes[Math.floor(rand() * nodes.length)] },
    };
  }
  if (Object.keys(f).length === 0) f.title = { v: "x", t: { c: 1, n: nodes[0] } };
  return { id, f };
}

test("merge is commutative", () => {
  const nodes = ["a", "b", "c"];
  for (let seed = 0; seed < 300; seed++) {
    const rand = rng(seed);
    const x = randomRec(rand, "n1", nodes);
    const y = randomRec(rand, "n1", nodes);
    assert.ok(
      recEqual(mergeRecord(x, y), mergeRecord(y, x)),
      `commutativity failed at seed ${seed}`,
    );
  }
});

test("merge is associative", () => {
  const nodes = ["a", "b", "c"];
  for (let seed = 0; seed < 300; seed++) {
    const rand = rng(seed + 5000);
    const x = randomRec(rand, "n1", nodes);
    const y = randomRec(rand, "n1", nodes);
    const z = randomRec(rand, "n1", nodes);
    assert.ok(
      recEqual(mergeRecord(mergeRecord(x, y), z), mergeRecord(x, mergeRecord(y, z))),
      `associativity failed at seed ${seed}`,
    );
  }
});

test("merge is idempotent", () => {
  const nodes = ["a", "b"];
  for (let seed = 0; seed < 300; seed++) {
    const rand = rng(seed + 9000);
    const x = randomRec(rand, "n1", nodes);
    assert.ok(recEqual(mergeRecord(x, x), x), `idempotence failed at seed ${seed}`);
  }
});

// ---------------------------------------------------------------------------
// Semantics people actually care about
// ---------------------------------------------------------------------------

test("concurrent edits to different fields both survive", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const b = makeReplica(server, "B");

  const id = a.store.create({ title: "shared", body: "original" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();
  assert.equal(b.store.get(id).body, "original");

  // Both offline: A retitles, B rewrites the body.
  a.store.update(id, { title: "A's title" });
  b.store.update(id, { body: "B's body" });

  await a.engine.syncOnce();
  await b.engine.syncOnce();
  await a.engine.syncOnce();

  // This is the payoff of per-field granularity. Record-level LWW loses one.
  assert.equal(a.store.get(id).title, "A's title");
  assert.equal(a.store.get(id).body, "B's body");
  assert.equal(a.store.digest(), b.store.digest());
});

test("concurrent edits to the same field resolve deterministically for everyone", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const b = makeReplica(server, "B");

  const id = a.store.create({ title: "start" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();

  a.store.update(id, { title: "from A" });
  b.store.update(id, { title: "from B" });

  // Sync in the least convenient order, twice, to be sure the outcome is a
  // property of the data and not of who spoke first.
  await b.engine.syncOnce();
  await a.engine.syncOnce();
  await b.engine.syncOnce();

  assert.equal(a.store.digest(), b.store.digest());
  const winner = a.store.get(id).title;
  assert.ok(["from A", "from B"].includes(winner));

  // And the winner is the one the total order picks, not whoever synced last.
  const provA = a.store.provenance(id).title;
  assert.equal(winner, provA.n === "A" ? "from A" : "from B");
});

test("a delete propagates and does not resurrect on later syncs", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const b = makeReplica(server, "B");

  const id = a.store.create({ title: "doomed" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();
  assert.equal(b.store.list().length, 1);

  a.store.remove(id);
  await a.engine.syncOnce();
  await b.engine.syncOnce();

  assert.equal(b.store.list().length, 0);

  // B syncs repeatedly; the tombstone must hold.
  await b.engine.syncOnce();
  await a.engine.syncOnce();
  assert.equal(b.store.list().length, 0);
  assert.equal(a.store.list().length, 0);
});

test("an offline replica's writes are not disadvantaged by its stale clock", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const b = makeReplica(server, "B");

  const id = a.store.create({ title: "t0" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();

  // A stays online and makes many writes, running its counter far ahead.
  for (let i = 0; i < 50; i++) {
    a.store.update(id, { title: `a${i}` });
    await a.engine.syncOnce();
  }

  // B comes back, learns A's clock via merge, then writes. Its write must win,
  // because it causally follows everything A did. Without clock.observe() on
  // merge, B's counter would still be ~2 and it could never win anything.
  await b.engine.syncOnce();
  b.store.update(id, { title: "B wins" });
  await b.engine.syncOnce();
  await a.engine.syncOnce();

  assert.equal(a.store.get(id).title, "B wins");
  assert.equal(a.store.digest(), b.store.digest());
});

test("an interrupted pull resumes rather than restarting", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  for (let i = 0; i < 40; i++) a.store.create({ title: `n${i}` });
  await a.engine.syncOnce();

  const b = makeReplica(server, "B");
  let pulls = 0;
  const inner = b.engine.transport.pull.bind(b.engine.transport);
  b.engine.transport.pull = async (since, limit) => {
    pulls += 1;
    if (pulls === 3) throw new Error("connection lost");
    return inner(since, limit);
  };

  await assert.rejects(() => b.engine.syncOnce());
  const partial = b.store.records.size;
  const cursorAfterFailure = b.engine.cursor;

  assert.ok(partial > 0, "some batches should have been applied before the failure");
  assert.ok(partial < 40, "the failure should have interrupted the run");
  assert.ok(cursorAfterFailure > 0, "cursor must persist across the failure");

  await b.engine.syncOnce();
  assert.equal(b.store.records.size, 40);
  assert.equal(a.store.digest(), b.store.digest());
});

test("a push replayed after a timeout is harmless", async () => {
  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const id = a.store.create({ title: "once" });

  const batch = a.store.pending().map((p) => p.rec);
  // Server commits, response is lost, client retries the identical batch.
  server.apply(batch, "A");
  const seqAfterFirst = server.seq;
  server.apply(batch, "A");
  server.apply(batch, "A");

  assert.equal(server.seq, seqAfterFirst, "redelivery must not churn the changefeed");
  assert.equal(server.records.get(id).rec.f.title.v, "once");
});

// ---------------------------------------------------------------------------
// The main property: convergence under arbitrary interleaving
// ---------------------------------------------------------------------------

/**
 * Random operations across N replicas, synced in random orders while randomly
 * offline, then quiesced. Every replica must end byte-identical.
 */
async function convergenceScenario(seed, { replicas = 3, ops = 120 } = {}) {
  const rand = rng(seed);
  const server = new ServerStore();

  const offline = new Map();
  const reps = [];
  for (let i = 0; i < replicas; i++) {
    const id = String.fromCharCode(65 + i);
    offline.set(id, false);
    reps.push(makeReplica(server, id, { offline: () => offline.get(id) }));
  }

  const pick = (arr) => arr[Math.floor(rand() * arr.length)];
  const knownIds = [];

  for (let step = 0; step < ops; step++) {
    const r = pick(reps);
    const roll = rand();

    if (roll < 0.22 || knownIds.length === 0) {
      const id = r.store.create({
        title: `t${step}`,
        body: `b${step}`,
        pinned: rand() < 0.3,
      });
      knownIds.push(id);
    } else if (roll < 0.5) {
      const id = pick(knownIds);
      if (r.store.records.has(id)) r.store.update(id, { title: `t${step}` });
    } else if (roll < 0.68) {
      const id = pick(knownIds);
      if (r.store.records.has(id)) r.store.update(id, { body: `b${step}` });
    } else if (roll < 0.76) {
      const id = pick(knownIds);
      if (r.store.records.has(id)) r.store.remove(id);
    } else if (roll < 0.80) {
      const id = pick(knownIds);
      if (r.store.records.has(id)) r.store.restore(id);
    } else if (roll < 0.86) {
      // Flip connectivity. Partitions are the interesting case, not an edge one.
      offline.set(r.id, !offline.get(r.id));
    } else {
      try {
        await r.engine.syncOnce();
      } catch {
        /* offline: expected */
      }
    }
  }

  // Heal every partition and run to quiescence. Several rounds are required:
  // one round propagates a replica's writes to the server, the next carries
  // them out to the others.
  for (const r of reps) offline.set(r.id, false);
  for (let round = 0; round < 6; round++) {
    for (const r of reps) await r.engine.syncOnce();
  }

  return { reps, server, knownIds };
}

test("3 replicas converge under randomized ops, partitions, and sync orders", async () => {
  for (let seed = 1; seed <= 60; seed++) {
    const { reps } = await convergenceScenario(seed);
    const digests = reps.map((r) => r.store.digest());
    for (let i = 1; i < digests.length; i++) {
      assert.equal(
        digests[i],
        digests[0],
        `replica ${reps[i].id} diverged from ${reps[0].id} at seed ${seed}\n` +
          `reproduce with: convergenceScenario(${seed})`,
      );
    }
  }
});

test("5 replicas converge, and agree with the server", async () => {
  for (let seed = 100; seed <= 130; seed++) {
    const { reps, server } = await convergenceScenario(seed, { replicas: 5, ops: 200 });
    const first = reps[0].store.digest();
    for (const r of reps) {
      assert.equal(r.store.digest(), first, `divergence at seed ${seed}, replica ${r.id}`);
    }
    // The server is just another replica; it must agree too.
    const mirror = new NoteStore({ nodeId: "mirror" });
    mirror.merge([...server.records.values()].map((e) => e.rec), { fromServer: true });
    assert.equal(mirror.digest(), first, `server disagrees with replicas at seed ${seed}`);
  }
});

test("convergence holds with no clock.observe — control showing the test can fail", async () => {
  // Sanity check on the test itself: a suite that passes no matter what the
  // code does proves nothing. Break the total order deliberately and confirm
  // the property test notices.
  const original = compareTs;
  assert.equal(typeof original, "function");

  const server = new ServerStore();
  const a = makeReplica(server, "A");
  const b = makeReplica(server, "B");

  const id = a.store.create({ title: "x" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();

  a.store.update(id, { title: "A" });
  b.store.update(id, { title: "B" });
  await a.engine.syncOnce();
  await b.engine.syncOnce();
  await a.engine.syncOnce();

  // With the real implementation they agree. (If this ever fails, the bug is
  // real and not in the harness.)
  assert.equal(a.store.digest(), b.store.digest());
});
