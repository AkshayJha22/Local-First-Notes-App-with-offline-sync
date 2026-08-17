# localfirst-notes

A notes app where the local database is the source of truth and the network is
an optimization. Works fully offline, syncs when it can, and two devices editing
the same note converge without a server refereeing.

The interesting part is not the notes. It is the three places where the obvious
implementation is quietly wrong, and the test suite that proves this one isn't.

```
  device A                    server                     device B
┌──────────┐            ┌──────────────┐            ┌──────────┐
│ NoteStore│──push─────>│  ServerStore │<─────push──│ NoteStore│
│  IndexedDB│<──pull────│  changefeed  │────pull───>│ IndexedDB│
└──────────┘            │  + tombstone │            └──────────┘
                        │      GC      │
  same merge()          └──────────────┘          same merge()
```

The server runs the *identical* merge function the clients do. It is not an
authority on conflicts — it is a well-connected replica with a stable address.
What it uniquely provides is a total order over deliveries, which is what makes
an incremental sync cursor possible.

---

## Quick start

```bash
npm start          # http://localhost:8090
npm test           # 25 tests
npm run slow       # same, with 400ms of simulated API latency
```

No build step. No bundler. No dependencies outside of TypeScript for
typechecking JSDoc annotations.

**The demo worth doing:** open the page in two windows. Click *Go offline* on
one. Edit the same note in both — the title in one, the body in the other.
Bring the first back online. Both survive.

Then edit the *same field* in both while offline, reconnect, and watch them
agree on a winner. Which one wins is determined by the data, not by who
reconnected first.

---

## The three problems

### 1. Wall-clock time cannot order events across devices

Two laptops disagree by seconds routinely and by hours occasionally — wrong
timezone, dead CMOS battery, a user "fixing" their clock. If last-write-wins is
decided by `Date.now()`, one device with a fast clock wins every conflict
forever, and a device with a slow clock can never save anything.

So conflicts are ordered by **Lamport timestamps** (`src/core/timestamp.js`),
which replace "when did this happen" with "what had this device already seen
when it did this" — the question that actually matters for causality.

Two concurrent events can still tie on the counter, so the node ID breaks ties.
That makes the order *total*, and identical on every replica, which is the
precondition for LWW converging at all.

The half people forget is `clock.observe()` on merge. Without it, a replica that
has been offline keeps issuing low counters, so everything it writes loses every
conflict against a replica that stayed online. It silently becomes a
second-class citizen whose edits vanish. There is a test for exactly this.

### 2. Record-level LWW eats your work

```
Device A edits the title.  Device B edits the body.  Both offline.
```

With record-level LWW one of those is destroyed — the loser's whole record is
overwritten, body and all. Users experience this as "the app ate my work", and
they are right.

So merge is **per field** (`src/core/merge.js`). Each field carries its own
timestamp, and an edit to the title carries no opinion about the body. The UI
surfaces this directly: selecting a note shows which device last wrote each
field, which turns the conflict model from magic into something legible.

Three properties make convergence work, and the suite checks each directly
against 300 randomized record pairs:

| property | meaning |
|---|---|
| commutative | `merge(a,b) == merge(b,a)` |
| associative | `merge(merge(a,b),c) == merge(a,merge(b,c))` |
| idempotent | `merge(a,a) == a` |

Together they mean replicas converge regardless of arrival order, redelivery, or
batching. **That is what lets the sync protocol be dumb**: at-least-once delivery
with no ordering guarantees is sufficient, so a push that times out after the
server committed it can simply be retried — no dedupe table, no request IDs, no
exactly-once machinery. The protocol is simple because the data type absorbs
duplicates. If the operations were "append" or "increment", none of it would
work.

**Known limitation, chosen deliberately:** field-level LWW is still lossy
*within* a field. Two concurrent body edits mean one is discarded. Fixing that
needs a sequence CRDT (RGA, Yjs, Automerge) where the unit of conflict is the
character. That is thousands of lines with subtle interleaving bugs; this merge
is thirty lines and provably convergent. For a notes app it is the right trade —
but it is a trade, not an oversight.

### 3. Deleting a record is not the same as deleting a record

Actually remove it, and a replica that never heard about the delete still has
it. On its next sync it re-uploads it as new. The note comes back from the dead,
and retrying never fixes it, because from that replica's point of view nothing
is wrong.

So deletion is a **tombstone** — an ordinary field, `deleted: true`, which means
delete-vs-edit resolves by the same rule as everything else.

But tombstones cannot accumulate forever, and collecting them is where it gets
genuinely subtle. The safety condition is not "old enough":

> A tombstone may be dropped only once **every device the server knows about has
> a cursor at or beyond that tombstone's seq.**

`test/gc.test.js` runs the violation and asserts the damage precisely, because
the blast radius is what makes this bug so hard to diagnose from a support
ticket:

- The device that issued the delete is **fine** — it still holds its own
  tombstone, which outranks the stale record, so the system looks healthy from
  the machine most likely to notice.
- The server and every device *without* the tombstone — a new phone, a
  reinstall, anyone who full-resyncs — see a ghost note.
- The two states are now permanently inconsistent, and neither device is doing
  anything wrong.

A device that never comes back would block GC forever, so idle devices are
evicted. The evicted device is then no longer protected — so when it returns,
its cursor is below the server's `gcFloor`, it is told to full-resync, and it
recovers deletions **by absence** from the complete snapshot. Correctness is
preserved; the cost is one expensive sync for a device that vanished for a
month.

Two exceptions during full resync, both tested: records that never reached the
server are kept (absence says nothing about them), and records with unsynced
local edits are kept and re-pushed. That second one is a product judgement, not
a correctness one — silently destroying work the user did by hand is worse than
resurfacing a note they can delete again.

---

## Tests

```
# tests 25
# pass 25
# fail 0
```

The centerpiece is property-based. `convergenceScenario(seed)` runs randomized
create/update/delete/restore operations across N replicas, flipping them offline
and syncing in random orders, then heals every partition and runs to quiescence.
All replicas — and the server — must end byte-identical.

- 60 seeds × 3 replicas × 120 operations
- 30 seeds × 5 replicas × 200 operations

The PRNG is seeded (mulberry32) and every assertion prints its seed, because a
failure you cannot replay is a rumour rather than a bug report.

`test/http.test.js` is slower and narrower on purpose: it spawns the real
server and drives it over real HTTP, which is the only way to catch
serialization, route wiring, and the fact that `JSON.stringify` turns a `Map`
into `{}`. It includes a server restart to verify durability.

---

## What broke

**Property test seed 92 found a real divergence bug.**

`mergeField` resolved timestamp ties by returning the left argument. That is
safe under the invariant that equal timestamps imply equal values — a node never
reuses a counter — and every hand-written test agreed. Then the generator
produced two fields sharing a timestamp with *different* values, and
commutativity broke: `merge(a,b)` returned `a`, `merge(b,a)` returned `b`.

Under honest clients that state is unreachable. But it is reachable via a
corrupt payload, a hand-edited export, or a client with a bug — and the
consequence is not a bad value. It is **permanent divergence**: two replicas
disagree forever, and no amount of syncing repairs it, because each one believes
it is already correct.

The fix breaks the tie on the value itself (canonicalized, so key order can't
affect it), making the order total under all inputs rather than only well-formed
ones. **Convergence should be a property of the merge function, not a reward for
good behaviour upstream.**

I would not have found this by hand. The invariant was true, my reasoning about
it was correct, and the bug was in what happens when the invariant is violated
by someone else.

**Two tests I wrote wrong, which is its own lesson.** My first resurrection test
asserted that the deleting device would see the ghost. It doesn't — it is
protected by its own tombstone. And my first GC test assumed a device was caught
up after one sync, when the ack is deliberately conservative (a pull acks the
cursor the client *arrived* with, never the one it is being handed — optimistic
acking would credit a device for a batch it might never apply, which is the
exact hole GC safety exists to close). Both times the code was right and my
mental model was wrong. Writing down what you expect and being contradicted is
the point.

---

## Layout

```
src/core/            shared by the browser AND the tests — same files, no bundle
  timestamp.js       Lamport clock, total order
  merge.js           per-field LWW  ← the correctness core
  store.js           replica state, dirty tracking
  sync.js            resumable, interruptible, idempotent sync
src/server/
  store.js           changefeed + tombstone GC safety
  server.js          HTTP, static, atomic persistence
public/
  app.js             UI, imports /core/* directly
  idb.js             IndexedDB persistence
  transport.js       fetch transport + offline gate
test/
  convergence.test.js  property tests
  gc.test.js           tombstone safety, including the failure mode
  http.test.js         real server, real HTTP
```

The server maps `/core/` straight onto `src/core/`. The browser loads the same
files the test suite imports — not a bundle, not a copy that has since drifted.
Whatever the property tests proved about convergence, they proved about the code
running in the tab.
#   L o c a l - F i r s t - N o t e s - A p p - w i t h - o f f l i n e - s y n c  
 