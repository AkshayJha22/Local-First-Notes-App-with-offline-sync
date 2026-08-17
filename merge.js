import { compareTs, tsEqual } from "./timestamp.js";

/**
 * Per-field last-write-wins registers.
 *
 * A record is a map of field name to `{ v, t }` — a value and the Lamport
 * timestamp of the write that produced it. Merging is per field, not per
 * record, and that granularity is the whole point:
 *
 *   Device A edits the title. Device B edits the body. Both offline.
 *
 * With record-level LWW, one of those edits is destroyed — the loser's entire
 * record is overwritten, body and all. With field-level LWW, both survive,
 * because they touched disjoint fields and neither write has an opinion about
 * the other. Users experience record-level LWW as "the app ate my work", and
 * they are right.
 *
 * Field-level LWW is still lossy *within* a field: two concurrent body edits
 * mean one is discarded. Fixing that requires a sequence CRDT (RGA, Yjs,
 * Automerge) where the unit of conflict is the character rather than the field.
 * That is a real and known limitation, chosen deliberately: the merge function
 * below is about thirty lines and is provably convergent, and a text CRDT is
 * several thousand lines with subtle interleaving bugs. For a notes app where
 * simultaneous edits to the same note body are rare, this is the right trade —
 * but it is a trade, not an oversight.
 *
 * The three properties that make convergence work, and which the test suite
 * checks directly:
 *
 *   commutative   merge(a, b) == merge(b, a)
 *   associative   merge(merge(a, b), c) == merge(a, merge(b, c))
 *   idempotent    merge(a, a) == a
 *
 * Together these mean replicas converge regardless of the order updates arrive
 * in, how often they are redelivered, or how they are batched. That is what
 * lets the sync protocol be dumb: at-least-once delivery with no ordering
 * guarantees is sufficient, so retries need no bookkeeping and a duplicated
 * batch is harmless.
 *
 * @typedef {import("./timestamp.js").Timestamp} Timestamp
 * @typedef {{ v: any, t: Timestamp }} Field
 * @typedef {{ id: string, f: Record<string, Field> }} Rec
 */

/**
 * Merge two versions of a single field. Higher timestamp wins.
 * @param {Field|undefined} a
 * @param {Field|undefined} b
 * @returns {Field|undefined}
 */
export function mergeField(a, b) {
  if (!a) return b;
  if (!b) return a;
  const cmp = compareTs(a.t, b.t);
  if (cmp > 0) return a;
  if (cmp < 0) return b;

  // Equal timestamps *should* mean the same write, since a node never reuses a
  // counter. Returning `a` here would therefore be harmless — and that is
  // exactly what this function did until a property test at seed 92 generated
  // two fields sharing a timestamp with different values, and commutativity
  // broke: merge(a,b) returned a, merge(b,a) returned b.
  //
  // Under honest clients that state is unreachable. But it is reachable via a
  // corrupt payload, a hand-edited export, or a client with a bug, and the
  // consequence is not a bad value — it is *permanent divergence*, where two
  // replicas disagree forever and no amount of syncing repairs it, because each
  // one thinks it is already correct.
  //
  // Breaking the tie on the value itself makes the order total under all
  // inputs, not just well-formed ones. Convergence should be a property of the
  // merge function, not a reward for good behaviour upstream.
  const va = canonical(a.v);
  const vb = canonical(b.v);
  if (va === vb) return a;
  return va > vb ? a : b;
}

/** Order-independent JSON, so key insertion order cannot affect the tiebreak. */
function canonical(v) {
  if (v === null || typeof v !== "object") return JSON.stringify(v) ?? "null";
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  const keys = Object.keys(v).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`).join(",")}}`;
}

/**
 * Merge two versions of a record. Field sets are unioned, so a field that only
 * one side knows about is preserved rather than dropped.
 * @param {Rec|undefined} a
 * @param {Rec|undefined} b
 * @returns {Rec}
 */
export function mergeRecord(a, b) {
  if (!a) return cloneRec(/** @type {Rec} */ (b));
  if (!b) return cloneRec(a);
  if (a.id !== b.id) throw new Error(`cannot merge different records: ${a.id} vs ${b.id}`);

  /** @type {Record<string, Field>} */
  const f = {};
  for (const key of new Set([...Object.keys(a.f), ...Object.keys(b.f)])) {
    const merged = mergeField(a.f[key], b.f[key]);
    if (merged) f[key] = { v: merged.v, t: { ...merged.t } };
  }
  return { id: a.id, f };
}

/**
 * Structural equality. Used to decide whether a merge actually changed
 * anything, which is what stops sync from looping forever re-pushing records
 * that are already in agreement.
 * @param {Rec|undefined} a
 * @param {Rec|undefined} b
 */
export function recEqual(a, b) {
  if (a === b) return true;
  if (!a || !b) return false;
  if (a.id !== b.id) return false;
  const ka = Object.keys(a.f);
  const kb = Object.keys(b.f);
  if (ka.length !== kb.length) return false;
  for (const k of ka) {
    const fa = a.f[k];
    const fb = b.f[k];
    if (!fb) return false;
    if (!tsEqual(fa.t, fb.t)) return false;
    // Timestamps being equal implies values are equal (same write), but compare
    // anyway so a corrupt or hand-edited payload is caught rather than trusted.
    if (JSON.stringify(fa.v) !== JSON.stringify(fb.v)) return false;
  }
  return true;
}

/** @param {Rec} r */
export function cloneRec(r) {
  /** @type {Record<string, Field>} */
  const f = {};
  for (const [k, val] of Object.entries(r.f)) f[k] = { v: val.v, t: { ...val.t } };
  return { id: r.id, f };
}

/**
 * Highest timestamp anywhere in the record. Used to advance the local clock
 * past everything a peer has seen.
 * @param {Rec} r
 * @returns {Timestamp|null}
 */
export function maxTs(r) {
  /** @type {Timestamp|null} */
  let best = null;
  for (const field of Object.values(r.f)) {
    if (!best || compareTs(field.t, best) > 0) best = field.t;
  }
  return best;
}

/**
 * Project a record to plain values, dropping timestamp metadata.
 * @param {Rec} r
 */
export function materialize(r) {
  /** @type {Record<string, any>} */
  const out = { id: r.id };
  for (const [k, field] of Object.entries(r.f)) out[k] = field.v;
  return out;
}

/** @param {Rec} r */
export function isDeleted(r) {
  return r.f.deleted?.v === true;
}
