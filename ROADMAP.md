# Aquifer roadmap

The goal: make Aquifer the most *trustworthy* small data layer for Kotlin and Android — the
library you reach for when "show cached data instantly, refresh intelligently, survive
process death, never resurrect deleted data" must actually be true, with every guarantee
tested and every trade-off written down.

**How to read this:** milestones are sorted by importance; items within a milestone are
sorted by leverage (impact ÷ effort). Effort: **S** ≈ a day, **M** ≈ a few days, **L** ≈ a
week+, **XL** ≈ multiple weeks. Checked boxes are shipped. Numbers like #12 are tracked
issues.

---

## Now — ship v0.1.0 🚢

Everything else compounds once there's a public artifact.

- [ ] **Publish v0.1.0 to Maven Central** — add the four secrets from
  [CONTRIBUTING](CONTRIBUTING.md), bump versions off `-SNAPSHOT`, date the CHANGELOG, push
  `v0.1.0`; the guarded release workflow does the rest. *(owner action — S)*
- [ ] **Repo hygiene** — flip the default branch to `main`, then (once this roadmap PR has
  landed) delete the merged development branch. The Dependabot PRs (#7–#11) are already
  retargeted to base `main`, so they merge independently once green; the Gradle group bump
  (Kotlin/AGP) deserves the closest look. *(owner action — S)*
- [ ] **Maven Central badge + install snippet verification** after the first release. *(S)*

## 0.2 — Compose & everyday ergonomics

What every consuming app touches daily; highest user-facing leverage.

- [ ] **`aquifer-compose` module** — `Flow<DataState<V>>.collectAsAquiferState()` built on
  `collectAsStateWithLifecycle`, a `rememberStream(aquifer, key)` helper, and
  preview-friendly fake stores. Runtime-only (no UI-test infrastructure needed). *(M)*
- [ ] **`DataState` ergonomics** — `map`, `onContent`/`onFailure`, `getOrNull`,
  `valueOrThrow`, `isLoading`; pure additive API. *(S)*
- [ ] **Static analysis in CI** — detekt + ktlint; cheap and raises the floor for every
  later PR. *(S)*
- [ ] **Per-call freshness parameters** — `CacheFirst(maxAge = 1.minutes)`,
  `StaleWhileRevalidate(maxStale = 1.hours)`; the sealed `Freshness` hierarchy was designed
  to grow this way, the data objects stay as defaults. Needs a short API design note (binary
  compatibility of the sealed hierarchy). *(M)*
- [ ] **Bounded disk store** — `JsonFileSourceOfTruth` currently grows without limit; add
  `maxEntries`/`maxBytes` LRU eviction (access-ordered index file or mtime-based) plus
  orphaned-temp-file GC on open. A real product gap, not a nice-to-have. *(M)*
- [ ] **`DataState.Empty` / observable deletion** — today `CacheOnly` observers render
  deleted data forever (documented in #6's review); an explicit empty state fixes the
  contract. API-shaping RFC first: it touches every `when` over `DataState`. *(M)*

## 0.3 — Network efficiency & resilience

Make the fetch path cheap and stampede-proof under real-world conditions.

- [ ] **Conditional fetching (ETag / Last-Modified)** — give the fetcher access to the
  cached entry's metadata and a `NotModified` result so 304s refresh TTLs without
  re-downloading; ship an `aquifer-okhttp` helper for header wiring. The single biggest
  bandwidth win available. *(L)*
- [ ] **Negative caching** — remember fetch failures per key with their own short TTL and
  backoff memory, so a failing endpoint isn't hammered by every new subscriber. *(M)*
- [ ] **TTL jitter** — optional fuzz on expiry so entries fetched together don't all go
  stale together (the request-stampede mirror of retry jitter). *(S)*
- [ ] **`prefetch(key)`** — fire-and-forget warmup honoring freshness and dedup; trivial
  API, large perceived-performance payoff for predictable navigation. *(S)*
- [ ] **Batched fetching** — `getAll(keys)`/`streamMany(keys)` with an optional coalescing
  window mapping N keys to one backend call (DataLoader-style); solves the N+1 pattern for
  list screens. *(L)*
- [ ] **#12 — benchmark, then stripe the commit guard** — JMH-style harness for concurrent
  commit throughput against a real file store; implement per-key lock striping only if the
  numbers justify it (constraints documented in the issue). *(M–L)*

## 0.4 — Persistence expansion

Meet apps where their storage already is.

- [ ] **Proto DataStore adapter** (`aquifer-persistence-datastore`) — the modern AndroidX
  default. *(M)*
- [ ] **SQLDelight adapter** — queryable persistence and the natural stepping stone to
  multiplatform. *(M)*
- [ ] **Encryption hook** — a pluggable encrypt/decrypt seam on the file store (Tink-ready)
  for caching sensitive data. *(M)*
- [ ] **Schema-migration helper** — versioned envelopes plus a `migrate(fromVersion, json)`
  callback, so breaking model changes stop meaning "wipe the cache directory". *(M)*

## 0.5 — Proof-grade quality & observability

The engine's guarantees deserve machine-checked evidence.

- [ ] **Lincheck concurrency tests** — model-check the fencing/single-flight invariants
  (linearizability of put/invalidate/fetch-commit) instead of relying on hand-written
  interleavings; the strongest possible backing for the epoch design. *(L)*
- [ ] **#13 — bounded `keyEpochs`** — implement the live-fetch refcount eviction sketched in
  the issue, with the proof written down (the naive evictions are provably unsound). *(M)*
- [ ] **`stats()` snapshot API** — hits, misses, evictions, in-flight count, per-store; the
  numbers `AquiferEvents` can't aggregate. *(S)*
- [ ] **Coverage gate** — Kover + a CI threshold + badge. *(S)*
- [ ] **Docs site** — publish the aggregated Dokka output via GitHub Pages on release. *(S)*
- [ ] **Sample Android app** — a small Compose app demoing airplane-mode survival,
  pull-to-refresh coherence, and reconnect revalidation on a device. *(L)*

## 1.0 — the stability contract

- [ ] **API freeze review** — a deliberate pass over every public signature against the
  locked BCV dumps; rename/remove debts now or never. *(M)*
- [ ] **Semver policy + CHANGELOG discipline** documented; release-notes automation. *(S)*
- [ ] **"Coming from…" guides** — migration recipes from Store5 and from hand-rolled
  repository patterns; this is how libraries actually get adopted. *(M)*
- [ ] **SECURITY.md + issue/PR templates.** *(S)*

## Beyond 1.0 — strategic bets

- [ ] **Kotlin Multiplatform core** — dispatcher-clean coroutines are the easy half; the
  engine's JVM concurrency also has to move: the `ConcurrentHashMap` CAS loops (`inFlight`,
  `activeKeys`, `keyEpochs`), `AtomicLong`/`AtomicBoolean`, and the `LinkedHashMap`-based
  LRU need KMP equivalents (atomicfu, mutex-guarded maps), plus a `kotlinx-io`/okio port of
  the file store, iOS/desktop targets, and an iOS sample. The largest differentiator on the
  list. *(XL)*
- [ ] **Offline mutations** (`aquifer-mutations`) — the write-side counterpart to Aquifer's
  read-side: an optimistic-update queue with rollback and conflict hooks, surviving process
  death via the same `SourceOfTruth` machinery. *(XL)*
- [ ] **Paging bridge** (`aquifer-paging`) — keyed page caching behind AndroidX Paging 3.
  *(L)*

## Non-goals

Declared so the scope stays honest: image/blob caching (use Coil), cross-process shared
caches, full sync engines/CRDTs, GraphQL response normalization, reflection-based
serialization, and Java-first API surface.

---

Suggestions welcome — open an issue. Roadmap ordering is by expected leverage and gets
re-sorted as reality disagrees.
