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
- [ ] **Repo hygiene** *(owner action — S)* — flip the default branch to `main` and delete the
  merged development branch once the in-flight work lands. Dependabot's grouped toolchain
  bumps (#7–#11) were superseded by the #19 refresh and are closed; one open minor bump
  (#20, junit-bom 5.14.x) remains to triage against the deliberate JVM-11 JUnit pin.
- [ ] **Maven Central badge + install snippet verification** after the first release. *(S)*

## 0.2 — Compose & everyday ergonomics

What every consuming app touches daily; highest user-facing leverage.

- [x] **`aquifer-compose` module** — `Aquifer.collectAsState(key)` built on
  `collectAsStateWithLifecycle`, a `rememberStream` helper, and a `previewAquifer` fake for
  `@Preview`s; behavior-tested with molecule (no UI-test infrastructure). *(M)*
- [x] **`DataState` ergonomics** — `map`, `onContent`/`onFailure`, `valueOrThrow`,
  `isLoading`; pure additive API (`getOrNull` dropped as redundant with `.value`). *(S)*
- [x] **Static analysis in CI** — detekt with the bundled ktlint formatting rules, zero
  tolerated issues, wired into `check`. *(S)*
- [x] **Per-call freshness parameters** — shipped as a `maxAge` parameter on
  `get`/`stream`/`collectAsState` rather than parameterizing the sealed `Freshness` types:
  one orthogonal knob composes with every strategy instead of multiplying sealed variants,
  and the data objects stay simple defaults. Fetch decisions change only for the
  staleness-aware strategies; a stream's `isStale` flags follow the caller's bar under
  every strategy, and `Duration.INFINITE` means "serve anything cached". *(M)*
- [x] **Bounded disk store** — `maxEntries`/`maxBytes` LRU eviction on
  `JsonFileSourceOfTruth` plus orphaned-temp-file GC on first use. Shipped with an
  in-memory access-ordered index (exact within a process) seeded from file mtimes across
  restarts — no index file to keep crash-consistent — and an absolute byte cap
  (DiskLruCache-style: an entry exceeding `maxBytes` alone is not retained). *(M)*
- [x] **`DataState.Empty` / observable deletion** — designed in RFC #23, shipped as a new
  sealed member emitted only to `CacheOnly` streams (initial miss and observed
  `invalidate`/`invalidateAll`); fetch-capable streams keep signalling through their
  refetch. Replaces the dishonest `Failure(CacheMissException)` miss emission;
  source-breaking for exhaustive `when`s, taken deliberately pre-1.0. *(M)*

## 0.3 — Network efficiency & resilience

Make the fetch path cheap and stampede-proof under real-world conditions.

- [x] **Conditional fetching (ETag / Last-Modified)** — shipped as
  `conditionalFetcher { key, validator -> FetchResult }` with `Fresh(value, validator)` /
  `NotModified`: validators are stored next to the value (memory, `PersistedEntry`, the
  JSON file store) and a 304 re-ages the entry under normal epoch fencing instead of
  re-downloading. `aquifer-okhttp` ships `okHttpConditionalFetcher` for automatic
  `ETag`/`If-None-Match` + `Last-Modified`/`If-Modified-Since` wiring. Plain fetchers keep
  their exact pre-existing path. *(L)*
- [x] **Negative caching** — shipped as opt-in `negativeCache { timeToLive,
  backoffMultiplier, maxTimeToLive }`: terminal failures suppress strategy-driven refetches
  for a per-key window (stale values still served, valueless reads fail fast with the
  remembered error), `NetworkOnly` bypasses, success/mutation clears, and the
  consecutive-failure streak — which survives window expiry — stretches the window.
  Suppressions are observable via `onFetchSuppressed`. *(M)*
- [x] **TTL jitter** — shipped as `freshness { ttlJitter = 0.1 }`: shorten-only (the
  configured TTL stays the hard cap, mirroring retry jitter's rule), with each entry's
  factor derived deterministically from its key and write timestamp — same-tick bursts
  spread, no fresh/stale flicker, nothing extra persisted, and restart-stable for keys
  with value-based `hashCode`s. `maxAge` overrides stay
  exact. *(S)*
- [x] **`prefetch(key)`** — shipped as `fun prefetch(key, freshness = CacheFirst)`:
  fire-and-forget warmup that returns immediately, honours the freshness fetch decision
  (a fresh entry triggers nothing), shares the single-flight fetch with concurrent
  reads, stands down under negative caching, and never throws (failures surface through
  events). *(S)*
- [x] **Batched fetching, phase 1** (RFC #29) — a `batchFetcher { keys -> Map }` builder
  option and `getAll(keys, freshness)` that collapses N keys into one backend call, reusing
  the per-key machinery (single-flight, fencing, negative caching, persistence, events) so
  batching is a pure transport optimization; a single `get`/`stream`/`prefetch` is a batch of
  one. Returns the resolved subset (per-key failures omitted, not thrown). *(L)*
- [x] **Batched fetching, phase 2 — coalescing window** (RFC #29) — the DataLoader window
  (`batchFetcher(coalesceWindow, maxBatchSize)`) that auto-batches individual
  `get`/`stream`/`prefetch` fetches: window/size-triggered flush, same-key slot sharing, and
  retry that re-enters the next window. *(M)*
- [x] **Batched fetching, phase 2 — remaining** (RFC #29) — shipped `streamMany(keys)` (the
  reactive twin of `getAll`, with the member keys' initial fetches batched into one immediate
  call), `prefetchAll(keys)` (the fire-and-forget batch warmup), and whole-batch retry: the store
  `retry` policy now wraps the multi-key `batchFetcher` call (retry-all — a partial map's omitted
  keys are definitive misses, never a retried slice), firing `onFetchRetried` per key and
  reporting the batch's attempt count to each key's `onFetchFailed`. Completes RFC #29 phase 2. *(M)*
- [x] **Conditional batch fetching** — shipped as
  `conditionalBatchFetcher { validators -> Map<K, FetchResult> }`, the batch mirror of
  `conditionalFetcher`: each key arrives mapped to its cached validator and may answer
  `NotModified`, so ETag/304 composes with `getAll`/`streamMany`/`prefetchAll` (and batch-of-one
  single reads). Whole-batch retry, per-key miss (`BatchKeyMissingException`), and the
  `NotModified`-without-validator contract violation all carry over; the auto-coalescing window
  stays `batchFetcher`-only. *(M)*
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
- [x] **`stats()` snapshot API** — shipped as `stats(): CacheStats`: non-suspending per-store
  counters (hits, misses, evictions, in-flight gauge, plus derived reads/hitRate), the numbers
  `AquiferEvents` can't aggregate. Counted at the caller-read chokepoints (get/getAll/stream
  prime); background revalidation and prefetch warmups are excluded. *(S)*
- [x] **`aquifer-test` module** — shipped: a published, programmable fake `Aquifer`
  (`fakeAquifer(scope) { … }` with scripted values/failures/delays and assertable fetch counts,
  re-scriptable at runtime) plus the deterministic `FakeClock` and the `settle()` helper, so
  consuming apps can unit-test their repositories the way this library tests itself — the
  unit-test sibling of `previewAquifer`. *(M)*
- [ ] **Coverage gate** — Kover + a CI threshold + badge. *(S)*
- [ ] **Docs site** — publish the aggregated Dokka output via GitHub Pages on release. *(S)*
- [ ] **Sample Android app** — a small Compose app demoing airplane-mode survival,
  pull-to-refresh coherence, and reconnect revalidation on a device. *(L)*

## 0.6 — API ergonomics & polish

Small, high-frequency conveniences surfaced while building the feature set; each must keep
the existing fencing and single-flight guarantees.

- [x] **`invalidateWhere { key -> Boolean }`** — shipped: predicate/bulk invalidation between the
  surgical `invalidate(key)` and the nuclear `invalidateAll()`, for "drop everything for this
  tenant/scope" resets. Each matched key is dropped and fenced under `commitGuard` exactly like
  `invalidate`, in one commit. The predicate is tested against the keys the store currently tracks
  in-process; persisted-only keys it has never touched are out of reach (use `invalidateAll`). *(S)*
- [x] **`putAll(entries)`** — shipped: bulk local write, the write-side mirror of `getAll` — seed
  many keys from a manually-fetched batch in one fenced commit, one broadcast per key, each key
  fenced exactly like `put`. *(S)*
- [x] **`snapshot()` / cached-key introspection** — shipped as `snapshot(): Set<K>`, a
  non-suspending peek at the keys resident in memory (`.size` is the live count), for debug
  overlays and eviction tuning; never suspends and never triggers I/O, and is safe to call on a
  closed store. Lists memory only (persisted-but-evicted keys excluded) and returns a stable
  copy. `MemoryCache` now guards its LRU map with a plain monitor (its critical sections never
  suspend) so the read needn't suspend. *(S)*

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
  the file store, iOS/desktop targets, an iOS sample, and lifting `aquifer-compose` to Compose
  Multiplatform. The largest differentiator on the list. *(XL)*
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
