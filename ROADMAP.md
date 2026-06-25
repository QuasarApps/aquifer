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
- [ ] **Repo hygiene** *(owner action — S)* — branching model is settled: `develop` is the
  integration branch that every PR targets, and `main` is release-only (releases are cut by
  pushing a `v*` tag). Remaining owner action: set `develop` as the GitHub default branch so
  PRs and Dependabot target it by default, keeping `main` protected. The earlier Dependabot
  toolchain bumps (#7–#11, #19, #20) and the #44/#45 follow-ups are all resolved.
- [ ] **Maven Central badge + install snippet verification** after the first release. *(S)*
- [x] **JDK 11/17/21 CI matrix** (shipped — #46) — CI ran only Temurin 21, but every
  module compiles to JVM-11 bytecode and CONTRIBUTING promises JDK-17 builds; neither is
  actually tested, so a newer-API slip or 11-incompatible bytecode could ship undetected. A
  cheap matrix turns two stated-but-unverified compatibility claims into tested guarantees
  before the first artifact reaches users on older toolchains and before the 1.0 bytecode
  contract locks. *(S)*
- [x] **Fence fetches at registration (correctness fix, shipped — #42)** — `refreshWith`
  captured the fetch's epoch in the lazily-started body, which runs *after* `inFlight.putIfAbsent`;
  a `put`/`invalidate` in that gap bumped the epoch but the fetch then read the *post-bump* epoch,
  so its commit passed the epoch check and overwrote the just-written local value — a silent loss
  of a user's write in the headline "never resurrect deleted/edited data" guarantee. Fixed by
  capturing the epoch before `scope.async` (it only ever fails safe), with a deterministic
  register-then-fence interleaving test `MutationFencingTest` didn't cover. *(S)*

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
- [ ] **Multi-key Compose binding** — `collectAsState(keys): State<Map<K, DataState<V>>>` and
  `rememberStreamMany`, the Compose counterparts to the shipped `streamMany`/`getAll`: one
  lifecycle-aware collector for a list or grid screen instead of a per-item collector that
  restarts on scroll (or hand-rolling `collectAsStateWithLifecycle` over the raw `Flow`). The
  engine and `previewAquifer` already implement `streamMany`; only the Compose binding is
  missing. The lighter, in-scope half of multi-key support — distinct from the deferred Paging
  bridge. *(M)*
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
- [x] **Typed OkHttp errors** (shipped — #48) — `okHttpConditionalFetcher` collapsed every
  non-2xx/304 into a bare `IOException` whose only status signal was the message string, so
  `retry`'s `retryOn` predicate and `negativeCache` branched blind to status: a permanent 404
  burned every retry attempt. Now throws a typed `HttpException(code, url)` (still an
  `IOException`, so it flows through the normal failure path) that callers can branch on to route
  404 → empty, retry only 5xx, etc. *(S)*
- [x] **Plain `okHttpFetcher` + public `Call.await` seam** (shipped — #49) — a non-conditional
  OkHttp fetcher for backends without ETag/Last-Modified validators, plus the suspend
  `Call.await` bridge promoted to public API, so a plain JSON-over-OkHttp fetcher no longer has
  to be hand-rolled. *(S)*
- [x] **Cache-Control-aware freshness** (design first; shipped — #50, #51) — an origin's
  `Cache-Control`/`Expires` can inform the staleness decision under an explicit precedence
  (per-call `maxAge` > server `freshFor` > builder `timeToLive`; server freshness is never
  jittered). Core seam #50 added `FetchResult.Fresh(value, validator, freshFor)` — stored next to
  the value, carried forward across a 304, persisted in `PersistedEntry`/the JSON file store;
  #51 wired `okHttpConditionalFetcher(respectCacheControl = true)` to parse `max-age` (minus
  `Age`), `no-store`/`no-cache`/`max-age=0` → immediately stale, and `Expires` as a fallback.
  Opt-in, so *the app declares how fresh data must be* stays the default stance. *(M)*
- [ ] **#12 — benchmark, then stripe the commit guard** — JMH-style harness for concurrent
  commit throughput against a real file store; implement per-key lock striping only if the
  numbers justify it (constraints documented in the issue). *(M–L)*

## 0.4 — Persistence expansion

Meet apps where their storage already is. The two SPI capabilities come first: both adapters'
whole point (native batched transactions, a disk-wide `invalidateWhere`) is inexpressible
through today's single-key `SourceOfTruth`, so building the adapters first would either hardcode
N-round-trip behavior or force a contract break mid-milestone.

- [ ] **Bulk `SourceOfTruth` capability** — optional `readAll(keys)` / `writeAll(entries)` /
  `deleteMany(keys)` on the SPI, defaulting to the current per-key loop. Today `getAll`,
  `putAll`, and `invalidateWhere` do N storage round-trips (per-key `read`/`write`/`delete`),
  and a queryable backend cannot express its native batched transaction or `IN` query through
  the four single-key methods. Default implementations keep the JSON file store and existing
  custom stores source-compatible, and make the already-shipped batch paths batch at the
  storage layer too. **Prerequisite for the adapters below.** *(M)*
- [ ] **Key-enumeration capability** — an opt-in `keys()` / `keysWhere(...)` seam so a queryable
  store can back a *disk-wide* `invalidateWhere` instead of today's in-process-keys-only
  predicate. The JSON file store opts out by design: its filenames are one-way SHA-256 of the
  key, so enumeration would demand a separate key→hash manifest with its own crash-consistency
  story — exactly what the LRU index deliberately avoids. **Shapes the adapters below.** *(L)*
- [ ] **Proto DataStore adapter** (`aquifer-persistence-datastore`) — the modern AndroidX
  default; builds on the bulk + enumeration capabilities above. *(M)*
- [ ] **SQLDelight adapter** — queryable persistence (its enumerability backs a correct
  disk-wide `invalidateWhere`) and the natural stepping stone to multiplatform. *(M)*
- [x] **Encryption hook** — shipped as a `cipher: ValueCipher?` on `JsonFileSourceOfTruth`: a
  two-method `encrypt`/`decrypt` seam applied to each entry's serialized bytes, depending on
  nothing beyond the JDK so Google Tink's `Aead` (Android Keystore) plugs in through a thin
  adapter. The on-disk bytes and the `maxBytes` budget are the ciphertext; a `decrypt` that
  throws `GeneralSecurityException` heals the slot. Composes with bounding, conditional
  fetching, and `schemaVersion`/`migrate`. *(M)*
- [x] **Schema-migration helper** — shipped on `JsonFileSourceOfTruth` as `schemaVersion` +
  `migrate(fromVersion, value)`: writes are stamped with the current version, and an entry read
  back at a lower version is passed to the callback to rewrite its stored JSON to the current
  shape (lazily on read; rewritten in the new format on the next write). Returning `null` drops
  the entry — as does one stored above the current version (an app downgrade) — so breaking
  model changes stop meaning "wipe the cache directory". A version-0 store (the default) writes
  no version field: byte-for-byte the previous on-disk format. *(M)*

## 0.5 — Proof-grade quality & observability

The engine's guarantees deserve machine-checked evidence.

- [ ] **Lincheck concurrency tests** — model-check the fencing/single-flight invariants
  (linearizability of put/invalidate/fetch-commit) instead of relying on hand-written
  interleavings; the strongest possible backing for the epoch design. The register-then-fence
  window fixed in #42 (a latent epoch-capture race the hand-written `MutationFencingTest` missed
  for months) is exactly the kind of bug this would have caught mechanically — its successor
  windows belong in the target set. *(L)*
- [ ] **#13 — bounded `keyEpochs` *and* the negative-cache map** — implement the live-fetch
  refcount eviction sketched in the issue, with the proof written down (the naive evictions are
  provably unsound). Fold in the `negative` map: it has the identical unbounded-growth
  lifecycle (records are reclaimed only on success/`put`/`invalidate`, and an *expired* record
  is deliberately kept to preserve the failure streak), so a wide key space of one-time
  failures — a search/autocomplete store hitting transient 5xx — retains an entry per key
  forever. Same eviction reasoning, one proof, no second leak shipped after the first. *(M)*
- [x] **`stats()` snapshot API** — shipped as `stats(): CacheStats`: non-suspending per-store
  counters (hits, misses, evictions, in-flight gauge, plus derived reads/hitRate), the numbers
  `AquiferEvents` can't aggregate. Counted at the caller-read chokepoints (get/getAll/stream
  prime); background revalidation and prefetch warmups are excluded. *(S)*
- [x] **`aquifer-test` module** — shipped: a published, programmable fake `Aquifer`
  (`fakeAquifer(scope) { … }` with scripted values/failures/delays and assertable fetch counts,
  re-scriptable at runtime) plus the deterministic `FakeClock` and the `settle()` helper, so
  consuming apps can unit-test their repositories the way this library tests itself — the
  unit-test sibling of `previewAquifer`. *(M)*
- [ ] **Coverage gate** — Kover + a CI threshold + badge. CI runs the (substantial) test suite
  but captures no coverage signal, so there is no machine-readable evidence of which branches of
  the branchy fencing/eviction/negative-cache logic are exercised, and no guard against erosion.
  *(S)*
- [ ] **Docs site** — `dokkaGenerate` already runs in CI as a compile check; only the GitHub
  Pages deploy step is missing. Publish the aggregated HTML on release for a versioned, browsable
  API reference. *(S)*
- [ ] **`streamMany` scale ceiling — document, then guard** — `streamMany` opens one
  bus-collector coroutine (each with an unbounded buffer) per member and rebuilds the whole
  result `Map` on every per-key change: O(N) work per emission and O(N) live buffers, and a
  member set larger than `memoryCache.maxEntries` (default 256) thrashes. Document the
  interaction and that it is *not* a paging replacement, add a characterization test at large
  member counts, and consider a soft cap or chunked/diff emission. *(M)*
- [ ] **Docs-accuracy pass** — fix the inconsistencies the project review surfaced: link the
  cited RFC/issue numbers (#12, #13, #23, #29) to their GitHub items instead of citing bare
  internal numbers; surface `fakeAquifer`'s deliberate divergences (no TTL, no single-flight
  dedup, `CacheStats.EMPTY`) in the README testing section, not just the CHANGELOG; align the
  `collectAsState` initial-state wording (`Loading(null)`) across README/CHANGELOG/ROADMAP;
  state "JVM/Android only today" prominently near the top of the README; and fix the stale
  `TestHelpers.kt` reference in CONTRIBUTING (the helper now lives in `aquifer-test`). *(S)*
- [ ] **Sample Android app** — a small Compose app demoing airplane-mode survival,
  pull-to-refresh coherence, and reconnect revalidation on a device. *(L)*

## 0.6 — API ergonomics & polish

Small, high-frequency conveniences surfaced while building the feature set; each must keep
the existing fencing and single-flight guarantees.

- [ ] **`evictMemory()` / `trimToSize(n)`** — shed the in-memory tier without touching
  persistence (rehydrating from disk on the next read), so a long-lived store can answer
  Android's `onTrimMemory`/`onLowMemory`. Today only `invalidateAll` drops memory, and it wipes
  persistence too; the memory cache is purely count-bounded (`maxEntries`) with no size or
  pressure awareness. Composes with the existing fenced commit and hydrating `load()`. An
  optional proactive memory-TTL sweep (drop entries past their TTL instead of waiting for LRU
  pressure) is a natural companion. *(M)*
- [ ] **Key-scoped policy resolver** — let one store apply heterogeneous TTL (and later
  retry/negative-cache) by key subtype — `freshness { timeToLiveFor = { key -> … } }` — instead
  of spinning up a separate `Aquifer` per policy (which duplicates the memory cache, scope, and
  persistence wiring). Per-call `maxAge` already covers the read-time staleness bar but not
  retry/jitter/negative-cache, and must be threaded through every call site. This is meaningful
  new public surface, so land it *with* (or just after) the 1.0 API-freeze review; a
  freshness-only first slice can validate the seam before adding retry/negative resolvers. *(L)*
- [ ] **Tag/group invalidation** — an opt-in tag index so a write can drop every key carrying a
  tag without the caller enumerating them — the relationship-invalidation ergonomic TanStack
  (key patterns) and RTK Query (`providesTags`/`invalidatesTags`) make first-class. Strictly a
  tag index, **not** response normalization (a declared non-goal). Sequence *after* the
  key-enumeration capability so it can be disk-correct rather than carrying today's
  `invalidateWhere` in-process-only caveat. *(M)*
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
- [ ] **Semver policy + CHANGELOG discipline** documented; release-notes automation — the
  release workflow publishes to Maven Central but never cuts a GitHub Release, so watchers get
  no signal beyond a bare tag. Auto-create one from the tagged CHANGELOG section. *(S)*
- [ ] **"Coming from…" guides** — migration recipes from Store5 and from hand-rolled
  repository patterns; this is how libraries actually get adopted. The Store5 mapping
  (`Fetcher`→`fetcher`, `SourceOfTruth`→`SourceOfTruth`, `StoreRequest`→`Freshness`) is the
  concrete unblocker for the most likely switchers. *(M)*
- [ ] **SECURITY.md + issue/PR templates.** A published artifact with an encryption-at-rest
  feature and no private vulnerability-disclosure path is a 1.0 gap. *(S)*
- [ ] **Supply-chain hardening** — a `dependency-review-action` gate and a CodeQL workflow on
  PRs (Dependabot bumps versions but does not CVE-alert the existing tree), GitHub Actions
  pinned to commit SHAs, and build-provenance/SLSA attestation on the release artifacts (the
  release job currently has no top-level `permissions` block and signs only with the Maven PGP
  signature). Cheap, standard insurance for a widely-embeddable library. *(S)*

## Beyond 1.0 — strategic bets

- [ ] **Kotlin Multiplatform core** — dispatcher-clean coroutines are the easy half; the
  engine's JVM concurrency also has to move: the `ConcurrentHashMap` CAS loops (`inFlight`,
  `activeKeys`, `keyEpochs`), `AtomicLong`/`AtomicBoolean`, and the `LinkedHashMap`-based
  LRU need KMP equivalents (atomicfu, mutex-guarded maps), plus a `kotlinx-io`/okio port of
  the file store, iOS/desktop targets, an iOS sample, and lifting `aquifer-compose` to Compose
  Multiplatform. The load-bearing portability risk is the file store's durability guarantee:
  it rests on `FileChannel.force(true)` + `ATOMIC_MOVE`; okio's `FileSystem.atomicMove` provides
  the rename but a portable `fsync` is not yet a given, and the SHA-256 filename needs a
  multiplatform hash (okio `HashingSink` or kotlin-crypto) — scope the backend before committing.
  The largest differentiator on the list. *(XL)*
  - [ ] **Ktor client fetcher helper** (`aquifer-ktor`) — a sub-step of this bet, not a
    free-standing item: a `ktorConditionalFetcher`/`ktorFetcher` mirroring the OkHttp helper
    (ETag/Last-Modified ↔ `If-None-Match`/`If-Modified-Since`, 304 → `NotModified`, sharing the
    typed-status-error contract). OkHttp is JVM-only, so this is what serves non-JVM targets;
    build it alongside the `kotlinx-io`/okio file-store port so a real target exercises it. *(M)*
- [ ] **Offline mutations** (`aquifer-mutations`) — the write-side counterpart to Aquifer's
  read-side: an optimistic-update queue with rollback and conflict hooks, surviving process
  death via the same `SourceOfTruth` machinery. This is the single biggest capability gap vs
  both incumbents (Store5's `MutableStore`/`Updater`/`Bookkeeper`; TanStack/RTK `useMutation`
  with optimistic update + rollback), and the current `put()` optimistic-write README example
  already invites the expectation. Consider pulling a **minimal optimistic-`put`-with-rollback
  slice** forward as its own smaller item to de-risk the top differentiator before the full
  module — note it leans on the registration-fencing fix (#42), since a racing in-flight fetch
  must not clobber an optimistic local write. *(XL)*
- [ ] **Paging bridge** (`aquifer-paging`) — keyed page caching behind AndroidX Paging 3.
  *(L)*

## Non-goals

Declared so the scope stays honest: image/blob caching (use Coil), cross-process shared
caches, full sync engines/CRDTs, GraphQL response normalization, reflection-based
serialization, and Java-first API surface.

---

Suggestions welcome — open an issue. Roadmap ordering is by expected leverage and gets
re-sorted as reality disagrees.
