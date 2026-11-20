# Decision: the database store (v0.2)

Status: Proposed (2026-11-20). Adopted by ragentic, see its `docs/features.md`.

## Context

v0.1 has a database provider (`AshFeatureFlags.Provider.AshResource` +
`AshFeatureFlags.FlagStore`), but every evaluation that misses the per-actor
TTL cache (`AshFeatureFlags.Cache`) reads the table, and a change is only seen
when the TTL runs out — on each node separately. Users want:

- flags in their own database (Postgres or SQLite), nothing else to run;
- evaluation on the hot path with no database hit;
- a change visible on every node of a cluster within a second, with no TTL;
- a node that missed a message to heal by itself;
- targeting per tenant, per actor, per role, by percentage, with an expiry;
- an audit of who changed what.

## Decision

Add a **store**: a supervised process per flag table that loads the table into
an ETS table, evaluates from ETS with a pure function, writes through Ash, and
propagates changes to its peers. The old `AshResource` provider and the TTL
cache stay as they are for v0.1 users.

### 1. The flag resource

`AshFeatureFlags.FlagStore` keeps its v0.1 fields and adds (all with
`add_new_*`, so a resource may declare its own):

| Field | Type | Meaning |
| --- | --- | --- |
| `key` | string, unique | `^[a-z0-9][a-z0-9_.-]{0,63}$`; never turned into an atom |
| `description` | string | what it does, who owns it |
| `enabled` | boolean, default `false` | targeting on. Off = the flag is **off for everyone** (kill switch) |
| `default` | boolean, default `false` | the value when targeting is on and no rule matched (fallthrough), and after expiry |
| `allowed_tenants` | `{:array, :string}` | if non-empty, off for any other tenant (a gate, not a grant) |
| `allowed_actors` | `{:array, :string}` | targeting keys (ash_authentication subjects) the flag is on for |
| `allowed_roles` | `{:array, :string}` | roles the flag is on for |
| `rollout_percentage` | integer 0–100, nullable | on for this share of the bucket space |
| `rollout_by` | `:actor` \| `:tenant`, default `:actor` | what is hashed: one user, or a whole organization at a time |
| `expires_at` | utc_datetime_usec, nullable | after it, rules stop applying and `default` is served; telemetry flags it as stale |
| `variant` | string | v0.1 multivariate value, unchanged |
| `archived_at` | utc_datetime_usec, nullable | soft delete. Rows are never hard-deleted, so `version` stays monotonic and the audit stays joinable |
| `version` | integer, default 1 | +1 atomically on every change; the optimistic lock and the propagation order |
| `updated_by` | string | subject of the actor who made the last change |

Actions added: `read`, `by_key`, `create`, `set` (update; requires the
expected `version` argument, fails with `StaleRecord` on a mismatch), `archive`.
`set` and `create` write an audit row in the same transaction.

**Audit resource.** A new extension `AshFeatureFlags.FlagChange` turns a
resource into the change log: `key`, `version`, `actor` (subject string),
`changes` (map of field → `[before, after]`), `inserted_at`, `node`. The
store resource names it: `flag_store do audit_resource MyApp.Flags.FlagChange end`.

Postgres and SQLite are a data-layer choice, as in v0.1. Arrays on SQLite are
stored by AshSqlite as JSON; nothing in the store queries inside them.

### 2. Evaluation order

One pure function, `AshFeatureFlags.Rules.evaluate(compiled, context, now)`,
returns `{value, reason}`. The store adds the two outer steps:

| # | Step | Result | `reason` |
| --- | --- | --- | --- |
| 1 | Override for the key (store option `overrides`, e.g. from env) | its value | `:override` |
| 2 | DSL `disabled_for_roles` / `enabled_for_roles` on the guarding resource (v0.1, unchanged) | false / true | `:role` |
| 3 | No row (or archived) | the flag's declared `default` via `on_error` (v0.1 behaviour) | `:missing` |
| 4 | `enabled == false` | false | `:off` |
| 5 | `expires_at` ≤ now | `default` | `:expired` |
| 6 | `allowed_tenants` non-empty and tenant not in it | false | `:tenant` |
| 7 | targeting key in `allowed_actors` | true | `:actor` |
| 8 | a role in `allowed_roles` | true | `:role` |
| 9 | `rollout_percentage` set | `bucket < pct * 100` | `:rollout` |
| 10 | otherwise | `default` | `:default` |

**Bucket.** `bucket = :binary.decode_unsigned(binary_part(:crypto.hash(:sha256, key <> ":" <> unit), 0, 4)) rem 10_000`,
where `unit` is the targeting key (`rollout_by: :actor`, `"anonymous"` when
there is none) or the tenant (`:tenant`). Salted by the flag key so two 20%
flags pick different users; SHA-256 rather than `:erlang.phash2` so a
non-Erlang client (a CLI, a browser) computes the same bucket. Raising the
percentage only adds units; nobody who had it loses it.

Variants: step 7–10 deciding "on" returns the row's `variant`, as v0.1.

### 3. ETS layout

One table per store, named by the store, owned by the store process:
`[:set, :protected, :named_table, read_concurrency: true]`.

| Key | Value |
| --- | --- |
| `key` (binary) | `%AshFeatureFlags.Compiled{key, enabled, default, tenants :: MapSet, actors :: MapSet, roles :: MapSet, pct_bp :: 0..10_000 \| nil, rollout_by, expires_at_ms, variant, version}` |
| `{:"$override", key}` | `boolean` |
| `:"$meta"` | `%{checksum, loaded_at, status: :loaded \| :degraded, proto: 1}` |

- Reads (`Store.enabled?/3`, the provider) are an `:ets.lookup/2` plus the pure
  function: no message to the store process, no database, no per-actor cache.
  `AshFeatureFlags.Provider.Store` declares itself uncached, so the evaluator
  skips `AshFeatureFlags.Cache` for it (a TTL cache would reintroduce staleness).
- Only the store process writes, so applying changes is serialized; an
  incoming row replaces the entry only if its `version` is greater.
- Rows are compiled once on load/change (MapSets, basis points, ms epoch), so
  the hot path does no parsing.
- Why ETS and not `:persistent_term`: `:persistent_term` reads are faster, but
  every write triggers a global GC scan of all processes; flag writes are rare
  but tenants with many processes would pay for each one. ETS reads are
  ~100 ns, enough.

### 4. Propagation protocol

```
admin ──Store.set(store, key, changes, version:, actor:)──▶ Ash `set` (transaction:
      UPDATE … version = version + 1 WHERE version = $expected; INSERT audit row)
        │ commit
        ▼ after_transaction
local store: apply row (sync, so the writer reads its own write)
        │
        ▼ broadcast {:aff, 1, store, :changed, row_map}   (topic "ash_feature_flags:<store>")
peer stores: apply if row.version > local version; else ignore
```

- **Transport** (`AshFeatureFlags.Store.Broadcast` behaviour):
  `Phoenix.PubSub` when the `pubsub:` option names one; otherwise `:pg` (scope
  `:ash_feature_flags`, group = store name, `send/2` to each member); `:local`
  when neither or on a single node. PubSub is an optional dependency.
- The message carries the **whole row**, so peers do not read the database to
  apply it. Archiving is a change like any other (`archived_at` set).
- Messages are unordered and may be lost; the per-flag `version` makes
  re-delivery and reordering harmless, and self-healing covers loss.
- `proto: 1` in the message; a store that receives an unknown proto reloads
  from the database instead of applying it (rolling upgrades).

**Self-healing.** The database is the only source of truth; ETS is a replica.

| Trigger | Action |
| --- | --- |
| Boot (`init/1`, before the endpoint starts) | full load; blocks up to `boot_timeout` (5 s) |
| `nodeup` (`:net_kernel.monitor_nodes/1`) | full reload — we may have missed messages while apart |
| Every `sync_interval` (30 s, ±20% jitter) | `SELECT count(*), coalesce(sum(version), 0)` (an Ash aggregate); if it differs from `:"$meta".checksum`, full reload |
| Unknown `proto`, or a failed apply | full reload |

Because versions only grow and rows are never deleted, `(count, sum(version))`
changes on every write: a node that missed a message converges within
`sync_interval`. A full reload builds the new set, inserts it, then deletes
keys no longer present (never an empty window).

### 5. Failure modes

| Failure | Behaviour |
| --- | --- |
| DB down at boot | Store starts `:degraded` after `boot_timeout`: overrides, then each flag's declared `default`. Retries with backoff 1 s → 30 s. `[:ash_feature_flags, :store, :sync_error]` each failure. Booting is not blocked forever |
| DB down while running | Keeps serving the last load. Writes fail with the DB error. Checksum errors are logged; reload on recovery |
| Commit succeeds, broadcast lost / writer crashes before broadcasting | Peers heal by checksum within `sync_interval` |
| Netsplit (both sides reach the DB) | The DB serializes writes; the side that did not get the message heals by checksum, and fully on `nodeup` |
| Netsplit (a node loses the DB) | It serves its last-known flags, consistent but possibly stale; it cannot write |
| Two admins edit the same flag | Optimistic lock on `version`: the second gets `StaleRecord` and must reload |
| Reordered or duplicate messages | Ignored unless `version` is newer |
| Clock skew | `expires_at` is judged by each node's clock; seconds of skew are accepted |
| Mixed versions during a deploy | Unknown `proto` → reload from DB |

There is no split-brain *write* conflict by construction: nodes never accept
writes into ETS that did not first commit to the database.

### 6. Telemetry

- `[:ash_feature_flags, :store, :evaluate]` — `%{count: 1}`, metadata
  `%{store, key, value, reason}`. A single `execute`, not a span: it runs on
  every guarded action.
- `[:ash_feature_flags, :store, :change]` — metadata `%{store, key, version, actor, origin: :local | :remote, node}`
- `[:ash_feature_flags, :store, :reload]` — `%{duration, count}`, metadata `%{store, reason: :boot | :nodeup | :checksum | :proto | :retry}`
- `[:ash_feature_flags, :store, :sync_error]` — metadata `%{store, error}`
- `[:ash_feature_flags, :store, :expired]` — once per key per node when a flag
  is first evaluated past `expires_at` (stale-flag reports)
- The v0.1 `[:ash_feature_flags, :evaluate, *]` span still fires for guards.

### 7. API

```elixir
children = [
  MyApp.Repo,
  {Phoenix.PubSub, name: MyApp.PubSub},
  {AshFeatureFlags.Store,
   name: MyApp.Flags,
   resource: MyApp.Flags.Flag,
   pubsub: MyApp.PubSub,          # or omit: :pg, or :local
   overrides: %{"registration" => true},
   sync_interval: 30_000},
  MyAppWeb.Endpoint
]

config :ash_feature_flags, provider: {AshFeatureFlags.Provider.Store, store: MyApp.Flags}

AshFeatureFlags.Store.enabled?(MyApp.Flags, "new_checkout", actor: user, tenant: org_id)
AshFeatureFlags.Store.explain(MyApp.Flags, "new_checkout", actor: user)   #=> {true, :rollout}
AshFeatureFlags.Store.list(MyApp.Flags)                                    # compiled rows + overrides
AshFeatureFlags.Store.set(MyApp.Flags, "new_checkout", %{rollout_percentage: 20}, version: 3, actor: admin)
AshFeatureFlags.Store.history(MyApp.Flags, "new_checkout")
```

Guards (`guard_action`, `guard_attribute`, `flag_enabled/1`) are unchanged:
they reach the store through the provider.

### 8. Tests

- `Rules` unit tests for every row of the evaluation table; a property test
  that buckets are stable, salted per key, monotonic in percentage and roughly
  uniform (±2% over 100k units).
- Store tests on `Ash.DataLayer.Ets`; integration suites on AshPostgres and
  AshSqlite in CI (two jobs).
- Multi-node with `:peer` (3 nodes, `:pg` and PubSub transports): a change on
  one node is visible on all within 1 s; a change written straight to the DB
  (no broadcast) heals by checksum; a node disconnected during a change heals
  on `nodeup`; an out-of-order message is ignored; DB unavailable at boot
  starts `:degraded` and recovers.

### 9. Release

`v0.2.0` on Hex from this repository (fix `@source_url`, add `docs/` to the
ExDoc extras, CHANGELOG). Publishing needs the maintainer's Hex key; until
then adopters pin the git tag `v0.2.0`.

## Alternatives considered

- **Postgres `LISTEN/NOTIFY`** for propagation: no extra process, but it does
  not work through PgBouncer in transaction mode and SQLite has no
  equivalent. May come later as one more `Broadcast` transport.
- **Keep the per-actor TTL cache**: staleness equals the TTL on each node, and
  a short TTL means a DB hit per actor per TTL. Rejected for the store.
- **Mnesia / CRDTs (Horde, DeltaCrdt)**: a second source of truth to reconcile
  with the database. Rejected: the database already orders writes.
- **Hard deletes**: would make `sum(version)` non-monotonic and leave
  tombstones to manage. Rejected for soft archive.

## Consequences

- Easier: live flags with no extra service; same code on one SQLite node and on
  a Postgres cluster; an audit trail for free.
- Harder: the store must start before anything that evaluates flags; ETS holds
  every flag in memory on every node (fine for thousands, not millions);
  staleness after a lost message is bounded by `sync_interval`, not zero.
