# Multi-Raft + cross-shard 2PC (in-memory)

This document is the authoritative design for evolving the replicated hash table from **static owner/replica + 2PC** to **one Raft group per shard**, with **two-phase commit across shard leaders** when a single client operation (e.g. `put3`) touches more than one shard. **No persistence**: all state is RAM-only; processes that exit are gone forever.

---

## Goals

1. **Per-shard linearizability** for writes that go through the **current leader** of that shard’s Raft group.
2. **Automatic failover** inside a shard: leader crash triggers **election**; clients retry with **redirect hints** until they hit the new leader.
3. **Atomic `put3` across shards**: if the three keys map to **multiple shard ids** (`key % num_shards`), run **2PC** whose participants are the **leaders** of each involved shard; each prepare/commit/abort is **replicated via that shard’s Raft log** so followers stay consistent.
4. **Fast path**: keys for `put` / `put3` that all belong to **one shard** use **one Raft proposal** (no cross-shard 2PC).

Non-goals: disk WAL, dynamic membership changes, shrinking `N`, catch-up after restart.

---

## Sharding and Raft groups

- **Cluster**: `N` processes, indices `0 .. N-1`, full TCP mesh (existing connection setup).
- **Shard id**: `shard(key) = key % N` (same as today’s owner mapping; `N` is `all_nodes.size()`).
- **Raft group for shard `s`**: replica set  
  `members(s) = { s, (s+1) % N, (s+2) % N }`  
  (deduplicate if `N < 3`; quorum = `|members| / 2 + 1` with integer division).

Each physical node **hosts one `RaftEngine` per shard it belongs to**:

- Unique shard ids: `(my_idx - k + N) % N` for `k ∈ {0,1,2}` (dedupe), so at most **three** engines per node when `N ≥ 3`.

Only the **leader** of a shard may **propose** client-facing mutations for that shard; followers **apply** committed log entries to their local shard map.

---

## Log commands (`LogCmd` in `src/raft_rpc.hpp`)

| Command | Meaning |
|-------------|---------|
| `Noop`      | Heartbeat / placeholder. |
| `Put`       | Single key update for this shard. |
| `Put3`      | Up to three KV updates (same shard only). |
| `TxPrepare` | Phase 1 of cross-shard 2PC: reserve keys in `pending_txs` on all replicas. |
| `TxCommit`  | Phase 2: apply pending txn `tx_id` to KV store. |
| `TxAbort`   | Abort pending txn `tx_id`. |

Application of `TxPrepare` / `TxCommit` / `TxAbort` mirrors the previous `Node` pending-map logic, but **state lives per `RaftEngine`** (per shard).

---

## Wire protocol

### Application RPCs (`libs/utils.hpp`)

Existing: `GET`, `PUT`, `PUT3` (local benchmark path).

Internal (node-to-node), routed to the **current shard leader**:

- `INTERNAL_RAFT_PUT`, `INTERNAL_RAFT_PUT3` — carry payload in `Request_Full.inputs`; handler builds `LogEntryData` and calls `RaftEngine::propose`.
- `INTERNAL_RAFT_GET` — leader serves read from replicated state (after waiting out pending keys).
- `INTERNAL_TX_PREPARE`, `INTERNAL_TX_COMMIT`, `INTERNAL_TX_ABORT` — cross-shard 2PC; each message targets the **leader** of one shard (with redirect via `Response_Full.redirect_leader` if stale).

`Response_Full` includes **`redirect_leader`** (`int8_t`, `-1` if none): not leader or unknown leader → client/coordinator retries to that node id.

### Raft RPCs (`src/raft_rpc.hpp`)

Packed structs (`#pragma pack`), magic `RAFT_MAGIC`:

- `RequestVote` / `RequestVoteReply`
- `AppendEntries` / `AppendEntriesReply` (optional one `LogEntryData` per RPC)

Same TCP connections as today; **discriminate** frames by magic / `RaftRpcKind` vs normal `Request_Full` on the first bytes of each message.

**Node** exposes `send_raft_raw(dest, send_buf, recv_buf)` mirroring `send_request` locking.

---

## Client operation flow

### `get(key)`

1. `s = key % N`.
2. If this node has a `RaftEngine` for `s` and it is **leader**: read from engine state (shared lock + pending wait).
3. Else if local engine exists but **not leader**: forward `INTERNAL_RAFT_GET` to `leader_id()` (or first member of shard if unknown).
4. Else (node not in `members(s)`): forward to **`s`** (deterministic first member), which repeats the above.

### `put(key, val)` (single-shard)

1. `s = key % N`.
2. Build `LogEntryData{ Put, n_kv=1, ... }`.
3. On **leader** for `s`: `propose(entry)` (replicate + commit).
4. Else: `INTERNAL_RAFT_PUT` toward known leader; follow **redirect** up to a small retry cap.

### `put3` (multi-shard 2PC)

1. Partition the three KV pairs by `shard(key)`.
2. **One shard only**: one `LogEntryData{ Put3, ... }` and one `propose` (or internal forward).
3. **Multiple shards**:
   - `tx_id = next_tx_id()` (cluster-unique id).
   - **Phase 1**: For each involved shard `s` in **sorted order** (deadlock avoidance), send `INTERNAL_TX_PREPARE` to shard `s`’s leader with only the KVs for that shard. Leader runs `propose(TxPrepare{tx_id, subset})`. Any failure → **Phase 2a abort** on all shards that voted yes.
   - **Phase 2b**: On all prepares OK, for each shard send `INTERNAL_TX_COMMIT` with `tx_id`; leader `propose(TxCommit{tx_id})`.

Coordinator is whichever node runs the benchmark `put3`; it uses the same mesh RPCs as today.

---

## Concurrency and shutdown

- **Raft**: `RaftEngine` holds `raft_mtx_`; **never** hold `raft_mtx_` across `send_raft_raw` / `send_request` (avoid deadlocks with recv threads).
- **Ticker**: background thread (or shared tick in `Node::stop`) calls `RaftEngine::tick()` periodically for elections + heartbeats.
- **Shutdown**: set `running = false`; raft / recv loops must **not** treat intentional socket close as peer failure if you add liveness later.

---

## File map

| File | Role |
|------|------|
| `src/raft_rpc.hpp` | `LogEntryData`, packed Raft RPC structs, `RAFT_MTU`, helpers. |
| `src/RaftEngine.hpp` / `RaftEngine.cpp` | Per-shard Raft state, `propose`, apply, GET redirect helper, RPC handlers. |
| `src/Node.hpp` / `Node.cpp` | TCP mesh, `send_request` + `send_raft_raw`, dispatch raft vs app, `put` / `put3` / `get`, 2PC orchestration, engine ownership + tick thread. |
| `libs/utils.hpp` | `Op` enum extensions, `PendingTx`, `Response_Full.redirect_leader`. |
| `CMakeLists.txt` | Add `src/RaftEngine.cpp` to the executable. |

---

## Implementation checklist

Use this list to track what is done vs remaining in the repo.

- [x] `raft_rpc.hpp` with log entry types and packed Raft messages.
- [x] `RaftEngine` core: election, append entries, commit index, apply to KV + pending tx.
- [x] `utils.hpp`: internal ops + `redirect_leader` + `PendingTx`.
- [x] `Node` refactored: `std::vector<std::unique_ptr<RaftEngine>>`, shard → engine map, tick thread, `send_raft_raw`.
- [x] `recv_request`: branch on `RAFT_MAGIC` → raft dispatch; else `handle_request` with internal ops.
- [x] `put` / `put3` / `get` per flows above; `send_with_redirect` for leader hints.
- [x] `CMakeLists.txt` lists `RaftEngine.cpp`.
- [ ] Optional smoke test: `N ≥ 3` nodes, kill leader process, confirm redirects + new leader.

---

## Performance optimizations

### Already landed

- [x] **Parallel peer replication** — `replicate_new_entry` and `send_heartbeats` send AppendEntries to all peers concurrently (one thread per peer, joined before processing replies). Eliminated serial round-trip bottleneck. (`RaftEngine.cpp`)
- [x] **Faster tick / heartbeat** — Tick loop sleep reduced from 15 ms to 2 ms; heartbeat interval from 40 ms to 15 ms. Faster commit propagation and election response. (`RaftEngine.cpp`, `Node.cpp`)
- [x] **Increased concurrency** — Server workers 4 → 8; client request threads 2 → 4; client verify threads 2 → 4; max inflight 32 → 128. (`Server.hpp`, `Client.hpp`)
- [x] **Follower reads** — GETs served from any local replica (leader or follower) without redirect. Followers may lag leader commit index; acceptable for this workload. (`RaftEngine.cpp`, `Node.cpp`)

### Proposed (not yet implemented)

Listed in order of expected impact (highest first).

#### 1. ~~Remove `wait_until_key_not_pending` from GET path~~ [DONE]

**Where:** `RaftEngine::get_value()` calls `wait_until_key_not_pending(key)` before reading.

**Problem:** With a small key space (10 keys) and frequent cross-shard `put3`, reads block on pending 2PC transactions. Since followers are already allowed to be stale, blocking on pending state is unnecessary for reads.

**Change:** Remove the `wait_until_key_not_pending` call in `get_value()`. Reads return whatever value is currently applied, even if a `TxPrepare` is in flight for that key. This is consistent with the "followers may be stale" contract.

**Files:** `src/RaftEngine.cpp`

#### 2. ~~Persistent per-peer replication threads~~ [DONE]

**Where:** `replicate_new_entry()` and `send_heartbeats()` spawn and join `std::thread` objects on every call.

**Problem:** Thread creation/destruction overhead on every propose and every heartbeat (every 15 ms per shard, 3 shards = ~200 thread spawns/s just for heartbeats). This adds latency and CPU overhead.

**Change:** On `RaftEngine` construction, launch one long-lived replication thread per peer. Each thread blocks on a condition variable; when the leader appends a new log entry or a heartbeat fires, signal the CV. The thread wakes, sends the AppendEntries RPC, updates `match_index` / `next_index` under `raft_mtx_`, and signals back to the proposer via `apply_cv_`. On shutdown, set a flag and join.

**Files:** `src/RaftEngine.hpp`, `src/RaftEngine.cpp`

#### 3. Proposal batching

**Where:** `RaftEngine::propose()` appends one entry and then calls `replicate_new_entry()` synchronously.

**Problem:** If 8 server workers all propose concurrently on the same shard leader, each one independently replicates its single entry—8 serial replication rounds instead of 1.

**Change:** Introduce a proposal queue (lock-free or mutex-protected). A single "batcher" drains N pending proposals, appends all N entries to the log in one lock acquisition, then replicates once up to the latest index. All N proposers wait on `apply_cv_` for `commit_index >= their_target_index`. This amortizes the replication round-trip across multiple proposals.

**Files:** `src/RaftEngine.hpp`, `src/RaftEngine.cpp`

#### 4. ~~Separate Raft connections from application connections~~ [DONE]

**Where:** `send_raft_raw()` and `send_request()` both use `conns[dest][c]` with round-robin + mutex.

**Problem:** Raft heartbeats / AppendEntries and application-level RPCs (forwarded PUTs, cross-shard 2PC) compete for the same 4 sockets per peer. A slow application RPC blocks a Raft heartbeat (or vice versa), causing head-of-line blocking and potentially triggering false election timeouts.

**Change:** Allocate a second set of connections (`raft_conns`) during `establish_conns`, used exclusively by `send_raft_raw`. Application RPCs continue using the existing `conns`. Dedicated `run_raft` recv threads read only `RAFT_MTU`-sized frames on the Raft connections. Connection handshake uses high bit of the tag byte (`0x80`) to distinguish Raft from application connections on the acceptor side.

**Files:** `src/Node.hpp`, `src/Node.cpp`

#### 5. Length-prefixed framing (variable-size messages)

**Where:** Every `send_request` / `recv_request` / `send_raft_raw` reads and writes exactly `BUF_SIZE` (1024) or `RAFT_MTU` (1024) bytes.

**Problem:** Most messages are much smaller than 1024 bytes. A single-key PUT serializes to ~150 bytes; a heartbeat reply is ~30 bytes. Sending full 1024-byte buffers wastes ~6x bandwidth and increases syscall time (more bytes per `send`/`recv`).

**Change:** Prefix each frame with a `uint32_t` length. Sender writes `[4-byte len][payload]`; receiver reads 4 bytes, then reads exactly `len` bytes. Requires changing `recv_request` to do a two-phase read but reduces bytes on the wire significantly.

**Files:** `libs/utils.hpp`, `src/raft_rpc.hpp`, `src/Node.cpp`

#### 6. Increase `CONNS_PER_PEER`

**Where:** `Node::CONNS_PER_PEER = 4`.

**Problem:** With 8 server workers + Raft replication threads + heartbeat threads all contending on 4 mutexed sockets per peer, lock contention on `conn_mtx[dest][c]` becomes a bottleneck. Round-robin helps but doesn't eliminate it.

**Change:** Increase `CONNS_PER_PEER` to 8 (or 16). Simple constant change; costs more file descriptors and connection setup time but reduces runtime contention proportionally.

**Files:** `src/Node.hpp`

#### 7. Replace `shared_timed_mutex` with `shared_mutex`

**Where:** `RaftEngine::stripes_` uses `std::array<std::shared_timed_mutex, 256>`.

**Problem:** `std::shared_timed_mutex` has measurably higher overhead than `std::shared_mutex` on Linux (extra bookkeeping for timed lock variants). The codebase never calls `try_lock_for` or `try_lock_shared_for`—only `lock`, `unlock`, `lock_shared`, `unlock_shared`.

**Change:** Replace `std::shared_timed_mutex` with `std::shared_mutex` in the stripe array declaration and all lock types (`std::shared_lock<std::shared_mutex>`, `std::unique_lock<std::shared_mutex>`).

**Files:** `src/RaftEngine.hpp`, `src/RaftEngine.cpp`

#### 8. Parallel 2PC phases

**Where:** `Node::put3()` sends `INTERNAL_TX_PREPARE` to each shard sequentially (sorted order), then `INTERNAL_TX_COMMIT` sequentially.

**Problem:** A cross-shard `put3` touching 3 shards performs 6 sequential Raft proposals (3 prepares + 3 commits). Each proposal involves a network round-trip to the shard leader plus Raft replication latency. With 6 nodes, this dominates `put3` latency.

**Change:** Send all prepares in parallel (one `std::thread` or `std::async` per shard), wait for all results, then send all commits in parallel. This reduces the 2PC from `O(n_shards * 2)` sequential rounds to `O(2)` parallel rounds. The sorted-order deadlock avoidance is no longer needed since `insert_pending_tx` no longer blocks on key conflicts.

**Files:** `src/Node.cpp`

#### 9. Async commit phase (fire-and-forget)

**Where:** `Node::put3()` waits for every `INTERNAL_TX_COMMIT` to be proposed and committed via Raft before returning success.

**Problem:** Once all prepares succeed, the transaction is guaranteed to commit — no participant can unilaterally abort after voting yes. Waiting for commit proposals to replicate doubles the client-visible latency of every cross-shard `put3`.

**Change:** After all prepares succeed, return success to the caller immediately. Fire off the commit messages asynchronously (detached threads or a background queue). Commits are still replicated via Raft for consistency, but the client doesn't block on them. If the coordinator crashes before sending commits, a background recovery sweep (or timeout) on each shard can commit orphaned prepared transactions.

**Files:** `src/Node.cpp`

#### 10. Multi-entry AppendEntries

**Where:** Each `AppendEntriesRpc` carries at most 1 `LogEntryData` (`has_entry` + `entry` fields).

**Problem:** When a follower is behind or multiple proposals are batched, the replication thread sends one entry per RPC, requiring N round-trips to replicate N entries. Each round-trip pays full TCP + Raft protocol overhead.

**Change:** Replace the single `entry` field with a variable-length entry array (e.g., up to 8 or 16 entries per RPC). The replication thread packs all entries from `next_index[peer]` through `last_log_index()` into one RPC. The follower appends all of them and responds once. This requires changing the `AppendEntriesRpc` struct and the serialization format (length-prefix the entry array or use a fixed max batch size within `RAFT_MTU`).

**Files:** `src/raft_rpc.hpp`, `src/RaftEngine.hpp`, `src/RaftEngine.cpp`

#### 11. Proposal batching

*(Same as proposal 3 — repeated here for priority ordering after parallel 2PC.)*

**Where:** `RaftEngine::propose()` appends one entry and then calls `replicate_new_entry()` (wakes replication threads).

**Problem:** If 8 server workers all propose concurrently on the same shard leader, each independently appends to the log and wakes the replication threads. The threads process entries one at a time, but each proposer still pays its own wake-up and wait cycle.

**Change:** Introduce a proposal queue. A single batcher thread (or the first proposer to arrive) drains N pending proposals, appends all N entries to the log in one lock acquisition, then wakes replication threads once. All N proposers wait on `apply_cv_` for `commit_index >= their_target_index`. Combined with multi-entry AppendEntries (#10), this amortizes the full replication round-trip across N proposals.

**Files:** `src/RaftEngine.hpp`, `src/RaftEngine.cpp`

#### 12. Per-shard leader cache

**Where:** `Node::put()`, `Node::put3()`, `Node::get()` determine the shard leader by calling `engine_for_shard(s)->leader_id()` or falling back to `first_member_of_shard(s)`. When forwarding to a remote shard, the first hop often hits a non-leader, triggering a redirect.

**Problem:** Every forwarded operation potentially wastes one full application-level round-trip on a redirect before reaching the actual leader. With 6 shards and 6 nodes, most operations touch remote shards.

**Change:** Maintain a `std::array<std::atomic<int8_t>, MAX_SHARDS> leader_cache_` in `Node`. Populate it from `Response_Full.redirect_leader` hints and from local `RaftEngine::leader_id()`. On each forwarded PUT/PUT3/GET, send directly to `leader_cache_[shard]` instead of `first_member_of_shard`. Update the cache on every redirect response. This eliminates most redirect hops after the first operation per shard.

**Files:** `src/Node.hpp`, `src/Node.cpp`

---

## References (conceptual)

- Raft: leader election + log replication (Diego Ongaro’s thesis).
- Multi-Raft: many independent Raft groups, often co-located on shared nodes.
- Cross-shard atomicity: **two-phase commit** with **participants = shard leaders**, **prepare/commit replicated** in each shard’s log (not only in leader RAM).

---

*Last updated: plan authored for this repository; align code with the checklist above.*
