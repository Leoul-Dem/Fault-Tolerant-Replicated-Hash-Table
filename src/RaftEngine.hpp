#pragma once

#include "raft_rpc.hpp"
#include "../libs/ds/hashmap/phmap.hpp"
#include "../libs/utils.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class Node;

enum class RaftRole : uint8_t { Follower, Candidate, Leader };

class RaftEngine {
public:
    RaftEngine(Node* node, int cluster_n, int8_t self_id, int8_t shard_id);
    ~RaftEngine();

    void tick();

    bool is_leader() const {
        std::lock_guard<std::mutex> lk(raft_mtx_);
        return role_ == RaftRole::Leader;
    }

    int8_t leader_id() const {
        std::lock_guard<std::mutex> lk(raft_mtx_);
        return leader_id_;
    }

    int8_t shard_id() const { return shard_id_; }

    int8_t first_member() const { return members_.empty() ? int8_t(-1) : members_.front(); }

    /** Block until entry at log index is committed (or timeout). Leader only. */
    bool propose(LogEntryData e, std::chrono::milliseconds timeout);

    bool rpc_request_vote(const RequestVoteRpc& in, RequestVoteReplyRpc& out);
    bool rpc_append_entries(const AppendEntriesRpc& in, AppendEntriesReplyRpc& out);

    /** Local replica read (leader or follower); may lag leader commit index. */
    std::string get_value(int32_t key);

private:
    Node* node_;
    const int cluster_n_;
    const int8_t self_id_;
    const int8_t shard_id_;
    std::vector<int8_t> members_;
    std::vector<int8_t> peers_;
    int quorum_ = 1;

    mutable std::mutex raft_mtx_;
    RaftRole role_ = RaftRole::Follower;
    uint64_t current_term_ = 0;
    int8_t voted_for_ = -1;
    int8_t leader_id_ = -1;

    std::vector<LogEntryData> log_;
    uint32_t commit_index_ = 0;
    uint32_t last_applied_ = 0;

    std::unordered_map<int8_t, uint32_t> next_index_;
    std::unordered_map<int8_t, uint32_t> match_index_;

    std::chrono::steady_clock::time_point election_deadline_{};
    std::chrono::steady_clock::time_point last_leader_tick_{};

    std::mt19937 rng_;

    std::mutex apply_mtx_;
    std::condition_variable apply_cv_;

    static constexpr size_t NUM_STRIPES = 256;
    std::array<std::shared_timed_mutex, NUM_STRIPES> stripes_;
    gtl::parallel_flat_hash_map_m<int32_t, std::string> kv_;

    std::mutex pending_mtx_;
    std::condition_variable pending_cv_;
    std::unordered_map<uint64_t, PendingTx> pending_txs_;

    static size_t stripe(int32_t key) {
        return static_cast<uint32_t>(key) % NUM_STRIPES;
    }

    struct PeerReplicator {
        std::mutex mtx;
        std::condition_variable cv;
        bool wake = false;
        bool shutdown = false;
    };

    std::unordered_map<int8_t, std::unique_ptr<PeerReplicator>> repl_state_;
    std::vector<std::thread> repl_threads_;

    void peer_replication_loop(int8_t peer);
    void wake_replicators();

    void become_follower(uint64_t term);
    void become_candidate();
    void become_leader();

    uint32_t last_log_index() const;
    uint64_t last_log_term() const;
    uint64_t term_at(uint32_t idx) const;

    void election_reset_deadline();
    void send_heartbeats();
    bool append_entries_to(int8_t peer, uint32_t prev_log_index, bool with_entry,
 const LogEntryData* entry, AppendEntriesReplyRpc& reply);

    void replicate_new_entry(uint32_t new_index);
    void advance_commit_for_leader();
    void apply_through_commit();

    void apply_one(const LogEntryData& e);
    void wait_until_key_not_pending(int32_t key);
    void insert_pending_tx(uint64_t tx_id, PendingTx tx);
    void commit_tx(uint64_t tx_id);
    void abort_tx(uint64_t tx_id);

    bool log_up_to_date(uint32_t cand_last_idx, uint64_t cand_last_term) const;
};
