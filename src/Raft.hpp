#pragma once

#include "raft_rpc.hpp"
#include "Log.hpp"
#include "Election.hpp"
#include "HashTable.hpp"

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class Node;

class Raft {
    Node* node;
    const int cluster_size;
    const int8_t id;
    const int8_t shard;
    std::vector<int8_t> members;
    std::vector<int8_t> peers;
    int quorum = 1;

    mutable std::mutex mtx;
    Election election;
    Log log;

    uint32_t commit_idx = 0;
    uint32_t applied = 0;

    std::unordered_map<int8_t, uint32_t> next_idx;
    std::unordered_map<int8_t, uint32_t> match_idx;

    std::condition_variable apply_cond;

    HashTable table;

    struct PeerReplicator {
        std::mutex mtx;
        std::condition_variable cv;
        bool wake = false;
        bool shutdown = false;
    };

    std::unordered_map<int8_t, std::unique_ptr<PeerReplicator>> repl_state;
    std::vector<std::thread> repl_threads;

    void peer_replication_loop(int8_t peer);
    void wake_replicators();

    void run_election();
    void send_heartbeats();
    bool append_entries_to(int8_t peer, uint32_t prev_log_index, bool with_entry,
                           const LogEntryData* entry, AppendEntriesReplyRpc& reply);

    void replicate_new_entry(uint32_t new_index);
    void advance_commit();
    void apply_committed();

    void apply_one(const LogEntryData& e);

public:
    Raft(Node* node, int cluster_n, int8_t self_id, int8_t shard_id);
    ~Raft();

    void tick();

    bool is_leader() const {
        std::lock_guard<std::mutex> lk(mtx);
        return election.role() == RaftRole::Leader;
    }

    int8_t leader_id() const {
        std::lock_guard<std::mutex> lk(mtx);
        return election.leader_id();
    }

    int8_t shard_id() const { return shard; }

    int8_t first_member() const { return members.empty() ? int8_t(-1) : members.front(); }

    bool propose(LogEntryData e, std::chrono::milliseconds timeout);

    bool rpc_request_vote(const RequestVoteRpc& in, RequestVoteReplyRpc& out);
    bool rpc_append_entries(const AppendEntriesRpc& in, AppendEntriesReplyRpc& out);

    std::string get_value(int32_t key);
};
