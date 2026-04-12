#pragma once

#include "../libs/utils.hpp"
#include <array>
#include <cstdint>
#include <cstring>

constexpr size_t RAFT_MTU = 1024;
constexpr uint32_t RAFT_MAGIC = 0x52465441u; // 'RFRA'

enum class RaftRpcKind : uint8_t {
    RequestVote = 1,
    RequestVoteReply = 2,
    AppendEntries = 3,
    AppendEntriesReply = 4,
};

enum class LogCmd : uint8_t {
    Noop = 0,
    Put = 1,
    Put3 = 2,
    TxPrepare = 3,
    TxCommit = 4,
    TxAbort = 5,
};

struct LogEntryData {
    uint64_t term = 0;
    LogCmd cmd = LogCmd::Noop;
    uint64_t tx_id = 0;
    uint8_t n_kv = 0;
    std::array<SocketKV, 3> kvs{};
};

#pragma pack(push, 1)
struct RequestVoteRpc {
    uint32_t magic = RAFT_MAGIC;
    RaftRpcKind kind = RaftRpcKind::RequestVote;
    int8_t shard_id = -1;
    uint64_t term = 0;
    int8_t candidate_id = -1;
    uint32_t last_log_index = 0;
    uint64_t last_log_term = 0;
};

struct RequestVoteReplyRpc {
    uint32_t magic = RAFT_MAGIC;
    RaftRpcKind kind = RaftRpcKind::RequestVoteReply;
    int8_t shard_id = -1;
    uint64_t term = 0;
    bool vote_granted = false;
};

struct AppendEntriesRpc {
    uint32_t magic = RAFT_MAGIC;
    RaftRpcKind kind = RaftRpcKind::AppendEntries;
    int8_t shard_id = -1;
    uint64_t term = 0;
    int8_t leader_id = -1;
    uint32_t prev_log_index = 0;
    uint64_t prev_log_term = 0;
    uint32_t leader_commit = 0;
    bool has_entry = false;
    LogEntryData entry{};
};

struct AppendEntriesReplyRpc {
    uint32_t magic = RAFT_MAGIC;
    RaftRpcKind kind = RaftRpcKind::AppendEntriesReply;
    int8_t shard_id = -1;
    uint64_t term = 0;
    bool success = false;
    uint32_t match_index = 0;
};
#pragma pack(pop)

inline bool raft_magic_ok(const void* p) {
    uint32_t m = 0;
    std::memcpy(&m, p, sizeof(m));
    return m == RAFT_MAGIC;
}

inline void raft_write(std::array<uint8_t, RAFT_MTU>& buf, const void* data, size_t n) {
    std::memset(buf.data(), 0, buf.size());
    std::memcpy(buf.data(), data, n);
}

static_assert(sizeof(RequestVoteRpc) <= RAFT_MTU);
static_assert(sizeof(RequestVoteReplyRpc) <= RAFT_MTU);
static_assert(sizeof(AppendEntriesRpc) <= RAFT_MTU);
static_assert(sizeof(AppendEntriesReplyRpc) <= RAFT_MTU);
