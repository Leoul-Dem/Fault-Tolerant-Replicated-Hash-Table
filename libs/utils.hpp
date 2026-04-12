#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <array>
#include <utility>
#include <vector>

// Max value size for socket format
constexpr size_t MAX_VAL_SIZE = 128;
constexpr size_t BUF_SIZE = 1024;

enum Op : uint8_t {
    PUT,
    GET,
    PUT3,
    PREPARE_PUT,
    PREPARE_PUT3,
    COMMIT,
    ABORT,
    INTERNAL_RAFT_PUT,
    INTERNAL_RAFT_PUT3,
    INTERNAL_RAFT_GET,
    INTERNAL_TX_PREPARE,
    INTERNAL_TX_COMMIT,
    INTERNAL_TX_ABORT,
};

struct KV_Pair {
    int32_t key;
    std::string value;
};

struct PendingTx {
    std::vector<std::pair<int32_t, std::string>> kvs;
};

struct SocketKV {
    int32_t key;
    char value[MAX_VAL_SIZE];
};

struct Request_Full {
    uint32_t id;
    uint64_t tx_id;            
    int8_t src;
    int8_t dest;
    Op op;
    uint8_t input_count;       
    std::array<SocketKV, 3> inputs;
};

struct Response_Full {
    uint32_t id;
    uint64_t tx_id;
    int8_t src;
    int8_t dest;
    bool success;
    char output[MAX_VAL_SIZE];
    int8_t redirect_leader; // if not leader / stale, hint current leader id; else -1
};

struct Request_Cut {
    uint32_t id;
    Op op;
    uint8_t input_count;
    std::array<SocketKV, 3> inputs;
};

struct Response_Cut {
    uint32_t id;
    bool success;
    char output[MAX_VAL_SIZE];
};

static_assert(sizeof(Request_Full) <= BUF_SIZE);
static_assert(sizeof(Response_Full) <= BUF_SIZE);

inline void serialize(const Request_Full &req, std::array<uint8_t, BUF_SIZE> &buf) {
    std::memcpy(buf.data(), &req, sizeof(Request_Full));
}

inline void deserialize(const std::array<uint8_t, BUF_SIZE> &buf, Request_Full &req) {
    std::memcpy(&req, buf.data(), sizeof(Request_Full));
}

inline void serialize(const Response_Full &resp, std::array<uint8_t, BUF_SIZE> &buf) {
    std::memcpy(buf.data(), &resp, sizeof(Response_Full));
}

inline void deserialize(const std::array<uint8_t, BUF_SIZE> &buf, Response_Full &resp) {
    std::memcpy(&resp, buf.data(), sizeof(Response_Full));
}


