#pragma once

#include "../libs/networking/asio.hpp"
#include "../libs/utils.hpp"
#include "raft_rpc.hpp"
#include "ConnPool.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

class Raft;

class Node {
    static constexpr int APP_CONNS = 8;
    static constexpr int RAFT_CONNS = 8;

    int port = 0;
    asio::io_context io_ctx;
    std::unique_ptr<asio::ip::tcp::acceptor> acceptor;

    std::vector<asio::ip::tcp::endpoint> nodes;
    ConnPool<APP_CONNS> app_pool;
    ConnPool<RAFT_CONNS> raft_pool;

    int8_t idx = 0;

    std::atomic<uint32_t> tx_counter{0};

    std::vector<std::unique_ptr<Raft>> engines;
    std::thread tick_thread;

    void parse_addrs(const int argc, const char** argv);
    void create_server(int listen_port);
    void establish_conns(int my_idx);
    void init_engines();
    void tick_loop();

    Raft* engine_for(int8_t shard_id);
    static int8_t first_member_of(int8_t shard_id) { return shard_id; }

    uint64_t next_tx_id() {
        return (static_cast<uint64_t>(idx) << 32) | tx_counter.fetch_add(1);
    }

    void handle_request(const Request_Full& req, Response_Full& resp);
    void handle_raft_io(const std::array<uint8_t, RAFT_MTU>& in, std::array<uint8_t, RAFT_MTU>& out);

    bool send_with_redirect(int8_t& dest, Request_Full& req, Response_Full& resp, int max_hops = 12);

    bool propose_on_leader(int8_t shard, LogEntryData entry,
                           std::chrono::milliseconds timeout = std::chrono::seconds(2));

    std::string forward_get(int32_t key, int8_t dest);

public:
    std::atomic<bool> running{true};

    Node(int port, const int argc, const char** argv);
    ~Node();

    bool put(const int32_t& key, const std::string& val);
    bool put3(const std::array<KV_Pair, 3>& kvs);
    std::string get(const int32_t& key);

    void recv_request();
    void stop();

    size_t send_request(int8_t dest, const Request_Full& req, Response_Full& resp);
    size_t send_raft_raw(int8_t dest, const std::array<uint8_t, RAFT_MTU>& send_buf,
                         std::array<uint8_t, RAFT_MTU>& recv_buf);
};
