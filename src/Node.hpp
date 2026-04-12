#pragma once

#include "../libs/networking/asio.hpp"
#include "../libs/utils.hpp"
#include "raft_rpc.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class RaftEngine;

class Node {
public:
    static constexpr int CONNS_PER_PEER = 8;

    std::atomic<bool> running{true};

    Node(int port, const int argc, const char** argv);
    ~Node();

    bool put(const int32_t &key, const std::string &val);
    bool put3(const std::array<KV_Pair, 3> &kvs);
    std::string get(const int32_t &key);

    void recv_request();
    void stop();

    size_t send_request(int8_t dest, const Request_Full &req, Response_Full &resp);
    size_t send_raft_raw(int8_t dest, const std::array<uint8_t, RAFT_MTU> &send_buf,
                         std::array<uint8_t, RAFT_MTU> &recv_buf);

private:
    int PORT = 0;
    asio::io_context io_ctx;
    std::unique_ptr<asio::ip::tcp::acceptor> acceptor;

    std::vector<asio::ip::tcp::endpoint> all_nodes;
    std::vector<std::array<std::unique_ptr<asio::ip::tcp::socket>, CONNS_PER_PEER>> conns;
    std::vector<std::array<std::mutex, CONNS_PER_PEER>> conn_mtx;
    std::vector<std::atomic<uint32_t>> conn_rr;

    static constexpr int RAFT_CONNS_PER_PEER = 4;
    std::vector<std::array<std::unique_ptr<asio::ip::tcp::socket>, RAFT_CONNS_PER_PEER>> raft_conns;
    std::vector<std::array<std::mutex, RAFT_CONNS_PER_PEER>> raft_conn_mtx;
    std::vector<std::atomic<uint32_t>> raft_conn_rr;

    int8_t my_idx = 0;

    std::atomic<uint32_t> tx_counter{0};

    std::vector<std::unique_ptr<RaftEngine>> engines_;
    std::thread raft_tick_thread_;

    void parse_node_addrs(const int argc, const char** argv);
    void create_server(const int port);
    void establish_conns(int myIdx);
    void init_raft_engines();
    void raft_tick_loop();

    RaftEngine *engine_for_shard(int8_t shard_id);
    static int8_t first_member_of_shard(int8_t shard_id) { return shard_id; }

    uint64_t next_tx_id() {
        return (static_cast<uint64_t>(my_idx) << 32) | tx_counter.fetch_add(1);
    }

    void handle_request(const Request_Full &req, Response_Full &resp);
    void handle_raft_io(const std::array<uint8_t, RAFT_MTU> &in, std::array<uint8_t, RAFT_MTU> &out);

    bool send_with_redirect(int8_t &dest, Request_Full &req, Response_Full &resp, int max_hops = 12);

    bool propose_on_shard_leader(int8_t shard, LogEntryData entry,
                                 std::chrono::milliseconds timeout = std::chrono::seconds(2));

    std::string forward_get_with_redirect(int32_t key, int8_t dest);
};
