#include "Node.hpp"
#include "Raft.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <sys/socket.h>
#include <thread>
#include <vector>

static void set_sock_timeout(asio::ip::tcp::socket& sock, int seconds) {
    struct timeval tv;
    tv.tv_sec = seconds;
    tv.tv_usec = 0;
    int fd = sock.native_handle();
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

static void fill_socket_kv(SocketKV& skv, int32_t key, const std::string& val) {
    skv.key = key;
    std::memset(skv.value, 0, MAX_VAL_SIZE);
    std::memcpy(skv.value, val.data(), std::min(val.size(), MAX_VAL_SIZE));
}

Node::Node(int port, const int argc, const char** argv)
    : port(port) {
    parse_addrs(argc, argv);
    create_server(port);
    establish_conns(idx);
    init_engines();
}

Node::~Node() {
    if (tick_thread.joinable()) {
        running.store(false);
        tick_thread.join();
    }
}

void Node::parse_addrs(const int argc, const char** argv) {
    idx = static_cast<int8_t>(std::atoi(argv[1]));

    for (int i = 3; i < argc; i++) {
        std::string arg(argv[i]);
        auto colon = arg.rfind(':');
        if (colon == std::string::npos) {
            std::fprintf(stderr, "Bad address (expected ip:port): %s\n", argv[i]);
            std::exit(1);
        }
        std::string ip = arg.substr(0, colon);
        int p = std::atoi(arg.substr(colon + 1).c_str());
        asio::ip::tcp::endpoint ep(asio::ip::make_address(ip), static_cast<unsigned short>(p));
        nodes.push_back(ep);
    }
}

void Node::create_server(int listen_port) {
    acceptor = std::make_unique<asio::ip::tcp::acceptor>(io_ctx);
    acceptor->open(asio::ip::tcp::v4());
    acceptor->set_option(asio::ip::tcp::acceptor::reuse_address(true));
    acceptor->bind(asio::ip::tcp::endpoint(asio::ip::tcp::v4(), static_cast<unsigned short>(listen_port)));
}

void Node::establish_conns(int my_idx) {
    const int N = static_cast<int>(nodes.size());

    app_pool.resize(N);
    raft_pool.resize(N);

    int lower_peers = 0;
    for (int i = 0; i < N; i++)
        if (i < my_idx) lower_peers++;

    int accept_count = lower_peers * (APP_CONNS + RAFT_CONNS);

    acceptor->listen(accept_count + 16);

    std::vector<int> accepted_app(N, 0);
    std::vector<int> accepted_raft(N, 0);

    std::thread accept_thread([&]() {
        for (int a = 0; a < accept_count; a++) {
            auto sock = std::make_unique<asio::ip::tcp::socket>(acceptor->accept());
            uint8_t tag = 0;
            std::error_code ec;
            asio::read(*sock, asio::buffer(&tag, 1), ec);
            if (ec) continue;
            bool is_raft = (tag & 0x80) != 0;
            int pi = static_cast<int>(tag & 0x7F);
            if (pi >= N || pi >= my_idx) continue;
            if (is_raft) {
                if (accepted_raft[pi] < RAFT_CONNS) {
                    raft_pool.set(pi, accepted_raft[pi], std::move(sock));
                    accepted_raft[pi]++;
                }
            } else {
                if (accepted_app[pi] < APP_CONNS) {
                    app_pool.set(pi, accepted_app[pi], std::move(sock));
                    accepted_app[pi]++;
                }
            }
        }
    });

    auto connect = [&](int peer, int count, bool is_raft) {
        for (int c = 0; c < count; c++) {
            std::error_code ec;
            for (int attempt = 0; attempt < 120; attempt++) {
                auto sock = std::make_unique<asio::ip::tcp::socket>(io_ctx);
                sock->connect(nodes[peer], ec);
                if (!ec) {
                    uint8_t tag = static_cast<uint8_t>(my_idx);
                    if (is_raft) tag |= 0x80;
                    asio::write(*sock, asio::buffer(&tag, 1), ec);
                    if (!ec) {
                        if (is_raft)
                            raft_pool.set(peer, c, std::move(sock));
                        else
                            app_pool.set(peer, c, std::move(sock));
                        break;
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            if (ec) {
                std::fprintf(stderr, "Failed to connect to node %d (%s): %s\n",
                             peer, is_raft ? "raft" : "app", ec.message().c_str());
                std::exit(1);
            }
        }
    };

    for (int i = 0; i < N; i++) {
        if (i <= my_idx) continue;
        connect(i, APP_CONNS, false);
        connect(i, RAFT_CONNS, true);
    }

    accept_thread.join();

    for (int i = 0; i < N; i++) {
        if (i == my_idx) continue;
        for (int c = 0; c < APP_CONNS; c++) {
            auto* s = app_pool.get(i, c);
            if (s) {
                set_sock_timeout(*s, 1);
                s->set_option(asio::ip::tcp::no_delay(true));
            }
        }
        for (int c = 0; c < RAFT_CONNS; c++) {
            auto* s = raft_pool.get(i, c);
            if (s) {
                set_sock_timeout(*s, 1);
                s->set_option(asio::ip::tcp::no_delay(true));
            }
        }
    }
}

void Node::init_engines() {
    const int N = static_cast<int>(nodes.size());
    std::set<int8_t> seen;
    std::vector<int8_t> my_shards;
    for (int k = 0; k < 3; k++) {
        int8_t s = static_cast<int8_t>((idx - k + N * 10) % N);
        if (!seen.count(s)) {
            seen.insert(s);
            my_shards.push_back(s);
        }
    }
    for (int8_t s : my_shards)
        engines.push_back(std::make_unique<Raft>(this, N, idx, s));

    tick_thread = std::thread([this] { tick_loop(); });
}

void Node::tick_loop() {
    while (running.load()) {
        for (auto& e : engines)
            e->tick();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

Raft* Node::engine_for(int8_t shard_id) {
    for (auto& e : engines) {
        if (e->shard_id() == shard_id)
            return e.get();
    }
    return nullptr;
}

size_t Node::send_request(int8_t dest, const Request_Full& req, Response_Full& resp) {
    std::array<uint8_t, BUF_SIZE> send_buf{};
    std::array<uint8_t, BUF_SIZE> recv_buf{};
    serialize(req, send_buf);

    return app_pool.with_conn(dest, [&](asio::ip::tcp::socket& sock) -> size_t {
        std::error_code ec;
        asio::write(sock, asio::buffer(send_buf), ec);
        if (ec) return 0;
        asio::read(sock, asio::buffer(recv_buf), ec);
        if (ec) return 0;
        deserialize(recv_buf, resp);
        return recv_buf.size();
    });
}

size_t Node::send_raft_raw(int8_t dest, const std::array<uint8_t, RAFT_MTU>& send_buf,
                           std::array<uint8_t, RAFT_MTU>& recv_buf) {
    return raft_pool.with_conn(dest, [&](asio::ip::tcp::socket& sock) -> size_t {
        std::error_code ec;
        asio::write(sock, asio::buffer(send_buf), ec);
        if (ec) return 0;
        asio::read(sock, asio::buffer(recv_buf), ec);
        if (ec) return 0;
        return recv_buf.size();
    });
}

bool Node::send_with_redirect(int8_t& dest, Request_Full& req, Response_Full& resp, int max_hops) {
    for (int h = 0; h < max_hops; h++) {
        if (send_request(dest, req, resp) == 0)
            return false;
        if (resp.redirect_leader >= 0) {
            dest = resp.redirect_leader;
            continue;
        }
        return resp.success;
    }
    return false;
}

bool Node::propose_on_leader(int8_t shard, LogEntryData entry, std::chrono::milliseconds timeout) {
    auto* eng = engine_for(shard);
    if (eng && eng->is_leader())
        return eng->propose(entry, timeout);

    int8_t dest = eng ? eng->leader_id() : first_member_of(shard);
    if (dest < 0)
        dest = first_member_of(shard);
    if (eng && dest < 0)
        dest = eng->first_member();

    Request_Full req{};
    req.src = idx;
    req.dest = dest;
    req.tx_id = entry.tx_id;

    switch (entry.cmd) {
        case LogCmd::Put:
            req.op = INTERNAL_RAFT_PUT;
            req.input_count = entry.n_kv;
            for (int i = 0; i < entry.n_kv && i < 3; i++)
                req.inputs[i] = entry.kvs[i];
            break;
        case LogCmd::Put3:
            req.op = INTERNAL_RAFT_PUT3;
            req.input_count = entry.n_kv;
            for (int i = 0; i < entry.n_kv && i < 3; i++)
                req.inputs[i] = entry.kvs[i];
            break;
        case LogCmd::TxPrepare:
            req.op = INTERNAL_TX_PREPARE;
            req.input_count = entry.n_kv;
            for (int i = 0; i < entry.n_kv && i < 3; i++)
                req.inputs[i] = entry.kvs[i];
            break;
        case LogCmd::TxCommit:
            req.op = INTERNAL_TX_COMMIT;
            req.input_count = 0;
            req.dest = shard;
            break;
        case LogCmd::TxAbort:
            req.op = INTERNAL_TX_ABORT;
            req.input_count = 0;
            req.dest = shard;
            break;
        default:
            return false;
    }

    Response_Full resp{};
    return send_with_redirect(dest, req, resp);
}

std::string Node::forward_get(int32_t key, int8_t dest) {
    Request_Full req{};
    req.src = idx;
    req.op = INTERNAL_RAFT_GET;
    req.input_count = 1;
    req.inputs[0].key = key;
    Response_Full resp{};
    int8_t d = dest;
    if (!send_with_redirect(d, req, resp))
        return {};
    return std::string(resp.output, strnlen(resp.output, MAX_VAL_SIZE));
}

void Node::handle_raft_io(const std::array<uint8_t, RAFT_MTU>& in, std::array<uint8_t, RAFT_MTU>& out) {
    std::memset(out.data(), 0, out.size());
    if (!raft_magic_ok(in.data()))
        return;

    RaftRpcKind kind;
    std::memcpy(&kind, in.data() + sizeof(uint32_t), sizeof(kind));

    switch (kind) {
        case RaftRpcKind::RequestVote: {
            RequestVoteRpc rv{};
            std::memcpy(&rv, in.data(), sizeof(rv));
            auto* eng = engine_for(rv.shard_id);
            RequestVoteReplyRpc rep{};
            if (eng)
                eng->rpc_request_vote(rv, rep);
            raft_write(out, &rep, sizeof(rep));
            break;
        }
        case RaftRpcKind::AppendEntries: {
            AppendEntriesRpc ae{};
            std::memcpy(&ae, in.data(), sizeof(ae));
            auto* eng = engine_for(ae.shard_id);
            AppendEntriesReplyRpc rep{};
            if (eng)
                eng->rpc_append_entries(ae, rep);
            raft_write(out, &rep, sizeof(rep));
            break;
        }
        default:
            break;
    }
}

void Node::handle_request(const Request_Full& req, Response_Full& resp) {
    resp.id = req.id;
    resp.tx_id = req.tx_id;
    resp.src = req.src;
    resp.dest = req.dest;
    resp.redirect_leader = -1;
    resp.success = false;

    const int N = static_cast<int>(nodes.size());

    auto do_get = [&](int32_t key) {
        int8_t s = static_cast<int8_t>(key % N);
        auto* eng = engine_for(s);
        if (!eng) {
            resp.redirect_leader = s;
            resp.success = false;
            return;
        }
        std::string out = eng->get_value(key);
        std::memset(resp.output, 0, MAX_VAL_SIZE);
        std::memcpy(resp.output, out.data(), std::min(out.size(), MAX_VAL_SIZE));
        resp.success = !out.empty();
    };

    switch (req.op) {
        case GET:
        case INTERNAL_RAFT_GET:
            if (req.input_count < 1)
                break;
            do_get(req.inputs[0].key);
            break;

        case INTERNAL_RAFT_PUT: {
            if (req.input_count < 1)
                break;
            int32_t key = req.inputs[0].key;
            int8_t s = static_cast<int8_t>(key % N);
            auto* eng = engine_for(s);
            if (!eng) {
                resp.redirect_leader = s;
                break;
            }
            if (!eng->is_leader()) {
                resp.redirect_leader = eng->leader_id();
                if (resp.redirect_leader < 0)
                    resp.redirect_leader = eng->first_member();
                break;
            }
            LogEntryData e{};
            e.cmd = LogCmd::Put;
            e.n_kv = 1;
            e.kvs[0] = req.inputs[0];
            resp.success = eng->propose(e, std::chrono::seconds(2));
            break;
        }

        case INTERNAL_RAFT_PUT3: {
            if (req.input_count < 1)
                break;
            int8_t s0 = static_cast<int8_t>(req.inputs[0].key % N);
            for (uint8_t i = 0; i < req.input_count; i++) {
                if (static_cast<int8_t>(req.inputs[i].key % N) != s0) {
                    resp.success = false;
                    return;
                }
            }
            auto* eng = engine_for(s0);
            if (!eng) {
                resp.redirect_leader = s0;
                break;
            }
            if (!eng->is_leader()) {
                resp.redirect_leader = eng->leader_id();
                if (resp.redirect_leader < 0)
                    resp.redirect_leader = eng->first_member();
                break;
            }
            LogEntryData e{};
            e.cmd = LogCmd::Put3;
            e.n_kv = req.input_count;
            for (int i = 0; i < req.input_count && i < 3; i++)
                e.kvs[i] = req.inputs[i];
            resp.success = eng->propose(e, std::chrono::seconds(2));
            break;
        }

        case INTERNAL_TX_PREPARE: {
            if (req.input_count < 1)
                break;
            int8_t s = static_cast<int8_t>(req.inputs[0].key % N);
            for (uint8_t i = 0; i < req.input_count; i++) {
                if (static_cast<int8_t>(req.inputs[i].key % N) != s) {
                    resp.success = false;
                    return;
                }
            }
            auto* eng = engine_for(s);
            if (!eng) {
                resp.redirect_leader = s;
                break;
            }
            if (!eng->is_leader()) {
                resp.redirect_leader = eng->leader_id();
                if (resp.redirect_leader < 0)
                    resp.redirect_leader = eng->first_member();
                break;
            }
            LogEntryData e{};
            e.cmd = LogCmd::TxPrepare;
            e.tx_id = req.tx_id;
            e.n_kv = req.input_count;
            for (int i = 0; i < req.input_count && i < 3; i++)
                e.kvs[i] = req.inputs[i];
            resp.success = eng->propose(e, std::chrono::seconds(2));
            break;
        }

        case INTERNAL_TX_COMMIT: {
            int8_t s = req.dest;
            if (s < 0 || s >= N)
                break;
            auto* eng = engine_for(s);
            if (!eng) {
                resp.redirect_leader = s;
                break;
            }
            if (!eng->is_leader()) {
                resp.redirect_leader = eng->leader_id();
                if (resp.redirect_leader < 0)
                    resp.redirect_leader = eng->first_member();
                break;
            }
            LogEntryData e{};
            e.cmd = LogCmd::TxCommit;
            e.tx_id = req.tx_id;
            e.n_kv = 0;
            resp.success = eng->propose(e, std::chrono::seconds(2));
            break;
        }

        case INTERNAL_TX_ABORT: {
            int8_t s = req.dest;
            if (s < 0 || s >= N)
                break;
            auto* eng = engine_for(s);
            if (!eng) {
                resp.redirect_leader = s;
                break;
            }
            if (!eng->is_leader()) {
                resp.redirect_leader = eng->leader_id();
                if (resp.redirect_leader < 0)
                    resp.redirect_leader = eng->first_member();
                break;
            }
            LogEntryData e{};
            e.cmd = LogCmd::TxAbort;
            e.tx_id = req.tx_id;
            e.n_kv = 0;
            resp.success = eng->propose(e, std::chrono::seconds(2));
            break;
        }

        default:
            break;
    }
}

void Node::recv_request() {
    auto run = [this](asio::ip::tcp::socket& conn) {
        while (running.load()) {
            std::array<uint8_t, BUF_SIZE> recv_buf{};
            std::array<uint8_t, BUF_SIZE> send_buf{};
            std::error_code ec;
            asio::read(conn, asio::buffer(recv_buf), ec);
            if (ec) {
                if (ec == asio::error::would_block || ec == asio::error::try_again)
                    continue;
                break;
            }

            uint32_t mag = 0;
            std::memcpy(&mag, recv_buf.data(), sizeof(mag));
            if (mag == RAFT_MAGIC) {
                std::array<uint8_t, RAFT_MTU> rin{};
                std::array<uint8_t, RAFT_MTU> rout{};
                std::memcpy(rin.data(), recv_buf.data(), std::min(recv_buf.size(), rin.size()));
                handle_raft_io(rin, rout);
                std::memcpy(send_buf.data(), rout.data(), std::min(send_buf.size(), rout.size()));
            } else {
                Request_Full req{};
                Response_Full resp{};
                deserialize(recv_buf, req);
                handle_request(req, resp);
                serialize(resp, send_buf);
            }

            asio::write(conn, asio::buffer(send_buf), ec);
            if (ec)
                break;
        }
    };

    auto run_raft = [this](asio::ip::tcp::socket& conn) {
        while (running.load()) {
            std::array<uint8_t, RAFT_MTU> recv_buf{};
            std::array<uint8_t, RAFT_MTU> send_buf{};
            std::error_code ec;
            asio::read(conn, asio::buffer(recv_buf), ec);
            if (ec) {
                if (ec == asio::error::would_block || ec == asio::error::try_again)
                    continue;
                break;
            }
            handle_raft_io(recv_buf, send_buf);
            asio::write(conn, asio::buffer(send_buf), ec);
            if (ec)
                break;
        }
    };

    const int N = static_cast<int>(nodes.size());
    std::vector<std::thread> threads;
    for (int i = 0; i < N; i++) {
        if (i == idx)
            continue;
        for (int c = 0; c < APP_CONNS; c++) {
            threads.emplace_back(run, std::ref(*app_pool.get(i, c)));
        }
        for (int c = 0; c < RAFT_CONNS; c++) {
            threads.emplace_back(run_raft, std::ref(*raft_pool.get(i, c)));
        }
    }

    for (auto& t : threads)
        t.join();
}

void Node::stop() {
    running.store(false);
    if (tick_thread.joinable())
        tick_thread.join();

    std::error_code ec;
    if (acceptor && acceptor->is_open())
        acceptor->close(ec);

    app_pool.shutdown_all(idx);
    raft_pool.shutdown_all(idx);
}

bool Node::put(const int32_t& key, const std::string& val) {
    const int N = static_cast<int>(nodes.size());
    int8_t s = static_cast<int8_t>(key % N);
    LogEntryData e{};
    e.cmd = LogCmd::Put;
    e.n_kv = 1;
    fill_socket_kv(e.kvs[0], key, val);
    return propose_on_leader(s, e);
}

bool Node::put3(const std::array<KV_Pair, 3>& kvs) {
    const int N = static_cast<int>(nodes.size());
    std::map<int8_t, std::vector<std::pair<int32_t, std::string>>> by_shard;
    for (int i = 0; i < 3; i++) {
        int8_t s = static_cast<int8_t>(kvs[i].key % N);
        by_shard[s].emplace_back(kvs[i].key, kvs[i].value);
    }

    if (by_shard.size() == 1) {
        LogEntryData e{};
        e.cmd = LogCmd::Put3;
        e.n_kv = 3;
        for (int i = 0; i < 3; i++)
            fill_socket_kv(e.kvs[i], kvs[i].key, kvs[i].value);
        int8_t s = by_shard.begin()->first;
        return propose_on_leader(s, e);
    }

    uint64_t txid = next_tx_id();
    std::vector<int8_t> order;
    order.reserve(by_shard.size());
    for (auto& p : by_shard)
        order.push_back(p.first);

    std::vector<int8_t> voted;
    for (int8_t s : order) {
        LogEntryData prep{};
        prep.cmd = LogCmd::TxPrepare;
        prep.tx_id = txid;
        auto& vec = by_shard[s];
        prep.n_kv = static_cast<uint8_t>(vec.size());
        for (size_t i = 0; i < vec.size() && i < 3; i++)
            fill_socket_kv(prep.kvs[i], vec[i].first, vec[i].second);

        if (!propose_on_leader(s, prep)) {
            for (int8_t v : voted) {
                Request_Full rq{};
                rq.src = idx;
                rq.op = INTERNAL_TX_ABORT;
                rq.tx_id = txid;
                rq.dest = v;
                rq.input_count = 0;
                int8_t d = v;
                Response_Full rs{};
                send_with_redirect(d, rq, rs);
            }
            return false;
        }
        voted.push_back(s);
    }

    for (int8_t s : voted) {
        Request_Full rq{};
        rq.src = idx;
        rq.op = INTERNAL_TX_COMMIT;
        rq.tx_id = txid;
        rq.dest = s;
        rq.input_count = 0;
        int8_t d = s;
        Response_Full rs{};
        if (!send_with_redirect(d, rq, rs) || !rs.success)
            return false;
    }
    return true;
}

std::string Node::get(const int32_t& key) {
    const int N = static_cast<int>(nodes.size());
    int8_t s = static_cast<int8_t>(key % N);
    auto* eng = engine_for(s);
    if (eng)
        return eng->get_value(key);
    return forward_get(key, s);
}
