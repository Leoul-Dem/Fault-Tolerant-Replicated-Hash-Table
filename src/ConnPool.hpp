#pragma once

#include "../libs/networking/asio.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

template <int N>
class ConnPool {
    std::vector<std::array<std::unique_ptr<asio::ip::tcp::socket>, N>> sockets;
    std::vector<std::array<std::mutex, N>> locks;
    std::vector<std::atomic<uint32_t>> counter;

public:
    void resize(int n_peers) {
        sockets.resize(n_peers);
        locks = std::vector<std::array<std::mutex, N>>(n_peers);
        counter = std::vector<std::atomic<uint32_t>>(n_peers);
    }

    void set(int peer, int slot, std::unique_ptr<asio::ip::tcp::socket> sock) {
        sockets[peer][slot] = std::move(sock);
    }

    asio::ip::tcp::socket* get(int peer, int slot) {
        return sockets[peer][slot].get();
    }

    int size() const { return static_cast<int>(sockets.size()); }

    static constexpr int per_peer() { return N; }

    template <typename Fn>
    size_t with_conn(int8_t dest, Fn&& fn) {
        if (dest < 0 || dest >= static_cast<int8_t>(sockets.size()))
            return 0;
        int c = static_cast<int>(counter[dest].fetch_add(1, std::memory_order_relaxed) % N);
        std::lock_guard<std::mutex> lock(locks[dest][c]);
        if (!sockets[dest][c])
            return 0;
        return fn(*sockets[dest][c]);
    }

    void shutdown_all(int skip) {
        std::error_code ec;
        for (int i = 0; i < static_cast<int>(sockets.size()); i++) {
            if (i == skip) continue;
            for (int c = 0; c < N; c++) {
                if (sockets[i][c] && sockets[i][c]->is_open()) {
                    sockets[i][c]->shutdown(asio::ip::tcp::socket::shutdown_both, ec);
                    sockets[i][c]->close(ec);
                }
            }
        }
    }
};
