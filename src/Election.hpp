#pragma once

#include <chrono>
#include <cstdint>
#include <random>

enum class RaftRole : uint8_t { Follower, Candidate, Leader };

class Election {
    const int8_t id;
    RaftRole state = RaftRole::Follower;
    uint64_t cur_term = 0;
    int8_t vote = -1;
    int8_t leader = -1;

    std::chrono::steady_clock::time_point deadline{};
    std::chrono::steady_clock::time_point last_heartbeat{};
    std::mt19937 rng;

public:
    explicit Election(int8_t self_id)
        : id(self_id), rng(std::random_device{}()) {
        reset_deadline();
    }

    RaftRole role() const { return state; }
    uint64_t term() const { return cur_term; }
    int8_t voted_for() const { return vote; }
    int8_t leader_id() const { return leader; }

    void reset_deadline() {
        std::uniform_int_distribution<int> dist(150, 350);
        deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(rng));
    }

    bool election_timeout() const {
        return std::chrono::steady_clock::now() > deadline;
    }

    bool heartbeat_due() const {
        auto now = std::chrono::steady_clock::now();
        return now - last_heartbeat > std::chrono::milliseconds(15);
    }

    void mark_heartbeat() {
        last_heartbeat = std::chrono::steady_clock::now();
    }

    void become_follower(uint64_t t) {
        state = RaftRole::Follower;
        cur_term = t;
        vote = -1;
        leader = -1;
        reset_deadline();
    }

    void start_election() {
        state = RaftRole::Candidate;
        cur_term++;
        vote = id;
        leader = -1;
        reset_deadline();
    }

    void become_leader() {
        state = RaftRole::Leader;
        leader = id;
        last_heartbeat = std::chrono::steady_clock::now();
    }

    bool try_grant_vote(int8_t candidate) {
        if (vote == -1 || vote == candidate) {
            vote = candidate;
            reset_deadline();
            return true;
        }
        return false;
    }

    void set_leader(int8_t l) { leader = l; }
    void set_role(RaftRole r) { state = r; }
    void set_term(uint64_t t) { cur_term = t; }
};
