#include "Raft.hpp"
#include "Node.hpp"

#include <algorithm>
#include <cstring>
#include <set>
#include <thread>

Raft::Raft(Node* node, int cluster_n, int8_t self_id, int8_t shard_id)
    : node(node),
      cluster_size(cluster_n),
      id(self_id),
      shard(shard_id),
      election(self_id) {
    std::set<int8_t> mem;
    for (int k = 0; k < 3; k++)
        mem.insert(static_cast<int8_t>((shard_id + k) % cluster_n));
    for (int8_t m : mem)
        members.push_back(m);
    for (int8_t m : members)
        if (m != id)
            peers.push_back(m);

    quorum = static_cast<int>(members.size()) / 2 + 1;

    for (int8_t p : peers)
        repl_state[p] = std::make_unique<PeerReplicator>();
    for (int8_t p : peers)
        repl_threads.emplace_back(&Raft::peer_replication_loop, this, p);
}

Raft::~Raft() {
    for (auto& [peer, rs] : repl_state) {
        {
            std::lock_guard<std::mutex> lk(rs->mtx);
            rs->shutdown = true;
        }
        rs->cv.notify_one();
    }
    for (auto& t : repl_threads)
        if (t.joinable()) t.join();
}

void Raft::run_election() {
    election.start_election();

    int votes = 1;
    RequestVoteRpc rv{};
    rv.kind = RaftRpcKind::RequestVote;
    rv.shard_id = shard;
    rv.term = election.term();
    rv.candidate_id = id;
    rv.last_log_index = log.last_index();
    rv.last_log_term = log.last_term();

    for (int8_t p : peers) {
        std::array<uint8_t, RAFT_MTU> s{};
        std::array<uint8_t, RAFT_MTU> r{};
        raft_write(s, &rv, sizeof(rv));
        if (node->send_raft_raw(p, s, r) == 0)
            continue;
        RequestVoteReplyRpc rep{};
        if (sizeof(rep) > r.size())
            continue;
        std::memcpy(&rep, r.data(), sizeof(rep));
        if (!raft_magic_ok(&rep))
            continue;
        if (rep.term > election.term()) {
            election.become_follower(rep.term);
            return;
        }
        if (rep.vote_granted && rep.term == election.term())
            votes++;
    }

    std::lock_guard<std::mutex> lk(mtx);
    if (election.role() != RaftRole::Candidate || election.term() != rv.term)
        return;
    if (votes >= quorum) {
        election.become_leader();
        for (int8_t p : peers) {
            next_idx[p] = log.last_index() + 1;
            match_idx[p] = 0;
        }
    }
}

bool Raft::rpc_request_vote(const RequestVoteRpc& in, RequestVoteReplyRpc& out) {
    std::lock_guard<std::mutex> lk(mtx);
    out = RequestVoteReplyRpc{};
    out.shard_id = shard;
    if (in.term < election.term()) {
        out.term = election.term();
        out.vote_granted = false;
        return true;
    }
    if (in.term > election.term())
        election.become_follower(in.term);

    bool grant = false;
    if (election.term() == in.term &&
        log.up_to_date(in.last_log_index, in.last_log_term)) {
        grant = election.try_grant_vote(in.candidate_id);
    }

    out.term = election.term();
    out.vote_granted = grant;
    return true;
}

void Raft::apply_one(const LogEntryData& e) {
    table.apply(e);
}

void Raft::apply_committed() {
    std::unique_lock<std::mutex> lk(mtx);
    while (applied < commit_idx) {
        applied++;
        LogEntryData e = log.at(applied);
        lk.unlock();
        apply_one(e);
        lk.lock();
    }
}

void Raft::advance_commit() {
    if (election.role() != RaftRole::Leader)
        return;
    for (int n = static_cast<int>(log.size()) - 1; n > static_cast<int>(commit_idx); n--) {
        if (log.at(static_cast<uint32_t>(n)).term != election.term())
            continue;
        int count = 1;
        for (int8_t p : peers) {
            if (match_idx[p] >= static_cast<uint32_t>(n))
                count++;
        }
        if (count >= quorum) {
            commit_idx = static_cast<uint32_t>(n);
            break;
        }
    }
}

bool Raft::append_entries_to(int8_t peer, uint32_t prev_log_index, bool with_entry,
                             const LogEntryData* entry, AppendEntriesReplyRpc& reply) {
    uint64_t term = 0;
    uint32_t lc = 0;
    uint64_t prev_term = 0;
    {
        std::lock_guard<std::mutex> lk(mtx);
        term = election.term();
        lc = commit_idx;
        prev_term = log.term_at(prev_log_index);
    }

    AppendEntriesRpc ae{};
    ae.kind = RaftRpcKind::AppendEntries;
    ae.shard_id = shard;
    ae.term = term;
    ae.leader_id = id;
    ae.prev_log_index = prev_log_index;
    ae.prev_log_term = prev_term;
    ae.leader_commit = lc;
    ae.has_entry = with_entry;
    if (with_entry && entry)
        ae.entry = *entry;

    std::array<uint8_t, RAFT_MTU> s{};
    std::array<uint8_t, RAFT_MTU> r{};
    raft_write(s, &ae, sizeof(ae));
    if (node->send_raft_raw(peer, s, r) == 0)
        return false;
    std::memcpy(&reply, r.data(), sizeof(reply));
    return raft_magic_ok(&reply);
}

void Raft::wake_replicators() {
    for (auto& [peer, rs] : repl_state) {
        {
            std::lock_guard<std::mutex> lk(rs->mtx);
            rs->wake = true;
        }
        rs->cv.notify_one();
    }
}

void Raft::peer_replication_loop(int8_t peer) {
    auto& rs = *repl_state[peer];
    while (true) {
        {
            std::unique_lock<std::mutex> lk(rs.mtx);
            rs.cv.wait(lk, [&] { return rs.wake || rs.shutdown; });
            if (rs.shutdown) return;
            rs.wake = false;
        }

        while (node->running.load()) {
            uint32_t prev;
            LogEntryData ent{};
            bool send_ent = false;
            uint64_t term_copy;
            {
                std::lock_guard<std::mutex> lk(mtx);
                if (election.role() != RaftRole::Leader) break;
                term_copy = election.term();
                uint32_t ni = next_idx[peer];
                if (ni == 0) ni = 1;
                prev = ni - 1;
                if (ni <= log.last_index()) {
                    ent = log.at(ni);
                    send_ent = true;
                }
            }

            AppendEntriesReplyRpc rep{};
            bool ok = append_entries_to(peer, prev, send_ent, send_ent ? &ent : nullptr, rep);

            bool need_retry = false;
            {
                std::lock_guard<std::mutex> lk(mtx);
                if (election.role() != RaftRole::Leader || election.term() != term_copy) break;
                if (!ok) break;
                if (rep.term > election.term()) {
                    election.become_follower(rep.term);
                    break;
                }
                if (rep.success) {
                    match_idx[peer] = rep.match_index;
                    next_idx[peer] = rep.match_index + 1;
                    advance_commit();
                    if (next_idx[peer] <= log.last_index())
                        need_retry = true;
                } else {
                    if (next_idx[peer] > 1)
                        next_idx[peer]--;
                    need_retry = true;
                }
            }

            apply_committed();
            apply_cond.notify_all();

            if (!need_retry) break;
        }
    }
}

void Raft::replicate_new_entry(uint32_t) {
    wake_replicators();
}

void Raft::send_heartbeats() {
    wake_replicators();
}

bool Raft::rpc_append_entries(const AppendEntriesRpc& in, AppendEntriesReplyRpc& out) {
    std::unique_lock<std::mutex> lk(mtx);
    out = AppendEntriesReplyRpc{};
    out.shard_id = shard;
    out.match_index = 0;

    if (in.term < election.term()) {
        out.term = election.term();
        out.success = false;
        return true;
    }

    if (in.term > election.term())
        election.become_follower(in.term);
    else if (in.term == election.term())
        election.reset_deadline();

    election.set_role(RaftRole::Follower);
    election.set_leader(in.leader_id);
    election.set_term(in.term);

    if (in.prev_log_index > log.last_index()) {
        out.term = election.term();
        out.success = false;
        return true;
    }
    if (log.term_at(in.prev_log_index) != in.prev_log_term) {
        out.term = election.term();
        out.success = false;
        return true;
    }

    if (in.has_entry) {
        uint32_t new_idx = in.prev_log_index + 1;
        if (new_idx < log.size() && log.at(new_idx).term != in.entry.term) {
            log.truncate(new_idx);
        }
        if (new_idx == log.size()) {
            log.append(in.entry);
        } else {
            log.set(new_idx, in.entry);
        }
    }

    if (in.leader_commit > commit_idx) {
        commit_idx = std::min(in.leader_commit, log.last_index());
    }

    out.term = election.term();
    out.success = true;
    out.match_index = log.last_index();
    lk.unlock();
    apply_committed();
    apply_cond.notify_all();
    return true;
}

void Raft::tick() {
    if (!node->running.load())
        return;

    {
        std::unique_lock<std::mutex> lk(mtx);
        if (election.role() == RaftRole::Leader) {
            if (election.heartbeat_due()) {
                election.mark_heartbeat();
                lk.unlock();
                send_heartbeats();
                return;
            }
            return;
        }

        if (election.election_timeout()) {
            lk.unlock();
            run_election();
        }
    }
}

bool Raft::propose(LogEntryData e, std::chrono::milliseconds timeout) {
    uint32_t target = 0;
    {
        std::lock_guard<std::mutex> lk(mtx);
        if (election.role() != RaftRole::Leader)
            return false;
        e.term = election.term();
        log.append(e);
        target = log.last_index();
    }

    replicate_new_entry(target);

    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (node->running.load() && std::chrono::steady_clock::now() < deadline) {
        {
            std::lock_guard<std::mutex> lk(mtx);
            if (commit_idx >= target)
                return true;
        }
        std::unique_lock<std::mutex> lk(mtx);
        apply_cond.wait_for(lk, std::chrono::milliseconds(5), [this, target] {
            return commit_idx >= target || !node->running.load();
        });
    }
    std::lock_guard<std::mutex> lk(mtx);
    return commit_idx >= target;
}

std::string Raft::get_value(int32_t key) {
    return table.get(key);
}
