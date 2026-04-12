#include "RaftEngine.hpp"
#include "Node.hpp"

#include <algorithm>
#include <cstring>
#include <set>
#include <thread>

RaftEngine::RaftEngine(Node* node, int cluster_n, int8_t self_id, int8_t shard_id)
    : node_(node),
      cluster_n_(cluster_n),
      self_id_(self_id),
      shard_id_(shard_id),
      rng_(std::random_device{}()) {
    std::set<int8_t> mem;
    for (int k = 0; k < 3; k++)
        mem.insert(static_cast<int8_t>((shard_id + k) % cluster_n));
    for (int8_t m : mem)
        members_.push_back(m);
    for (int8_t m : members_)
        if (m != self_id_)
            peers_.push_back(m);

    quorum_ = static_cast<int>(members_.size()) / 2 + 1;

    LogEntryData dummy;
    dummy.term = 0;
    log_.push_back(dummy);

    election_reset_deadline();
    leader_id_ = -1;

    for (int8_t p : peers_)
        repl_state_[p] = std::make_unique<PeerReplicator>();
    for (int8_t p : peers_)
        repl_threads_.emplace_back(&RaftEngine::peer_replication_loop, this, p);
}

RaftEngine::~RaftEngine() {
    for (auto& [peer, rs] : repl_state_) {
        {
            std::lock_guard<std::mutex> lk(rs->mtx);
            rs->shutdown = true;
        }
        rs->cv.notify_one();
    }
    for (auto& t : repl_threads_)
        if (t.joinable()) t.join();
}

uint32_t RaftEngine::last_log_index() const {
    return static_cast<uint32_t>(log_.size() - 1);
}

uint64_t RaftEngine::last_log_term() const {
    return log_.back().term;
}

uint64_t RaftEngine::term_at(uint32_t idx) const {
    if (idx == 0 || idx >= log_.size())
        return 0;
    return log_[idx].term;
}

void RaftEngine::election_reset_deadline() {
    std::uniform_int_distribution<int> dist(150, 350);
    election_deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(rng_));
}

void RaftEngine::become_follower(uint64_t term) {
    role_ = RaftRole::Follower;
    current_term_ = term;
    voted_for_ = -1;
    leader_id_ = -1;
    election_reset_deadline();
}

void RaftEngine::become_candidate() {
    role_ = RaftRole::Candidate;
    current_term_++;
    voted_for_ = self_id_;
    leader_id_ = -1;
    election_reset_deadline();

    int votes = 1;
    RequestVoteRpc rv{};
    rv.kind = RaftRpcKind::RequestVote;
    rv.shard_id = shard_id_;
    rv.term = current_term_;
    rv.candidate_id = self_id_;
    rv.last_log_index = last_log_index();
    rv.last_log_term = last_log_term();

    for (int8_t p : peers_) {
        std::array<uint8_t, RAFT_MTU> s{};
        std::array<uint8_t, RAFT_MTU> r{};
        raft_write(s, &rv, sizeof(rv));
        if (node_->send_raft_raw(p, s, r) == 0)
            continue;
        RequestVoteReplyRpc rep{};
        if (sizeof(rep) > r.size())
            continue;
        std::memcpy(&rep, r.data(), sizeof(rep));
        if (!raft_magic_ok(&rep))
            continue;
        if (rep.term > current_term_) {
            become_follower(rep.term);
            return;
        }
        if (rep.vote_granted && rep.term == current_term_)
            votes++;
    }

    std::lock_guard<std::mutex> lk(raft_mtx_);
    if (role_ != RaftRole::Candidate || current_term_ != rv.term)
        return;
    if (votes >= quorum_) {
        role_ = RaftRole::Leader;
        leader_id_ = self_id_;
        for (int8_t p : peers_) {
            next_index_[p] = last_log_index() + 1;
            match_index_[p] = 0;
        }
        last_leader_tick_ = std::chrono::steady_clock::now();
    }
}

void RaftEngine::become_leader() {
    // unused separate path; folded into become_candidate
}

bool RaftEngine::log_up_to_date(uint32_t cand_last_idx, uint64_t cand_last_term) const {
    uint64_t my_term = last_log_term();
    uint32_t my_idx = last_log_index();
    if (cand_last_term != my_term)
        return cand_last_term > my_term;
    return cand_last_idx >= my_idx;
}

bool RaftEngine::rpc_request_vote(const RequestVoteRpc& in, RequestVoteReplyRpc& out) {
    std::lock_guard<std::mutex> lk(raft_mtx_);
    out = RequestVoteReplyRpc{};
    out.shard_id = shard_id_;
    if (in.term < current_term_) {
        out.term = current_term_;
        out.vote_granted = false;
        return true;
    }
    if (in.term > current_term_)
        become_follower(in.term);

    bool grant = false;
    if (current_term_ == in.term &&
        (voted_for_ == -1 || voted_for_ == in.candidate_id) &&
        log_up_to_date(in.last_log_index, in.last_log_term)) {
        grant = true;
        voted_for_ = in.candidate_id;
 election_reset_deadline();
    }

    out.term = current_term_;
    out.vote_granted = grant;
    return true;
}

void RaftEngine::apply_one(const LogEntryData& e) {
    switch (e.cmd) {
        case LogCmd::Noop:
            break;
        case LogCmd::Put: {
            if (e.n_kv < 1)
                break;
            int32_t k = e.kvs[0].key;
            std::string v(e.kvs[0].value, strnlen(e.kvs[0].value, MAX_VAL_SIZE));
            std::unique_lock<std::shared_timed_mutex> lk(stripes_[stripe(k)]);
            kv_.insert_or_assign(k, std::move(v));
            break;
        }
        case LogCmd::Put3: {
            for (uint8_t i = 0; i < e.n_kv && i < 3; i++) {
                int32_t k = e.kvs[i].key;
                std::string v(e.kvs[i].value, strnlen(e.kvs[i].value, MAX_VAL_SIZE));
                std::unique_lock<std::shared_timed_mutex> lk(stripes_[stripe(k)]);
                kv_.insert_or_assign(k, std::move(v));
            }
            break;
        }
        case LogCmd::TxPrepare: {
            PendingTx ptx;
            for (uint8_t i = 0; i < e.n_kv && i < 3; i++) {
                std::string v(e.kvs[i].value, strnlen(e.kvs[i].value, MAX_VAL_SIZE));
                ptx.kvs.emplace_back(e.kvs[i].key, std::move(v));
            }
            insert_pending_tx(e.tx_id, std::move(ptx));
            break;
        }
        case LogCmd::TxCommit:
            commit_tx(e.tx_id);
            break;
        case LogCmd::TxAbort:
            abort_tx(e.tx_id);
            break;
    }
}

void RaftEngine::wait_until_key_not_pending(int32_t key) {
    std::unique_lock<std::mutex> lk(pending_mtx_);
    pending_cv_.wait(lk, [this, key] {
        for (const auto& [tid, ptx] : pending_txs_) {
            for (const auto& [pk, pv] : ptx.kvs) {
                if (pk == key)
                    return false;
            }
        }
        return true;
    });
}

void RaftEngine::insert_pending_tx(uint64_t tx_id, PendingTx tx) {
    std::lock_guard<std::mutex> lk(pending_mtx_);
    pending_txs_[tx_id] = std::move(tx);
}

void RaftEngine::commit_tx(uint64_t tx_id) {
    PendingTx tx;
    {
        std::lock_guard<std::mutex> lk(pending_mtx_);
        auto it = pending_txs_.find(tx_id);
        if (it == pending_txs_.end())
            return;
        tx = std::move(it->second);
        pending_txs_.erase(it);
    }
    pending_cv_.notify_all();

    std::set<size_t> stripes;
    for (auto& [key, val] : tx.kvs)
        stripes.insert(stripe(key));

    std::vector<std::unique_lock<std::shared_timed_mutex>> held;
    for (size_t s : stripes)
        held.emplace_back(stripes_[s]);

    for (auto& [key, val] : tx.kvs)
        kv_.insert_or_assign(key, val);
}

void RaftEngine::abort_tx(uint64_t tx_id) {
    {
        std::lock_guard<std::mutex> lk(pending_mtx_);
        pending_txs_.erase(tx_id);
    }
    pending_cv_.notify_all();
}

void RaftEngine::apply_through_commit() {
    std::unique_lock<std::mutex> lk(raft_mtx_);
    while (last_applied_ < commit_index_) {
        last_applied_++;
        LogEntryData e = log_[last_applied_];
        lk.unlock();
        apply_one(e);
        lk.lock();
    }
}

void RaftEngine::advance_commit_for_leader() {
    if (role_ != RaftRole::Leader)
        return;
    for (int n = static_cast<int>(log_.size()) - 1; n > static_cast<int>(commit_index_); n--) {
        if (log_[static_cast<size_t>(n)].term != current_term_)
            continue;
        int count = 1;
        for (int8_t p : peers_) {
            if (match_index_[p] >= static_cast<uint32_t>(n))
                count++;
        }
        if (count >= quorum_) {
            commit_index_ = static_cast<uint32_t>(n);
            break;
        }
    }
}

bool RaftEngine::append_entries_to(int8_t peer, uint32_t prev_log_index, bool with_entry,
                                   const LogEntryData* entry, AppendEntriesReplyRpc& reply) {
    uint64_t term = 0;
    uint32_t lc = 0;
    uint64_t prev_term = 0;
    {
        std::lock_guard<std::mutex> lk(raft_mtx_);
        term = current_term_;
        lc = commit_index_;
        prev_term = term_at(prev_log_index);
    }

    AppendEntriesRpc ae{};
    ae.kind = RaftRpcKind::AppendEntries;
    ae.shard_id = shard_id_;
    ae.term = term;
    ae.leader_id = self_id_;
    ae.prev_log_index = prev_log_index;
    ae.prev_log_term = prev_term;
    ae.leader_commit = lc;
    ae.has_entry = with_entry;
    if (with_entry && entry)
        ae.entry = *entry;

    std::array<uint8_t, RAFT_MTU> s{};
    std::array<uint8_t, RAFT_MTU> r{};
    raft_write(s, &ae, sizeof(ae));
    if (node_->send_raft_raw(peer, s, r) == 0)
        return false;
    std::memcpy(&reply, r.data(), sizeof(reply));
    return raft_magic_ok(&reply);
}

void RaftEngine::wake_replicators() {
    for (auto& [peer, rs] : repl_state_) {
        {
            std::lock_guard<std::mutex> lk(rs->mtx);
            rs->wake = true;
        }
        rs->cv.notify_one();
    }
}

void RaftEngine::peer_replication_loop(int8_t peer) {
    auto& rs = *repl_state_[peer];
    while (true) {
        {
            std::unique_lock<std::mutex> lk(rs.mtx);
            rs.cv.wait(lk, [&] { return rs.wake || rs.shutdown; });
            if (rs.shutdown) return;
            rs.wake = false;
        }

        while (node_->running.load()) {
            uint32_t prev_idx;
            LogEntryData ent{};
            bool send_ent = false;
            uint64_t term_copy;
            {
                std::lock_guard<std::mutex> lk(raft_mtx_);
                if (role_ != RaftRole::Leader) break;
                term_copy = current_term_;
                uint32_t ni = next_index_[peer];
                if (ni == 0) ni = 1;
                prev_idx = ni - 1;
                if (ni <= last_log_index()) {
                    ent = log_[ni];
                    send_ent = true;
                }
            }

            AppendEntriesReplyRpc rep{};
            bool ok = append_entries_to(peer, prev_idx, send_ent, send_ent ? &ent : nullptr, rep);

            bool need_retry = false;
            {
                std::lock_guard<std::mutex> lk(raft_mtx_);
                if (role_ != RaftRole::Leader || current_term_ != term_copy) break;
                if (!ok) break;
                if (rep.term > current_term_) {
                    become_follower(rep.term);
                    break;
                }
                if (rep.success) {
                    match_index_[peer] = rep.match_index;
                    next_index_[peer] = rep.match_index + 1;
                    advance_commit_for_leader();
                    if (next_index_[peer] <= last_log_index())
                        need_retry = true;
                } else {
                    if (next_index_[peer] > 1)
                        next_index_[peer]--;
                    need_retry = true;
                }
            }

            apply_through_commit();
            apply_cv_.notify_all();

            if (!need_retry) break;
        }
    }
}

void RaftEngine::replicate_new_entry(uint32_t) {
    wake_replicators();
}

void RaftEngine::send_heartbeats() {
    wake_replicators();
}

bool RaftEngine::rpc_append_entries(const AppendEntriesRpc& in, AppendEntriesReplyRpc& out) {
    std::unique_lock<std::mutex> lk(raft_mtx_);
    out = AppendEntriesReplyRpc{};
    out.shard_id = shard_id_;
    out.match_index = 0;

    if (in.term < current_term_) {
        out.term = current_term_;
        out.success = false;
        return true;
    }

    if (in.term > current_term_)
        become_follower(in.term);
    else if (in.term == current_term_)
        election_reset_deadline();

    role_ = RaftRole::Follower;
    leader_id_ = in.leader_id;
    current_term_ = in.term;

    if (in.prev_log_index > last_log_index()) {
        out.term = current_term_;
        out.success = false;
        return true;
    }
    if (term_at(in.prev_log_index) != in.prev_log_term) {
        out.term = current_term_;
        out.success = false;
        return true;
    }

    if (in.has_entry) {
        uint32_t new_idx = in.prev_log_index + 1;
        if (new_idx < log_.size() && log_[new_idx].term != in.entry.term) {
            log_.resize(new_idx);
        }
        if (new_idx == log_.size()) {
            log_.push_back(in.entry);
        } else {
            log_[new_idx] = in.entry;
        }
    }

    if (in.leader_commit > commit_index_) {
        commit_index_ = std::min(in.leader_commit, last_log_index());
    }

    out.term = current_term_;
    out.success = true;
    out.match_index = last_log_index();
    lk.unlock();
    apply_through_commit();
    apply_cv_.notify_all();
    return true;
}

void RaftEngine::tick() {
    if (!node_->running.load())
        return;

    {
        std::unique_lock<std::mutex> lk(raft_mtx_);
        if (role_ == RaftRole::Leader) {
            auto now = std::chrono::steady_clock::now();
            if (now - last_leader_tick_ > std::chrono::milliseconds(15)) {
                last_leader_tick_ = now;
                lk.unlock();
                send_heartbeats();
                return;
            }
            return;
        }

        if (std::chrono::steady_clock::now() > election_deadline_) {
            lk.unlock();
            become_candidate();
        }
    }
}

bool RaftEngine::propose(LogEntryData e, std::chrono::milliseconds timeout) {
    uint32_t target_index = 0;
    {
        std::lock_guard<std::mutex> lk(raft_mtx_);
        if (role_ != RaftRole::Leader)
            return false;
        e.term = current_term_;
        log_.push_back(e);
        target_index = last_log_index();
    }

    replicate_new_entry(target_index);

    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (node_->running.load() && std::chrono::steady_clock::now() < deadline) {
        {
            std::lock_guard<std::mutex> lk(raft_mtx_);
            if (commit_index_ >= target_index)
                return true;
        }
        std::unique_lock<std::mutex> lk(raft_mtx_);
        apply_cv_.wait_for(lk, std::chrono::milliseconds(5), [this, target_index] {
            return commit_index_ >= target_index || !node_->running.load();
        });
    }
    std::lock_guard<std::mutex> lk(raft_mtx_);
    return commit_index_ >= target_index;
}

std::string RaftEngine::get_value(int32_t key) {
    std::shared_lock<std::shared_timed_mutex> lk(stripes_[stripe(key)]);
    std::string result;
    kv_.if_contains(key, [&result](const auto& item) { result = item.second; });
    return result;
}

