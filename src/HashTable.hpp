#pragma once

#include "raft_rpc.hpp"
#include "../libs/ds/hashmap/phmap.hpp"
#include "../libs/utils.hpp"

#include <array>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

class HashTable {
    static constexpr size_t NUM_STRIPES = 256;
    std::array<std::shared_timed_mutex, NUM_STRIPES> stripes;
    gtl::parallel_flat_hash_map_m<int32_t, std::string> data;

    std::mutex pending_lock;
    std::condition_variable pending_cond;
    std::unordered_map<uint64_t, PendingTx> pending;

    static size_t stripe_of(int32_t key) {
        return static_cast<uint32_t>(key) % NUM_STRIPES;
    }

    void put(int32_t key, const char* raw_value) {
        std::string v(raw_value, strnlen(raw_value, MAX_VAL_SIZE));
        std::unique_lock<std::shared_timed_mutex> lk(stripes[stripe_of(key)]);
        data.insert_or_assign(key, std::move(v));
    }

    void insert_pending(uint64_t tx_id, PendingTx tx) {
        std::lock_guard<std::mutex> lk(pending_lock);
        pending[tx_id] = std::move(tx);
    }

    void commit_tx(uint64_t tx_id) {
        PendingTx tx;
        {
            std::lock_guard<std::mutex> lk(pending_lock);
            auto it = pending.find(tx_id);
            if (it == pending.end())
                return;
            tx = std::move(it->second);
            pending.erase(it);
        }
        pending_cond.notify_all();

        std::set<size_t> ids;
        for (auto& [key, val] : tx.kvs)
            ids.insert(stripe_of(key));

        std::vector<std::unique_lock<std::shared_timed_mutex>> held;
        for (size_t s : ids)
            held.emplace_back(stripes[s]);

        for (auto& [key, val] : tx.kvs)
            data.insert_or_assign(key, val);
    }

    void abort_tx(uint64_t tx_id) {
        {
            std::lock_guard<std::mutex> lk(pending_lock);
            pending.erase(tx_id);
        }
        pending_cond.notify_all();
    }

public:
    void apply(const LogEntryData& e) {
        switch (e.cmd) {
            case LogCmd::Noop:
                break;
            case LogCmd::Put:
                if (e.n_kv >= 1)
                    put(e.kvs[0].key, e.kvs[0].value);
                break;
            case LogCmd::Put3:
                for (uint8_t i = 0; i < e.n_kv && i < 3; i++)
                    put(e.kvs[i].key, e.kvs[i].value);
                break;
            case LogCmd::TxPrepare: {
                PendingTx ptx;
                for (uint8_t i = 0; i < e.n_kv && i < 3; i++) {
                    std::string v(e.kvs[i].value, strnlen(e.kvs[i].value, MAX_VAL_SIZE));
                    ptx.kvs.emplace_back(e.kvs[i].key, std::move(v));
                }
                insert_pending(e.tx_id, std::move(ptx));
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

    std::string get(int32_t key) {
        std::shared_lock<std::shared_timed_mutex> lk(stripes[stripe_of(key)]);
        std::string result;
        data.if_contains(key, [&result](const auto& item) { result = item.second; });
        return result;
    }
};
