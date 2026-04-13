#pragma once

#include "raft_rpc.hpp"
#include <cstdint>
#include <vector>

class Log {
    std::vector<LogEntryData> entries;

public:
    Log() {
        LogEntryData dummy;
        dummy.term = 0;
        entries.push_back(dummy);
    }

    uint32_t last_index() const {
        return static_cast<uint32_t>(entries.size() - 1);
    }

    uint64_t last_term() const {
        return entries.back().term;
    }

    uint64_t term_at(uint32_t idx) const {
        if (idx == 0 || idx >= entries.size())
            return 0;
        return entries[idx].term;
    }

    const LogEntryData& at(uint32_t idx) const {
        return entries[idx];
    }

    void append(const LogEntryData& e) {
        entries.push_back(e);
    }

    void truncate(uint32_t new_size) {
        entries.resize(new_size);
    }

    void set(uint32_t idx, const LogEntryData& e) {
        entries[idx] = e;
    }

    size_t size() const { return entries.size(); }

    bool up_to_date(uint32_t cand_last_idx, uint64_t cand_last_term) const {
        uint64_t my_term = last_term();
        uint32_t my_idx = last_index();
        if (cand_last_term != my_term)
            return cand_last_term > my_term;
        return cand_last_idx >= my_idx;
    }
};
