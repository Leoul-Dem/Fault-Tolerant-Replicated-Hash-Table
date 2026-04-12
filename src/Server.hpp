#pragma once

#include "Node.hpp"
#include "../libs/ds/concurrentqueue.h"
#include "../libs/utils.hpp"
#include <atomic>
#include <thread>
#include <chrono>
#include <vector>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <mutex>

inline int runServer(const int port, const int argc, const char** argv,
              moodycamel::ConcurrentQueue<Request_Cut> &req_queue,
              moodycamel::ConcurrentQueue<Response_Cut> &resp_queue,
              std::atomic<bool> &running){
    Node node{port, argc, argv};

    // Metrics
    std::atomic<uint64_t> successful_ops{0};
    std::atomic<uint64_t> latency_sum_us{0};
    std::atomic<uint64_t> latency_count{0};
    std::mutex latency_mtx;
    std::vector<double> latencies_us;

    auto start_time = std::chrono::steady_clock::now();

    auto run = [&](){
        std::vector<double> local_latencies;
        local_latencies.reserve(100000);

        while(running.load()){
            Request_Cut req{};
            Response_Cut resp{};

            if(!req_queue.try_dequeue(req)){
                std::this_thread::sleep_for(std::chrono::microseconds(50));
                continue;
            }

            auto op_start = std::chrono::steady_clock::now();

            bool success = false;
            switch (req.op) {
                case PUT: {
                    resp.id = req.id;
                    std::string val(req.inputs[0].value, strnlen(req.inputs[0].value, MAX_VAL_SIZE));
                    resp.success = node.put(req.inputs[0].key, val);
                    success = resp.success;
                    resp_queue.enqueue(resp);
                    break;
                }
                case PUT3: {
                    resp.id = req.id;
                    std::array<KV_Pair, 3> kvs;
                    for (int i = 0; i < 3; i++) {
                        kvs[i].key = req.inputs[i].key;
                        kvs[i].value = std::string(req.inputs[i].value, strnlen(req.inputs[i].value, MAX_VAL_SIZE));
                    }
                    resp.success = node.put3(kvs);
                    success = resp.success;
                    resp_queue.enqueue(resp);
                    break;
                }
                case GET: {
                    resp.id = req.id;
                    std::string result = node.get(req.inputs[0].key);
                    std::memset(resp.output, 0, MAX_VAL_SIZE);
                    std::memcpy(resp.output, result.data(),
                                std::min(result.size(), MAX_VAL_SIZE));
                    success = true;
                    resp.success = true;
                    resp_queue.enqueue(resp);
                    break;
                }
                default:
                    break;
            }

            auto op_end = std::chrono::steady_clock::now();
            double us = std::chrono::duration<double, std::micro>(op_end - op_start).count();
            local_latencies.push_back(us);

            if(success) {
                successful_ops.fetch_add(1, std::memory_order_relaxed);
                latency_sum_us.fetch_add(static_cast<uint64_t>(us), std::memory_order_relaxed);
                latency_count.fetch_add(1, std::memory_order_relaxed);
            }
        }

        std::lock_guard<std::mutex> lock(latency_mtx);
        latencies_us.insert(latencies_us.end(), local_latencies.begin(), local_latencies.end());
    };

    constexpr int NUM_WORKERS = 8;
    std::vector<std::thread> workers;
    workers.reserve(NUM_WORKERS);
    for (int i = 0; i < NUM_WORKERS; i++)
        workers.emplace_back(run);

    std::thread recv_thread(&Node::recv_request, &node);

    // Per-second throughput + latency reporter
    uint64_t prev_ops = 0;
    uint64_t prev_lat_sum = 0;
    uint64_t prev_lat_cnt = 0;
    auto prev_tick = std::chrono::steady_clock::now();
    int elapsed_sec = 0;
    while(running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        auto now = std::chrono::steady_clock::now();
        double dt = std::chrono::duration<double>(now - prev_tick).count();
        prev_tick = now;
        uint64_t cur = successful_ops.load(std::memory_order_relaxed);
        uint64_t cur_lat_sum = latency_sum_us.load(std::memory_order_relaxed);
        uint64_t cur_lat_cnt = latency_count.load(std::memory_order_relaxed);
        uint64_t delta_ops = cur - prev_ops;
        uint64_t delta_lat_sum = cur_lat_sum - prev_lat_sum;
        uint64_t delta_lat_cnt = cur_lat_cnt - prev_lat_cnt;
        prev_ops = cur;
        prev_lat_sum = cur_lat_sum;
        prev_lat_cnt = cur_lat_cnt;
        elapsed_sec++;
        double avg_lat = delta_lat_cnt > 0 ? static_cast<double>(delta_lat_sum) / delta_lat_cnt : 0.0;
        std::fprintf(stdout, "[T=%ds] %lu ops/s %.0f us\n", elapsed_sec,
                     static_cast<unsigned long>(delta_ops / dt), avg_lat);
    }

    node.stop();

    for (auto& t : workers) t.join();
    recv_thread.join();

    // Print stats
    auto end_time = std::chrono::steady_clock::now();
    double elapsed_s = std::chrono::duration<double>(end_time - start_time).count();
    uint64_t total_success = successful_ops.load();

    std::fprintf(stdout, "\n=== Performance Stats ===\n");
    std::fprintf(stdout, "Elapsed:          %.2f s\n", elapsed_s);
    std::fprintf(stdout, "Successful ops:   %lu\n", total_success);
    std::fprintf(stdout, "Throughput:       %.2f ops/s\n", total_success / elapsed_s);

    if(!latencies_us.empty()){
        // Filter out shutdown artifacts (ops > 2s are stuck during teardown)
        constexpr double CUTOFF_US = 2'000'000.0;
        std::vector<double> filtered;
        filtered.reserve(latencies_us.size());
        for (double v : latencies_us)
            if (v <= CUTOFF_US) filtered.push_back(v);

        std::sort(filtered.begin(), filtered.end());
        size_t n = filtered.size();

        if (n > 0) {
            double sum = 0;
            for(double v : filtered) sum += v;

            std::fprintf(stdout, "Total operations: %zu\n", n);
            std::fprintf(stdout, "Avg latency:      %.2f us\n", sum / n);
            std::fprintf(stdout, "P50 latency:      %.2f us\n", filtered[n / 2]);
            std::fprintf(stdout, "P90 latency:      %.2f us\n", filtered[n * 90 / 100]);
            std::fprintf(stdout, "P99 latency:      %.2f us\n", filtered[n * 99 / 100]);
            std::fprintf(stdout, "Min latency:      %.2f us\n", filtered.front());
            std::fprintf(stdout, "Max latency:      %.2f us\n", filtered.back());
        }
    }
    std::fprintf(stdout, "=========================\n");

    return 0;
}
