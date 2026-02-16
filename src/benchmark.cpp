#include "benchmark.h"
#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <algorithm>
#include <filesystem>
#include <iomanip>

using namespace std::chrono;

static std::string random_string(size_t length) {
    auto randchar = []() -> char {
        const char charset[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}

static void print_metrics(const KVStore& store, double duration_s, size_t ops) {
    const auto& m = store.metrics();
    double throughput = ops / duration_s;
    double write_amp = m.user_bytes_written > 0 ? (double)m.storage_bytes_written / m.user_bytes_written : 0.0;
    double read_amp = m.get_calls > 0 ? (double)(m.sst_searches + m.vlog_reads) / m.get_calls : 0.0;

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "=== RESULT ===\n";
    std::cout << "Throughput: " << throughput << " ops/sec\n";
    std::cout << "Write Amp:  " << write_amp << "x\n";
    std::cout << "Read Amp:   " << read_amp << "x\n\n";
}

void Benchmark::run_all(const std::string& type, int num_ops) {
    std::string bench_dir = "stdb_bench_dir";
    std::filesystem::remove_all(bench_dir);
    
    std::cout << "=== BENCHMARK: " << type << " ===\n";
    
    // Cold run generates the initial state and runs the benchmark cold
    std::cout << "[COLD RUN]\n";
    run_workload(bench_dir, type, false, num_ops);

    // Warm run reuses the exact same state, now with OS pages cached
    std::cout << "[WARM RUN]\n";
    run_workload(bench_dir, type, true, num_ops);
    
    std::filesystem::remove_all(bench_dir);
}

void Benchmark::run_workload(const std::string& dir, const std::string& type, bool is_warm, int num_ops) {
    KVStore store(dir);
    store.metrics().reset(); // ensure clean metrics for this run
    
    std::vector<double> latencies;
    latencies.reserve(num_ops);
    
    // Pre-populate if this is a read test and it's the cold run (to have data to read)
    if (!is_warm && (type == "random_read" || type == "mixed")) {
        for (int i = 0; i < num_ops; ++i) {
            store.put("key_" + std::to_string(i), random_string(100));
        }
        store.metrics().reset(); // reset metrics after setup
    }

    auto start_time = high_resolution_clock::now();

    for (int i = 0; i < num_ops; ++i) {
        auto op_start = high_resolution_clock::now();
        
        if (type == "random_write") {
            store.put("key_" + std::to_string(rand() % num_ops), random_string(100));
        } 
        else if (type == "sequential_write") {
            // Formatted to ensure sequential lexicographical sorting
            char buf[32];
            snprintf(buf, sizeof(buf), "seq_%08d", i);
            store.put(buf, random_string(100));
        } 
        else if (type == "random_read") {
            std::string val;
            store.get("key_" + std::to_string(rand() % num_ops), val);
        } 
        else if (type == "mixed") {
            if (rand() % 100 < 70) {
                std::string val;
                store.get("key_" + std::to_string(rand() % num_ops), val);
            } else {
                store.put("key_" + std::to_string(rand() % num_ops), random_string(100));
            }
        }

        auto op_end = high_resolution_clock::now();
        double lat_us = duration_cast<microseconds>(op_end - op_start).count();
        latencies.push_back(lat_us);
    }

    auto end_time = high_resolution_clock::now();
    double duration_s = duration_cast<milliseconds>(end_time - start_time).count() / 1000.0;

    std::sort(latencies.begin(), latencies.end());
    double p50 = latencies[num_ops * 0.50];
    double p95 = latencies[num_ops * 0.95];
    double p99 = latencies[num_ops * 0.99];

    std::cout << std::fixed << std::setprecision(0);
    std::cout << "Latency: P50=" << p50 << "us, P95=" << p95 << "us, P99=" << p99 << "us\n";
    print_metrics(store, duration_s, num_ops);
}
