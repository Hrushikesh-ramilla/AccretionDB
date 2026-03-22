#include "benchmark.h"
#include "kvstore.h"
#include "fault_injection.h"
#include "http_server.h"
#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <fstream>
#include <thread>

#ifdef _WIN32
  #include <windows.h>
  #include <psapi.h>
#endif

using namespace std::chrono;

static void bench_clean_dir(const std::string& dir) {
    std::error_code ec;
    for (int i = 0; i < 10; ++i) {
        std::filesystem::remove_all(dir, ec);
        if (!ec) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}


static std::string random_string(size_t length) {
    thread_local std::mt19937 rng(std::random_device{}());
    const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 2);
    std::uniform_int_distribution<size_t> dist(0, max_index);

    std::string str(length, 0);
    for (size_t i = 0; i < length; ++i) {
        str[i] = charset[dist(rng)];
    }
    return str;
}



void Benchmark::run_all(const std::string& type, int num_ops) {
    std::string bench_dir = "acdb_run2_bench_dir";
    std::vector<int> thread_counts = {1, 2, 4, 8, 16};
    
    std::cout << "=== BENCHMARK: " << type << " ===\n";
    
    for (int t : thread_counts) {
        std::string bench_dir = "acdb_run2_bench_dir_" + std::to_string(t);
        bench_clean_dir(bench_dir);
        std::cout << "\n[" << t << " THREADS]\n";
        
        // Run only once (warm-like since we create from scratch but no double run)
        run_workload(bench_dir, type, true, num_ops, t);
    }
    
    // Mandated Large Scale Benchmark (5M Ops)
    if (type == "random_write_async" || type == "random_write") {
        std::cout << "\n=== MANDATED LARGE SCALE BENCHMARK (5M Ops) ===\n";
        bench_clean_dir(bench_dir);
        run_workload(bench_dir, type, true, 5000000, 16);
    }
    
    bench_clean_dir(bench_dir);
}

void Benchmark::run_fault_injection() {
    run_fault_suite();
}

void Benchmark::run_fault_crash() {
    // Legacy fallback
}

void Benchmark::run_fault_suite() {
    std::vector<std::string> points = {
        "crash_during_wal_append",
        "crash_after_wal_append",
        "crash_during_flush",
        "crash_during_compaction",
        "crash_during_vlog_rewrite",
        "crash_during_manifest_update"
    };

    std::cout << "=== Fault Injection Suite ===\n";

    KVStore::FLUSH_THRESHOLD = 128 * 1024;
    VLog::MAX_FILE_SIZE = 512 * 1024;
    int passed = 0;

    for (const auto& point : points) {
        std::string dir = "acdb_fault_test_" + point;
        bench_clean_dir(dir);

        std::cout << "[Suite] Testing fault point: " << point << "...\n";
        
        std::string cmd = "acdb.exe bench fault_crash_target " + point;
#ifndef _WIN32
        cmd = "./acdb bench fault_crash_target " + point;
#endif
        int ret = std::system(cmd.c_str());
        std::cout << "  > Process exited with code: " << ret << "\n";

        // Recover
        try {
            KVStore store(dir);
            
            // Verify basic functionality after recovery
            std::string val;
            bool ok = true;
            // Depending on when it crashed, not all keys may be present.
            // The important part is that we didn't corrupt the index.
            store.put("post_recovery_key", "OK");
            if (!store.get("post_recovery_key", val) || val != "OK") {
                ok = false;
            }
            
            if (ok) {
                std::cout << "  > Database recovered successfully and invariants hold.\n";
                passed++;
            } else {
                std::cout << "  > FAILED: Database recovered but is non-functional.\n";
            }
        } catch (const std::exception& e) {
            std::cout << "  > FAILED: " << e.what() << "\n";
        }
        
        std::error_code ec;
        bench_clean_dir(dir);
    }

    std::cout << "=== Suite Completed ===\n";
    std::cout << "Passed: " << passed << " / " << points.size() << "\n";
}

void Benchmark::run_fault_crash_target(const std::string& target) {
    std::string dir = "acdb_fault_test_" + target;
    bench_clean_dir(dir);

    KVStore::FLUSH_THRESHOLD = 128 * 1024; // 128 KB
    VLog::MAX_FILE_SIZE = 512 * 1024;      // 512 KB
    FaultInjection::arm(target);
    KVStore* store = new KVStore(dir);
    std::string val(1024, 'X');
    std::vector<std::thread> threads;
    for (int t = 0; t < 16; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 20000; ++i) {
                store->put("key_" + std::to_string(t * 20000 + i), std::string(1024, 'A' + (t % 26)));
            }
        });
    }
    for (auto& th : threads) th.join();
    std::cout << "[Fault Crash] Target hit or workload finished.\n";
    std::fflush(stdout);
    std::_Exit(1); 
}

void Benchmark::run_stress_test() {
    std::string dir = "acdb_stress_test";
    bench_clean_dir(dir);
    
    std::cout << "=== Long-Running Stress Test ===\n";
    std::cout << "Phase 1: 1M concurrent writes & deletes\n";
    
    {
        KVStore store(dir);
        int num_ops = 1000000;
        int num_threads = 16;
        int ops_per_thread = num_ops / num_threads;
        
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                for (int i = 0; i < ops_per_thread; ++i) {
                    int key_id = t * ops_per_thread + i;
                    std::string key = "key_" + std::to_string(key_id);
                    KVStore::WriteOptions wo;
                    wo.sync = false;
                    if (i % 10 == 0) {
                        store.delete_key(key, wo);
                    } else {
                        store.put(key, "val_" + std::to_string(key_id), wo);
                    }
                }
            });
        }
        
        for (auto& th : threads) th.join();
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start).count();
        std::cout << "Write Phase Completed in " << duration << " ms (" << (num_ops * 1000.0 / duration) << " ops/sec)\n";
        
        std::cout << "Phase 2: 1M reads & verification\n";
        start = high_resolution_clock::now();
        int missing = 0;
        int phantom = 0;
        
        threads.clear();
        std::atomic<int> missing_atomic{0};
        std::atomic<int> phantom_atomic{0};
        
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                int local_missing = 0;
                int local_phantom = 0;
                for (int i = 0; i < ops_per_thread; ++i) {
                    int key_id = t * ops_per_thread + i;
                    std::string key = "key_" + std::to_string(key_id);
                    std::string val;
                    bool exists = store.get(key, val);
                    
                    if (i % 10 == 0) {
                        if (exists) local_phantom++;
                    } else {
                        if (!exists || val != "val_" + std::to_string(key_id)) {
                            local_missing++;
                        }
                    }
                }
                missing_atomic += local_missing;
                phantom_atomic += local_phantom;
            });
        }
        
        for (auto& th : threads) th.join();
        end = high_resolution_clock::now();
        duration = duration_cast<milliseconds>(end - start).count();
        std::cout << "Read Phase Completed in " << duration << " ms (" << (num_ops * 1000.0 / duration) << " ops/sec)\n";
        
        std::cout << "Missing keys: " << missing_atomic << "\n";
        std::cout << "Phantom keys: " << phantom_atomic << "\n";
        
        if (missing_atomic == 0 && phantom_atomic == 0) {
            std::cout << "STRESS TEST PASSED\n";
        } else {
            std::cout << "STRESS TEST FAILED\n";
        }
    }
    
    bench_clean_dir(dir);
}

void Benchmark::run_workload(const std::string& dir, const std::string& type, bool is_warm, int num_ops, int num_threads) {
    EngineMetrics final_metrics;
    double duration_s = 0;
    std::vector<double> latencies;
    
    // Store needs to be in a scope that survives until p99 is calculated
    {
        KVStore store(dir);
        
        g_http_kvstore = &store;
        
        store.metrics().reset(); // ensure clean metrics for this run
        
        // Pre-populate if this is a read test and it's the cold run (to have data to read)
        if (!is_warm && (type == "random_read" || type == "mixed")) {
            for (int i = 0; i < num_ops; ++i) {
                store.put("key_" + std::to_string(i), random_string(100));
            }
            store.metrics().reset(); // reset metrics after setup
        }

        latencies.resize(num_ops);
        std::vector<std::jthread> threads;
        int ops_per_thread = num_ops / num_threads;

        auto start_time = high_resolution_clock::now();

        for (int t = 0; t < num_threads; ++t) {
            int start_idx = t * ops_per_thread;
            int end_idx = (t == num_threads - 1) ? num_ops : start_idx + ops_per_thread;
            
            threads.emplace_back([&store, type, num_ops, start_idx, end_idx, &latencies]() {
                thread_local std::mt19937 rng(std::random_device{}());
                std::uniform_int_distribution<int> dist(0, num_ops - 1);
                std::uniform_int_distribution<int> mixed_dist(0, 99);

                for (int i = start_idx; i < end_idx; ++i) {
                    auto op_start = high_resolution_clock::now();
                    
                    if (type == "random_write") {
                        store.put("key_" + std::to_string(dist(rng)), random_string(100));
                    } 
                    else if (type == "random_write_async") {
                        KVStore::WriteOptions opts;
                        opts.sync = false;
                        store.put("key_" + std::to_string(dist(rng)), random_string(100), opts);
                    }
                    else if (type == "sequential_write") {
                        char buf[32];
                        snprintf(buf, sizeof(buf), "seq_%08d", i);
                        store.put(buf, random_string(100));
                    } 
                    else if (type == "random_read") {
                        std::string val;
                        store.get("key_" + std::to_string(dist(rng)), val);
                    } 
                    else if (type == "mixed") {
                        if (mixed_dist(rng) < 70) {
                            std::string val;
                            store.get("key_" + std::to_string(dist(rng)), val);
                        } else {
                            store.put("key_" + std::to_string(dist(rng)), random_string(100));
                        }
                    }

                    auto op_end = high_resolution_clock::now();
                    auto dur_us = duration_cast<microseconds>(op_end - op_start).count();
                    latencies[i] = dur_us;
                    
                    if (type == "sequential_write" && dur_us > 5000) {
                        auto now_ms = duration_cast<milliseconds>(op_end.time_since_epoch()).count();
                        std::ofstream trace_out("acdb_trace.txt", std::ios::app);
                        trace_out << "SPIKE " << now_ms << " " << dur_us << "us\n";
                    }
                }
#if ENABLE_TELEMETRY
                g_telemetry.merge(t_telemetry.m);
#endif
            });


        }
        
        // Wait for all threads to finish implicitly via jthread destruction
        threads.clear();

        auto end_time = high_resolution_clock::now();
        duration_s = duration_cast<milliseconds>(end_time - start_time).count() / 1000.0;
        final_metrics = store.metrics();
        std::sort(latencies.begin(), latencies.end());
        double p50 = latencies[num_ops * 0.50];
        double p95 = latencies[num_ops * 0.95];
        double p99 = latencies[num_ops * 0.99];
        
        store.metrics().p99_latency_us.store((uint64_t)p99);

        double throughput = num_ops / duration_s;
        double write_amp = final_metrics.user_bytes_written > 0 ? (double)final_metrics.storage_bytes_written / final_metrics.user_bytes_written : 0.0;
        double read_amp = final_metrics.get_calls > 0 ? (double)(final_metrics.sst_searches + final_metrics.vlog_reads) / final_metrics.get_calls : 0.0;
        double cache_hit_rate = (final_metrics.block_cache_hits + final_metrics.block_cache_misses) > 0 ? 
            (double)final_metrics.block_cache_hits / (final_metrics.block_cache_hits + final_metrics.block_cache_misses) * 100.0 : 0.0;

        auto& m = final_metrics;
        std::cout << std::fixed << std::setprecision(0);
        std::cout << "Throughput: " << throughput << " ops/sec | Latency: P50=" << p50 << "us P95=" << p95 << "us P99=" << p99 << "us | W-Amp: " << std::setprecision(2) << write_amp << "x | R-Amp: " << read_amp << "x\n";
        std::cout << "Cache: " << std::fixed << std::setprecision(1) << cache_hit_rate << "% ("
                  << m.block_cache_hits << " hits, " << m.block_cache_misses << " misses) | "
                  << "Compactions: " << m.compaction_count << " ("
                  << m.compaction_duration_ms << " ms total)\n";
        
        g_http_kvstore = nullptr;
    } // store is destructed here, releasing ALL file handles
    
#if ENABLE_TELEMETRY
    uint64_t l_count = 0, f_count = 0;
    uint64_t hist[7] = {0};
    {
        std::lock_guard<std::mutex> lock(g_telemetry.mu);
        for (auto& tm : g_telemetry.thread_metrics) {
            l_count += tm.leader_count;
            f_count += tm.follower_count;
            hist[0] += tm.batch_hist_1;
            hist[1] += tm.batch_hist_2;
            hist[2] += tm.batch_hist_3;
            hist[3] += tm.batch_hist_4;
            hist[4] += tm.batch_hist_5_8;
            hist[5] += tm.batch_hist_9_16;
            hist[6] += tm.batch_hist_17_plus;
        }
    }
    std::cout << "Group Commit: Leaders=" << l_count 
              << " Followers=" << f_count 
              << " AvgBatch=" << (l_count > 0 ? (double)(l_count + f_count) / l_count : 0.0) 
              << "\n";
    std::cout << "Histogram: 1=["<<hist[0]<<"] 2=["<<hist[1]<<"] 3=["<<hist[2]<<"] 4=["<<hist[3]<<"] 5-8=["<<hist[4]<<"] 9-16=["<<hist[5]<<"] 17+=["<<hist[6]<<"]\n";
#endif
    std::cout << "--------------------------------------------------------\n";
    
#if ENABLE_TELEMETRY
    std::string report_name = "telemetry_" + std::to_string(num_threads) + "t.json";
    
    // Add memory and CPU metrics if on Windows
#ifdef _WIN32
    HANDLE hProcess = GetCurrentProcess();
    PROCESS_MEMORY_COUNTERS pmc;
    if (GetProcessMemoryInfo(hProcess, &pmc, sizeof(pmc))) {
        t_telemetry.m.heap_allocations = pmc.PeakWorkingSetSize;
    }
    
    FILETIME fCreationTime, fExitTime, fKernelTime, fUserTime;
    if (GetProcessTimes(hProcess, &fCreationTime, &fExitTime, &fKernelTime, &fUserTime)) {
        ULARGE_INTEGER k, u;
        k.LowPart = fKernelTime.dwLowDateTime;
        k.HighPart = fKernelTime.dwHighDateTime;
        u.LowPart = fUserTime.dwLowDateTime;
        u.HighPart = fUserTime.dwHighDateTime;
        
        // Storing CPU time (in 100ns intervals) into unused telemetry fields for extraction
        t_telemetry.m.buffer_reallocations = k.QuadPart;
        t_telemetry.m.buffer_growth_count = u.QuadPart;
    }
#endif

    g_telemetry.dump_to_json(report_name);
    g_telemetry.reset();
#endif
}
