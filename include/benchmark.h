#ifndef ACDB_BENCHMARK_H
#define ACDB_BENCHMARK_H

#include "kvstore.h"
#include <string>

class Benchmark {
public:
    static void run_all(const std::string& type, int num_ops = 20000); 
    static void run_fault_injection();
    static void run_fault_crash();
    static void run_fault_suite();
    static void run_fault_crash_target(const std::string& target);
    static void run_stress_test();
private:
    static void run_workload(const std::string& dir, const std::string& type, bool is_warm, int num_ops, int num_threads);
};

#endif // ACDB_BENCHMARK_H
