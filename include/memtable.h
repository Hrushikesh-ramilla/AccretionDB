#ifndef ACDB_MEMTABLE_H
#define ACDB_MEMTABLE_H

#include "vlog.h"
#include "arena.h"
#include "skiplist.h"
#include <string_view>
#include <cstddef>
#include <memory>

// Ordered in-memory key → VLogPointer store backed by a lock-free SkipList
// and a monotonic Arena allocator. 
// This replaces the naive std::map, eliminating heap fragmentation and 
// ensuring high cache locality and high concurrent read throughput.
class Memtable {
public:
    Memtable();
    ~Memtable();

    // Disable copy/move to pin internal memory.
    Memtable(const Memtable&) = delete;
    Memtable& operator=(const Memtable&) = delete;

    void put(std::string_view key, const VLogPointer& pointer);
    bool get(std::string_view key, VLogPointer& out_pointer) const;

    // We no longer track exact entry count as it requires atomic counters
    // that bottleneck the fast path. Use byte_size() for flush thresholds.
    // size_t size() const; // Removed

    size_t byte_size() const;   // Exact bytes consumed from Arena.

    // Provides forward-only lock-free iteration.
    using Iterator = acdb::ConcurrentSkipList::Iterator;
    Iterator begin() const;

private:
    std::unique_ptr<acdb::Arena> arena_;
    std::unique_ptr<acdb::ConcurrentSkipList> table_;
};

#endif // ACDB_MEMTABLE_H
