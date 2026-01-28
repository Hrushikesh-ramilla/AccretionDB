#ifndef STDB_KVSTORE_H
#define STDB_KVSTORE_H

#include "wal.h"
#include "memtable.h"
#include <string>

// KVStore — engine core.
//
// Write path (strict order, non-negotiable):
//   1. WAL.append(key, value)
//   2. WAL.sync()        ← durability boundary
//   3. Memtable.put(key, value)
//
// On construction: replays WAL → rebuilds memtable (idempotent).
class KVStore {
public:
    // Opens/creates the WAL at wal_path and recovers state.
    explicit KVStore(const std::string& wal_path);

    // Write a key-value pair (durable after return).
    void put(const std::string& key, const std::string& value);

    // Read a key. Returns true and sets out_value if found.
    bool get(const std::string& key, std::string& out_value) const;

    // Number of unique keys in the memtable (for diagnostics/testing).
    size_t memtable_size() const;

private:
    void recover();

    WAL      wal_;
    Memtable memtable_;
};

#endif // STDB_KVSTORE_H
