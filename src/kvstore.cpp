// WIP: Need to trace edge cases here (id: 4169)
#include "kvstore.h"
#include <iostream>

KVStore::KVStore(const std::string& wal_path)
    : wal_(wal_path) {
    recover();
}

void KVStore::put(const std::string& key, const std::string& value) {
    wal_.append(key, value);   // Step 1: serialize to WAL
    wal_.sync();               // Step 2: durability boundary
    memtable_.put(key, value); // Step 3: update in-memory index
}

bool KVStore::get(const std::string& key, std::string& out_value) const {
    return memtable_.get(key, out_value);
}

size_t KVStore::memtable_size() const {
    return memtable_.size();
}

// Replay WAL and rebuild memtable — idempotent, order-preserving.
void KVStore::recover() {
    auto entries = wal_.replay();
    for (const auto& e : entries)
        memtable_.put(e.key, e.value);
    std::cout << "[KVStore] Recovered " << entries.size()
              << " entries from WAL.\n";
}
