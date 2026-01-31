#include "kvstore.h"
#include <iostream>
#include <stdexcept>

KVStore::KVStore(const std::string& wal_path)
    : wal_(wal_path) {
    recover();
}

void KVStore::put(const std::string& key, const std::string& value) {
    // Step 1: serialize to WAL.
    if (!wal_.append(key, value)) {
        throw std::runtime_error("[KVStore] WAL append failed — memtable NOT updated");
    }

    // Step 2: durability boundary.
    if (!wal_.sync()) {
        throw std::runtime_error("[KVStore] WAL sync failed — memtable NOT updated");
    }

    // Step 3: update in-memory index (only reached if WAL is durable).
    memtable_.put(key, value);
}

bool KVStore::get(const std::string& key, std::string& out_value) const {
    return memtable_.get(key, out_value);
}

size_t KVStore::memtable_size() const {
    return memtable_.size();
}

bool KVStore::wal_tainted() const {
    return wal_.is_tainted();
}

// Replay WAL and rebuild memtable — idempotent, order-preserving.
void KVStore::recover() {
    auto result = wal_.replay();
    for (const auto& e : result.entries)
        memtable_.put(e.key, e.value);
    std::cout << "[KVStore] Recovered " << result.entries.size()
              << " entries from WAL.";
    if (result.tainted)
        std::cout << " (WAL TAINTED — corrupt tail detected)";
    std::cout << "\n";
}
