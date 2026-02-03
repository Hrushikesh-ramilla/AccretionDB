#ifndef STDB_MEMTABLE_H
#define STDB_MEMTABLE_H

#include <map>
#include <string>
#include <cstddef>

// Ordered in-memory key-value store backed by std::map (red-black tree).
// Phase 1: no flush, no eviction, unbounded growth.
class Memtable {
public:
    // Insert or update a key-value pair.
    void put(const std::string& key, const std::string& value);

    // Lookup a key. Returns true and sets out_value if found.
    bool get(const std::string& key, std::string& out_value) const;

    // Number of unique keys stored.
    size_t size() const;

private:
    std::map<std::string, std::string> table_;
};

#endif // STDB_MEMTABLE_H

// partial state 1066
