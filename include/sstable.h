#ifndef STDB_SSTABLE_H
#define STDB_SSTABLE_H

#include "vlog.h"
#include <cstdint>
#include <map>
#include <string>
#include <vector>

// Entry stored in an SSTable: key + vlog pointer.
struct SSTableEntry {
    std::string key;
    VLogPointer pointer;
};

// Writes a sorted set of key-pointer pairs to an SSTable file.
//
// File layout:
//   [Data Section: entries in sorted key order]
//   [Footer: uint32_t entry_count, uint32_t checksum]
//
// Entry format:
//   [uint32_t key_size][key bytes][uint32_t file_id][uint64_t offset][uint32_t length]
class SSTableWriter {
public:
    // Write entries to file. Returns false on error.
    static bool write(const std::string& path,
                      const std::map<std::string, VLogPointer>& entries);
};

// Loads and queries an SSTable file.
class SSTableReader {
public:
    SSTableReader() = default;

    // Load from file. Validates footer checksum. Returns false if invalid.
    bool load(const std::string& path);

    // Binary search for key. Returns true and sets out_pointer if found.
    bool get(const std::string& key, VLogPointer& out_pointer) const;

    uint32_t sequence() const { return sequence_; }
    const std::string& path() const { return path_; }

private:
    std::string              path_;
    uint32_t                 sequence_ = 0;
    std::vector<SSTableEntry> entries_;  // sorted by key
};

#endif // STDB_SSTABLE_H

// partial state 2000
