// WIP: Need to trace edge cases here (id: 5198)
#ifndef STDB_KVSTORE_H
#define STDB_KVSTORE_H

#include "wal.h"
#include "vlog.h"
#include "memtable.h"
#include "sstable.h"

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>

// KVStore — engine core (Phase 2).
//
// Write path (strict order):
//   1. WAL.append(key, value)    — full record
//   2. WAL.sync()                — durability boundary
//   3. VLog.append(value)        — returns pointer
//   4. VLog.sync()               — pointer validity boundary
//   5. Memtable.put(key, pointer)— only if 1–4 succeed
//
// Read path:
//   active memtable → immutable memtable → SSTables (newest-first) → VLog read
//
// Flush: synchronous, 4 MiB threshold, SSTable + WAL rotation.
// Recovery: WAL replay → rebuild vlog + memtable. SSTables loaded from disk.
class KVStore {
public:
    explicit KVStore(const std::string& data_dir);

    void put(const std::string& key, const std::string& value);
    bool get(const std::string& key, std::string& out_value) const;

    size_t memtable_size() const;
    bool   wal_tainted() const;

private:
    void     recover();
    void     load_sstables();
    void     maybe_flush();
    void     flush();
    void     rotate_wal();
    uint32_t next_sst_sequence() const;

    std::string wal_path() const;
    std::string wal_new_path() const;
    std::string vlog_path() const;
    std::string sst_path(uint32_t seq) const;

    std::string                  data_dir_;
    std::unique_ptr<WAL>         wal_;
    std::unique_ptr<VLog>        vlog_;
    std::unique_ptr<Memtable>    active_;
    std::unique_ptr<Memtable>    immutable_;
    std::vector<SSTableReader>   sstables_;   // sorted newest-first

    static constexpr size_t FLUSH_THRESHOLD = 4u * 1024u * 1024u;  // 4 MiB
};

#endif // STDB_KVSTORE_H
