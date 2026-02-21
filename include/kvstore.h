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
// WAL files: wal_NNNNNN.log (monotonically increasing).
// Rotation: create new WAL → fsync → switch → delete old (I19 safe).
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
    void     scan_wal_files(std::vector<std::string>& paths, uint32_t& max_id) const;
    void     maybe_flush();
    void     flush();
    void     rotate_wal();
    uint32_t next_sst_sequence() const;

    std::string wal_path(uint32_t id) const;
    std::string vlog_path() const;
    std::string sst_path(uint32_t seq) const;

    std::string                  data_dir_;
    std::unique_ptr<WAL>         wal_;
    std::unique_ptr<VLog>        vlog_;
    std::unique_ptr<Memtable>    active_;
    std::unique_ptr<Memtable>    immutable_;
    std::vector<SSTableReader>   sstables_;   // sorted newest-first
    uint32_t                     current_wal_id_ = 1;

    static constexpr size_t FLUSH_THRESHOLD = 4u * 1024u * 1024u;  // 4 MiB
};

#endif // STDB_KVSTORE_H
