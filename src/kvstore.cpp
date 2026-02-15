// WIP: Need to trace edge cases here (id: 1192)
#include "kvstore.h"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <stdexcept>

// ── Path helpers ───────────────────────────────────────────────

std::string KVStore::wal_path()     const { return data_dir_ + "/wal.bin"; }
std::string KVStore::wal_new_path() const { return data_dir_ + "/wal.bin.new"; }
std::string KVStore::vlog_path()    const { return data_dir_ + "/vlog.bin"; }

std::string KVStore::sst_path(uint32_t seq) const {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "/sst_%06u.sst", seq);
    return data_dir_ + buf;
}

uint32_t KVStore::next_sst_sequence() const {
    uint32_t max_seq = 0;
    for (const auto& s : sstables_)
        if (s.sequence() > max_seq) max_seq = s.sequence();
    return max_seq + 1;
}

// ── Constructor ────────────────────────────────────────────────

KVStore::KVStore(const std::string& data_dir)
    : data_dir_(data_dir) {
    std::filesystem::create_directories(data_dir_);
    recover();
}

// ── Write path ─────────────────────────────────────────────────

void KVStore::put(const std::string& key, const std::string& value) {
    maybe_flush();

    // Step 1: WAL append (full key + value).
    if (!wal_->append(key, value))
        throw std::runtime_error("[KVStore] WAL append failed");

    // Step 2: WAL sync — durability boundary.
    if (!wal_->sync())
        throw std::runtime_error("[KVStore] WAL sync failed");

    // Step 3: VLog append — returns pointer.
    VLogPointer ptr;
    if (!vlog_->append(value, ptr))
        throw std::runtime_error("[KVStore] VLog append failed");

    // Step 4: VLog sync — pointer validity boundary.
    if (!vlog_->sync())
        throw std::runtime_error("[KVStore] VLog sync failed — pointer NOT inserted");

    // Step 5: Memtable put — only reached if all above succeeded.
    active_->put(key, ptr);
}

// ── Read path ──────────────────────────────────────────────────

bool KVStore::get(const std::string& key, std::string& out_value) const {
    VLogPointer ptr;

    // 1. Active memtable.
    if (active_ && active_->get(key, ptr))
        return vlog_->read_at(ptr, out_value);

    // 2. Immutable memtable (exists during flush).
    if (immutable_ && immutable_->get(key, ptr))
        return vlog_->read_at(ptr, out_value);

    // 3. SSTables — newest first.
    for (const auto& sst : sstables_) {
        if (sst.get(key, ptr))
            return vlog_->read_at(ptr, out_value);
    }

    return false;
}

// ── Flush ──────────────────────────────────────────────────────

void KVStore::maybe_flush() {
    if (!active_ || active_->byte_size() < FLUSH_THRESHOLD) return;
    flush();
}

void KVStore::flush() {
    if (!active_ || active_->size() == 0) return;

    // 1. Freeze active → immutable.
    immutable_ = std::move(active_);
    active_ = std::make_unique<Memtable>();

    // 2. Write SSTable from immutable memtable.
    uint32_t seq = next_sst_sequence();
    std::string path = sst_path(seq);

    if (!SSTableWriter::write(path, immutable_->entries()))
        throw std::runtime_error("[KVStore] SSTable flush failed");

    // 3. Load the new SSTable.
    SSTableReader reader;
    if (!reader.load(path))
        throw std::runtime_error("[KVStore] Failed to load flushed SSTable");

    sstables_.insert(sstables_.begin(), std::move(reader));

    // 4. WAL rotation (crash-safe: create before delete).
    rotate_wal();

    // 5. Discard immutable memtable.
    immutable_.reset();

    std::cout << "[KVStore] Flushed SSTable sst_"
              << std::string(6 - std::to_string(seq).size(), '0') + std::to_string(seq)
              << "\n";
}

void KVStore::rotate_wal() {
    auto wp = wal_path();

    // SSTable is already fsynced. Synchronous flush means no concurrent writes.
    // Safe to close → delete → create at same path.
    // If crash after delete but before create: SSTable has the data, empty WAL on
    // next startup is correct (all flushed data is in SSTable).

    // 1. Close old WAL fd.
    wal_.reset();

    // 2. Delete old WAL.
    std::filesystem::remove(wp);

    // 3. Create fresh WAL at same path, fsync.
    wal_ = std::make_unique<WAL>(wp);
    wal_->sync();
}

// ── Recovery ───────────────────────────────────────────────────

void KVStore::recover() {
    auto wp = wal_path();

    // Load existing SSTables (validate each).
    load_sstables();

    // VLog handling:
    //   If SSTables exist → keep vlog (SST pointers reference it).
    //   If no SSTables    → safe to recreate vlog from WAL.
    auto vp = vlog_path();
    if (sstables_.empty())
        std::filesystem::remove(vp);

    vlog_ = std::make_unique<VLog>(vp);

    // Open WAL.
    wal_ = std::make_unique<WAL>(wp);

    // Replay WAL: append values to vlog, insert pointers into memtable.
    active_ = std::make_unique<Memtable>();
    auto result = wal_->replay();

    for (const auto& e : result.entries) {
        VLogPointer ptr;
        if (!vlog_->append(e.value, ptr)) {
            std::cerr << "[KVStore] ERROR: vlog append failed during recovery\n";
            continue;
        }
        active_->put(e.key, ptr);
    }
    vlog_->sync();

    std::cout << "[KVStore] Recovered " << result.entries.size() << " entries from WAL";
    if (!sstables_.empty())
        std::cout << ", loaded " << sstables_.size() << " SSTables";
    if (result.tainted)
        std::cout << " (WAL TAINTED)";
    std::cout << "\n";
}

// ── SSTable loading ────────────────────────────────────────────

void KVStore::load_sstables() {
    sstables_.clear();
    if (!std::filesystem::exists(data_dir_)) return;

    for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
        auto name = entry.path().filename().string();
        if (name.size() > 4 && name.substr(0, 4) == "sst_" &&
            name.substr(name.size() - 4) == ".sst") {
            SSTableReader reader;
            if (reader.load(entry.path().string())) {
                sstables_.push_back(std::move(reader));
            } else {
                std::cerr << "[KVStore] WARNING: discarding invalid SSTable: "
                          << name << "\n";
                std::filesystem::remove(entry.path());
            }
        }
    }

    // Sort newest-first (descending sequence number).
    std::sort(sstables_.begin(), sstables_.end(),
              [](const SSTableReader& a, const SSTableReader& b) {
                  return a.sequence() > b.sequence();
              });
}

// ── Diagnostics ────────────────────────────────────────────────

size_t KVStore::memtable_size() const {
    size_t n = active_ ? active_->size() : 0;
    if (immutable_) n += immutable_->size();
    return n;
}

bool KVStore::wal_tainted() const {
    return wal_ && wal_->is_tainted();
}
