#include "kvstore.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>

// ── Path helpers ───────────────────────────────────────────────

std::string KVStore::wal_path(uint32_t id) const {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "/wal_%06u.log", id);
    return data_dir_ + buf;
}

std::string KVStore::vlog_path() const { return data_dir_ + "/vlog.bin"; }

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

// Scan data_dir_ for wal_*.log files. Returns sorted paths and max id found.
void KVStore::scan_wal_files(std::vector<std::string>& paths, uint32_t& max_id) const {
    paths.clear();
    max_id = 0;
    if (!std::filesystem::exists(data_dir_)) return;

    for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
        auto name = entry.path().filename().string();
        if (name.size() > 4 && name.substr(0, 4) == "wal_" &&
            name.size() > 4 && name.substr(name.size() - 4) == ".log") {
            uint32_t id = static_cast<uint32_t>(
                std::strtoul(name.c_str() + 4, nullptr, 10));
            if (id > max_id) max_id = id;
            paths.push_back(entry.path().string());
        }
    }
    // Sort ascending by filename (lexicographic = numeric due to zero-padding).
    std::sort(paths.begin(), paths.end());
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

    // 4. WAL rotation (crash-safe: create-before-delete, I19).
    rotate_wal();

    // 5. Discard immutable memtable.
    immutable_.reset();

    std::cout << "[KVStore] Flushed SSTable sst_"
              << std::string(6 - std::to_string(seq).size(), '0') + std::to_string(seq)
              << "\n";
}

// ── WAL rotation (crash-safe, I19) ─────────────────────────────
//
// Sequence:
//   1. Create NEW WAL at wal_{id+1}.log → fsync
//   2. Switch KVStore to new WAL (close old fd)
//   3. Delete old WAL at wal_{id}.log
//
// Old WAL is NEVER deleted before new WAL is durable.
// If crash between steps 1 and 3: both WAL files exist on disk.
// Recovery replays all WAL files in order — duplicates resolved by I8.

void KVStore::rotate_wal() {
    uint32_t old_id = current_wal_id_;
    uint32_t new_id = old_id + 1;
    std::string old_wp = wal_path(old_id);
    std::string new_wp = wal_path(new_id);

    // 1. Create new WAL, fsync (durable BEFORE we touch old).
    auto new_wal = std::make_unique<WAL>(new_wp);
    new_wal->sync();

    // 2. Switch: old WAL destructor closes its fd.
    wal_ = std::move(new_wal);
    current_wal_id_ = new_id;

    // 3. NOW safe to delete old WAL (new WAL is durable).
    std::filesystem::remove(old_wp);
}

// ── Recovery ───────────────────────────────────────────────────

void KVStore::recover() {
    // Load existing SSTables (validate each).
    load_sstables();

    // Scan for WAL files.
    std::vector<std::string> wal_files;
    uint32_t max_wal_id = 0;
    scan_wal_files(wal_files, max_wal_id);

    // VLog handling:
    //   If SSTables exist → keep vlog (SST pointers reference it).
    //   If no SSTables    → safe to recreate vlog from WAL.
    auto vp = vlog_path();
    if (sstables_.empty())
        std::filesystem::remove(vp);

    vlog_ = std::make_unique<VLog>(vp);

    // Replay ALL WAL files in order (oldest → newest).
    active_ = std::make_unique<Memtable>();
    size_t total_entries = 0;
    bool   any_tainted = false;

    for (const auto& wf : wal_files) {
        WAL temp_wal(wf);
        auto result = temp_wal.replay();
        any_tainted = any_tainted || result.tainted;

        for (const auto& e : result.entries) {
            VLogPointer ptr;
            if (!vlog_->append(e.value, ptr)) {
                std::cerr << "[KVStore] ERROR: vlog append failed during recovery\n";
                continue;
            }
            active_->put(e.key, ptr);
        }
        total_entries += result.entries.size();
    }
    vlog_->sync();

    // Set current WAL id and open the active WAL.
    current_wal_id_ = (max_wal_id > 0) ? max_wal_id : 1;

    // If WAL files existed, the newest is already the active one.
    // If no WAL files existed, create the first one.
    wal_ = std::make_unique<WAL>(wal_path(current_wal_id_));

    std::cout << "[KVStore] Recovered " << total_entries << " entries from "
              << wal_files.size() << " WAL(s)";
    if (!sstables_.empty())
        std::cout << ", loaded " << sstables_.size() << " SSTables";
    if (any_tainted)
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
