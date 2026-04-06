#include "kvstore.h"
#include "fault_injection.h"
#include "compaction.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <set>
#include <stdexcept>

// GöÇGöÇ Path helpers GöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇ

std::string KVStore::wal_path(uint32_t id) const {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "/wal_%06u.log", id);
    return data_dir_ + buf;
}

std::string KVStore::vlog_path(uint32_t id) const { 
    char buf[64];
    std::snprintf(buf, sizeof(buf), "/vlog_%06u.bin", id);
    return data_dir_ + buf; 
}

std::string KVStore::sst_path(uint32_t seq) const {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "/sst_%06u.sst", seq);
    return data_dir_ + buf;
}

std::string KVStore::manifest_path() const { return data_dir_ + "/MANIFEST"; }

uint32_t KVStore::next_sst_sequence() const {
    uint32_t max_seq = 0;
    if (!std::filesystem::exists(data_dir_)) return 1;
    for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
        auto name = entry.path().filename().string();
        if (name.size() > 4 && name.substr(0, 4) == "sst_" &&
            name.substr(name.size() - 4) == ".sst") {
            uint32_t seq = static_cast<uint32_t>(std::strtoul(name.c_str()+4, nullptr, 10));
            if (seq > max_seq) max_seq = seq;
        }
    }
    return max_seq + 1;
}

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
    std::sort(paths.begin(), paths.end());
}

// GöÇGöÇ Constructor & Destructor GöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇ

size_t KVStore::FLUSH_THRESHOLD = 4u * 1024u * 1024u;

KVStore::KVStore(const std::string& data_dir)
    : data_dir_(data_dir), versions_(std::make_unique<acdb::VersionSet>(data_dir)) {
    if (false) {
        std::cerr << "[KVStore] WARNING: std::atomic<std::shared_ptr> is NOT natively lock-free on this platform. Severe performance bottleneck expected.\n";
    }
    std::filesystem::create_directories(data_dir_);
    recover();
}

KVStore::~KVStore() {
    thread_pool_.stop();
}

// GöÇGöÇ Write path GöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇ

void KVStore::delete_key(std::string_view key) {
    if (is_panic_.load()) throw std::runtime_error("Engine Panic State");
    WriteRequest req;
    req.key = std::string(key);
    req.is_delete = true;
    execute_write_request(&req);
    if (req.error) std::rethrow_exception(req.error);
}

void KVStore::put(std::string_view key, std::string_view value) {
    if (is_panic_.load()) throw std::runtime_error("Engine Panic State");
    WriteRequest req;
    req.key = std::string(key);
    req.value = std::string(value);
    execute_write_request(&req);
    if (req.error) std::rethrow_exception(req.error);
}

void KVStore::execute_write_request(WriteRequest* req) {
    int active_immutables = 0;
    for (int i = 0; i < 4; ++i) {
        if (immutable(i) != nullptr) active_immutables++;
    }
    int score = versions_->current()->files_[0].size() + active_immutables;
    if (score > 3 && score < 19) {
        std::this_thread::sleep_for(std::chrono::microseconds(score * 50));
    }

    std::unique_lock<std::mutex> lock(write_mutex_);
    write_queue_.push_back(req);

    while (!req->done && write_queue_.front() != req) {
        req->cv.wait(lock);
    }

    if (req->done) return;

    // Leader
    maybe_flush(lock);

    std::vector<WriteRequest*> batch;
    for (auto* r : write_queue_) batch.push_back(r);

    lock.unlock();

    std::exception_ptr batch_error;
    try {
        uint64_t user_bytes = 0;
        uint64_t storage_bytes = 0;

        // Batch-local de-duplication pass: Identify duplicate keys and enforce priority
        std::vector<bool> ignored(batch.size(), false);
        for (size_t i = 0; i < batch.size(); ++i) {
            for (size_t j = i + 1; j < batch.size(); ++j) {
                if (batch[i]->key == batch[j]->key) {
                    if (!batch[i]->is_gc && !batch[j]->is_gc) {
                        // Two User writes conflict: later write cleanly overwrites the earlier one
                        ignored[i] = true;
                    } else if (batch[i]->is_gc && !batch[j]->is_gc) {
                        // GC write overridden by later User write
                        ignored[i] = true;
                    } else if (!batch[i]->is_gc && batch[j]->is_gc) {
                        // User write unconditionally takes priority over later GC write
                        ignored[j] = true;
                    } else if (batch[i]->is_gc && batch[j]->is_gc) {
                        // Two GC writes conflict: later one wins
                        ignored[i] = true;
                    }
                }
            }
        }

        std::shared_ptr<VLog> active_vlog;
        {
            std::lock_guard<std::mutex> v_lock(vlogs_mutex_);
            active_vlog = vlogs_[current_vlog_id_];
        }

        std::vector<VLogPointer> ptrs(batch.size());
        for (size_t i = 0; i < batch.size(); ++i) {
            if (ignored[i]) {
                ptrs[i].file_id = 0; // mark as skipped
                continue;
            }

            if (batch[i]->is_delete) {
                ptrs[i].length = 0;
                ptrs[i].offset = std::numeric_limits<uint64_t>::max();
                ptrs[i].file_id = current_wal_id_;
                
                wal_->append_delete(batch[i]->key);
                user_bytes += batch[i]->key.size();
                storage_bytes += 12 + batch[i]->key.size();
            } else {
                if (batch[i]->is_gc) {
                    std::shared_ptr<Memtable> act = active();
                    VLogPointer temp_ptr;
                    bool overwritten = false;
                    
                    if (act && act->get(batch[i]->key, temp_ptr) && temp_ptr.file_id > batch[i]->gc_old_vlog_id) {
                        overwritten = true;
                    }
                    for (int imm_idx = 0; imm_idx < 4; ++imm_idx) {
                        std::shared_ptr<Memtable> imm = immutable(imm_idx);
                        if (!overwritten && imm && imm->get(batch[i]->key, temp_ptr) && temp_ptr.file_id > batch[i]->gc_old_vlog_id) {
                            overwritten = true;
                        }
                    }
                    
                    if (overwritten) {
                        ptrs[i].file_id = 0; // mark as skipped
                        continue;
                    }
                    
                    if (!active_vlog->append(batch[i]->key, batch[i]->value, ptrs[i])) {
                        is_panic_.store(true);
                        throw std::runtime_error("ENOSPC / I/O Panic during VLog append");
                    }
                    // Bypassing WAL! Only charging VLog bytes
                    storage_bytes += 8 + batch[i]->key.size() + batch[i]->value.size();
                } else {
                    if (!active_vlog->append(batch[i]->key, batch[i]->value, ptrs[i])) {
                        is_panic_.store(true);
                        throw std::runtime_error("ENOSPC / I/O Panic during VLog append");
                    }
                    if (!wal_->append(batch[i]->key, ptrs[i])) {
                        is_panic_.store(true);
                        throw std::runtime_error("ENOSPC / I/O Panic during WAL append");
                    }
                    user_bytes += batch[i]->key.size() + batch[i]->value.size();
                    storage_bytes += 12 + batch[i]->key.size() + batch[i]->value.size() + 8 + batch[i]->key.size() + batch[i]->value.size();
                }
            }
        }
        if (!active_vlog->sync()) {
            is_panic_.store(true);
            throw std::runtime_error("ENOSPC / I/O Panic during VLog sync");
        }
        if (!wal_->sync()) {
            is_panic_.store(true);
            throw std::runtime_error("ENOSPC / I/O Panic during WAL sync");
        }

        metrics_.user_bytes_written += user_bytes;
        metrics_.storage_bytes_written += storage_bytes;

        FaultInjection::check("crash_after_wal_append");

        std::shared_ptr<Memtable> act = active();
        for (size_t i = 0; i < batch.size(); ++i) {
            if (ignored[i] || (batch[i]->is_gc && ptrs[i].file_id == 0)) continue; // skipped
            act->put(batch[i]->key, ptrs[i]);
        }
    } catch (...) {
        batch_error = std::current_exception();
    }

    lock.lock();
    for (size_t i = 0; i < batch.size(); ++i) {
        write_queue_.pop_front();
        batch[i]->error = batch_error;
        batch[i]->done = true;
        batch[i]->cv.notify_one();
    }
    
    // WAKE UP THE NEW LEADER!
    if (!write_queue_.empty()) {
        write_queue_.front()->cv.notify_one();
    }
}

// GöÇGöÇ Read path GöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇ

bool KVStore::get(std::string_view key, std::string& out_value) const {
    metrics_.get_calls++;
    VLogPointer ptr;

    // 1. Active memtable
    std::shared_ptr<Memtable> act = active();
    if (act && act->get(key, ptr)) {
        if (is_tombstone(ptr)) return false;
        metrics_.vlog_reads++;
        std::shared_ptr<VLog> target;
        {
            std::lock_guard<std::mutex> lk(vlogs_mutex_);
            auto it = vlogs_.find(ptr.file_id);
            if (it != vlogs_.end()) target = it->second;
        }
        return target ? target->read_at(ptr, out_value) : false;
    }

    // 2. Immutable memtables (newest to oldest)
    uint64_t max_idx = current_imm_idx_.load(std::memory_order_acquire);
    for (int i = 0; i < 4 && max_idx > static_cast<uint64_t>(i); ++i) {
        std::shared_ptr<Memtable> imm = immutable((max_idx - 1 - i) % 4);
        if (imm && imm->get(key, ptr)) {
            if (is_tombstone(ptr)) return false;
            metrics_.vlog_reads++;
            std::shared_ptr<VLog> target;
            {
                std::lock_guard<std::mutex> lk(vlogs_mutex_);
                auto it = vlogs_.find(ptr.file_id);
                if (it != vlogs_.end()) target = it->second;
            }
            return target ? target->read_at(ptr, out_value) : false;
        }
    }

    auto current_v = versions_->current();

    bool found_in_sst = false;

    // 3. L0 SSTables
    for (const auto& meta : current_v->files_[0]) {
        if (key < meta.min_key || key > meta.max_key) continue;
        metrics_.sst_considered++;
        auto sst = get_sstable_reader(meta.sequence);
        if (!sst) continue;

        if (!disable_bloom_ && !sst->bloom().may_contain(key)) {
            metrics_.bloom_skips++;
            continue;
        }
        
        metrics_.sst_searches++;
        if (sst->get(key, ptr, &block_cache_, &metrics_)) {
            found_in_sst = true;
            break;
        }
    }

    // 4. L1 SSTables
    if (!found_in_sst) {
        for (const auto& meta : current_v->files_[1]) {
            if (key < meta.min_key || key > meta.max_key) continue;
            auto sst = get_sstable_reader(meta.sequence);
            if (!sst) continue;

            if (sst->overlaps(key, key)) {
                metrics_.sst_considered++;
                if (!disable_bloom_ && !sst->bloom().may_contain(key)) {
                    metrics_.bloom_skips++;
                    continue;
                }
                
                metrics_.sst_searches++;
                if (sst->get(key, ptr, &block_cache_, &metrics_)) {
                    found_in_sst = true;
                    break;
                }
            }
        }
    }

    if (found_in_sst) {
        if (is_tombstone(ptr)) return false;
        metrics_.vlog_reads++;
        std::shared_ptr<VLog> target;
        {
            std::lock_guard<std::mutex> lk(vlogs_mutex_);
            auto it = vlogs_.find(ptr.file_id);
            if (it != vlogs_.end()) target = it->second;
        }
        return target ? target->read_at(ptr, out_value) : false;
    }

    return false;
}

bool KVStore::get_pointer(std::string_view key, VLogPointer& out_ptr) const {
    // 1. Active memtable
    std::shared_ptr<Memtable> act = active();
    if (act && act->get(key, out_ptr)) {
        return !is_tombstone(out_ptr);
    }

    // 2. Immutable memtables
    uint64_t max_idx = current_imm_idx_.load(std::memory_order_acquire);
    for (int i = 0; i < 4 && max_idx > static_cast<uint64_t>(i); ++i) {
        std::shared_ptr<Memtable> imm = immutable((max_idx - 1 - i) % 4);
        if (imm && imm->get(key, out_ptr)) {
            return !is_tombstone(out_ptr);
        }
    }

    auto current_v = versions_->current();
    bool found_in_sst = false;

    // 3. L0 SSTables
    for (const auto& meta : current_v->files_[0]) {
        if (key < meta.min_key || key > meta.max_key) continue;
        auto sst = get_sstable_reader(meta.sequence);
        if (!sst) continue;

        if (!disable_bloom_ && !sst->bloom().may_contain(key)) continue;
        if (sst->get(key, out_ptr, &block_cache_, &metrics_)) {
            found_in_sst = true;
            break;
        }
    }

    // 4. L1 SSTables
    if (!found_in_sst) {
        for (const auto& meta : current_v->files_[1]) {
            if (key < meta.min_key || key > meta.max_key) continue;
            auto sst = get_sstable_reader(meta.sequence);
            if (!sst) continue;

            if (sst->overlaps(key, key)) {
                if (!disable_bloom_ && !sst->bloom().may_contain(key)) continue;
                if (sst->get(key, out_ptr, &block_cache_, &metrics_)) {
                    found_in_sst = true;
                    break;
                }
            }
        }
    }

    if (found_in_sst) {
        return !is_tombstone(out_ptr);
    }
    return false;
}

// GöÇGöÇ Flush GöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇ

void KVStore::maybe_flush(std::unique_lock<std::mutex>& lock) {
    while (versions_->current()->files_[0].size() >= L0_HARD_LIMIT) {
        bg_compaction_cv_.wait(lock);
    }
    
    std::shared_ptr<Memtable> act = active();
    if (!act || act->byte_size() < FLUSH_THRESHOLD) return;

    uint64_t idx = current_imm_idx_.load(std::memory_order_relaxed);
    int target_slot = idx % 4;
    while (immutable(target_slot) != nullptr) {
        bg_flush_cv_.wait(lock);
    }

    {
        std::unique_lock<std::mutex> mem_lk(memtable_mutex_);
        immutables_[target_slot] = act;
    }
    current_imm_idx_.store(idx + 1, std::memory_order_release);
    {
        std::unique_lock<std::mutex> mem_lk(memtable_mutex_);
        active_ = std::make_shared<Memtable>();
    }

    rotate_wal(); 
    
    // Rotate VLog if it gets too large
    {
        std::unique_lock<std::mutex> v_lock(vlogs_mutex_);
        if (vlogs_[current_vlog_id_]->current_offset() > FLUSH_THRESHOLD * 2) {
            uint32_t next_id = current_vlog_id_ + 1;
            vlogs_[next_id] = std::make_shared<VLog>(vlog_path(next_id), next_id);
            current_vlog_id_ = next_id;
            
            thread_pool_.enqueue([this]() {
                run_vlog_gc(this);
            });
        }
    }

    thread_pool_.enqueue([this, act, target_slot]() {
        this->flush_immutable(act);
        {
            std::lock_guard<std::mutex> lk(write_mutex_);
            {
            std::unique_lock<std::mutex> mem_lk(memtable_mutex_);
            immutables_[target_slot] = nullptr;
        }
        }
        bg_flush_cv_.notify_all();
    });
}

void KVStore::flush_immutable(std::shared_ptr<Memtable> imm) {
    uint32_t seq = versions_->new_file_number();
    std::string path = sst_path(seq);

    size_t sst_est = 24;
    std::vector<SSTableEntry> entries_to_flush;
    for (auto it = imm->begin(); it.valid(); it.next()) {
        sst_est += 20 + it.key().size();
        entries_to_flush.push_back({std::string(it.key()), it.value()});
    }

    if (entries_to_flush.empty()) {
        return;
    }

    metrics_.storage_bytes_written += sst_est;

    if (!SSTableWriter::write(path, entries_to_flush)) {
        std::cerr << "[KVStore] ERROR: SSTable flush failed for " << path << "\n";
        throw std::runtime_error("[KVStore] SSTable flush failed");
    }

    SSTableReader reader;
    if (!reader.load(path)) {
        std::cerr << "[KVStore] ERROR: Failed to load flushed SSTable " << path << "\n";
        throw std::runtime_error("[KVStore] Failed to load flushed SSTable");
    }

    {
        acdb::VersionEdit edit;
        edit.add_file(0, seq, sst_est, reader.min_key(), reader.max_key());
        
        FaultInjection::check("crash_during_flush");

        if (!versions_->log_and_apply(&edit)) {
            std::cerr << "[KVStore] ERROR: VersionSet log_and_apply failed during flush\n";
        }
    }
    
    std::cout << "[KVStore] Flushed SSTable sst_"
              << std::string(6 - std::to_string(seq).size(), '0') + std::to_string(seq)
              << "\n";

    std::vector<uint32_t> to_delete;
    {
        std::lock_guard<std::mutex> lk(write_mutex_);
        to_delete = std::move(pending_gc_vlogs_);
        pending_gc_vlogs_.clear();
        
        if (versions_->current()->files_[0].size() >= 4 && !bg_compaction_running_.load()) {
            bg_compaction_running_.store(true);
            thread_pool_.enqueue([this]() {
                this->compact_l0_to_l1();
                {
                    std::lock_guard<std::mutex> lk(write_mutex_);
                    bg_compaction_running_.store(false);
                }
                bg_compaction_cv_.notify_all();
            });
        }
    }

    for (uint32_t id : to_delete) {
        std::shared_ptr<VLog> old_vlog;
        {
            std::unique_lock<std::mutex> v_lock(vlogs_mutex_);
            auto it = vlogs_.find(id);
            if (it != vlogs_.end()) {
                old_vlog = it->second;
                vlogs_.erase(it);
            }
        }
        while (old_vlog && old_vlog.use_count() > 1) {
            std::this_thread::yield();
        }
        old_vlog.reset();
        std::error_code ec;
        std::filesystem::remove(vlog_path(id), ec);
    }
}

void KVStore::rotate_wal() {
    uint32_t old_id = current_wal_id_;
    uint32_t new_id = old_id + 1;
    std::string old_wp = wal_path(old_id);
    std::string new_wp = wal_path(new_id);

    auto new_wal = std::make_unique<WAL>(new_wp);
    new_wal->sync();

    wal_ = std::move(new_wal);
    current_wal_id_ = new_id;

    std::error_code ec;
    std::filesystem::remove(old_wp, ec);
    if (ec) {
        std::cerr << "[KVStore] WARNING: could not delete old WAL " << old_wp << ": " << ec.message() << "\n";
    }
}

// GöÇGöÇ Recovery GöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇ

void KVStore::recover() {
    versions_->recover();
    
    auto current_v = versions_->current();
    std::set<uint32_t> active_ssts;
    size_t active_count = 0;
    for (int level = 0; level < 2; ++level) {
        for (const auto& meta : current_v->files_[level]) {
            active_ssts.insert(meta.sequence);
            get_sstable_reader(meta.sequence); // pre-load
            active_count++;
        }
    }
    
    // Purge orphans
    if (std::filesystem::exists(data_dir_)) {
        for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
            auto name = entry.path().filename().string();
            if (name.size() > 4 && name.substr(0, 4) == "sst_" && name.substr(name.size() - 4) == ".sst") {
                uint32_t seq = static_cast<uint32_t>(std::strtoul(name.c_str()+4, nullptr, 10));
                if (active_ssts.find(seq) == active_ssts.end()) {
                    std::error_code ec;
                    std::filesystem::remove(entry.path(), ec);
                    std::cout << "[KVStore] Deleted orphan SSTable " << name << "\n";
                }
            }
        }
    }

    std::vector<std::string> wal_files;
    uint32_t max_wal_id = 0;
    scan_wal_files(wal_files, max_wal_id);

    // To correctly migrate Phase 2 vlog.bin if it exists
    if (std::filesystem::exists(data_dir_ + "/vlog.bin")) {
        std::filesystem::rename(data_dir_ + "/vlog.bin", vlog_path(1));
    }

    uint32_t max_vlog_id = 0;
    if (std::filesystem::exists(data_dir_)) {
        for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
            auto name = entry.path().filename().string();
            if (name.size() > 5 && name.substr(0, 5) == "vlog_" && name.substr(name.size() - 4) == ".bin") {
                uint32_t id = static_cast<uint32_t>(std::strtoul(name.c_str()+5, nullptr, 10));
                if (id > max_vlog_id) max_vlog_id = id;
                vlogs_[id] = std::make_shared<VLog>(entry.path().string(), id);
            }
        }
    }
    if (max_vlog_id == 0) {
        max_vlog_id = 1;
        vlogs_[1] = std::make_shared<VLog>(vlog_path(1), 1);
    }
    current_vlog_id_ = max_vlog_id;

    {
        std::unique_lock<std::mutex> mem_lk(memtable_mutex_);
        active_ = std::make_shared<Memtable>();
    }
    size_t total_entries = 0;
    bool   any_tainted = false;

    for (const auto& wf : wal_files) {
        WAL temp_wal(wf);
        auto result = temp_wal.replay();
        any_tainted = any_tainted || result.tainted;

        for (const auto& e : result.entries) {
            if (e.is_tombstone) {
                VLogPointer ptr;
                ptr.length = 0;
                ptr.offset = std::numeric_limits<uint64_t>::max();
                ptr.file_id = current_wal_id_;
                active()->put(e.key, ptr);
                continue;
            }

            active()->put(e.key, e.pointer);
        }
        total_entries += result.entries.size();
    }
    vlogs_[current_vlog_id_]->sync();

    current_wal_id_ = (max_wal_id > 0) ? max_wal_id : 1;

    wal_ = std::make_unique<WAL>(wal_path(current_wal_id_));

    std::cout << "[KVStore] Recovered " << total_entries << " entries from "
              << wal_files.size() << " WAL(s)";
    if (active_count > 0)
        std::cout << ", loaded " << active_count << " SSTables";
    if (any_tainted)
        std::cout << " (WAL TAINTED)";
    std::cout << "\n";
}

std::shared_ptr<SSTableReader> KVStore::get_sstable_reader(uint32_t seq) const {
    {
        std::lock_guard<std::mutex> r_lock(table_cache_mutex_);
        auto it = table_cache_.find(seq);
        if (it != table_cache_.end()) return it->second;
    }
    
    std::string path = sst_path(seq);
    auto reader = std::make_shared<SSTableReader>();
    if (!reader->load(path)) {
        return nullptr;
    }
    
    std::unique_lock<std::mutex> w_lock(table_cache_mutex_);
    auto it = table_cache_.find(seq);
    if (it != table_cache_.end()) return it->second;
    
    table_cache_[seq] = reader;
    return reader;
}

void KVStore::compact_l0_to_l1() {
    run_compaction(this);
}

// GöÇGöÇ Diagnostics GöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇGöÇ

bool KVStore::wal_tainted() const {
    return wal_ && wal_->is_tainted();
}

size_t KVStore::memtable_entries() const {
    size_t count = 0;
    std::shared_ptr<Memtable> act = active();
    if (act) {
        for (auto it = act->begin(); it.valid(); it.next()) {
            count++;
        }
    }
    for (int i = 0; i < 4; ++i) {
        std::shared_ptr<Memtable> imm = immutable(i);
        if (imm) {
            for (auto it = imm->begin(); it.valid(); it.next()) {
                count++;
            }
        }
    }
    return count;
}
