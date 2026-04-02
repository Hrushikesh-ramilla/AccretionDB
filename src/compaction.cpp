#include "compaction.h"
#include "fault_injection.h"
#include "kvstore.h"
#include "sstable.h"
#include "sstable_iterator.h"
#include "version_edit.h"
#include "version_set.h"

#include <filesystem>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>
#include <queue>
#include <memory>
#include <chrono>

void run_compaction(KVStore* store) {
    auto start_time = std::chrono::high_resolution_clock::now();
    auto current_v = store->versions_->current();
    if (current_v->files_[0].empty()) return;

    // 1. Snapshot inputs.
    std::vector<acdb::FileMetaData> l0_inputs = current_v->files_[0];
    std::string global_min = "\xFF", global_max = "";

    auto get_sstable_reader = [&](uint32_t seq) -> std::shared_ptr<SSTableReader> {
        return store->get_sstable_reader(seq);
    };

    for (const auto& meta : l0_inputs) {
        if (meta.min_key < global_min) global_min = meta.min_key;
        if (meta.max_key > global_max) global_max = meta.max_key;
    }

    // 2. Find overlapping L1 files.
    std::vector<acdb::FileMetaData> l1_inputs;

    for (const auto& meta : current_v->files_[1]) {
        auto r = get_sstable_reader(meta.sequence);
        if (!r) continue;
        if (r->overlaps(global_min, global_max)) {
            l1_inputs.push_back(meta);
        }
    }

    // 3. (Removed) We no longer collect keys from input L1 files.
    // L1 is the terminal level in AccretionDB's 2-level LSM, so we
    // unconditionally emit tombstones during L0->L1 compactions, avoiding O(N) memory.

    // 4. Constant-Memory Streaming K-Way Merge
    struct IteratorWrapper {
        SSTableReader* reader;
        SSTableIterator* iter;
        int level;
        uint32_t seq;
    };

    struct CompareIter {
        bool operator()(const IteratorWrapper& a, const IteratorWrapper& b) const {
            std::string_view k1 = a.iter->key();
            std::string_view k2 = b.iter->key();
            if (k1 != k2) return k1 > k2;
            if (a.level != b.level) return a.level > b.level;
            return a.seq < b.seq;
        }
    };

    std::vector<std::unique_ptr<SSTableIterator>> all_iters;
    std::priority_queue<IteratorWrapper, std::vector<IteratorWrapper>, CompareIter> pq;
    std::vector<std::shared_ptr<SSTableReader>> active_readers;

    for (const auto& meta : l1_inputs) {
        auto r = get_sstable_reader(meta.sequence);
        if (!r) continue;
        auto iter = std::make_unique<SSTableIterator>(r.get(), &store->block_cache_);
        if (iter->valid()) {
            pq.push({r.get(), iter.get(), 1, meta.sequence});
            all_iters.push_back(std::move(iter));
            active_readers.push_back(std::move(r));
        }
    }

    for (const auto& meta : l0_inputs) {
        auto r = get_sstable_reader(meta.sequence);
        if (!r) continue;
        auto iter = std::make_unique<SSTableIterator>(r.get(), &store->block_cache_);
        if (iter->valid()) {
            pq.push({r.get(), iter.get(), 0, meta.sequence});
            all_iters.push_back(std::move(iter));
            active_readers.push_back(std::move(r));
        }
    }

    // 5. Write new L1 SSTables streamingly
    acdb::VersionEdit edit;
    
    for (const auto& meta : l0_inputs) edit.delete_file(0, meta.sequence);
    for (const auto& meta : l1_inputs) edit.delete_file(1, meta.sequence);

    std::vector<SSTableEntry> chunk;
    size_t chunk_size = 0;

    auto flush_chunk = [&]() {
        if (chunk.empty()) return;
        uint32_t seq = store->versions_->new_file_number();
        std::string path = store->sst_path(seq);
        if (!SSTableWriter::write(path, chunk)) {
            store->versions_->remove_pending_output(seq);
            throw std::runtime_error("[Compaction] Failed to write new L1 SSTable");
        }
        store->add_storage_bytes(24);
        
        SSTableReader temp_reader;
        temp_reader.load(path);
        
        size_t est_size = chunk_size + 24; 
        edit.add_file(1, seq, est_size, temp_reader.min_key(), temp_reader.max_key());
        
        chunk.clear();
        chunk_size = 0;
    };

    std::string last_key = "";
    bool first_key = true;

    while (!pq.empty()) {
        auto top = pq.top();
        pq.pop();

        std::string current_key(top.iter->key());
        VLogPointer current_val = top.iter->value();

        top.iter->next();
        if (top.iter->valid()) {
            pq.push(top);
        }

        if (!first_key && current_key == last_key) continue;
        last_key = current_key;
        first_key = false;

        // Since L1 is the terminal level and this compaction includes ALL overlapping L1 files,
        // any tombstone from L0 has now overshadowed and deleted any potential older value in L1.
        // We can safely discard the tombstone entirely to prevent infinite accumulation.
        if (is_tombstone(current_val)) continue;

        chunk.push_back({current_key, current_val});
        chunk_size += current_key.size() + 20;
        store->add_storage_bytes(current_key.size() + 20);
        if (chunk_size >= KVStore::FLUSH_THRESHOLD) flush_chunk();
    }
    flush_chunk();

    // Clear structures holding active readers to release file handles before deletion
    while (!pq.empty()) pq.pop();
    all_iters.clear();
    active_readers.clear();

    // 7. Concurrency safe state swap
    FaultInjection::check("crash_during_compaction");
    if (!store->versions_->log_and_apply(&edit)) {
        throw std::runtime_error("[Compaction] VersionSet commit failed");
    }
    
    // 8. Safely delete old compacted files from disk using strict MVCC.
    store->versions_->purge_obsolete_files(store->data_dir());

    auto end_time = std::chrono::high_resolution_clock::now();
    uint64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    store->metrics().compaction_duration_ms.fetch_add(duration, std::memory_order_relaxed);
    store->metrics().compaction_count.fetch_add(1, std::memory_order_relaxed);
}
