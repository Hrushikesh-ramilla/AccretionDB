#include "vlog_gc.h"
#include "fault_injection.h"
#include "kvstore.h"
#include "sstable_iterator.h"

#include <filesystem>
#include <iostream>
#include <vector>
#include <set>

// Definition for run_vlog_gc
void run_vlog_gc(KVStore* store) {
    if (!store) return;

    uint32_t gc_target_id = 0;
    std::shared_ptr<VLog> old_vlog;
    {
        std::lock_guard<std::mutex> lk(store->vlogs_mutex_);
        if (store->vlogs_.size() > 1) {
            auto it = store->vlogs_.begin();
            if (it->first < store->current_vlog_id_) {
                gc_target_id = it->first;
                old_vlog = it->second;
            }
        }
    }

    if (gc_target_id == 0 || !old_vlog) return; // Nothing to GC

    size_t rewritten = 0;
    uint64_t offset = 0;
    VLogRecord record;

    // Stream the old VLog sequentially with O(1) memory
    while (old_vlog->read_next(offset, record)) {
        VLogPointer current_ptr;
        bool is_live = store->get_pointer(record.key, current_ptr);

        // A value is live if the key exists AND the pointer points to the exact same location
        if (is_live && current_ptr.file_id == gc_target_id && current_ptr.offset == record.pointer.offset) {
            // Enqueue GC write request
            KVStore::WriteRequest gc_req;
            gc_req.key = record.key;
            gc_req.value = record.value;
            gc_req.is_gc = true;
            gc_req.gc_old_vlog_id = gc_target_id;
            
            store->execute_write_request(&gc_req);
            
            if (!gc_req.error) {
                store->subtract_user_bytes(record.key.size() + record.value.size());
                rewritten++;
            } else {
                std::cerr << "[VLog GC] WARNING: failed to rewrite live pointer for key " << record.key << "\n";
            }
        }
    }

    FaultInjection::check("crash_during_vlog_rewrite");

    // Mark the VLog for deletion after the next memtable flush
    {
        std::lock_guard<std::mutex> lk(store->flush_wait_mutex_);
        store->pending_gc_vlogs_.push_back(gc_target_id);
    }

    std::cout << "[VLog GC] Rewrote " << rewritten << " live values and queued old VLog " << gc_target_id << " for deletion.\n";
}

