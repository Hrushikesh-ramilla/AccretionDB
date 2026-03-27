#ifndef ACDB_SSTABLE_ITERATOR_H
#define ACDB_SSTABLE_ITERATOR_H

#include "sstable.h"
#include "cache.h"
#include <string>
#include <string_view>
#include <vector>
#include <memory>
#include <cstring>
#include <fstream>

class SSTableIterator {
public:
    SSTableIterator(const SSTableReader* reader, acdb::ShardedLRUCache* cache)
        : reader_(reader), cache_(cache) {
        if (reader_) {
            in_.open(reader_->path(), std::ios::binary);
        }
        seek_to_first();
    }

    void seek_to_first() {
        block_idx_ = 0;
        offset_in_block_ = 0;
        valid_ = false;
        if (reader_ && !reader_->index().empty()) {
            load_block(0);
            parse_current_entry();
        }
    }

    bool valid() const { return valid_; }

    void next() {
        if (!valid_) return;
        
        uint32_t num_restarts;
        std::memcpy(&num_restarts, current_block_->data() + current_block_->size() - 4, 4);
        uint32_t restarts_offset = current_block_->size() - 4 - num_restarts * 4;

        if (offset_in_block_ >= restarts_offset) {
            block_idx_++;
            offset_in_block_ = 0;
            if (block_idx_ < reader_->index().size()) {
                load_block(block_idx_);
                parse_current_entry();
            } else {
                valid_ = false;
            }
        } else {
            parse_current_entry();
        }
    }

    std::string_view key() const { return current_key_; }
    VLogPointer value() const { return current_value_; }

private:
    const SSTableReader* reader_;
    acdb::ShardedLRUCache* cache_;

    size_t block_idx_ = 0;
    acdb::BlockPtr current_block_;
    size_t offset_in_block_ = 0;
    
    bool valid_ = false;
    std::string current_key_;
    VLogPointer current_value_;
    std::ifstream in_;

    void load_block(size_t idx) {
        const auto& idx_entry = reader_->index()[idx];
        std::string cache_key = reader_->path() + ":" + std::to_string(idx_entry.offset);
        
        current_block_ = cache_->get(cache_key);
        if (!current_block_) {
            // Cache miss: read from disk.
            current_block_ = std::make_shared<acdb::Block>(idx_entry.length);
            
            // Read using standard C I/O or platform specific
            // Since this runs on Windows or POSIX, we can just use std::ifstream for simplicity in Iterator
            // For production, pread is preferred, but std::ifstream with seekg is cross platform standard C++.
            // Wait, we can use pread equivalent here if needed.
            // But let's use standard ifstream for the iterator since it's sequential or not performance critical vs get().
            // Actually, Windows has _lseeki64 and _read, or we can use `vlog_pread` equivalent? 
            // Better to use a clean std::ifstream here since it's just block loading and we hold no locks.
            if (in_.is_open()) {
                in_.seekg(idx_entry.offset);
                in_.read(current_block_->data(), idx_entry.length);
                if (in_.fail()) {
                    throw std::runtime_error("SSTableIterator: I/O error reading block");
                }
            } else {
                throw std::runtime_error("SSTableIterator: Failed to open file: " + reader_->path());
            }
            cache_->put(cache_key, current_block_);
        }
    }

    void parse_current_entry() {
        if (!current_block_ || offset_in_block_ >= current_block_->size()) {
            valid_ = false;
            return;
        }

        uint32_t num_restarts;
        if (current_block_->size() < 4) { valid_ = false; return; }
        std::memcpy(&num_restarts, current_block_->data() + current_block_->size() - 4, 4);
        
        if (num_restarts == 0 || num_restarts > (current_block_->size() - 4) / 4) {
            throw std::runtime_error("SSTableIterator: Corrupted num_restarts");
        }

        uint32_t restarts_offset = current_block_->size() - 4 - num_restarts * 4;
        
        if (offset_in_block_ >= restarts_offset) {
            valid_ = false;
            return;
        }

        if (offset_in_block_ + 12 > restarts_offset) {
            throw std::runtime_error("SSTableIterator: Corrupted entry header bounds");
        }

        const char* p = current_block_->data() + offset_in_block_;
        
        uint32_t shared, unshared, val_len;
        std::memcpy(&shared, p, 4); p += 4;
        std::memcpy(&unshared, p, 4); p += 4;
        std::memcpy(&val_len, p, 4); p += 4;

        if (offset_in_block_ + 12 + unshared + val_len > restarts_offset) {
            throw std::runtime_error("SSTableIterator: Corrupted entry payload bounds");
        }

        current_key_.resize(shared);
        current_key_.append(p, unshared); p += unshared;
        
        if (val_len == 16) {
            std::memcpy(&current_value_.file_id, p, 4);
            std::memcpy(&current_value_.offset, p + 4, 8);
            std::memcpy(&current_value_.length, p + 12, 4);
        }
        p += val_len;

        offset_in_block_ = (p - current_block_->data());
        valid_ = true;
    }
};

#endif // ACDB_SSTABLE_ITERATOR_H
