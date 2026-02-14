// WIP: Need to trace edge cases here (id: 4272)
#include "sstable.h"
#include "crc32.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <vector>

// ── SSTableWriter ──────────────────────────────────────────────

bool SSTableWriter::write(const std::string& path,
                          const std::map<std::string, VLogPointer>& entries) {
    // Serialize the data section into a buffer.
    std::vector<uint8_t> data;

    for (const auto& [key, ptr] : entries) {
        uint32_t ks = static_cast<uint32_t>(key.size());
        size_t old = data.size();
        data.resize(old + sizeof(uint32_t) + ks + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t));
        uint8_t* p = data.data() + old;

        std::memcpy(p, &ks, sizeof(uint32_t));           p += sizeof(uint32_t);
        std::memcpy(p, key.data(), ks);                   p += ks;
        std::memcpy(p, &ptr.file_id, sizeof(uint32_t));   p += sizeof(uint32_t);
        std::memcpy(p, &ptr.offset,  sizeof(uint64_t));   p += sizeof(uint64_t);
        std::memcpy(p, &ptr.length,  sizeof(uint32_t));
    }

    // Footer: entry_count + CRC32 over data section.
    uint32_t entry_count = static_cast<uint32_t>(entries.size());
    uint32_t checksum    = compute_crc32(data.data(), data.size());

    // Write data + footer to file.
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) return false;

    out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    out.write(reinterpret_cast<const char*>(&entry_count), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(&checksum),    sizeof(uint32_t));
    out.flush();

    if (!out.good()) return false;
    out.close();
    return true;
}

// ── SSTableReader ──────────────────────────────────────────────

// Extract sequence number from filename like "sst_000001.sst".
static uint32_t parse_sequence(const std::string& path) {
    auto pos = path.rfind("sst_");
    if (pos == std::string::npos) return 0;
    return static_cast<uint32_t>(std::strtoul(path.c_str() + pos + 4, nullptr, 10));
}

bool SSTableReader::load(const std::string& path) {
    path_ = path;
    sequence_ = parse_sequence(path);
    entries_.clear();

    // Read entire file.
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in.is_open()) return false;

    auto file_size = in.tellg();
    if (file_size < 8) return false;   // too small for footer

    std::vector<uint8_t> buf(static_cast<size_t>(file_size));
    in.seekg(0);
    in.read(reinterpret_cast<char*>(buf.data()), file_size);
    if (!in.good()) return false;

    // Read footer (last 8 bytes).
    size_t footer_offset = buf.size() - 8;
    uint32_t entry_count = 0, stored_checksum = 0;
    std::memcpy(&entry_count,     buf.data() + footer_offset,     sizeof(uint32_t));
    std::memcpy(&stored_checksum, buf.data() + footer_offset + 4, sizeof(uint32_t));

    if (entry_count == 0) return false;

    // Validate CRC32 over data section.
    uint32_t computed = compute_crc32(buf.data(), footer_offset);
    if (computed != stored_checksum) {
        std::cerr << "[SSTable] WARNING: checksum mismatch in " << path << "\n";
        return false;
    }

    // Parse entries from data section.
    size_t off = 0;
    for (uint32_t i = 0; i < entry_count; ++i) {
        if (off + sizeof(uint32_t) > footer_offset) return false;

        uint32_t ks = 0;
        std::memcpy(&ks, buf.data() + off, sizeof(uint32_t)); off += sizeof(uint32_t);

        if (off + ks + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) > footer_offset)
            return false;

        SSTableEntry e;
        e.key.assign(reinterpret_cast<const char*>(buf.data() + off), ks); off += ks;
        std::memcpy(&e.pointer.file_id, buf.data() + off, sizeof(uint32_t)); off += sizeof(uint32_t);
        std::memcpy(&e.pointer.offset,  buf.data() + off, sizeof(uint64_t)); off += sizeof(uint64_t);
        std::memcpy(&e.pointer.length,  buf.data() + off, sizeof(uint32_t)); off += sizeof(uint32_t);

        entries_.push_back(std::move(e));
    }

    return true;
}

bool SSTableReader::get(const std::string& key, VLogPointer& out_pointer) const {
    // Binary search on sorted entries.
    auto it = std::lower_bound(entries_.begin(), entries_.end(), key,
        [](const SSTableEntry& e, const std::string& k) { return e.key < k; });

    if (it != entries_.end() && it->key == key) {
        out_pointer = it->pointer;
        return true;
    }
    return false;
}
