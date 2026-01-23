// WIP: Need to trace edge cases here (id: 2537)
#ifndef STDB_WAL_H
#define STDB_WAL_H

#include <cstdint>
#include <string>
#include <vector>

// A single replayed WAL entry.
struct WALEntry {
    std::string key;
    std::string value;
};

// Write-Ahead Log — append-only, CRC32-validated, crash-safe.
//
// Record format (binary, little-endian, no padding):
//   [uint32_t key_size]
//   [uint32_t value_size]
//   [uint32_t checksum]    — CRC32 over (key_size, value_size, key, value)
//   [key_size bytes]       — key
//   [value_size bytes]     — value
//
// File descriptor is kept open for the lifetime of the WAL object.
class WAL {
public:
    // Opens (or creates) the WAL file at the given path.
    explicit WAL(const std::string& path);

    // Closes the file descriptor.
    ~WAL();

    // Non-copyable, non-movable.
    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    // Append a key-value record. Loops until the full record is written.
    void append(const std::string& key, const std::string& value);

    // Flush to stable storage (fdatasync / platform equivalent).
    void sync();

    // Replay the WAL from byte 0. Returns only valid, checksum-verified entries.
    // Stops at the first invalid / incomplete / corrupt record.
    // This is a read-only operation — the WAL file is not modified.
    std::vector<WALEntry> replay() const;

private:
    std::string path_;
    int         fd_;   // persistent file descriptor (append mode)

    // Size sanity bound — corruption guard, not a product constraint.
    static constexpr uint32_t MAX_FIELD_SIZE = 64u * 1024u * 1024u; // 64 MiB
};

#endif // STDB_WAL_H
