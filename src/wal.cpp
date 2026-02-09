#include "wal.h"
#include "crc32.h"

#include <cstring>
#include <iostream>
#include <vector>

// ── Platform abstraction for raw file I/O ──────────────────────
#ifdef _WIN32
  #include <io.h>
  #include <fcntl.h>
  #include <sys/stat.h>
  #define wal_open(path, flags, mode)  _open(path, flags, mode)
  #define wal_write(fd, buf, len)      _write(fd, buf, static_cast<unsigned int>(len))
  #define wal_read(fd, buf, len)       _read(fd, buf, static_cast<unsigned int>(len))
  #define wal_close(fd)                _close(fd)
  #define wal_fsync(fd)                _commit(fd)
  static constexpr int WAL_APPEND_FLAGS = _O_WRONLY | _O_APPEND | _O_CREAT | _O_BINARY;
  static constexpr int WAL_READ_FLAGS   = _O_RDONLY | _O_BINARY;
  static constexpr int WAL_MODE         = _S_IREAD | _S_IWRITE;
#else
  #include <unistd.h>
  #include <fcntl.h>
  #define wal_open(path, flags, mode)  open(path, flags, mode)
  #define wal_write(fd, buf, len)      write(fd, buf, len)
  #define wal_read(fd, buf, len)       read(fd, buf, len)
  #define wal_close(fd)                close(fd)
  #define wal_fsync(fd)                fdatasync(fd)
  static constexpr int WAL_APPEND_FLAGS = O_WRONLY | O_APPEND | O_CREAT;
  static constexpr int WAL_READ_FLAGS   = O_RDONLY;
  static constexpr int WAL_MODE         = 0644;
#endif

// ── Helper: write all bytes, looping on short writes ───────────
static bool write_all(int fd, const void* buf, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    size_t remaining = len;
    while (remaining > 0) {
        auto written = wal_write(fd, p, remaining);
        if (written <= 0) return false;   // I/O error
        p         += written;
        remaining -= static_cast<size_t>(written);
    }
    return true;
}

// ── Helper: read exactly `len` bytes; returns false on short/EOF ─
static bool read_exact(int fd, void* buf, size_t len) {
    uint8_t* p = static_cast<uint8_t*>(buf);
    size_t remaining = len;
    while (remaining > 0) {
        auto n = wal_read(fd, p, remaining);
        if (n <= 0) return false;   // EOF or error
        p         += n;
        remaining -= static_cast<size_t>(n);
    }
    return true;
}

// ── WAL constructor ────────────────────────────────────────────
WAL::WAL(const std::string& path) : path_(path), fd_(-1) {
    fd_ = wal_open(path_.c_str(), WAL_APPEND_FLAGS, WAL_MODE);
    if (fd_ < 0) {
        std::cerr << "[WAL] FATAL: cannot open " << path_ << "\n";
        std::abort();
    }
}

// ── WAL destructor ─────────────────────────────────────────────
WAL::~WAL() {
    if (fd_ >= 0) wal_close(fd_);
}

// ── append ─────────────────────────────────────────────────────
void WAL::append(const std::string& key, const std::string& value) {
    uint32_t key_size   = static_cast<uint32_t>(key.size());
    uint32_t value_size = static_cast<uint32_t>(value.size());
    uint32_t checksum   = record_checksum(key_size, value_size, key, value);

    // Serialize the full record into a single buffer to minimize syscalls.
    const size_t record_len = sizeof(uint32_t) * 3 + key_size + value_size;
    std::vector<uint8_t> record(record_len);
    size_t off = 0;
    std::memcpy(record.data() + off, &key_size,   sizeof(uint32_t)); off += sizeof(uint32_t);
    std::memcpy(record.data() + off, &value_size, sizeof(uint32_t)); off += sizeof(uint32_t);
    std::memcpy(record.data() + off, &checksum,   sizeof(uint32_t)); off += sizeof(uint32_t);
    std::memcpy(record.data() + off, key.data(),   key_size);        off += key_size;
    std::memcpy(record.data() + off, value.data(), value_size);

    // Loop until the entire record is written.
    if (!write_all(fd_, record.data(), record_len)) {
        std::cerr << "[WAL] ERROR: failed to write record\n";
    }
}

// ── sync ───────────────────────────────────────────────────────
void WAL::sync() {
    wal_fsync(fd_);
}

// ── replay ─────────────────────────────────────────────────────
std::vector<WALEntry> WAL::replay() const {
    std::vector<WALEntry> entries;

    int rfd = wal_open(path_.c_str(), WAL_READ_FLAGS, 0);
    if (rfd < 0) return entries;   // file does not exist yet

    while (true) {
        uint32_t key_size = 0, value_size = 0, stored_checksum = 0;

        // Read 12-byte header.
        if (!read_exact(rfd, &key_size,        sizeof(uint32_t))) break;
        if (!read_exact(rfd, &value_size,      sizeof(uint32_t))) break;
        if (!read_exact(rfd, &stored_checksum, sizeof(uint32_t))) break;

        // Size sanity check — corruption guard.
        if (key_size > MAX_FIELD_SIZE || value_size > MAX_FIELD_SIZE) break;

        // Read key.
        std::string key(key_size, '\0');
        if (key_size > 0 && !read_exact(rfd, key.data(), key_size)) break;

        // Read value.
        std::string value(value_size, '\0');
        if (value_size > 0 && !read_exact(rfd, value.data(), value_size)) break;

        // Verify checksum.
        uint32_t expected = record_checksum(key_size, value_size, key, value);
        if (stored_checksum != expected) break;

        entries.push_back({std::move(key), std::move(value)});
    }

    wal_close(rfd);
    return entries;
}

// partial state 9302
