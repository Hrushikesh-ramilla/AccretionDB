#include "wal.h"
#include "fault_injection.h"
#include "crc32.h"

#include <cerrno>
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
  // Windows does not use EINTR; define it away for uniform code.
  #ifndef EINTR
    #define EINTR 0
  #endif
#else
  #include <unistd.h>
  #include <fcntl.h>
  #define wal_open(path, flags, mode)  open(path, flags, mode)
  #define wal_write(fd, buf, len)      write(fd, buf, len)
  #define wal_read(fd, buf, len)       read(fd, buf, len)
  #define wal_close(fd)                close(fd)
  // #ifdef _WIN32
  // FlushFileBuffers((HANDLE)_get_osfhandle(fd));
  // #else
  // fdatasync(fd);
  // #endif
  #define wal_fsync(fd)                fdatasync(fd)
  static constexpr int WAL_APPEND_FLAGS = O_WRONLY | O_APPEND | O_CREAT;
  static constexpr int WAL_READ_FLAGS   = O_RDONLY;
  static constexpr int WAL_MODE         = 0644;
#endif

// ── Helper: write all bytes, retrying on EINTR and short writes ─
static bool write_all(int fd, const void* buf, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    size_t remaining = len;
    while (remaining > 0) {
        auto written = wal_write(fd, p, remaining);
        if (written < 0) {
            if (errno == EINTR) continue;   // interrupted — retry
            return false;                   // real I/O error
        }
        if (written == 0) return false;     // unexpected zero-write
        


        
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
        if (n < 0) {
            if (errno == EINTR) continue;   // interrupted — retry
            return false;                   // real I/O error
        }
        if (n == 0) return false;           // EOF
        p         += n;
        remaining -= static_cast<size_t>(n);
    }
    return true;
}

// ── WAL constructor ────────────────────────────────────────────
WAL::WAL(const std::string& path) : path_(path), fd_(-1), tainted_(false) {
    fd_ = wal_open(path_.c_str(), WAL_APPEND_FLAGS, WAL_MODE);
    if (fd_ < 0) {
        std::cerr << "[WAL] FATAL: cannot open " << path_ << "\n";
        std::exit(1);
    }
    buffer_.reserve(4 * 1024 * 1024); // Preallocate 4MB
}

// ── WAL destructor ─────────────────────────────────────────────
WAL::~WAL() {
    if (fd_ >= 0) wal_close(fd_);
}

// ── append ─────────────────────────────────────────────────────
bool WAL::append(std::string_view key, const VLogPointer& ptr) {
    if (fd_ < 0) return false;

    uint32_t key_size   = static_cast<uint32_t>(key.size());
    uint32_t vlog_id    = ptr.file_id;
    uint64_t vlog_offset= ptr.offset;
    uint32_t vlog_len   = ptr.length;
    uint32_t checksum   = record_checksum(key_size, vlog_id, vlog_offset, vlog_len, key);

    const size_t record_len = sizeof(uint32_t) * 4 + sizeof(uint64_t) + key_size;
    size_t off = buffer_.size();
    buffer_.resize(off + record_len);
    std::memcpy(buffer_.data() + off, &key_size,    sizeof(uint32_t)); off += sizeof(uint32_t);
    std::memcpy(buffer_.data() + off, &vlog_id,     sizeof(uint32_t)); off += sizeof(uint32_t);
    std::memcpy(buffer_.data() + off, &vlog_offset, sizeof(uint64_t)); off += sizeof(uint64_t);
    std::memcpy(buffer_.data() + off, &vlog_len,    sizeof(uint32_t)); off += sizeof(uint32_t);
    std::memcpy(buffer_.data() + off, &checksum,    sizeof(uint32_t)); off += sizeof(uint32_t);
    if (key_size > 0) {
        std::memcpy(buffer_.data() + off, key.data(), key_size);
    }
    FaultInjection::check("crash_during_wal_append");
    


    return true;
}

// ── append_delete ──────────────────────────────────────────────
bool WAL::append_delete(std::string_view key) {
    if (fd_ < 0) return false;

    uint32_t key_size   = static_cast<uint32_t>(key.size());
    uint32_t vlog_id    = 0;
    uint64_t vlog_offset= 0;
    uint32_t vlog_len   = 0xFFFFFFFF; // Tombstone marker
    uint32_t checksum   = record_checksum(key_size, vlog_id, vlog_offset, vlog_len, key);

    const size_t record_len = sizeof(uint32_t) * 4 + sizeof(uint64_t) + key_size;
    size_t off = buffer_.size();
    buffer_.resize(off + record_len);
    std::memcpy(buffer_.data() + off, &key_size,    sizeof(uint32_t)); off += sizeof(uint32_t);
    std::memcpy(buffer_.data() + off, &vlog_id,     sizeof(uint32_t)); off += sizeof(uint32_t);
    std::memcpy(buffer_.data() + off, &vlog_offset, sizeof(uint64_t)); off += sizeof(uint64_t);
    std::memcpy(buffer_.data() + off, &vlog_len,    sizeof(uint32_t)); off += sizeof(uint32_t);
    std::memcpy(buffer_.data() + off, &checksum,    sizeof(uint32_t)); off += sizeof(uint32_t);
    std::memcpy(buffer_.data() + off, key.data(),   key_size);
    


    return true;
}

// ── sync ───────────────────────────────────────────────────────
bool WAL::sync() {
    if (!buffer_.empty()) {


        {

            if (!write_all(fd_, buffer_.data(), buffer_.size())) return false;
        }
        buffer_.clear();
    }
    

    {

        if (wal_fsync(fd_) != 0) {
            std::cerr << "[WAL] ERROR: fsync failed (errno=" << errno << ")\n";
            return false;
        }
    }
    return true;
}

// ── replay ─────────────────────────────────────────────────────
//
// Memory safety: key_size and value_size are bounded by MAX_FIELD_SIZE
// (64 MiB) before any allocation. This prevents a corrupted WAL from
// causing unbounded memory consumption. See class-level comment in wal.h.
ReplayResult WAL::replay() const {
    ReplayResult result;
    result.tainted = false;

    int rfd = wal_open(path_.c_str(), WAL_READ_FLAGS, 0);
    if (rfd < 0) return result;   // file does not exist yet

    bool hit_eof_cleanly = false;

    while (true) {
        uint32_t key_size = 0, vlog_id = 0, vlog_len = 0, stored_checksum = 0;
        uint64_t vlog_offset = 0;

        // Read header
        if (!read_exact(rfd, &key_size, sizeof(uint32_t))) {
            hit_eof_cleanly = true;
            break;
        }
        if (!read_exact(rfd, &vlog_id,         sizeof(uint32_t))) break;
        if (!read_exact(rfd, &vlog_offset,     sizeof(uint64_t))) break;
        if (!read_exact(rfd, &vlog_len,        sizeof(uint32_t))) break;
        if (!read_exact(rfd, &stored_checksum, sizeof(uint32_t))) break;

        // Size sanity check
        if (key_size > MAX_FIELD_SIZE) break;

        // Read key.
        std::string key(key_size, '\0');
        if (key_size > 0 && !read_exact(rfd, key.data(), key_size)) break;

        uint32_t expected = record_checksum(key_size, vlog_id, vlog_offset, vlog_len, key);
        if (stored_checksum != expected) break;

        // Check if tombstone
        if (vlog_len == 0xFFFFFFFF) {
            result.entries.push_back({std::move(key), VLogPointer{0, 0, 0}, true});
            continue;
        }

        result.entries.push_back({std::move(key), VLogPointer{vlog_id, vlog_offset, vlog_len}, false});
    }

    wal_close(rfd);

    // If we didn't exit cleanly at a record boundary, the WAL is tainted.
    if (!hit_eof_cleanly) {
        result.tainted = true;
        std::cerr << "[WAL] WARNING: replay stopped at corrupt/incomplete record "
                  << "(recovered " << result.entries.size()
                  << " valid entries, WAL marked tainted)\n";
    }

    // Cache the tainted state on the WAL object.
    const_cast<WAL*>(this)->tainted_ = result.tainted;

    return result;
}
