#ifndef ACDB_IO_UTIL_H
#define ACDB_IO_UTIL_H

#include <cstdint>
#include <cstddef>

#ifdef _WIN32
  #include <windows.h>
  #include <io.h>
#else
  #include <unistd.h>
  #include <errno.h>
#endif

namespace acdb {

inline bool platform_pread(intptr_t fd, void* buf, size_t count, uint64_t offset) {
#ifdef _WIN32
    HANDLE h = (HANDLE)fd;
    if (h == INVALID_HANDLE_VALUE) return false;
    OVERLAPPED ov;
    memset(&ov, 0, sizeof(ov));
    ov.Offset = static_cast<DWORD>(offset & 0xFFFFFFFF);
    ov.OffsetHigh = static_cast<DWORD>(offset >> 32);
    DWORD read_bytes = 0;
    return ReadFile(h, buf, static_cast<DWORD>(count), &read_bytes, &ov) && read_bytes == count;
#else
    uint8_t* p = static_cast<uint8_t*>(buf);
    size_t rem = count;
    uint64_t cur_off = offset;
    while (rem > 0) {
        auto n = pread((int)fd, p, rem, cur_off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        p += n;
        cur_off += n;
        rem -= static_cast<size_t>(n);
    }
    return true;
#endif
}

} // namespace acdb

#endif // ACDB_IO_UTIL_H
