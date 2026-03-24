# ── Stage 1: Build ────────────────────────────────────────────────
# Use the official GCC 13 image on Debian slim — gives us C++20 support
# without pulling in the full Ubuntu bloat.
FROM gcc:13-bookworm AS builder

WORKDIR /build

# Copy source tree
COPY . .

# Compile the binary. Linux uses standard POSIX sockets; no -lws2_32 needed.
# We compile directly or via Makefile.
RUN g++ -std=c++20 -O2 -Wall -Wextra -Iinclude \
        src/crc32.cpp \
        src/wal.cpp \
        src/vlog.cpp \
        src/sstable.cpp \
        src/arena.cpp \
        src/skiplist.cpp \
        src/memtable.cpp \
        src/version_edit.cpp \
        src/version_set.cpp \
        src/compaction.cpp \
        src/vlog_gc.cpp \
        src/bloom.cpp \
        src/benchmark.cpp \
        src/kvstore.cpp \
        src/thread_pool.cpp \
        src/resp_server.cpp \
        src/fault_injection.cpp \
        main.cpp \
        -pthread \
        -static-libstdc++ -static-libgcc \
        -o acdb

# ── Stage 2: Runtime ──────────────────────────────────────────────
# Debian slim — minimal footprint, just enough to run the binary.
FROM debian:bookworm-slim AS runtime

# Install libstdc++ runtime (bundled with GCC, needed for std::filesystem)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary
COPY --from=builder /build/acdb ./acdb

# Copy the terminal dashboard assets
COPY --from=builder /build/dashboard/public ./dashboard/public

# Expose the dashboard port (Azure overrides this via PORT env var)
EXPOSE 8080
EXPOSE 6379

# Health check — poll /state every 30 seconds
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:${PORT:-8080}/state || exit 1

# Default: start the HTTP server in web mode for the dashboard
CMD ["./acdb", "web"]
