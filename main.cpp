// Phase 1 — Test Runner
// Validates: WAL, Memtable, KVStore, crash recovery, corruption handling.

#include "kvstore.h"

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

// ── Platform-specific raw write for corruption simulation ──────
#ifdef _WIN32
  #include <io.h>
  #include <fcntl.h>
  #include <sys/stat.h>
#else
  #include <unistd.h>
  #include <fcntl.h>
#endif

// ── Test helpers ───────────────────────────────────────────────

static int g_pass = 0;
static int g_fail = 0;

static void expect_eq(const std::string& actual, const std::string& expected,
                      const std::string& label) {
    if (actual == expected) {
        std::cout << "  [PASS] " << label << "\n";
        ++g_pass;
    } else {
        std::cout << "  [FAIL] " << label
                  << " -- expected \"" << expected
                  << "\", got \"" << actual << "\"\n";
        ++g_fail;
    }
}

static void expect_true(bool cond, const std::string& label) {
    if (cond) {
        std::cout << "  [PASS] " << label << "\n";
        ++g_pass;
    } else {
        std::cout << "  [FAIL] " << label << "\n";
        ++g_fail;
    }
}

// ── Raw file write helper for corruption simulation ────────────
static void append_raw_bytes(const std::string& path,
                             const void* data, size_t len) {
#ifdef _WIN32
    int fd = _open(path.c_str(),
                   _O_WRONLY | _O_APPEND | _O_CREAT | _O_BINARY,
                   _S_IREAD | _S_IWRITE);
    if (fd >= 0) {
        _write(fd, data, static_cast<unsigned int>(len));
        _close(fd);
    }
#else
    int fd = open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (fd >= 0) {
        [[maybe_unused]] auto _ = write(fd, data, len);
        close(fd);
    }
#endif
}

// ── Tests ──────────────────────────────────────────────────────

static void test_basic_put_get(const std::string& wal_path) {
    std::cout << "\n=== Test 1: Basic Put / Get ===\n";
    std::filesystem::remove(wal_path);

    KVStore store(wal_path);
    store.put("name",   "wisckey");
    store.put("engine", "lsm");
    store.put("phase",  "one");
    store.put("status", "active");

    std::string v;
    store.get("name", v);    expect_eq(v, "wisckey", "name");
    store.get("engine", v);  expect_eq(v, "lsm",     "engine");
    store.get("phase", v);   expect_eq(v, "one",     "phase");
    store.get("status", v);  expect_eq(v, "active",  "status");

    expect_true(!store.get("missing", v), "missing key returns false");
}

static void test_restart_recovery(const std::string& wal_path) {
    std::cout << "\n=== Test 2: Restart Recovery ===\n";
    // WAL from Test 1 persists on disk. Reconstruct KVStore.
    KVStore store(wal_path);

    expect_true(store.memtable_size() == 4, "recovered 4 entries");

    std::string v;
    store.get("name", v);    expect_eq(v, "wisckey", "name after recovery");
    store.get("engine", v);  expect_eq(v, "lsm",     "engine after recovery");
    store.get("phase", v);   expect_eq(v, "one",     "phase after recovery");
    store.get("status", v);  expect_eq(v, "active",  "status after recovery");
}

static void test_corrupt_tail(const std::string& wal_path) {
    std::cout << "\n=== Test 3: Corrupted WAL Tail ===\n";
    // WAL still has 4 valid records. Append garbage.
    uint32_t fake_key_size = 9999;
    append_raw_bytes(wal_path, &fake_key_size, sizeof(uint32_t));
    const char junk[] = "CORRUPT";
    append_raw_bytes(wal_path, junk, sizeof(junk));

    // Must NOT crash, must recover only valid records.
    KVStore store(wal_path);

    expect_true(store.memtable_size() == 4,
                "recovered exactly 4 valid entries (corrupt tail ignored)");

    std::string v;
    store.get("name", v);   expect_eq(v, "wisckey", "name survives corruption");
    store.get("engine", v); expect_eq(v, "lsm",     "engine survives corruption");
}

static void test_checksum_mismatch(const std::string& wal_path) {
    std::cout << "\n=== Test 4: Checksum Mismatch ===\n";
    std::filesystem::remove(wal_path);

    // Manually write a record with a WRONG checksum.
    {
        std::string key   = "bad";
        std::string value = "record";
        uint32_t ks      = static_cast<uint32_t>(key.size());
        uint32_t vs      = static_cast<uint32_t>(value.size());
        uint32_t bad_crc = 0xDEADBEEF;  // intentionally wrong

        append_raw_bytes(wal_path, &ks,      sizeof(uint32_t));
        append_raw_bytes(wal_path, &vs,      sizeof(uint32_t));
        append_raw_bytes(wal_path, &bad_crc, sizeof(uint32_t));
        append_raw_bytes(wal_path, key.data(),   ks);
        append_raw_bytes(wal_path, value.data(), vs);
    }

    KVStore store(wal_path);
    expect_true(store.memtable_size() == 0,
                "zero entries recovered from bad-checksum WAL");
}

static void test_overwrite_semantics(const std::string& wal_path) {
    std::cout << "\n=== Test 5: Overwrite Semantics ===\n";
    std::filesystem::remove(wal_path);

    {
        KVStore store(wal_path);
        store.put("key", "v1");
        store.put("key", "v2");
        store.put("key", "v3");

        std::string v;
        store.get("key", v);
        expect_eq(v, "v3", "latest value wins (live)");
    }

    // Verify overwrite survives recovery.
    {
        KVStore store(wal_path);
        std::string v;
        store.get("key", v);
        expect_eq(v, "v3", "latest value wins (after recovery)");
    }
}

static void test_empty_wal(const std::string& wal_path) {
    std::cout << "\n=== Test 6: Empty WAL ===\n";
    std::filesystem::remove(wal_path);

    KVStore store(wal_path);
    expect_true(store.memtable_size() == 0, "empty WAL yields empty memtable");

    std::string v;
    expect_true(!store.get("anything", v), "get on empty store returns false");
}

// ── main ───────────────────────────────────────────────────────

int main() {
    const std::string wal_path = "test_wal.bin";

    test_basic_put_get(wal_path);
    test_restart_recovery(wal_path);
    test_corrupt_tail(wal_path);
    test_checksum_mismatch(wal_path);
    test_overwrite_semantics(wal_path);
    test_empty_wal(wal_path);

    // Cleanup.
    std::filesystem::remove(wal_path);

    std::cout << "\n──────────────────────────────\n"
              << "Results: " << g_pass << " passed, "
              << g_fail << " failed.\n";

    return g_fail > 0 ? 1 : 0;
}

// partial state 1577
