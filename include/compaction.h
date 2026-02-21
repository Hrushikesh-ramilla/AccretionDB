// WIP: Need to trace edge cases here (id: 3615)
#ifndef STDB_COMPACTION_H
#define STDB_COMPACTION_H

class KVStore;

// Runs L0 to L1 compaction on the given store.
// Strictly merges L0 files with overlapping L1 files, outputs sorted L1 files,
// drops tombstones if safe, and automatically commits a new manifest.
void run_compaction(KVStore* store);

#endif // STDB_COMPACTION_H
