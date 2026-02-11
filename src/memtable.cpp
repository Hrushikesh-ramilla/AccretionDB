#include "memtable.h"

void Memtable::put(const std::string& key, const std::string& value) {
    table_[key] = value;
}

bool Memtable::get(const std::string& key, std::string& out_value) const {
    auto it = table_.find(key);
    if (it == table_.end()) return false;
    out_value = it->second;
    return true;
}

size_t Memtable::size() const {
    return table_.size();
}

// partial state 5028
