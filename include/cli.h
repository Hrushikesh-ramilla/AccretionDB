#ifndef ACDB_CLI_H
#define ACDB_CLI_H

#include "kvstore.h"
#include <string>

class CLI {
public:
    static void run(KVStore& store);
    static void parse_command(KVStore& store, const std::string& cmd_line);
};

#endif // ACDB_CLI_H
