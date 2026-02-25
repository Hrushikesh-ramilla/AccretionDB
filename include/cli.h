#ifndef STDB_CLI_H
#define STDB_CLI_H

#include "kvstore.h"
#include <string>

class CLI {
public:
    static void run(KVStore& store);
    static void parse_command(KVStore& store, const std::string& cmd_line);
};

#endif // STDB_CLI_H
