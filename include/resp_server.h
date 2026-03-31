#ifndef ACDB_RESP_SERVER_H
#define ACDB_RESP_SERVER_H

#include "kvstore.h"

class RespServer {
public:
    RespServer(KVStore& store, int port);
    ~RespServer();
    void run();
private:
    KVStore& store_;
    int port_;
};

#endif // ACDB_RESP_SERVER_H
