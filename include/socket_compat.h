#ifndef ACDB_SOCKET_COMPAT_H
#define ACDB_SOCKET_COMPAT_H

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #pragma comment(lib, "ws2_32.lib")

    inline void net_init() {
        WSADATA wsaData;
        WSAStartup(MAKEWORD(2, 2), &wsaData);
    }
    inline void net_cleanup() {
        WSACleanup();
    }
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    #include <cstring>
    
    #define SOCKET int
    #define INVALID_SOCKET -1
    #define SOCKET_ERROR -1
    
    inline void closesocket(int fd) { close(fd); }
    inline void net_init() {}
    inline void net_cleanup() {}
#endif

#endif // ACDB_SOCKET_COMPAT_H
