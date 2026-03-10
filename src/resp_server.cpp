#include "resp_server.h"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <sstream>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <algorithm>

#pragma comment(lib, "ws2_32.lib")

RespServer::RespServer(KVStore& store, int port) : store_(store), port_(port) {
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
}

RespServer::~RespServer() {
    WSACleanup();
}

// Helper to find \r\n
static size_t find_crlf(const std::string& buffer, size_t pos = 0) {
    return buffer.find("\r\n", pos);
}

// Basic RESP bulk string array parser
static bool parse_resp_request(std::string& buffer, std::vector<std::string>& args) {
    if (buffer.empty()) return false;
    
    // We expect an array: *<count>\r\n
    if (buffer[0] != '*') return false;
    
    size_t crlf = find_crlf(buffer);
    if (crlf == std::string::npos) return false;
    
    int arg_count = std::stoi(buffer.substr(1, crlf - 1));
    size_t pos = crlf + 2;
    
    args.clear();
    for (int i = 0; i < arg_count; ++i) {
        if (pos >= buffer.size()) return false; // Incomplete
        if (buffer[pos] != '$') return false; // Expected bulk string
        
        crlf = find_crlf(buffer, pos);
        if (crlf == std::string::npos) return false;
        
        int len = std::stoi(buffer.substr(pos + 1, crlf - pos - 1));
        pos = crlf + 2;
        
        if (pos + len + 2 > buffer.size()) return false; // Incomplete data
        
        args.push_back(buffer.substr(pos, len));
        pos += len + 2;
    }
    
    // Consume the parsed request from the buffer
    buffer.erase(0, pos);
    return true;
}

static std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
    return s;
}

static void handle_client(SOCKET client_socket, KVStore* store) {
    char recv_buf[4096];
    std::string buffer;
    
    while (true) {
        int bytes_received = recv(client_socket, recv_buf, sizeof(recv_buf), 0);
        if (bytes_received <= 0) break;
        
        buffer.append(recv_buf, bytes_received);
        
        std::vector<std::string> args;
        while (parse_resp_request(buffer, args)) {
            if (args.empty()) continue;
            
            std::string cmd = to_upper(args[0]);
            std::string response;
            
            if (cmd == "PING") {
                response = "+PONG\r\n";
            } else if (cmd == "SET" && args.size() >= 3) {
                store->put(args[1], args[2]);
                response = "+OK\r\n";
            } else if (cmd == "GET" && args.size() >= 2) {
                std::string val;
                if (store->get(args[1], val)) {
                    response = "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
                } else {
                    response = "$-1\r\n"; // Null bulk string (key not found)
                }
            } else if (cmd == "DEL" && args.size() >= 2) {
                store->delete_key(args[1]);
                response = ":1\r\n"; // Integer reply indicating 1 key deleted
            } else {
                response = "-ERR unknown command or invalid arguments\r\n";
            }
            
            send(client_socket, response.c_str(), response.size(), 0);
        }
    }
    closesocket(client_socket);
}

void RespServer::run() {
    SOCKET server_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    bind(server_socket, (sockaddr*)&server_addr, sizeof(server_addr));
    listen(server_socket, SOMAXCONN);

    std::cout << "RESP Server strictly listening on port " << port_ << "...\n";

    while (true) {
        SOCKET client_socket = accept(server_socket, nullptr, nullptr);
        if (client_socket == INVALID_SOCKET) continue;
        std::thread(handle_client, client_socket, &store_).detach();
    }
}
