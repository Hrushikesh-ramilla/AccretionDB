#ifndef ACDB_HTTP_SERVER_H
#define ACDB_HTTP_SERVER_H

#include "kvstore.h"
#include <thread>
#include <string>
#include <iostream>
#include <chrono>
#include <cstdlib>
#include <algorithm>
#include <cstring>

#include "socket_compat.h"

inline KVStore* g_http_kvstore = nullptr;

inline std::string extract_json_str(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\":\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return "";
    pos += search.length();
    size_t end = json.find("\"", pos);
    if (end == std::string::npos) return "";
    return json.substr(pos, end - pos);
}

inline int extract_json_int(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\":";
    size_t pos = json.find(search);
    if (pos == std::string::npos) {
        std::string val = extract_json_str(json, key);
        if (val.empty()) return 0;
        return std::stoi(val);
    }
    pos += search.length();
    size_t end = json.find_first_of(",}", pos);
    if (end == std::string::npos) return 0;
    return std::stoi(json.substr(pos, end - pos));
}

inline void http_server_loop() {
    net_init();

    SOCKET server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == INVALID_SOCKET) { net_cleanup(); return; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    int port = 8080;
    if (const char* env_p = std::getenv("PORT")) {
        port = std::stoi(env_p);
    }
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) == SOCKET_ERROR) {
        closesocket(server_fd);
        net_cleanup();
        return;
    }

    if (listen(server_fd, 5) == SOCKET_ERROR) {
        closesocket(server_fd);
        net_cleanup();
        return;
    }
    
    std::cout << "HTTP Server listening on port " << port << "\n";

    while (true) {
        SOCKET client_socket = accept(server_fd, nullptr, nullptr);
        if (client_socket == INVALID_SOCKET) continue;

        // Read the full HTTP request: headers + body
        std::string request;
        {
            char hdr_buf[8192] = {0};
            int total_recv = 0;
            // Read until we have the full headers (\r\n\r\n)
            while (total_recv < (int)sizeof(hdr_buf) - 1) {
                int n = recv(client_socket, hdr_buf + total_recv, sizeof(hdr_buf) - 1 - total_recv, 0);
                if (n <= 0) break;
                total_recv += n;
                hdr_buf[total_recv] = 0;
                // Check if headers are complete
                if (strstr(hdr_buf, "\r\n\r\n")) break;
            }
            request = std::string(hdr_buf, total_recv);

            // Handle Expect: 100-continue — send interim response so client sends body
            if (request.find("Expect: 100-continue") != std::string::npos ||
                request.find("Expect: 100-Continue") != std::string::npos) {
                const char* cont = "HTTP/1.1 100 Continue\r\n\r\n";
                send(client_socket, cont, (int)strlen(cont), 0);
            }

            // Extract Content-Length and read body
            size_t cl_pos = request.find("Content-Length: ");
            if (cl_pos == std::string::npos) cl_pos = request.find("content-length: ");
            if (cl_pos != std::string::npos) {
                cl_pos += 16; // skip "Content-Length: "
                size_t cl_end = request.find("\r\n", cl_pos);
                int content_length = std::stoi(request.substr(cl_pos, cl_end - cl_pos));

                // Find where body starts (after \r\n\r\n)
                size_t body_start = request.find("\r\n\r\n");
                if (body_start != std::string::npos) {
                    body_start += 4;
                    int body_already = (int)total_recv - (int)body_start;
                    // Read remaining body bytes
                    int body_remaining = content_length - body_already;
                    while (body_remaining > 0) {
                        char body_buf[4096] = {0};
                        int to_read = std::min(body_remaining, (int)sizeof(body_buf) - 1);
                        int n = recv(client_socket, body_buf, to_read, 0);
                        if (n <= 0) break;
                        request += std::string(body_buf, n);
                        body_remaining -= n;
                    }
                }
            }
        }

        auto send_response = [&](int status_code, const std::string& status_msg, const std::string& content_type, const std::string& body) {
            std::string response = 
                "HTTP/1.1 " + std::to_string(status_code) + " " + status_msg + "\r\n"
                "Content-Type: " + content_type + "\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "Access-Control-Allow-Methods: GET, POST, OPTIONS, DELETE, PUT\r\n"
                "Access-Control-Allow-Headers: Content-Type\r\n"
                "Connection: close\r\n"
                "Content-Length: " + std::to_string(body.length()) + "\r\n\r\n" + body;
            send(client_socket, response.c_str(), response.length(), 0);
        };

        if (!g_http_kvstore) {
            send_response(503, "Service Unavailable", "text/plain", "");
            closesocket(client_socket);
            continue;
        }

        if (request.find("OPTIONS") == 0) {
            send_response(204, "No Content", "text/plain", "");
        }
        else if (request.find("GET /state") != std::string::npos || request.find("GET /api/metrics") != std::string::npos) {
            size_t mem_size = g_http_kvstore->active_byte_size();
            size_t l0_count = g_http_kvstore->l0_count();
            size_t l1_count = g_http_kvstore->l1_count();
            size_t l0_size = g_http_kvstore->l0_size();
            size_t l1_size = g_http_kvstore->l1_size();
            uint64_t p99 = g_http_kvstore->metrics().p99_latency_us.load();
            
            std::string json = "{";
            json += "\"memtable_size\":" + std::to_string(mem_size) + ",";
            json += "\"l0_files\":{\"count\":" + std::to_string(l0_count) + ",\"size\":" + std::to_string(l0_size) + "},";
            json += "\"l1_files\":{\"count\":" + std::to_string(l1_count) + ",\"size\":" + std::to_string(l1_size) + "},";
            json += "\"p99_latency_us\":" + std::to_string(p99);
            json += "}";
            send_response(200, "OK", "application/json", json);
        }
        else if (request.find("POST /api/put") != std::string::npos) {
            std::string key = extract_json_str(request, "key");
            std::string val = extract_json_str(request, "value");
            if (key.empty()) {
                send_response(400, "Bad Request", "application/json", "{\"error\":\"missing key\"}");
            } else {
                g_http_kvstore->put(key, val);
                send_response(200, "OK", "application/json", "{\"message\":\"OK\"}");
            }
        }
        else if (request.find("POST /api/get") != std::string::npos) {
            std::string key = extract_json_str(request, "key");
            std::string val;
            if (key.empty()) {
                send_response(400, "Bad Request", "application/json", "{\"error\":\"missing key\"}");
            } else if (g_http_kvstore->get(key, val)) {
                send_response(200, "OK", "application/json", "{\"value\":\"" + val + "\"}");
            } else {
                send_response(404, "Not Found", "application/json", "{\"error\":\"not found\"}");
            }
        }
        else if (request.find("POST /api/del") != std::string::npos) {
            std::string key = extract_json_str(request, "key");
            if (key.empty()) {
                send_response(400, "Bad Request", "application/json", "{\"error\":\"missing key\"}");
            } else {
                g_http_kvstore->delete_key(key);
                send_response(200, "OK", "application/json", "{\"message\":\"OK\"}");
            }
        }
        else if (request.find("POST /api/bench") != std::string::npos) {
            int ops = extract_json_int(request, "ops");
            if (ops <= 0) ops = 1000;
            std::string type = extract_json_str(request, "type");
            
            auto t1 = std::chrono::high_resolution_clock::now();
            for (int i=0; i<ops; i++) {
                if (type == "random_read") {
                    std::string val;
                    g_http_kvstore->get("bench_key_" + std::to_string(rand() % ops), val);
                } else {
                    g_http_kvstore->put("bench_key_" + std::to_string(rand() % ops), "bench_value");
                }
            }
            auto t2 = std::chrono::high_resolution_clock::now();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
            if (ms == 0) ms = 1;
            
            int throughput = (ops * 1000) / ms;
            std::string json = "{";
            json += "\"throughput_ops_per_sec\":" + std::to_string(throughput) + ",";
            json += "\"elapsed_ms\":" + std::to_string(ms) + ",";
            json += "\"p50_us\":0,";
            json += "\"p99_us\":0";
            json += "}";
            send_response(200, "OK", "application/json", json);
        }
        else {
            send_response(404, "Not Found", "text/plain", "");
        }
        closesocket(client_socket);
    }
    net_cleanup();
}

inline void start_http_server() {
    std::thread(http_server_loop).detach();
}

#endif // ACDB_HTTP_SERVER_H
