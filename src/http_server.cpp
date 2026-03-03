#include "http_server.h"
#include "benchmark.h"

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

// ─────────────────────────────────────────────────────────────────
// Platform-specific recv / send wrappers
// ─────────────────────────────────────────────────────────────────
#ifdef _WIN32
  #pragma comment(lib, "ws2_32.lib")
  using ssize_t = int;
  static inline ssize_t sock_recv(socket_t fd, char* buf, int len, int flags) {
      return static_cast<ssize_t>(::recv(fd, buf, len, flags));
  }
  static inline ssize_t sock_send(socket_t fd, const char* buf, int len, int flags) {
      return static_cast<ssize_t>(::send(fd, buf, len, flags));
  }
#else
  static inline ssize_t sock_recv(socket_t fd, char* buf, int len, int flags) {
      return ::recv(fd, buf, static_cast<size_t>(len), flags);
  }
  static inline ssize_t sock_send(socket_t fd, const char* buf, int len, int flags) {
      return ::send(fd, buf, static_cast<size_t>(len), flags);
  }
#endif

// ─────────────────────────────────────────────────────────────────
// Constructor / Destructor
// ─────────────────────────────────────────────────────────────────
HttpServer::HttpServer(KVStore& store, int port)
    : store_(store), port_(port), server_fd_(INVALID_SOCK)
{
#ifdef _WIN32
    // Initialize Winsock (idempotent — safe to call multiple times)
    WSADATA wsa{};
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0)
        throw std::runtime_error("[HttpServer] WSAStartup failed");
#endif

    // Create TCP socket
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == INVALID_SOCK)
        throw std::runtime_error("[HttpServer] socket() failed");

    // SO_REUSEADDR — allows immediate rebind after restart
    int opt = 1;
#ifdef _WIN32
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR,
               reinterpret_cast<const char*>(&opt), sizeof(opt));
#else
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(port_));

    if (::bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close_sock(server_fd_);
        throw std::runtime_error("[HttpServer] bind() failed on port " + std::to_string(port_));
    }

    if (::listen(server_fd_, 8) != 0) {
        close_sock(server_fd_);
        throw std::runtime_error("[HttpServer] listen() failed");
    }
}

HttpServer::~HttpServer() {
    if (server_fd_ != INVALID_SOCK) {
        close_sock(server_fd_);
        server_fd_ = INVALID_SOCK;
    }
#ifdef _WIN32
    WSACleanup();
#endif
}

// ─────────────────────────────────────────────────────────────────
// Main accept loop
// ─────────────────────────────────────────────────────────────────
void HttpServer::run() {
    std::cout << "[HttpServer] Listening on http://localhost:" << port_ << "\n";
    std::cout << "[HttpServer] Dashboard: http://localhost:" << port_ << "/\n";

    while (true) {
        sockaddr_in client_addr{};
#ifdef _WIN32
        int addr_len = sizeof(client_addr);
#else
        socklen_t addr_len = sizeof(client_addr);
#endif
        socket_t client_fd = ::accept(
            server_fd_,
            reinterpret_cast<sockaddr*>(&client_addr),
            &addr_len
        );

        if (client_fd == INVALID_SOCK) {
            // Accept errors are non-fatal — log and continue
            std::cerr << "[HttpServer] accept() error, retrying\n";
            continue;
        }

        handle_client(client_fd);
        close_sock(client_fd);
    }
}

// ─────────────────────────────────────────────────────────────────
// Read a complete HTTP request from the socket into `raw`
// We accumulate bytes until we see \r\n\r\n (end of headers), then
// read any remaining body bytes indicated by Content-Length.
// ─────────────────────────────────────────────────────────────────
bool HttpServer::read_request(socket_t fd, std::string& raw) {
    raw.clear();
    std::array<char, 4096> buf{};

    // Phase 1: read until end-of-headers marker
    while (true) {
        ssize_t n = sock_recv(fd, buf.data(), static_cast<int>(buf.size()) - 1, 0);
        if (n <= 0) return false;
        raw.append(buf.data(), static_cast<size_t>(n));
        if (raw.find("\r\n\r\n") != std::string::npos) break;
        if (raw.size() > 65536) return false; // guard against giant headers
    }

    // Phase 2: read body bytes if Content-Length is set
    auto cl_pos = raw.find("Content-Length:");
    if (cl_pos == std::string::npos) cl_pos = raw.find("content-length:");
    if (cl_pos != std::string::npos) {
        auto val_start = raw.find_first_not_of(" \t", cl_pos + 15);
        auto val_end   = raw.find("\r\n", val_start);
        if (val_start != std::string::npos && val_end != std::string::npos) {
            size_t content_len = static_cast<size_t>(
                std::stoul(raw.substr(val_start, val_end - val_start))
            );
            auto header_end = raw.find("\r\n\r\n");
            size_t body_start = header_end + 4;
            size_t have = (body_start < raw.size()) ? raw.size() - body_start : 0;

            while (have < content_len) {
                ssize_t n = sock_recv(fd, buf.data(),
                    static_cast<int>(std::min(buf.size() - 1, content_len - have)), 0);
                if (n <= 0) break;
                raw.append(buf.data(), static_cast<size_t>(n));
                have += static_cast<size_t>(n);
            }
        }
    }
    return true;
}

// ─────────────────────────────────────────────────────────────────
// Parse the raw HTTP request string into an HttpRequest struct
// ─────────────────────────────────────────────────────────────────
bool HttpServer::parse_request(const std::string& raw, HttpRequest& req) {
    std::istringstream stream(raw);
    std::string request_line;
    if (!std::getline(stream, request_line)) return false;
    if (!request_line.empty() && request_line.back() == '\r')
        request_line.pop_back();

    // Parse: METHOD path HTTP/x.x
    std::istringstream rl(request_line);
    std::string version;
    rl >> req.method >> req.path >> version;
    if (req.method.empty() || req.path.empty()) return false;

    // Strip query string from path
    auto qs = req.path.find('?');
    if (qs != std::string::npos) req.path = req.path.substr(0, qs);

    // Parse headers until blank line
    std::string line;
    while (std::getline(stream, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) break;
        auto colon = line.find(':');
        if (colon != std::string::npos) {
            std::string hname = line.substr(0, colon);
            std::string hval  = line.substr(colon + 1);
            // Trim leading whitespace from value
            auto vstart = hval.find_first_not_of(" \t");
            if (vstart != std::string::npos) hval = hval.substr(vstart);
            // Lowercase the header name for case-insensitive lookup
            std::transform(hname.begin(), hname.end(), hname.begin(), ::tolower);
            req.headers[hname] = hval;
        }
    }

    // Extract body — everything after \r\n\r\n
    auto sep = raw.find("\r\n\r\n");
    if (sep != std::string::npos) req.body = raw.substr(sep + 4);

    return true;
}

// ─────────────────────────────────────────────────────────────────
// Build the raw HTTP response string to send back to the client
// ─────────────────────────────────────────────────────────────────
std::string HttpServer::build_response(const HttpResponse& resp) {
    std::ostringstream out;
    out << "HTTP/1.1 " << resp.status << " " << status_text(resp.status) << "\r\n";
    out << "Content-Type: " << resp.content_type << "; charset=utf-8\r\n";
    out << "Content-Length: " << resp.body.size() << "\r\n";
    out << "Access-Control-Allow-Origin: *\r\n";
    out << "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n";
    out << "Access-Control-Allow-Headers: Content-Type\r\n";
    out << "Connection: close\r\n";
    out << "\r\n";
    out << resp.body;
    return out.str();
}

// ─────────────────────────────────────────────────────────────────
// Handle one client connection end-to-end
// ─────────────────────────────────────────────────────────────────
void HttpServer::handle_client(socket_t client_fd) {
    std::string raw;
    if (!read_request(client_fd, raw)) return;

    HttpRequest req;
    if (!parse_request(raw, req)) return;

    // OPTIONS pre-flight (CORS)
    if (req.method == "OPTIONS") {
        HttpResponse cors;
        cors.status = 204;
        cors.body   = "";
        std::string r = build_response(cors);
        sock_send(client_fd, r.c_str(), static_cast<int>(r.size()), 0);
        return;
    }

    HttpResponse resp = route(req);
    std::string wire  = build_response(resp);
    sock_send(client_fd, wire.c_str(), static_cast<int>(wire.size()), 0);
}

// ─────────────────────────────────────────────────────────────────
// Router — dispatch to the appropriate handler
// ─────────────────────────────────────────────────────────────────
HttpResponse HttpServer::route(const HttpRequest& req) {
    // Static assets
    if (req.method == "GET") {
        if (req.path == "/")            return handle_static("web/index.html");
        if (req.path == "/style.css")   return handle_static("web/style.css");
        if (req.path == "/app.js")      return handle_static("web/app.js");
        if (req.path == "/api/metrics") return handle_metrics();
        if (req.path == "/api/lsm-state") return handle_lsm_state();
    }
    if (req.method == "POST") {
        if (req.path == "/api/put")    return handle_put(req);
        if (req.path == "/api/get")    return handle_get(req);
        if (req.path == "/api/delete") return handle_delete(req);
        if (req.path == "/api/bench")  return handle_bench(req);
    }

    HttpResponse not_found;
    not_found.status = 404;
    not_found.body   = R"({"error":"not found"})";
    return not_found;
}

// ─────────────────────────────────────────────────────────────────
// Static file serving from the web/ directory
// ─────────────────────────────────────────────────────────────────
HttpResponse HttpServer::handle_static(const std::string& rel_path) {
    std::ifstream f(rel_path, std::ios::binary);
    HttpResponse resp;
    if (!f) {
        resp.status = 404;
        resp.body   = R"({"error":"static file not found"})";
        return resp;
    }
    std::ostringstream ss;
    ss << f.rdbuf();
    resp.body = ss.str();

    // Set correct content type
    if (rel_path.ends_with(".html")) resp.content_type = "text/html";
    else if (rel_path.ends_with(".css")) resp.content_type = "text/css";
    else if (rel_path.ends_with(".js"))  resp.content_type = "application/javascript";

    return resp;
}

// ─────────────────────────────────────────────────────────────────
// GET /api/metrics  — raw + derived engine metrics
// ─────────────────────────────────────────────────────────────────
HttpResponse HttpServer::handle_metrics() {
    const auto& m = store_.metrics();

    double write_amp = (m.user_bytes_written > 0)
        ? static_cast<double>(m.storage_bytes_written) /
          static_cast<double>(m.user_bytes_written)
        : 0.0;

    double read_amp = (m.get_calls > 0)
        ? static_cast<double>(m.sst_searches + m.vlog_reads) /
          static_cast<double>(m.get_calls)
        : 0.0;

    double bloom_eff = (m.sst_considered > 0)
        ? static_cast<double>(m.bloom_skips) /
          static_cast<double>(m.sst_considered) * 100.0
        : 0.0;

    std::ostringstream j;
    j << "{\n";
    j << json_num ("user_bytes_written",    m.user_bytes_written);
    j << json_num ("storage_bytes_written", m.storage_bytes_written);
    j << json_num ("get_calls",             m.get_calls);
    j << json_num ("sst_considered",        m.sst_considered);
    j << json_num ("bloom_skips",           m.bloom_skips);
    j << json_num ("sst_searches",          m.sst_searches);
    j << json_num ("vlog_reads",            m.vlog_reads);
    j << json_dbl ("write_amplification",   write_amp);
    j << json_dbl ("read_amplification",    read_amp);
    j << json_dbl ("bloom_effectiveness",   bloom_eff, true);
    j << "}";

    HttpResponse resp;
    resp.body = j.str();
    return resp;
}

// ─────────────────────────────────────────────────────────────────
// GET /api/lsm-state  — LSM tree dimensions and health
// ─────────────────────────────────────────────────────────────────
HttpResponse HttpServer::handle_lsm_state() {
    size_t mem_entries  = store_.memtable_entries();
    size_t mem_bytes    = store_.active_byte_size();
    size_t flush_thresh = store_.flush_threshold_bytes();
    size_t l0           = store_.l0_count();
    size_t l1           = store_.l1_count();
    size_t l0_limit     = store_.l0_hard_limit();
    bool   tainted      = store_.wal_tainted();

    double mem_fill_pct = (flush_thresh > 0)
        ? static_cast<double>(mem_bytes) /
          static_cast<double>(flush_thresh) * 100.0
        : 0.0;

    double l0_pressure_pct = (l0_limit > 0)
        ? static_cast<double>(l0) /
          static_cast<double>(l0_limit) * 100.0
        : 0.0;

    std::ostringstream j;
    j << "{\n";
    j << json_num ("memtable_entries",    static_cast<uint64_t>(mem_entries));
    j << json_num ("memtable_bytes",      static_cast<uint64_t>(mem_bytes));
    j << json_num ("flush_threshold",     static_cast<uint64_t>(flush_thresh));
    j << json_dbl ("memtable_fill_pct",   mem_fill_pct);
    j << json_num ("l0_count",            static_cast<uint64_t>(l0));
    j << json_num ("l1_count",            static_cast<uint64_t>(l1));
    j << json_num ("l0_limit",            static_cast<uint64_t>(l0_limit));
    j << json_dbl ("l0_pressure_pct",     l0_pressure_pct);
    j << json_bool("wal_tainted",         tainted, true);
    j << "}";

    HttpResponse resp;
    resp.body = j.str();
    return resp;
}

// ─────────────────────────────────────────────────────────────────
// POST /api/put  — {"key":"k","value":"v"}
// ─────────────────────────────────────────────────────────────────
HttpResponse HttpServer::handle_put(const HttpRequest& req) {
    std::string key   = json_get_str(req.body, "key");
    std::string value = json_get_str(req.body, "value");
    HttpResponse resp;
    if (key.empty() || value.empty()) {
        resp.status = 400;
        resp.body   = R"({"error":"key and value required"})";
        return resp;
    }
    try {
        store_.put(key, value);
        resp.body = R"({"ok":true})";
    } catch (const std::exception& e) {
        resp.status = 500;
        resp.body   = std::string(R"({"error":)") + "\"" + json_escape(e.what()) + "\"}";
    }
    return resp;
}

// ─────────────────────────────────────────────────────────────────
// POST /api/get  — {"key":"k"}
// ─────────────────────────────────────────────────────────────────
HttpResponse HttpServer::handle_get(const HttpRequest& req) {
    std::string key = json_get_str(req.body, "key");
    HttpResponse resp;
    if (key.empty()) {
        resp.status = 400;
        resp.body   = R"({"error":"key required"})";
        return resp;
    }
    std::string value;
    if (store_.get(key, value)) {
        resp.body = std::string(R"({"found":true,"value":")") + json_escape(value) + "\"}";
    } else {
        resp.body = R"({"found":false})";
    }
    return resp;
}

// ─────────────────────────────────────────────────────────────────
// POST /api/delete  — {"key":"k"}
// ─────────────────────────────────────────────────────────────────
HttpResponse HttpServer::handle_delete(const HttpRequest& req) {
    std::string key = json_get_str(req.body, "key");
    HttpResponse resp;
    if (key.empty()) {
        resp.status = 400;
        resp.body   = R"({"error":"key required"})";
        return resp;
    }
    try {
        store_.delete_key(key);
        resp.body = R"({"ok":true})";
    } catch (const std::exception& e) {
        resp.status = 500;
        resp.body   = std::string(R"({"error":)") + "\"" + json_escape(e.what()) + "\"}";
    }
    return resp;
}

// ─────────────────────────────────────────────────────────────────
// POST /api/bench  — {"type":"random_write","ops":500}
// Runs a mini benchmark synchronously (ops capped at 2000 for web).
// ─────────────────────────────────────────────────────────────────
HttpResponse HttpServer::handle_bench(const HttpRequest& req) {
    std::string type = json_get_str(req.body, "type");
    long long ops    = json_get_int(req.body, "ops", 500);

    if (type != "random_write"    && type != "sequential_write" &&
        type != "random_read"     && type != "mixed") {
        HttpResponse bad;
        bad.status = 400;
        bad.body   = R"({"error":"invalid type"})";
        return bad;
    }

    // Cap at 2000 ops so the HTTP request doesn't time out in the browser
    if (ops < 50)   ops = 50;
    if (ops > 2000) ops = 2000;

    // Redirect stdout temporarily to capture benchmark text output
    // Instead, we capture the result via store metrics delta
    store_.metrics().reset();
    auto t0 = std::chrono::high_resolution_clock::now();
    Benchmark::run_all(type, static_cast<int>(ops));
    auto t1 = std::chrono::high_resolution_clock::now();

    double elapsed_s = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count() / 1000.0;
    double throughput = (elapsed_s > 0) ? ops / elapsed_s : 0.0;
    const auto& m = store_.metrics();
    double write_amp = (m.user_bytes_written > 0)
        ? static_cast<double>(m.storage_bytes_written) / static_cast<double>(m.user_bytes_written)
        : 0.0;
    double read_amp = (m.get_calls > 0)
        ? static_cast<double>(m.sst_searches + m.vlog_reads) / static_cast<double>(m.get_calls)
        : 0.0;

    std::ostringstream j;
    j << "{\n";
    j << json_str ("type",            type);
    j << json_num ("ops",             static_cast<uint64_t>(ops));
    j << json_dbl ("elapsed_s",       elapsed_s);
    j << json_dbl ("throughput",      throughput);
    j << json_dbl ("write_amp",       write_amp);
    j << json_dbl ("read_amp",        read_amp, true);
    j << "}";

    HttpResponse resp;
    resp.body = j.str();
    return resp;
}

// ─────────────────────────────────────────────────────────────────
// JSON helpers — minimal hand-rolled serialization
// ─────────────────────────────────────────────────────────────────
std::string HttpServer::json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:   out += c;      break;
        }
    }
    return out;
}

std::string HttpServer::json_str(const std::string& key, const std::string& val, bool last) {
    return "  \"" + key + "\": \"" + json_escape(val) + "\"" + (last ? "\n" : ",\n");
}

std::string HttpServer::json_num(const std::string& key, uint64_t val, bool last) {
    return "  \"" + key + "\": " + std::to_string(val) + (last ? "\n" : ",\n");
}

std::string HttpServer::json_dbl(const std::string& key, double val, bool last) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(4) << val;
    return "  \"" + key + "\": " + ss.str() + (last ? "\n" : ",\n");
}

std::string HttpServer::json_bool(const std::string& key, bool val, bool last) {
    return "  \"" + key + "\": " + (val ? "true" : "false") + (last ? "\n" : ",\n");
}

// Minimal flat JSON string extractor — finds "key": "value" or "key":"value"
std::string HttpServer::json_get_str(const std::string& json, const std::string& key) {
    std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return "";

    // Skip past the key and find the colon
    pos += needle.size();
    pos = json.find(':', pos);
    if (pos == std::string::npos) return "";

    // Skip whitespace and find the opening quote
    pos = json.find('"', pos + 1);
    if (pos == std::string::npos) return "";

    // Find the closing quote, handling escaped quotes
    std::string val;
    ++pos; // skip opening quote
    while (pos < json.size()) {
        char c = json[pos];
        if (c == '\\' && pos + 1 < json.size()) {
            char next = json[pos + 1];
            if (next == '"')  { val += '"';  pos += 2; continue; }
            if (next == '\\') { val += '\\'; pos += 2; continue; }
            if (next == 'n')  { val += '\n'; pos += 2; continue; }
            val += next; pos += 2; continue;
        }
        if (c == '"') break;
        val += c;
        ++pos;
    }
    return val;
}

long long HttpServer::json_get_int(const std::string& json, const std::string& key,
                                   long long default_val) {
    std::string needle = "\"" + key + "\"";
    auto pos = json.find(needle);
    if (pos == std::string::npos) return default_val;
    pos = json.find(':', pos + needle.size());
    if (pos == std::string::npos) return default_val;
    // Skip whitespace
    ++pos;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) ++pos;
    // Check if it looks like a number
    if (pos >= json.size() || (!std::isdigit(json[pos]) && json[pos] != '-'))
        return default_val;
    return std::stoll(json.substr(pos));
}

// ─────────────────────────────────────────────────────────────────
// HTTP status text
// ─────────────────────────────────────────────────────────────────
const char* HttpServer::status_text(int code) {
    switch (code) {
        case 200: return "OK";
        case 204: return "No Content";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 500: return "Internal Server Error";
        default:  return "Unknown";
    }
}
