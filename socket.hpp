// socket.hpp - Socket cross-platform Enterprise Production-Ready
#ifndef OUR_SOCKET_HPP
#define OUR_SOCKET_HPP

// Configurazione per Windows
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#define _WINSOCK_DEPRECATED_NO_WARNINGS
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <mstcpip.h>
#pragma comment(lib, "ws2_32.lib")

typedef SOCKET socket_t;
typedef int socklen_t;
#define INVALID_SOCKET_VALUE INVALID_SOCKET
#define SOCKET_ERROR_VALUE SOCKET_ERROR
#define CLOSE_SOCKET closesocket
#define LAST_ERROR WSAGetLastError()

#ifndef SIO_KEEPALIVE_VALS
#define SIO_KEEPALIVE_VALS _WSAIOW(IOC_VENDOR, 4)
#endif
#else
// Linux/Unix
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <netdb.h>
#include <sys/ioctl.h>
#include <poll.h>

typedef int socket_t;
#define INVALID_SOCKET_VALUE -1
#define SOCKET_ERROR_VALUE -1
#define CLOSE_SOCKET close
#define LAST_ERROR errno
#endif

#include <cstring>
#include <string>
#include <stdexcept>
#include <memory>
#include <vector>
#include <chrono>
#include <mutex>
#include <atomic>

namespace ourmqtt {

    // Forward declarations
    class TLSSocket;
    struct TLSConfig;

    // Socket system initialization
    class SocketSystem {
    private:
        static std::atomic<bool> initialized_;
        static std::mutex init_mutex_;

    public:
        static bool initialize() {
            std::lock_guard<std::mutex> lock(init_mutex_);

#ifdef _WIN32
            if (!initialized_.load()) {
                WSADATA wsaData;
                int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
                if (result == 0) {
                    initialized_ = true;
                }
                return result == 0;
            }
            return true;
#else
            initialized_ = true;
            return true;
#endif
        }

        static void cleanup() {
            std::lock_guard<std::mutex> lock(init_mutex_);

#ifdef _WIN32
            if (initialized_.load()) {
                WSACleanup();
                initialized_ = false;
            }
#else
            initialized_ = false;
#endif
        }

        static bool is_initialized() {
            return initialized_.load();
        }
    };

    // Main Socket class
    class Socket {
    protected:
        socket_t fd_;
        std::atomic<bool> is_connected_;
        mutable std::mutex socket_mutex_;

        // Internal close without lock
        void close_internal() {
            if (fd_ != INVALID_SOCKET_VALUE) {
#ifdef _WIN32
                ::shutdown(fd_, SD_BOTH);
#else
                ::shutdown(fd_, SHUT_RDWR);
#endif
                CLOSE_SOCKET(fd_);
                fd_ = INVALID_SOCKET_VALUE;
                is_connected_ = false;
            }
        }

    public:

        static int get_last_error() {
#ifdef _WIN32
            return WSAGetLastError();
#else
            return errno;
#endif
        }

        Socket() : fd_(INVALID_SOCKET_VALUE), is_connected_(false) {
            if (!SocketSystem::is_initialized()) {
                SocketSystem::initialize();
            }
        }

        explicit Socket(socket_t fd) : fd_(fd), is_connected_(true) {
            if (!SocketSystem::is_initialized()) {
                SocketSystem::initialize();
            }
        }

        virtual ~Socket() {
            close();
        }

        // Move constructor
        Socket(Socket&& other) noexcept
            : fd_(INVALID_SOCKET_VALUE), is_connected_(false) {
            std::lock_guard<std::mutex> lock(other.socket_mutex_);
            fd_ = other.fd_;
            is_connected_ = other.is_connected_.load();
            other.fd_ = INVALID_SOCKET_VALUE;
            other.is_connected_ = false;
        }

        // Move assignment - CORRECTED to avoid deadlock
        Socket& operator=(Socket&& other) noexcept {
            if (this != &other) {
                // Use std::lock to avoid deadlock
                std::unique_lock<std::mutex> lock1(socket_mutex_, std::defer_lock);
                std::unique_lock<std::mutex> lock2(other.socket_mutex_, std::defer_lock);
                std::lock(lock1, lock2);

                close_internal();
                fd_ = other.fd_;
                is_connected_ = other.is_connected_.load();
                other.fd_ = INVALID_SOCKET_VALUE;
                other.is_connected_ = false;
            }
            return *this;
        }

        // Delete copy operations
        Socket(const Socket&) = delete;
        Socket& operator=(const Socket&) = delete;

        // Factory method
        static std::unique_ptr<Socket> create(bool use_tls, const TLSConfig* config = nullptr);

        // Socket creation
        bool create() {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ != INVALID_SOCKET_VALUE) {
                return false;
            }

            fd_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if (fd_ == INVALID_SOCKET_VALUE) {
                return false;
            }

            return true;
        }

        // Set non-blocking mode
        bool set_non_blocking(bool enable) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;

#ifdef _WIN32
            u_long mode = enable ? 1 : 0;
            return ioctlsocket(fd_, FIONBIO, &mode) == 0;
#else
            int flags = fcntl(fd_, F_GETFL, 0);
            if (flags == -1) return false;

            if (enable) {
                flags |= O_NONBLOCK;
            }
            else {
                flags &= ~O_NONBLOCK;
            }

            return fcntl(fd_, F_SETFL, flags) != -1;
#endif
        }

        // Set reuse address
        bool set_reuse_address(bool enable) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;

            int flag = enable ? 1 : 0;
#ifdef _WIN32
            return setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR,
                (const char*)&flag, sizeof(flag)) == 0;
#else
            return setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR,
                &flag, sizeof(flag)) == 0;
#endif
        }

        // Set TCP nodelay
        bool set_tcp_nodelay(bool enable) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;

            int flag = enable ? 1 : 0;
#ifdef _WIN32
            return setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY,
                (const char*)&flag, sizeof(flag)) == 0;
#else
            return setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY,
                &flag, sizeof(flag)) == 0;
#endif
        }

        bool set_nodelay(bool enable) {
            return set_tcp_nodelay(enable);
        }

        // Set buffer sizes
        bool set_send_buffer_size(int size) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;

#ifdef _WIN32
            return setsockopt(fd_, SOL_SOCKET, SO_SNDBUF,
                (const char*)&size, sizeof(size)) == 0;
#else
            return setsockopt(fd_, SOL_SOCKET, SO_SNDBUF,
                &size, sizeof(size)) == 0;
#endif
        }

        bool set_receive_buffer_size(int size) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;

#ifdef _WIN32
            return setsockopt(fd_, SOL_SOCKET, SO_RCVBUF,
                (const char*)&size, sizeof(size)) == 0;
#else
            return setsockopt(fd_, SOL_SOCKET, SO_RCVBUF,
                &size, sizeof(size)) == 0;
#endif
        }

        // Set timeouts
        bool set_receive_timeout(int timeout_ms) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;

#ifdef _WIN32
            DWORD timeout = timeout_ms;
            return setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO,
                (const char*)&timeout, sizeof(timeout)) == 0;
#else
            struct timeval tv;
            tv.tv_sec = timeout_ms / 1000;
            tv.tv_usec = (timeout_ms % 1000) * 1000;
            return setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO,
                &tv, sizeof(tv)) == 0;
#endif
        }

        bool set_send_timeout(int timeout_ms) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;

#ifdef _WIN32
            DWORD timeout = timeout_ms;
            return setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO,
                (const char*)&timeout, sizeof(timeout)) == 0;
#else
            struct timeval tv;
            tv.tv_sec = timeout_ms / 1000;
            tv.tv_usec = (timeout_ms % 1000) * 1000;
            return setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO,
                &tv, sizeof(tv)) == 0;
#endif
        }

        // Server operations
        bool bind(uint16_t port, const std::string& address = "") {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);

            if (address.empty()) {
                addr.sin_addr.s_addr = INADDR_ANY;
            }
            else {
#ifdef _WIN32
                if (InetPtonA(AF_INET, address.c_str(), &addr.sin_addr) != 1) {
                    return false;
                }
#else
                if (inet_pton(AF_INET, address.c_str(), &addr.sin_addr) <= 0) {
                    return false;
                }
#endif
            }

            return ::bind(fd_, (sockaddr*)&addr, sizeof(addr)) == 0;
        }

        bool listen(int backlog = 128) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;
            return ::listen(fd_, backlog) == 0;
        }

        Socket accept() {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) {
                return Socket();
            }

            sockaddr_in client_addr{};
            socklen_t len = sizeof(client_addr);

            socket_t client_fd = ::accept(fd_, (sockaddr*)&client_addr, &len);
            if (client_fd != INVALID_SOCKET_VALUE) {
                return Socket(client_fd);
            }

            return Socket();
        }

        // Client operations
        bool connect(const std::string& host, uint16_t port) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);

            // Try to parse as IP address first
#ifdef _WIN32
            if (InetPtonA(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
                // Not an IP address, try DNS resolution
                struct addrinfo hints {}, * result = nullptr;
                hints.ai_family = AF_INET;
                hints.ai_socktype = SOCK_STREAM;

                std::string port_str = std::to_string(port);
                if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result) == 0) {
                    sockaddr_in* addr_in = (sockaddr_in*)result->ai_addr;
                    addr = *addr_in;
                    freeaddrinfo(result);
                }
                else {
                    return false;
                }
            }
#else
            if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
                // Not an IP address, try DNS resolution
                struct hostent* he = gethostbyname(host.c_str());
                if (he == nullptr) {
                    return false;
                }
                addr.sin_addr = *((struct in_addr*)he->h_addr);
            }
#endif

            int result = ::connect(fd_, (sockaddr*)&addr, sizeof(addr));

#ifdef _WIN32
            if (result == SOCKET_ERROR) {
                int error = WSAGetLastError();
                if (error == WSAEWOULDBLOCK || error == WSAEINPROGRESS) {
                    is_connected_ = true;
                    return true;
                }
                return false;
            }
#else
            if (result < 0) {
                if (errno == EINPROGRESS || errno == EWOULDBLOCK) {
                    is_connected_ = true;
                    return true;
                }
                return false;
            }
#endif

            is_connected_ = true;
            return true;
        }

        bool wait_for_connect(int timeout_ms) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return false;

#ifdef _WIN32
            fd_set write_set, error_set;
            FD_ZERO(&write_set);
            FD_ZERO(&error_set);
            FD_SET(fd_, &write_set);
            FD_SET(fd_, &error_set);

            struct timeval tv;
            tv.tv_sec = timeout_ms / 1000;
            tv.tv_usec = (timeout_ms % 1000) * 1000;

            int result = select(static_cast<int>(fd_) + 1, nullptr, &write_set, &error_set, &tv);

            if (result > 0) {
                if (FD_ISSET(fd_, &error_set)) {
                    return false;
                }
                if (FD_ISSET(fd_, &write_set)) {
                    int error = 0;
                    socklen_t len = sizeof(error);
                    if (getsockopt(fd_, SOL_SOCKET, SO_ERROR,
                        (char*)&error, &len) == 0) {
                        if (error == 0) {
                            is_connected_ = true;
                            return true;
                        }
                    }
                }
            }
#else
            struct pollfd pfd;
            pfd.fd = fd_;
            pfd.events = POLLOUT;

            int result = poll(&pfd, 1, timeout_ms);

            if (result > 0) {
                int error = 0;
                socklen_t len = sizeof(error);
                if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &len) == 0) {
                    if (error == 0) {
                        is_connected_ = true;
                        return true;
                    }
                }
            }
#endif
            return false;
        }

        // Data transfer
        virtual int send(const void* data, size_t len) {
            return send(static_cast<const uint8_t*>(data), len);
        }

        virtual int send(const uint8_t* data, size_t len) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE || !data || len == 0) return -1;

#ifdef _WIN32
            return ::send(fd_, (const char*)data, static_cast<int>(len), 0);
#else
            return ::send(fd_, data, len, MSG_NOSIGNAL);
#endif
        }

        int send(const std::string& data) {
            return send(data.c_str(), data.length());
        }

        int send(const char* data, size_t len) {
            return send(static_cast<const void*>(data), len);
        }

        virtual int receive(void* buffer, size_t max_len) {
            return receive(static_cast<uint8_t*>(buffer), max_len);
        }

        virtual int receive(uint8_t* buffer, size_t max_len) {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE || !buffer || max_len == 0) return -1;

#ifdef _WIN32
            return ::recv(fd_, (char*)buffer, static_cast<int>(max_len), 0);
#else
            return ::recv(fd_, buffer, max_len, 0);
#endif
        }

        int receive(char* buffer, size_t max_len) {
            return receive(static_cast<void*>(buffer), max_len);
        }

        // Socket state
        void close() {
            std::lock_guard<std::mutex> lock(socket_mutex_);
            close_internal();
        }

        bool is_valid() const {
            std::lock_guard<std::mutex> lock(socket_mutex_);
            return fd_ != INVALID_SOCKET_VALUE;
        }

        bool is_connected() const {
            return is_connected_.load() && is_valid();
        }

        socket_t get_handle() const {
            std::lock_guard<std::mutex> lock(socket_mutex_);
            return fd_;
        }

        virtual bool is_tls() const {
            return false;
        }

        bool would_block() const {
#ifdef _WIN32
            int error = WSAGetLastError();
            return (error == WSAEWOULDBLOCK || error == WSAEINPROGRESS);
#else
            return (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINPROGRESS);
#endif
        }

        // Get peer address
        std::string get_peer_address() const {
            std::lock_guard<std::mutex> lock(socket_mutex_);

            if (fd_ == INVALID_SOCKET_VALUE) return "";

            sockaddr_in addr{};
            socklen_t len = sizeof(addr);

            if (getpeername(fd_, (sockaddr*)&addr, &len) == 0) {
                char ip_str[INET_ADDRSTRLEN];
#ifdef _WIN32
                InetNtopA(AF_INET, &addr.sin_addr, ip_str, INET_ADDRSTRLEN);
#else
                inet_ntop(AF_INET, &addr.sin_addr, ip_str, INET_ADDRSTRLEN);
#endif
                return std::string(ip_str) + ":" + std::to_string(ntohs(addr.sin_port));
            }

            return "";
        }

        // Error handling


        bool set_reuse_port(bool enable) {
#ifdef SO_REUSEPORT
            int opt = enable ? 1 : 0;
            return setsockopt(sock_, SOL_SOCKET, SO_REUSEPORT,
                reinterpret_cast<const char*>(&opt), sizeof(opt)) == 0;
#else
            return true;  // Not supported on Windows
#endif
        }

        static std::string get_last_error_string() {
#ifdef _WIN32
            int error = WSAGetLastError();
            char buffer[256];
            FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                nullptr, error,
                MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                buffer, sizeof(buffer), nullptr);
            return std::string(buffer);
#else
            return std::string(strerror(errno));
#endif
        }

        static int get_last_error_code() {
#ifdef _WIN32
            return WSAGetLastError();
#else
            return errno;
#endif
        }
    };

    // Static member initialization
    inline std::atomic<bool> SocketSystem::initialized_{ false };
    inline std::mutex SocketSystem::init_mutex_;

} // namespace ourmqtt

#endif // OUR_SOCKET_HPP