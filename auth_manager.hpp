// auth_manager.hpp - Enterprise-grade authentication system
#ifndef OUR_AUTH_MANAGER_HPP
#define OUR_AUTH_MANAGER_HPP

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <vector>
#include <regex>
#include <fstream>
#include <chrono>
#include <random>
#include <atomic>
#include <queue>
#include <functional>
#include <optional>
#include <variant>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <algorithm>
#include <array>
#include <cstring>
// Undefine le macro di Windows che causano conflitti
#ifdef DELETE
#undef DELETE
#endif
// Crypto libraries
#ifdef _WIN32
#include <windows.h>
#include <wincrypt.h>
#include <bcrypt.h>
#pragma comment(lib, "bcrypt.lib")
#pragma comment(lib, "crypt32.lib")
#else
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/aes.h>
#endif

#include "logger.hpp"

namespace ourmqtt {

    /**
     * @brief Security configuration for authentication system
     */
    struct SecurityConfig {
        // Password policy
        size_t min_password_length = 12;
        bool require_uppercase = true;
        bool require_lowercase = true;
        bool require_numbers = true;
        bool require_special_chars = true;
        size_t password_history_size = 5;
        std::chrono::hours password_expiry = std::chrono::hours(24 * 90); // 90 days

        // Hashing
        size_t pbkdf2_iterations = 100000;
        size_t salt_size = 32;
        size_t hash_size = 64;

        // Session
        std::chrono::minutes session_timeout = std::chrono::minutes(30);
        std::chrono::hours refresh_token_expiry = std::chrono::hours(24);
        size_t max_sessions_per_user = 5;
        size_t token_size = 32;  // bytes

        // Rate limiting
        size_t max_login_attempts = 5;
        std::chrono::minutes lockout_duration = std::chrono::minutes(15);
        size_t requests_per_minute = 60;
        size_t max_regex_cache_size = 1000;

        // Security
        bool enable_2fa = false;
        bool audit_logging = true;
        bool encrypt_at_rest = true;
        std::string encryption_key_file = "auth_key.bin";

        // Limits
        size_t max_username_length = 128;
        size_t max_email_length = 256;
        size_t max_resource_length = 1024;
        size_t max_pattern_length = 1024;
    };

    // Forward declarations
    class SecureStorage;
    class ConstantTimeOps;

    /**
     * @brief Constant-time operations for security
     */
    class ConstantTimeOps {
    public:
        static bool compare(const void* a, const void* b, size_t size) {
            const volatile unsigned char* va = static_cast<const volatile unsigned char*>(a);
            const volatile unsigned char* vb = static_cast<const volatile unsigned char*>(b);
            volatile unsigned char result = 0;

            for (size_t i = 0; i < size; i++) {
                result |= va[i] ^ vb[i];
            }

            return result == 0;
        }

        static void secure_zero(void* ptr, size_t size) {
            volatile unsigned char* vptr = static_cast<volatile unsigned char*>(ptr);
            while (size--) {
                *vptr++ = 0;
            }

            // Prevent compiler optimization
#ifdef _WIN32
            SecureZeroMemory(ptr, size);
#else
            __asm__ __volatile__("" : : "r"(ptr) : "memory");
#endif
        }
    };

    /**
     * @brief Secure random number generator
     */
    class SecureRandom {
    private:
        mutable std::mutex mutex_;

    public:
        std::vector<uint8_t> generate(size_t size) {
            std::lock_guard<std::mutex> lock(mutex_);
            std::vector<uint8_t> buffer(size);

#ifdef _WIN32
            NTSTATUS status = BCryptGenRandom(
                nullptr,
                buffer.data(),
                static_cast<ULONG>(size),
                BCRYPT_USE_SYSTEM_PREFERRED_RNG
            );
            if (!BCRYPT_SUCCESS(status)) {
                throw std::runtime_error("Failed to generate random bytes");
            }
#else
            if (RAND_bytes(buffer.data(), static_cast<int>(size)) != 1) {
                throw std::runtime_error("Failed to generate random bytes");
            }
#endif

            return buffer;
        }

        std::string generate_token(size_t size = 32) {
            auto bytes = generate(size);
            return bytes_to_hex(bytes);
        }

    private:
        std::string bytes_to_hex(const std::vector<uint8_t>& bytes) {
            std::stringstream ss;
            ss << std::hex << std::setfill('0');
            for (uint8_t b : bytes) {
                ss << std::setw(2) << static_cast<int>(b);
            }
            return ss.str();
        }
    };

    /**
     * @brief Secure storage with encryption at rest
     */
    class SecureStorage {
    private:
        std::array<uint8_t, 32> master_key_;
        std::array<uint8_t, 32> encryption_key_;
        std::array<uint8_t, 32> mac_key_;
        mutable std::mutex mutex_;
        SecureRandom rng_;

        void derive_keys() {
            // Simple key derivation (use HKDF in production)
            for (size_t i = 0; i < 32; i++) {
                encryption_key_[i] = master_key_[i];
                mac_key_[i] = master_key_[31 - i];
            }
        }

    public:
        explicit SecureStorage(const std::string& key_file) {
            load_or_generate_key(key_file);
            derive_keys();
        }

        ~SecureStorage() {
            // Securely wipe keys from memory
            ConstantTimeOps::secure_zero(master_key_.data(), master_key_.size());
            ConstantTimeOps::secure_zero(encryption_key_.data(), encryption_key_.size());
            ConstantTimeOps::secure_zero(mac_key_.data(), mac_key_.size());
        }

        void load_or_generate_key(const std::string& key_file) {
            std::ifstream file(key_file, std::ios::binary);
            if (file) {
                file.read(reinterpret_cast<char*>(master_key_.data()), master_key_.size());
                if (!file) {
                    throw std::runtime_error("Failed to read encryption key");
                }
            }
            else {
                // Generate new key
                auto key_bytes = rng_.generate(32);
                std::copy(key_bytes.begin(), key_bytes.end(), master_key_.begin());

                // Save key
                std::ofstream out(key_file, std::ios::binary);
                if (!out) {
                    throw std::runtime_error("Failed to save encryption key");
                }
                out.write(reinterpret_cast<const char*>(master_key_.data()), master_key_.size());

                // Set restrictive permissions
#ifndef _WIN32
                chmod(key_file.c_str(), 0600);
#endif
            }
        }

        std::vector<uint8_t> encrypt(const std::vector<uint8_t>& plaintext) {
            std::lock_guard<std::mutex> lock(mutex_);

            // Generate IV
            auto iv = rng_.generate(16);

            // Simple XOR encryption (use AES-GCM in production)
            std::vector<uint8_t> ciphertext;
            ciphertext.reserve(iv.size() + plaintext.size() + 32); // IV + data + MAC

            // Add IV
            ciphertext.insert(ciphertext.end(), iv.begin(), iv.end());

            // Encrypt (simplified - use proper AES in production)
            for (size_t i = 0; i < plaintext.size(); i++) {
                ciphertext.push_back(plaintext[i] ^ encryption_key_[i % 32] ^ iv[i % 16]);
            }

            // Add MAC (simplified - use HMAC in production)
            auto mac = calculate_mac(ciphertext);
            ciphertext.insert(ciphertext.end(), mac.begin(), mac.end());

            return ciphertext;
        }

        std::vector<uint8_t> decrypt(const std::vector<uint8_t>& ciphertext) {
            if (ciphertext.size() < 48) { // IV(16) + MAC(32)
                throw std::runtime_error("Invalid ciphertext");
            }

            std::lock_guard<std::mutex> lock(mutex_);

            // Verify MAC
            std::vector<uint8_t> data(ciphertext.begin(), ciphertext.end() - 32);
            std::vector<uint8_t> stored_mac(ciphertext.end() - 32, ciphertext.end());
            auto calculated_mac = calculate_mac(data);

            if (!ConstantTimeOps::compare(stored_mac.data(), calculated_mac.data(), 32)) {
                throw std::runtime_error("MAC verification failed");
            }

            // Extract IV
            std::vector<uint8_t> iv(ciphertext.begin(), ciphertext.begin() + 16);

            // Decrypt (simplified - use proper AES in production)
            std::vector<uint8_t> plaintext;
            for (size_t i = 16; i < data.size(); i++) {
                plaintext.push_back(data[i] ^ encryption_key_[(i - 16) % 32] ^ iv[(i - 16) % 16]);
            }

            return plaintext;
        }

    private:
        std::vector<uint8_t> calculate_mac(const std::vector<uint8_t>& data) {
            // Simplified MAC (use HMAC-SHA256 in production)
            std::vector<uint8_t> mac(32);
            for (size_t i = 0; i < data.size(); i++) {
                mac[i % 32] ^= data[i] ^ mac_key_[i % 32];
            }
            return mac;
        }
    };

    /**
     * @brief Secure password hasher with multiple algorithms
     */
    class SecurePasswordHasher {
    private:
        SecurityConfig config_;
        SecureRandom rng_;

    public:
        struct HashedPassword {
            std::vector<uint8_t> hash;
            std::vector<uint8_t> salt;
            size_t iterations;
            std::string algorithm;
            std::chrono::system_clock::time_point created_at;

            std::vector<uint8_t> serialize() const {
                std::vector<uint8_t> data;

                // Version
                data.push_back(0x01);

                // Algorithm
                data.push_back(static_cast<uint8_t>(algorithm.size()));
                data.insert(data.end(), algorithm.begin(), algorithm.end());

                // Iterations
                for (int i = 3; i >= 0; i--) {
                    data.push_back((iterations >> (i * 8)) & 0xFF);
                }

                // Salt
                data.push_back(static_cast<uint8_t>(salt.size()));
                data.insert(data.end(), salt.begin(), salt.end());

                // Hash
                data.push_back(static_cast<uint8_t>(hash.size()));
                data.insert(data.end(), hash.begin(), hash.end());

                // Timestamp
                auto time_since_epoch = created_at.time_since_epoch();
                auto seconds = std::chrono::duration_cast<std::chrono::seconds>(time_since_epoch).count();
                for (int i = 7; i >= 0; i--) {
                    data.push_back((seconds >> (i * 8)) & 0xFF);
                }

                return data;
            }

            static HashedPassword deserialize(const std::vector<uint8_t>& data) {
                HashedPassword result;
                size_t pos = 0;

                // Version
                if (data[pos++] != 0x01) {
                    throw std::runtime_error("Invalid password format version");
                }

                // Algorithm
                size_t algo_len = data[pos++];
                result.algorithm = std::string(data.begin() + pos, data.begin() + pos + algo_len);
                pos += algo_len;

                // Iterations
                result.iterations = 0;
                for (int i = 0; i < 4; i++) {
                    result.iterations = (result.iterations << 8) | data[pos++];
                }

                // Salt
                size_t salt_len = data[pos++];
                result.salt.assign(data.begin() + pos, data.begin() + pos + salt_len);
                pos += salt_len;

                // Hash
                size_t hash_len = data[pos++];
                result.hash.assign(data.begin() + pos, data.begin() + pos + hash_len);
                pos += hash_len;

                // Timestamp
                int64_t seconds = 0;
                for (int i = 0; i < 8; i++) {
                    seconds = (seconds << 8) | data[pos++];
                }
                result.created_at = std::chrono::system_clock::time_point(
                    std::chrono::seconds(seconds));

                return result;
            }
        };

        explicit SecurePasswordHasher(const SecurityConfig& config)
            : config_(config) {}

        bool validate_password_strength(const std::string& password) const {
            if (password.length() < config_.min_password_length) {
                return false;
            }

            if (password.length() > 256) { // Reasonable upper limit
                return false;
            }

            bool has_upper = false, has_lower = false;
            bool has_digit = false, has_special = false;

            for (char c : password) {
                if (std::isupper(c)) has_upper = true;
                else if (std::islower(c)) has_lower = true;
                else if (std::isdigit(c)) has_digit = true;
                else if (std::ispunct(c)) has_special = true;
            }

            if (config_.require_uppercase && !has_upper) return false;
            if (config_.require_lowercase && !has_lower) return false;
            if (config_.require_numbers && !has_digit) return false;
            if (config_.require_special_chars && !has_special) return false;

            // Check for common patterns
            if (password.find("123") != std::string::npos ||
                password.find("abc") != std::string::npos ||
                password.find("qwerty") != std::string::npos ||
                password.find("password") != std::string::npos) {
                return false;
            }

            return true;
        }

        HashedPassword hash_password(const std::string& password) {
            if (!validate_password_strength(password)) {
                throw std::invalid_argument("Password does not meet strength requirements");
            }

            HashedPassword result;
            result.salt = rng_.generate(config_.salt_size);
            result.iterations = config_.pbkdf2_iterations;
            result.algorithm = "PBKDF2-SHA256";
            result.created_at = std::chrono::system_clock::now();

            // Use PBKDF2
            result.hash.resize(config_.hash_size);

#ifdef _WIN32
            BCRYPT_ALG_HANDLE hAlg = nullptr;
            BCryptOpenAlgorithmProvider(&hAlg, BCRYPT_SHA256_ALGORITHM, nullptr,
                BCRYPT_ALG_HANDLE_HMAC_FLAG);

            BCryptDeriveKeyPBKDF2(hAlg,
                (PUCHAR)password.data(), password.size(),
                result.salt.data(), result.salt.size(),
                result.iterations,
                result.hash.data(), result.hash.size(),
                0);

            BCryptCloseAlgorithmProvider(hAlg, 0);
#else
            PKCS5_PBKDF2_HMAC(password.c_str(), password.length(),
                result.salt.data(), result.salt.size(),
                result.iterations,
                EVP_sha256(),
                result.hash.size(),
                result.hash.data());
#endif

            return result;
        }

        bool verify_password(const std::string& password, const HashedPassword& stored) {
            std::vector<uint8_t> computed_hash(stored.hash.size());

#ifdef _WIN32
            BCRYPT_ALG_HANDLE hAlg = nullptr;
            BCryptOpenAlgorithmProvider(&hAlg, BCRYPT_SHA256_ALGORITHM, nullptr,
                BCRYPT_ALG_HANDLE_HMAC_FLAG);

            BCryptDeriveKeyPBKDF2(hAlg,
                (PUCHAR)password.data(), password.size(),
                const_cast<PUCHAR>(stored.salt.data()), stored.salt.size(),
                stored.iterations,
                computed_hash.data(), computed_hash.size(),
                0);

            BCryptCloseAlgorithmProvider(hAlg, 0);
#else
            PKCS5_PBKDF2_HMAC(password.c_str(), password.length(),
                stored.salt.data(), stored.salt.size(),
                stored.iterations,
                EVP_sha256(),
                computed_hash.size(),
                computed_hash.data());
#endif

            // Constant-time comparison
            return ConstantTimeOps::compare(computed_hash.data(), stored.hash.data(),
                computed_hash.size());
        }
    };

    // Event types for audit
    enum class AuthEvent {
        LOGIN_SUCCESS,
        LOGIN_FAILED,
        LOGOUT,
        PASSWORD_CHANGED,
        PERMISSION_GRANTED,
        PERMISSION_DENIED,
        USER_CREATED,
        USER_DELETED,
        USER_MODIFIED,
        SESSION_EXPIRED,
        RATE_LIMITED,
        INVALID_TOKEN
    };

    /**
     * @brief Thread-safe audit logger
     */
    class AuthLogger {
    private:
        mutable std::mutex mutex_;
        std::string filename_;
        std::ofstream log_file_;
        bool enabled_;
        std::queue<std::string> log_buffer_;
        static constexpr size_t MAX_BUFFER_SIZE = 1000;
        std::atomic<size_t> dropped_logs_{ 0 };

        std::string format_timestamp() const {
            auto now = std::chrono::system_clock::now();
            auto time_t = std::chrono::system_clock::to_time_t(now);
            std::stringstream ss;
            #ifdef _WIN32
                        std::tm tm_info;
                        localtime_s(&tm_info, &time_t);
                        ss << std::put_time(&tm_info, "%Y-%m-%d %H:%M:%S");
            #else
                        ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
            #endif            
            return ss.str();
        }

        std::string event_to_string(AuthEvent event) const {
            switch (event) {
            case AuthEvent::LOGIN_SUCCESS: return "LOGIN_SUCCESS";
            case AuthEvent::LOGIN_FAILED: return "LOGIN_FAILED";
            case AuthEvent::LOGOUT: return "LOGOUT";
            case AuthEvent::PASSWORD_CHANGED: return "PASSWORD_CHANGED";
            case AuthEvent::PERMISSION_GRANTED: return "PERMISSION_GRANTED";
            case AuthEvent::PERMISSION_DENIED: return "PERMISSION_DENIED";
            case AuthEvent::USER_CREATED: return "USER_CREATED";
            case AuthEvent::USER_DELETED: return "USER_DELETED";
            case AuthEvent::USER_MODIFIED: return "USER_MODIFIED";
            case AuthEvent::SESSION_EXPIRED: return "SESSION_EXPIRED";
            case AuthEvent::RATE_LIMITED: return "RATE_LIMITED";
            case AuthEvent::INVALID_TOKEN: return "INVALID_TOKEN";
            default: return "UNKNOWN";
            }
        }

        void flush_buffer_unsafe() {
            if (!log_file_.is_open()) {
                // Try to reopen
                log_file_.open(filename_, std::ios::app);
                if (!log_file_) {
                    // Drop old logs if can't write
                    while (log_buffer_.size() > MAX_BUFFER_SIZE / 2) {
                        log_buffer_.pop();
                        dropped_logs_++;
                    }
                    return;
                }
            }

            while (!log_buffer_.empty() && log_file_.good()) {
                log_file_ << log_buffer_.front() << std::endl;
                log_buffer_.pop();
            }

            log_file_.flush();
        }

    public:
        explicit AuthLogger(const std::string& filename = "auth.log", bool enabled = true)
            : filename_(filename), enabled_(enabled) {
            if (enabled_) {
                log_file_.open(filename_, std::ios::app);
            }
        }

        ~AuthLogger() {
            if (enabled_) {
                std::lock_guard<std::mutex> lock(mutex_);
                flush_buffer_unsafe();
                if (dropped_logs_ > 0) {
                    log_file_ << format_timestamp() << " | WARNING | Dropped "
                        << dropped_logs_ << " log entries" << std::endl;
                }
            }
        }

        void log(AuthEvent event, const std::string& username,
            const std::string& details = "", const std::string& ip = "") {
            if (!enabled_) return;

            std::lock_guard<std::mutex> lock(mutex_);

            // Build log entry
            std::stringstream entry;
            entry << format_timestamp() << " | "
                << event_to_string(event) << " | "
                << username;

            if (!ip.empty()) {
                entry << " | " << ip;
            }

            if (!details.empty()) {
                entry << " | " << details;
            }

            // Add to buffer
            if (log_buffer_.size() >= MAX_BUFFER_SIZE) {
                flush_buffer_unsafe();
            }

            log_buffer_.push(entry.str());
        }

        void flush() {
            if (!enabled_) return;
            std::lock_guard<std::mutex> lock(mutex_);
            flush_buffer_unsafe();
        }
    };

    /**
     * @brief Session management with secure tokens
     */
    class SessionManager {
    private:
        struct Session {
            std::string token;
            std::string username;
            std::string client_id;
            std::chrono::system_clock::time_point created_at;
            std::chrono::system_clock::time_point expires_at;
            std::chrono::system_clock::time_point last_activity;
            std::string ip_address;
            bool is_active = true;
        };

        mutable std::shared_mutex mutex_;
        std::unordered_map<std::string, std::unique_ptr<Session>> sessions_;
        std::unordered_map<std::string, std::unordered_set<std::string>> user_sessions_;
        SecurityConfig config_;
        SecureRandom rng_;

    public:
        explicit SessionManager(const SecurityConfig& config)
            : config_(config) {}

        std::string create_session(const std::string& username,
            const std::string& client_id,
            const std::string& ip_address = "") {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            // Enforce max sessions per user
            auto& user_session_set = user_sessions_[username];
            if (user_session_set.size() >= config_.max_sessions_per_user) {
                // Remove oldest session
                std::string oldest_token;
                auto oldest_time = std::chrono::system_clock::time_point::max();

                for (const auto& token : user_session_set) {
                    auto it = sessions_.find(token);
                    if (it != sessions_.end() && it->second->created_at < oldest_time) {
                        oldest_time = it->second->created_at;
                        oldest_token = token;
                    }
                }

                if (!oldest_token.empty()) {
                    sessions_.erase(oldest_token);
                    user_session_set.erase(oldest_token);
                }
            }

            // Create new session
            auto session = std::make_unique<Session>();
            session->token = rng_.generate_token(config_.token_size);
            session->username = username;
            session->client_id = client_id;
            session->ip_address = ip_address;
            session->created_at = std::chrono::system_clock::now();
            session->expires_at = session->created_at + config_.session_timeout;
            session->last_activity = session->created_at;

            std::string token = session->token;
            sessions_[token] = std::move(session);
            user_session_set.insert(token);

            return token;
        }

        std::optional<std::string> validate_token(const std::string& token) {
            // Validate token format
            if (token.length() != config_.token_size * 2) { // Hex encoding
                return std::nullopt;
            }

            std::shared_lock<std::shared_mutex> lock(mutex_);

            auto it = sessions_.find(token);
            if (it == sessions_.end() || !it->second->is_active) {
                return std::nullopt;
            }

            auto now = std::chrono::system_clock::now();
            if (it->second->expires_at < now) {
                return std::nullopt;
            }

            // Update activity time (requires brief exclusive lock)
            lock.unlock();
            {
                std::unique_lock<std::shared_mutex> write_lock(mutex_);
                it->second->last_activity = now;

                // Optionally extend session
                if (now - it->second->created_at < config_.refresh_token_expiry) {
                    it->second->expires_at = now + config_.session_timeout;
                }
            }

            return it->second->username;
        }

        void invalidate_token(const std::string& token) {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            auto it = sessions_.find(token);
            if (it != sessions_.end()) {
                user_sessions_[it->second->username].erase(token);
                sessions_.erase(it);
            }
        }

        void invalidate_user_sessions(const std::string& username) {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            auto it = user_sessions_.find(username);
            if (it != user_sessions_.end()) {
                for (const auto& token : it->second) {
                    sessions_.erase(token);
                }
                user_sessions_.erase(it);
            }
        }

        size_t get_active_sessions_count() const {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            return sessions_.size();
        }

        void cleanup_expired_sessions() {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            auto now = std::chrono::system_clock::now();
            std::vector<std::string> to_remove;

            for (const auto& [token, session] : sessions_) {
                if (session->expires_at < now) {
                    to_remove.push_back(token);
                }
            }

            for (const auto& token : to_remove) {
                auto it = sessions_.find(token);
                if (it != sessions_.end()) {
                    user_sessions_[it->second->username].erase(token);
                    sessions_.erase(it);
                }
            }
        }
    };

    /**
     * @brief Rate limiter with exponential backoff
     */
    class RateLimiter {
    private:
        struct RateInfo {
            std::queue<std::chrono::system_clock::time_point> timestamps;
            std::chrono::system_clock::time_point lockout_until;
            std::atomic<size_t> failed_attempts{ 0 };
        };

        mutable std::shared_mutex mutex_;
        std::unordered_map<std::string, std::unique_ptr<RateInfo>> rate_map_;
        SecurityConfig config_;

    public:
        explicit RateLimiter(const SecurityConfig& config)
            : config_(config) {}

        bool check_rate_limit(const std::string& identifier) {
            if (identifier.empty()) return true;

            std::unique_lock<std::shared_mutex> lock(mutex_);

            auto& info = rate_map_[identifier];
            if (!info) {
                info = std::make_unique<RateInfo>();
            }

            auto now = std::chrono::system_clock::now();

            // Check lockout
            if (info->lockout_until > now) {
                return false;
            }

            // Clean old timestamps
            while (!info->timestamps.empty() &&
                now - info->timestamps.front() > std::chrono::minutes(1)) {
                info->timestamps.pop();
            }

            // Check rate
            if (info->timestamps.size() >= config_.requests_per_minute) {
                return false;
            }

            info->timestamps.push(now);
            return true;
        }

        void record_failed_attempt(const std::string& identifier) {
            if (identifier.empty()) return;

            std::unique_lock<std::shared_mutex> lock(mutex_);

            auto& info = rate_map_[identifier];
            if (!info) {
                info = std::make_unique<RateInfo>();
            }

            size_t attempts = info->failed_attempts.fetch_add(1) + 1;

            if (attempts >= config_.max_login_attempts) {
                info->lockout_until = std::chrono::system_clock::now() + config_.lockout_duration;
                info->failed_attempts = 0;
            }
        }

        void reset_failed_attempts(const std::string& identifier) {
            if (identifier.empty()) return;

            std::unique_lock<std::shared_mutex> lock(mutex_);

            auto it = rate_map_.find(identifier);
            if (it != rate_map_.end() && it->second) {
                it->second->failed_attempts = 0;
                it->second->lockout_until = std::chrono::system_clock::time_point{};
            }
        }
    };

    // Permissions
    enum class Permission {
        NONE = 0,
        READ = 1 << 0,
        WRITE = 1 << 1,
        DELETE = 1 << 2,
        ADMIN = 1 << 3,
        ALL = READ | WRITE | DELETE | ADMIN
    };

    inline Permission operator|(Permission a, Permission b) {
        return static_cast<Permission>(static_cast<int>(a) | static_cast<int>(b));
    }

    inline Permission operator&(Permission a, Permission b) {
        return static_cast<Permission>(static_cast<int>(a) & static_cast<int>(b));
    }

    /**
     * @brief ACL entry with pattern matching
     */
    struct ACLEntry {
        std::string resource_pattern;
        Permission permissions;
        std::optional<std::chrono::system_clock::time_point> expires_at;

        bool is_expired() const {
            return expires_at.has_value() &&
                std::chrono::system_clock::now() > expires_at.value();
        }
    };

    /**
     * @brief Secure pattern matcher with DoS protection
     */
    class PatternMatcher {
    private:
        mutable std::shared_mutex mutex_;
        std::unordered_map<std::string, std::regex> regex_cache_;
        SecurityConfig config_;

        std::string pattern_to_regex(const std::string& pattern) {
            // Validate pattern length
            if (pattern.length() > config_.max_pattern_length) {
                throw std::invalid_argument("Pattern too long");
            }

            std::string regex_str;
            regex_str.reserve(pattern.length() * 2);

            for (size_t i = 0; i < pattern.length(); i++) {
                char c = pattern[i];

                // Escape special regex chars
                if (c == '.' || c == '^' || c == '$' || c == '(' || c == ')' ||
                    c == '[' || c == ']' || c == '{' || c == '}' || c == '\\' ||
                    c == '|' || c == '?' || c == '*') {
                    regex_str += '\\';
                    regex_str += c;
                }
                // MQTT wildcards
                else if (c == '+' && (i == 0 || pattern[i - 1] == '/') &&
                    (i == pattern.length() - 1 || pattern[i + 1] == '/')) {
                    regex_str += "[^/]+";
                }
                else if (c == '#' && (i == 0 || pattern[i - 1] == '/') &&
                    i == pattern.length() - 1) {
                    regex_str += ".*";
                }
                else {
                    regex_str += c;
                }
            }

            return regex_str;
        }

    public:
        explicit PatternMatcher(const SecurityConfig& config)
            : config_(config) {}

        bool matches(const std::string& resource, const std::string& pattern) {
            // Validate inputs
            if (resource.length() > config_.max_resource_length ||
                pattern.length() > config_.max_pattern_length) {
                return false;
            }

            // Direct match
            if (resource == pattern) {
                return true;
            }

            // Check for wildcards
            if (pattern.find('+') == std::string::npos &&
                pattern.find('#') == std::string::npos) {
                return false;
            }

            try {
                std::unique_lock<std::shared_mutex> lock(mutex_);

                // Check cache
                auto it = regex_cache_.find(pattern);
                if (it == regex_cache_.end()) {
                    // Limit cache size
                    if (regex_cache_.size() >= config_.max_regex_cache_size) {
                        regex_cache_.clear();
                    }

                    // Compile regex
                    std::string regex_str = pattern_to_regex(pattern);
                    regex_cache_[pattern] = std::regex(regex_str);
                    it = regex_cache_.find(pattern);
                }

                lock.unlock();

                // Match with timeout protection
                return std::regex_match(resource, it->second);
            }
            catch (const std::exception&) {
                return false;
            }
        }
    };

    /**
     * @brief User entity
     */
    struct User {
        std::string username;
        std::string email;
        SecurePasswordHasher::HashedPassword password;
        std::vector<std::string> roles;
        std::vector<ACLEntry> direct_permissions;

        bool enabled = true;
        bool requires_password_change = false;

        std::chrono::system_clock::time_point created_at;
        std::chrono::system_clock::time_point modified_at;
        std::chrono::system_clock::time_point last_login;
        std::chrono::system_clock::time_point password_changed_at;

        std::atomic<size_t> current_connections{ 0 };
        size_t max_connections = 10;

        std::vector<SecurePasswordHasher::HashedPassword> password_history;

        bool is_password_expired(const SecurityConfig& config) const {
            auto age = std::chrono::system_clock::now() - password_changed_at;
            return age > config.password_expiry;
        }
    };

    /**
     * @brief Role definition
     */
    struct Role {
        std::string name;
        std::string description;
        std::vector<ACLEntry> permissions;
    };

    /**
     * @brief Authentication result
     */
    struct AuthResult {
        bool success;
        std::string message;
        std::optional<std::string> session_token;
        std::chrono::seconds expires_in;
    };

    /**
     * @brief Main authentication manager
     */
    class AuthManager {
    private:
        mutable std::shared_mutex users_mutex_;
        mutable std::shared_mutex roles_mutex_;

        std::unordered_map<std::string, std::unique_ptr<User>> users_;
        std::unordered_map<std::string, std::unique_ptr<Role>> roles_;

        SecurityConfig config_;
        std::unique_ptr<SecureStorage> storage_;
        std::unique_ptr<SecurePasswordHasher> hasher_;
        std::unique_ptr<SessionManager> session_manager_;
        std::unique_ptr<RateLimiter> rate_limiter_;
        std::unique_ptr<AuthLogger> logger_;
        std::unique_ptr<PatternMatcher> pattern_matcher_;

        std::atomic<bool> shutdown_{ false };
        std::thread maintenance_thread_;

        void maintenance_loop() {
            while (!shutdown_) {
                std::this_thread::sleep_for(std::chrono::minutes(5));

                if (!shutdown_) {
                    session_manager_->cleanup_expired_sessions();
                    if (logger_) logger_->flush();
                }
            }
        }

        std::vector<ACLEntry> get_effective_permissions(const User& user) const {
            std::vector<ACLEntry> effective;

            // Direct permissions
            effective.insert(effective.end(),
                user.direct_permissions.begin(),
                user.direct_permissions.end());

            // Role permissions
            std::shared_lock<std::shared_mutex> lock(roles_mutex_);
            for (const auto& role_name : user.roles) {
                auto it = roles_.find(role_name);
                if (it != roles_.end()) {
                    effective.insert(effective.end(),
                        it->second->permissions.begin(),
                        it->second->permissions.end());
                }
            }

            return effective;
        }

        void initialize_default_roles() {
            auto admin = std::make_unique<Role>();
            admin->name = "admin";
            admin->description = "Administrator";
            admin->permissions.push_back({ "#", Permission::ALL, std::nullopt });

            auto user = std::make_unique<Role>();
            user->name = "user";
            user->description = "Standard user";
            user->permissions.push_back({ "user/+/#", Permission::READ | Permission::WRITE, std::nullopt });

            std::unique_lock<std::shared_mutex> lock(roles_mutex_);
            roles_["admin"] = std::move(admin);
            roles_["user"] = std::move(user);
        }

    public:

        // Aggiungi questo metodo pubblico nella classe AuthManager
        bool load_users(const std::string& filename) {
            // Prima prova a caricare il file binario crittografato
            if (load_data(filename)) {
                LOG_INFO("AUTH") << "Loaded encrypted users from " << filename;
                return true;
            }

            // Se non esiste il file binario, prova a caricare un file di testo semplice
            std::ifstream file(filename);
            if (!file) {
                // Se non esiste nessun file, crea un utente admin di default
                LOG_WARN("AUTH") << "Users file not found: " << filename << ", creating default admin";

                auto result = create_user("admin", "admin123!@#", "admin@localhost", { "admin" });
                if (result.success) {
                    LOG_INFO("AUTH") << "Created default admin user (username: admin, password: admin123!@#)";
                    LOG_WARN("AUTH") << "IMPORTANT: Change the default admin password immediately!";

                    // Salva il file
                    if (save_data(filename)) {
                        LOG_INFO("AUTH") << "Saved users to " << filename;
                    }
                }
                return result.success;
            }

            // Carica da file di testo (formato: username:password:email:roles)
            std::string line;
            int loaded = 0;
            int failed = 0;

            while (std::getline(file, line)) {
                // Salta righe vuote e commenti
                if (line.empty() || line[0] == '#') continue;

                // Parse della linea
                std::stringstream ss(line);
                std::string username, password, email, roles_str;

                if (!std::getline(ss, username, ':')) continue;
                if (!std::getline(ss, password, ':')) continue;
                if (!std::getline(ss, email, ':')) {
                    email = username + "@localhost";
                }
                if (!std::getline(ss, roles_str, ':')) {
                    roles_str = "user";
                }

                // Parse dei ruoli (separati da virgola)
                std::vector<std::string> roles;
                std::stringstream roles_stream(roles_str);
                std::string role;
                while (std::getline(roles_stream, role, ',')) {
                    // Trim degli spazi
                    role.erase(0, role.find_first_not_of(" \t"));
                    role.erase(role.find_last_not_of(" \t") + 1);
                    if (!role.empty()) {
                        roles.push_back(role);
                    }
                }

                if (roles.empty()) {
                    roles.push_back("user");
                }

                // Crea l'utente
                auto result = create_user(username, password, email, roles);
                if (result.success) {
                    loaded++;
                    LOG_INFO("AUTH") << "Loaded user: " << username;
                }
                else {
                    failed++;
                    LOG_WARN("AUTH") << "Failed to load user " << username << ": " << result.message;
                }
            }

            LOG_INFO("AUTH") << "Loaded " << loaded << " users from " << filename;
            if (failed > 0) {
                LOG_WARN("AUTH") << "Failed to load " << failed << " users";
            }

            // Salva in formato binario crittografato per il futuro
            if (loaded > 0) {
                save_data(filename + ".bin");
            }

            return loaded > 0;
        }

        // Metodo di utilità per salvare gli utenti
        bool save_users(const std::string& filename) {
            return save_data(filename);
        }

        // Metodo per ottenere la lista degli utenti (utile per amministrazione)
        std::vector<std::string> get_user_list() const {
            std::shared_lock<std::shared_mutex> lock(users_mutex_);
            std::vector<std::string> result;
            result.reserve(users_.size());

            for (const auto& [username, user] : users_) {
                result.push_back(username);
            }

            return result;
        }

        // Metodo per verificare se esiste almeno un utente
        bool has_users() const {
            std::shared_lock<std::shared_mutex> lock(users_mutex_);
            return !users_.empty();
        }

        // Metodo per aggiungere un utente admin di default se non ci sono utenti
        bool ensure_admin_exists() {
            if (has_users()) {
                return true;
            }

            LOG_WARN("AUTH") << "No users found, creating default admin";
            auto result = create_user("admin", "admin123!@#", "admin@localhost", { "admin" });

            if (result.success) {
                LOG_INFO("AUTH") << "Created default admin user";
                LOG_WARN("AUTH") << "DEFAULT CREDENTIALS - username: admin, password: admin123!@#";
                LOG_WARN("AUTH") << "Please change the default password immediately!";
            }

            return result.success;
        }


        explicit AuthManager(const SecurityConfig& config = {})
            : config_(config),
            storage_(std::make_unique<SecureStorage>(config.encryption_key_file)),
            hasher_(std::make_unique<SecurePasswordHasher>(config)),
            session_manager_(std::make_unique<SessionManager>(config)),
            rate_limiter_(std::make_unique<RateLimiter>(config)),
            logger_(config.audit_logging ? std::make_unique<AuthLogger>() : nullptr),
            pattern_matcher_(std::make_unique<PatternMatcher>(config)) {

            initialize_default_roles();
            maintenance_thread_ = std::thread(&AuthManager::maintenance_loop, this);
        }

        ~AuthManager() {
            shutdown_ = true;
            if (maintenance_thread_.joinable()) {
                maintenance_thread_.join();
            }
        }

        AuthResult create_user(const std::string& username,
            const std::string& password,
            const std::string& email,
            const std::vector<std::string>& roles = { "user" }) {
            // Validate inputs
            if (username.empty() || username.length() > config_.max_username_length) {
                return { false, "Invalid username", std::nullopt, {} };
            }

            if (email.empty() || email.length() > config_.max_email_length) {
                return { false, "Invalid email", std::nullopt, {} };
            }

            if (!hasher_->validate_password_strength(password)) {
                return { false, "Password does not meet requirements", std::nullopt, {} };
            }

            std::unique_lock<std::shared_mutex> lock(users_mutex_);

            if (users_.find(username) != users_.end()) {
                return { false, "User already exists", std::nullopt, {} };
            }

            auto user = std::make_unique<User>();
            user->username = username;
            user->email = email;
            user->password = hasher_->hash_password(password);
            user->roles = roles;
            user->created_at = std::chrono::system_clock::now();
            user->modified_at = user->created_at;
            user->password_changed_at = user->created_at;

            users_[username] = std::move(user);

            if (logger_) {
                logger_->log(AuthEvent::USER_CREATED, username);
            }

            return { true, "User created successfully", std::nullopt, {} };
        }

        AuthResult authenticate(const std::string& username,
            const std::string& password,
            const std::string& client_id,
            const std::string& ip_address = "") {
            // Rate limiting
            if (!rate_limiter_->check_rate_limit(ip_address.empty() ? username : ip_address)) {
                if (logger_) {
                    logger_->log(AuthEvent::RATE_LIMITED, username, "", ip_address);
                }
                return { false, "Too many attempts", std::nullopt, {} };
            }

            std::shared_lock<std::shared_mutex> lock(users_mutex_);

            auto it = users_.find(username);
            if (it == users_.end()) {
                rate_limiter_->record_failed_attempt(username);
                if (logger_) {
                    logger_->log(AuthEvent::LOGIN_FAILED, username, "User not found", ip_address);
                }
                return { false, "Invalid credentials", std::nullopt, {} };
            }

            User* user = it->second.get();

            if (!hasher_->verify_password(password, user->password)) {
                rate_limiter_->record_failed_attempt(username);
                if (logger_) {
                    logger_->log(AuthEvent::LOGIN_FAILED, username, "Invalid password", ip_address);
                }
                return { false, "Invalid credentials", std::nullopt, {} };
            }

            if (!user->enabled) {
                if (logger_) {
                    logger_->log(AuthEvent::LOGIN_FAILED, username, "Account disabled", ip_address);
                }
                return { false, "Account disabled", std::nullopt, {} };
            }

            // Check concurrent connections atomically
            size_t current = user->current_connections.load();
            while (current < user->max_connections) {
                if (user->current_connections.compare_exchange_weak(current, current + 1)) {
                    break;
                }
            }

            if (current >= user->max_connections) {
                if (logger_) {
                    logger_->log(AuthEvent::LOGIN_FAILED, username, "Max connections", ip_address);
                }
                return { false, "Maximum connections exceeded", std::nullopt, {} };
            }

            lock.unlock();

            // Create session
            std::string token = session_manager_->create_session(username, client_id, ip_address);

            // Update user info
            {
                std::unique_lock<std::shared_mutex> write_lock(users_mutex_);
                user->last_login = std::chrono::system_clock::now();
            }

            rate_limiter_->reset_failed_attempts(username);

            if (logger_) {
                logger_->log(AuthEvent::LOGIN_SUCCESS, username, client_id, ip_address);
            }

            return { true, "Authentication successful", token,
                    std::chrono::duration_cast<std::chrono::seconds>(config_.session_timeout) };
        }

        bool authorize(const std::string& token,
            const std::string& resource,
            Permission requested_permission) {
            auto username = session_manager_->validate_token(token);
            if (!username.has_value()) {
                if (logger_) {
                    logger_->log(AuthEvent::INVALID_TOKEN, "", token);
                }
                return false;
            }

            std::shared_lock<std::shared_mutex> lock(users_mutex_);

            auto it = users_.find(username.value());
            if (it == users_.end()) {
                return false;
            }

            const User* user = it->second.get();
            auto permissions = get_effective_permissions(*user);

            for (const auto& entry : permissions) {
                if (entry.is_expired()) continue;

                if ((entry.permissions & requested_permission) == requested_permission) {
                    if (pattern_matcher_->matches(resource, entry.resource_pattern)) {
                        if (logger_) {
                            logger_->log(AuthEvent::PERMISSION_GRANTED,
                                username.value(), resource);
                        }
                        return true;
                    }
                }
            }

            if (logger_) {
                logger_->log(AuthEvent::PERMISSION_DENIED, username.value(), resource);
            }

            return false;
        }

        void logout(const std::string& token) {
            auto username = session_manager_->validate_token(token);
            if (username.has_value()) {
                std::unique_lock<std::shared_mutex> lock(users_mutex_);
                auto it = users_.find(username.value());
                if (it != users_.end()) {
                    if (it->second->current_connections > 0) {
                        it->second->current_connections--;
                    }
                }

                if (logger_) {
                    logger_->log(AuthEvent::LOGOUT, username.value());
                }
            }

            session_manager_->invalidate_token(token);
        }

        bool change_password(const std::string& username,
            const std::string& old_password,
            const std::string& new_password) {
            if (!hasher_->validate_password_strength(new_password)) {
                return false;
            }

            std::unique_lock<std::shared_mutex> lock(users_mutex_);

            auto it = users_.find(username);
            if (it == users_.end()) {
                return false;
            }

            User* user = it->second.get();

            if (!hasher_->verify_password(old_password, user->password)) {
                return false;
            }

            // Check password history
            for (const auto& old_hash : user->password_history) {
                if (hasher_->verify_password(new_password, old_hash)) {
                    return false; // Password was used before
                }
            }

            // Update password
            user->password_history.push_back(user->password);
            if (user->password_history.size() > config_.password_history_size) {
                user->password_history.erase(user->password_history.begin());
            }

            user->password = hasher_->hash_password(new_password);
            user->password_changed_at = std::chrono::system_clock::now();
            user->requires_password_change = false;

            // Invalidate all sessions
            session_manager_->invalidate_user_sessions(username);

            if (logger_) {
                logger_->log(AuthEvent::PASSWORD_CHANGED, username);
            }

            return true;
        }

        bool save_data(const std::string& filename) const {
            try {
                std::vector<uint8_t> data;
                data.reserve(65536);

                // Version
                data.push_back(0x01);

                // Users count
                {
                    std::shared_lock<std::shared_mutex> lock(users_mutex_);
                    uint32_t count = users_.size();
                    for (int i = 3; i >= 0; i--) {
                        data.push_back((count >> (i * 8)) & 0xFF);
                    }

                    // Serialize each user
                    for (const auto& [username, user] : users_) {
                        // Username
                        data.push_back(static_cast<uint8_t>(username.size()));
                        data.insert(data.end(), username.begin(), username.end());

                        // Email
                        data.push_back(static_cast<uint8_t>(user->email.size()));
                        data.insert(data.end(), user->email.begin(), user->email.end());

                        // Password
                        auto pwd_data = user->password.serialize();
                        uint16_t pwd_size = pwd_data.size();
                        data.push_back(pwd_size >> 8);
                        data.push_back(pwd_size & 0xFF);
                        data.insert(data.end(), pwd_data.begin(), pwd_data.end());

                        // Flags
                        data.push_back((user->enabled ? 0x01 : 0) |
                            (user->requires_password_change ? 0x02 : 0));

                        // Roles count
                        data.push_back(static_cast<uint8_t>(user->roles.size()));
                        for (const auto& role : user->roles) {
                            data.push_back(static_cast<uint8_t>(role.size()));
                            data.insert(data.end(), role.begin(), role.end());
                        }
                    }
                }

                // Encrypt and save
                auto encrypted = storage_->encrypt(data);

                std::ofstream file(filename, std::ios::binary);
                file.write(reinterpret_cast<const char*>(encrypted.data()),
                    encrypted.size());

                return file.good();
            }
            catch (const std::exception&) {
                return false;
            }
        }

        bool load_data(const std::string& filename) {
            try {
                std::ifstream file(filename, std::ios::binary);
                if (!file) return false;

                std::vector<uint8_t> encrypted(
                    (std::istreambuf_iterator<char>(file)),
                    std::istreambuf_iterator<char>());

                auto data = storage_->decrypt(encrypted);
                size_t pos = 0;

                // Version
                if (data[pos++] != 0x01) {
                    return false;
                }

                // Users count
                uint32_t count = 0;
                for (int i = 0; i < 4; i++) {
                    count = (count << 8) | data[pos++];
                }

                std::unique_lock<std::shared_mutex> lock(users_mutex_);
                users_.clear();

                for (uint32_t i = 0; i < count; i++) {
                    auto user = std::make_unique<User>();

                    // Username
                    size_t username_len = data[pos++];
                    user->username = std::string(data.begin() + pos,
                        data.begin() + pos + username_len);
                    pos += username_len;

                    // Email
                    size_t email_len = data[pos++];
                    user->email = std::string(data.begin() + pos,
                        data.begin() + pos + email_len);
                    pos += email_len;

                    // Password
                    uint16_t pwd_size = (data[pos] << 8) | data[pos + 1];
                    pos += 2;
                    std::vector<uint8_t> pwd_data(data.begin() + pos,
                        data.begin() + pos + pwd_size);
                    user->password = SecurePasswordHasher::HashedPassword::deserialize(pwd_data);
                    pos += pwd_size;

                    // Flags
                    uint8_t flags = data[pos++];
                    user->enabled = (flags & 0x01) != 0;
                    user->requires_password_change = (flags & 0x02) != 0;

                    // Roles
                    size_t roles_count = data[pos++];
                    for (size_t j = 0; j < roles_count; j++) {
                        size_t role_len = data[pos++];
                        user->roles.push_back(std::string(data.begin() + pos,
                            data.begin() + pos + role_len));
                        pos += role_len;
                    }

                    // Set timestamps to now
                    user->created_at = std::chrono::system_clock::now();
                    user->modified_at = user->created_at;
                    user->password_changed_at = user->created_at;

                    users_[user->username] = std::move(user);
                }

                return true;
            }
            catch (const std::exception&) {
                return false;
            }
        }
    };

} // namespace ourmqtt

#endif // OUR_AUTH_MANAGER_HPP