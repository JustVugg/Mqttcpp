// persistence.hpp - Enterprise-grade persistence system with IMMEDIATE SHUTDOWN
#ifndef OUR_PERSISTENCE_HPP
#define OUR_PERSISTENCE_HPP

#include <string>
#include <vector>
#include <memory>
#include <queue>
#include <unordered_map>
#include <fstream>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <cstdint>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <shared_mutex>
#include <future>

// C++17 filesystem with fallback
#if __cplusplus >= 201703L && __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#define HAS_FILESYSTEM 1
#else
#include <sys/stat.h>
#include "dirent.h"
#define HAS_FILESYSTEM 0
#endif

#include "time_utils.hpp"
#include "logger.hpp"

namespace ourmqtt {

    struct PersistenceConfig {
        size_t max_message_size = 256 * 1024;
        size_t max_client_id_length = 256;
        size_t max_topic_length = 65535;
        size_t max_messages_per_client = 10000;
        size_t max_retained_messages = 100000;
        uint32_t flush_interval_seconds = 5;
        uint32_t cleanup_interval_hours = 24;
        uint32_t message_ttl_hours = 168;
        bool enable_compression = false;
        bool enable_encryption = false;
    };

    class CRC32 {
    private:
        static uint32_t crc_table[256];
        static bool table_initialized;

        static void init_table() {
            for (uint32_t i = 0; i < 256; i++) {
                uint32_t c = i;
                for (int j = 0; j < 8; j++) {
                    c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
                }
                crc_table[i] = c;
            }
            table_initialized = true;
        }

    public:
        static uint32_t calculate(const uint8_t* data, size_t len) {
            if (!table_initialized) {
                init_table();
            }

            uint32_t crc = 0xFFFFFFFF;
            for (size_t i = 0; i < len; i++) {
                crc = crc_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
            }
            return crc ^ 0xFFFFFFFF;
        }
    };

    inline uint32_t CRC32::crc_table[256];
    inline bool CRC32::table_initialized = false;

    struct PersistentMessage {
        uint64_t id = 0;
        std::string client_id;
        std::string topic;
        std::vector<uint8_t> payload;
        uint8_t qos = 0;
        bool retain = false;
        uint64_t timestamp = 0;
        uint16_t packet_id = 0;
        int retry_count = 0;

        std::vector<uint8_t> serialize() const {
            std::vector<uint8_t> data;
            data.reserve(1024);

            data.push_back(0x01);

            for (int i = 7; i >= 0; i--) {
                data.push_back((id >> (i * 8)) & 0xFF);
            }

            uint16_t client_len = static_cast<uint16_t>(client_id.length());
            data.push_back(client_len >> 8);
            data.push_back(client_len & 0xFF);
            data.insert(data.end(), client_id.begin(), client_id.end());

            uint16_t topic_len = static_cast<uint16_t>(topic.length());
            data.push_back(topic_len >> 8);
            data.push_back(topic_len & 0xFF);
            data.insert(data.end(), topic.begin(), topic.end());

            uint32_t payload_len = static_cast<uint32_t>(payload.size());
            data.push_back(payload_len >> 24);
            data.push_back(payload_len >> 16);
            data.push_back(payload_len >> 8);
            data.push_back(payload_len & 0xFF);
            data.insert(data.end(), payload.begin(), payload.end());

            data.push_back(qos);
            data.push_back(retain ? 1 : 0);

            for (int i = 7; i >= 0; i--) {
                data.push_back((timestamp >> (i * 8)) & 0xFF);
            }

            data.push_back(packet_id >> 8);
            data.push_back(packet_id & 0xFF);
            data.push_back(static_cast<uint8_t>(retry_count));

            uint32_t crc = CRC32::calculate(data.data(), data.size());
            data.push_back(crc >> 24);
            data.push_back(crc >> 16);
            data.push_back(crc >> 8);
            data.push_back(crc & 0xFF);

            return data;
        }

        static std::unique_ptr<PersistentMessage> deserialize(const uint8_t* data, size_t len,
            const PersistenceConfig& config) {

            if (len < 34) {
                LOG_WARN("PERSISTENCE") << "Message too short: " << len << " bytes";
                return nullptr;
            }

            uint32_t stored_crc = (data[len - 4] << 24) | (data[len - 3] << 16) |
                (data[len - 2] << 8) | data[len - 1];
            uint32_t calculated_crc = CRC32::calculate(data, len - 4);

            if (stored_crc != calculated_crc) {
                LOG_ERROR("PERSISTENCE") << "CRC mismatch";
                return nullptr;
            }

            auto msg = std::make_unique<PersistentMessage>();
            size_t pos = 0;

            uint8_t version = data[pos++];
            if (version != 0x01) {
                LOG_WARN("PERSISTENCE") << "Unknown message version: " << static_cast<int>(version);
                return nullptr;
            }

            msg->id = 0;
            for (int i = 0; i < 8; i++) {
                msg->id = (msg->id << 8) | data[pos++];
            }

            uint16_t client_len = (data[pos] << 8) | data[pos + 1];
            pos += 2;

            if (client_len > config.max_client_id_length) {
                LOG_WARN("PERSISTENCE") << "Client ID too long: " << client_len;
                return nullptr;
            }

            if (pos + client_len > len - 4) {
                LOG_WARN("PERSISTENCE") << "Invalid client ID length";
                return nullptr;
            }

            msg->client_id = std::string(reinterpret_cast<const char*>(&data[pos]), client_len);
            pos += client_len;

            if (pos + 2 > len - 4) return nullptr;
            uint16_t topic_len = (data[pos] << 8) | data[pos + 1];
            pos += 2;

            if (topic_len > config.max_topic_length) {
                LOG_WARN("PERSISTENCE") << "Topic too long: " << topic_len;
                return nullptr;
            }

            if (pos + topic_len > len - 4) {
                LOG_WARN("PERSISTENCE") << "Invalid topic length";
                return nullptr;
            }

            msg->topic = std::string(reinterpret_cast<const char*>(&data[pos]), topic_len);
            pos += topic_len;

            if (pos + 4 > len - 4) return nullptr;
            uint32_t payload_len = (data[pos] << 24) | (data[pos + 1] << 16) |
                (data[pos + 2] << 8) | data[pos + 3];
            pos += 4;

            if (payload_len > config.max_message_size) {
                LOG_WARN("PERSISTENCE") << "Payload too large: " << payload_len;
                return nullptr;
            }

            if (pos + payload_len > len - 4) {
                LOG_WARN("PERSISTENCE") << "Invalid payload length";
                return nullptr;
            }

            msg->payload.assign(&data[pos], &data[pos + payload_len]);
            pos += payload_len;

            if (pos + 13 > len - 4) {
                LOG_WARN("PERSISTENCE") << "Invalid metadata position";
                return nullptr;
            }

            msg->qos = data[pos++];
            if (msg->qos > 2) {
                LOG_WARN("PERSISTENCE") << "Invalid QoS: " << static_cast<int>(msg->qos);
                return nullptr;
            }

            msg->retain = data[pos++] != 0;

            msg->timestamp = 0;
            for (int i = 0; i < 8; i++) {
                msg->timestamp = (msg->timestamp << 8) | data[pos++];
            }

            msg->packet_id = (data[pos] << 8) | data[pos + 1];
            pos += 2;
            msg->retry_count = data[pos++];

            return msg;
        }
    };

    class PersistenceStore {
    public:
        virtual ~PersistenceStore() = default;

        virtual bool store_message(const PersistentMessage& msg) = 0;
        virtual bool remove_message(uint64_t id) = 0;
        virtual std::vector<PersistentMessage> get_messages_for_client(const std::string& client_id) = 0;
        virtual std::vector<PersistentMessage> get_retained_messages() = 0;
        virtual bool store_session(const std::string& client_id, const std::vector<uint8_t>& data) = 0;
        virtual std::vector<uint8_t> get_session(const std::string& client_id) = 0;
        virtual bool remove_session(const std::string& client_id) = 0;
        virtual void cleanup_old_messages(uint64_t older_than_timestamp) = 0;
        virtual void flush() = 0;
        virtual void shutdown() = 0;
        virtual size_t get_message_count() const = 0;
        virtual size_t get_storage_size() const = 0;
    };

    class FilePersistence : public PersistenceStore {
    private:
        std::string base_path_;
        PersistenceConfig config_;
        std::atomic<uint64_t> next_id_;
        mutable std::shared_mutex mutex_;
        std::condition_variable_any flush_cv_;

        std::unordered_map<uint64_t, PersistentMessage> message_cache_;
        std::unordered_map<std::string, std::vector<uint64_t>> client_messages_;
        std::vector<uint64_t> retained_messages_;

        std::thread flush_thread_;
        std::thread cleanup_thread_;
        std::atomic<bool> running_{ true };
        std::atomic<bool> shutting_down_{ false };

        std::atomic<size_t> total_storage_size_{ 0 };

        std::string get_message_filename(uint64_t id) const {
            std::stringstream ss;
            ss << base_path_ << "/messages/" << std::setfill('0')
                << std::setw(16) << id << ".msg";
            return ss.str();
        }

        std::string get_session_filename(const std::string& client_id) const {
            std::string safe_id;
            safe_id.reserve(client_id.length());

            for (char c : client_id) {
                if (std::isalnum(c) || c == '-' || c == '_' || c == '.') {
                    safe_id += c;
                }
                else {
                    safe_id += '_';
                }
            }

            if (safe_id.length() > 200) {
                std::hash<std::string> hasher;
                size_t hash = hasher(client_id);
                std::stringstream ss;
                ss << safe_id.substr(0, 180) << "_" << std::hex << hash;
                safe_id = ss.str();
            }

            return base_path_ + "/sessions/" + safe_id + ".session";
        }

        bool ensure_directories() {
            try {
#if HAS_FILESYSTEM
                fs::create_directories(base_path_ + "/messages");
                fs::create_directories(base_path_ + "/sessions");
                fs::create_directories(base_path_ + "/retained");
                return true;
#else
                auto create_dir = [](const std::string& path) {
#ifdef _WIN32
                    return CreateDirectoryA(path.c_str(), NULL) || GetLastError() == ERROR_ALREADY_EXISTS;
#else
                    return mkdir(path.c_str(), 0755) == 0 || errno == EEXIST;
#endif
                };

                return create_dir(base_path_) &&
                    create_dir(base_path_ + "/messages") &&
                    create_dir(base_path_ + "/sessions") &&
                    create_dir(base_path_ + "/retained");
#endif
            }
            catch (const std::exception& e) {
                LOG_ERROR("PERSISTENCE") << "Failed to create directories: " << e.what();
                return false;
            }
        }

        void load_existing_messages() {
            try {
#if HAS_FILESYSTEM
                std::string messages_path = base_path_ + "/messages";
                if (fs::exists(messages_path)) {
                    for (const auto& entry : fs::directory_iterator(messages_path)) {
                        if (entry.path().extension() == ".msg") {
                            load_message_file(entry.path().string());
                        }
                    }
                }
#else
                DIR* dir = opendir((base_path_ + "/messages").c_str());
                if (dir) {
                    struct dirent* entry;
                    while ((entry = readdir(dir)) != nullptr) {
                        std::string filename = entry->d_name;
                        if (filename.size() > 4 &&
                            filename.substr(filename.size() - 4) == ".msg") {
                            load_message_file(base_path_ + "/messages/" + filename);
                        }
                    }
                    closedir(dir);
                }
#endif
                LOG_INFO("PERSISTENCE") << "Loaded " << message_cache_.size()
                    << " messages from disk";
            }
            catch (const std::exception& e) {
                LOG_ERROR("PERSISTENCE") << "Error loading messages: " << e.what();
            }
        }

        void load_message_file(const std::string& filepath) {
            std::ifstream file(filepath, std::ios::binary);
            if (!file) {
                LOG_WARN("PERSISTENCE") << "Cannot open file: " << filepath;
                return;
            }

            file.seekg(0, std::ios::end);
            size_t size = file.tellg();
            file.seekg(0, std::ios::beg);

            if (size > config_.max_message_size + 1024) {
                LOG_WARN("PERSISTENCE") << "Message file too large: " << filepath;
                return;
            }

            std::vector<uint8_t> data(size);
            file.read(reinterpret_cast<char*>(data.data()), size);

            auto msg = PersistentMessage::deserialize(data.data(), data.size(), config_);
            if (msg) {
                message_cache_[msg->id] = *msg;
                client_messages_[msg->client_id].push_back(msg->id);

                if (msg->retain) {
                    retained_messages_.push_back(msg->id);
                }

                if (msg->id >= next_id_) {
                    next_id_ = msg->id + 1;
                }

                total_storage_size_ += size;
            }
            else {
                LOG_WARN("PERSISTENCE") << "Failed to deserialize message: " << filepath;
            }
        }

        void flush_worker() {
            LOG_INFO("PERSISTENCE") << "Flush worker started";

            while (running_.load()) {
                if (shutting_down_.load() || !running_.load()) {
                    break;
                }

                {
                    std::unique_lock<std::shared_mutex> lock(mutex_);

                    for (int i = 0; i < 50 && running_.load() && !shutting_down_.load(); i++) {
                        flush_cv_.wait_for(lock, std::chrono::milliseconds(100));

                        if (shutting_down_.load() || !running_.load()) {
                            break;
                        }
                    }

                    if (shutting_down_.load() || !running_.load()) {
                        break;
                    }
                }

                if (!shutting_down_.load() && running_.load()) {
                    try {
                        flush();
                    }
                    catch (...) {
                    }
                }
            }

            LOG_INFO("PERSISTENCE") << "Flush worker stopped";
        }

        void cleanup_worker() {
            LOG_INFO("PERSISTENCE") << "Cleanup worker started";

            int counter = 0;

            while (running_.load()) {
                if (shutting_down_.load() || !running_.load()) {
                    break;
                }

                for (int i = 0; i < 10; i++) {
                    if (shutting_down_.load() || !running_.load()) {
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }

                if (shutting_down_.load() || !running_.load()) {
                    break;
                }

                counter++;

                if (counter >= 3600 && !shutting_down_.load()) {
                    counter = 0;
                    try {
                        uint64_t cutoff = Time::now_ms() - (config_.message_ttl_hours * 3600000);
                        cleanup_old_messages(cutoff);
                    }
                    catch (...) {
                    }
                }
            }

            LOG_INFO("PERSISTENCE") << "Cleanup worker stopped";
        }

    public:
        explicit FilePersistence(const std::string& path,
            const PersistenceConfig& config = PersistenceConfig())
            : base_path_(path),
            config_(config),
            next_id_(1) {

            if (!ensure_directories()) {
                throw std::runtime_error("Failed to create persistence directories");
            }

            load_existing_messages();

            flush_thread_ = std::thread(&FilePersistence::flush_worker, this);
            cleanup_thread_ = std::thread(&FilePersistence::cleanup_worker, this);
        }

        ~FilePersistence() {
            shutdown();
        }

        void shutdown() override {
            bool expected = false;
            if (!shutting_down_.compare_exchange_strong(expected, true)) {
                return;
            }

            LOG_INFO("PERSISTENCE") << "IMMEDIATE shutdown initiated";

            running_ = false;

            for (int i = 0; i < 10; i++) {
                flush_cv_.notify_all();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }

            if (flush_thread_.joinable()) {
                auto future = std::async(std::launch::async, [this]() {
                    if (flush_thread_.joinable()) {
                        flush_thread_.join();
                    }
                    });

                if (future.wait_for(std::chrono::milliseconds(50)) != std::future_status::ready) {
                    LOG_WARN("PERSISTENCE") << "Flush thread timeout, detaching";
                    flush_thread_.detach();
                }
            }

            if (cleanup_thread_.joinable()) {
                auto future = std::async(std::launch::async, [this]() {
                    if (cleanup_thread_.joinable()) {
                        cleanup_thread_.join();
                    }
                    });

                if (future.wait_for(std::chrono::milliseconds(50)) != std::future_status::ready) {
                    LOG_WARN("PERSISTENCE") << "Cleanup thread timeout, detaching";
                    cleanup_thread_.detach();
                }
            }

            LOG_INFO("PERSISTENCE") << "Shutdown complete";
        }

        bool store_message(const PersistentMessage& msg) override {
            if (shutting_down_.load()) return false;

            if (msg.payload.size() > config_.max_message_size) {
                LOG_WARN("PERSISTENCE") << "Message too large: " << msg.payload.size();
                return false;
            }

            std::unique_lock<std::shared_mutex> lock(mutex_);

            if (client_messages_[msg.client_id].size() >= config_.max_messages_per_client) {
                LOG_WARN("PERSISTENCE") << "Client " << msg.client_id
                    << " has too many messages";
                return false;
            }

            PersistentMessage stored_msg = msg;
            stored_msg.id = next_id_++;
            stored_msg.timestamp = Time::now_ms();

            auto data = stored_msg.serialize();
            std::string filename = get_message_filename(stored_msg.id);

            std::ofstream file(filename, std::ios::binary);
            if (!file) {
                LOG_ERROR("PERSISTENCE") << "Cannot create file: " << filename;
                return false;
            }

            file.write(reinterpret_cast<const char*>(data.data()), data.size());
            if (!file.good()) {
                LOG_ERROR("PERSISTENCE") << "Failed to write file: " << filename;
                return false;
            }

            message_cache_[stored_msg.id] = stored_msg;
            client_messages_[stored_msg.client_id].push_back(stored_msg.id);

            if (stored_msg.retain) {
                retained_messages_.push_back(stored_msg.id);
            }

            total_storage_size_ += data.size();

            LOG_DEBUG("PERSISTENCE") << "Stored message " << stored_msg.id
                << " for client " << stored_msg.client_id;

            return true;
        }

        bool remove_message(uint64_t id) override {
            if (shutting_down_.load()) return false;

            std::unique_lock<std::shared_mutex> lock(mutex_);

            auto it = message_cache_.find(id);
            if (it == message_cache_.end()) {
                return false;
            }

            auto& client_msgs = client_messages_[it->second.client_id];
            client_msgs.erase(
                std::remove(client_msgs.begin(), client_msgs.end(), id),
                client_msgs.end()
            );

            if (it->second.retain) {
                retained_messages_.erase(
                    std::remove(retained_messages_.begin(), retained_messages_.end(), id),
                    retained_messages_.end()
                );
            }

            message_cache_.erase(it);

            std::string filename = get_message_filename(id);
            try {
#if HAS_FILESYSTEM
                if (fs::exists(filename)) {
                    total_storage_size_ -= fs::file_size(filename);
                    fs::remove(filename);
                }
#else
                std::remove(filename.c_str());
#endif
            }
            catch (const std::exception& e) {
                LOG_WARN("PERSISTENCE") << "Failed to remove file: " << e.what();
            }

            LOG_DEBUG("PERSISTENCE") << "Removed message " << id;
            return true;
        }

        std::vector<PersistentMessage> get_messages_for_client(const std::string& client_id) override {
            if (shutting_down_.load()) return {};

            std::shared_lock<std::shared_mutex> lock(mutex_);

            std::vector<PersistentMessage> result;

            auto it = client_messages_.find(client_id);
            if (it != client_messages_.end()) {
                result.reserve(it->second.size());

                for (uint64_t id : it->second) {
                    auto msg_it = message_cache_.find(id);
                    if (msg_it != message_cache_.end()) {
                        result.push_back(msg_it->second);
                    }
                }
            }

            return result;
        }

        std::vector<PersistentMessage> get_retained_messages() override {
            if (shutting_down_.load()) return {};

            std::shared_lock<std::shared_mutex> lock(mutex_);

            std::vector<PersistentMessage> result;
            result.reserve(retained_messages_.size());

            for (uint64_t id : retained_messages_) {
                auto it = message_cache_.find(id);
                if (it != message_cache_.end()) {
                    result.push_back(it->second);
                }
            }

            return result;
        }

        bool store_session(const std::string& client_id, const std::vector<uint8_t>& data) override {
            if (shutting_down_.load()) return false;

            if (data.size() > config_.max_message_size) {
                LOG_WARN("PERSISTENCE") << "Session data too large for " << client_id;
                return false;
            }

            std::string filename = get_session_filename(client_id);
            std::ofstream file(filename, std::ios::binary);

            if (!file) {
                LOG_ERROR("PERSISTENCE") << "Cannot create session file: " << filename;
                return false;
            }

            uint32_t crc = CRC32::calculate(data.data(), data.size());

            file.write(reinterpret_cast<const char*>(data.data()), data.size());
            file.write(reinterpret_cast<const char*>(&crc), sizeof(crc));

            if (!file.good()) {
                LOG_ERROR("PERSISTENCE") << "Failed to write session file: " << filename;
                return false;
            }

            LOG_DEBUG("PERSISTENCE") << "Stored session for " << client_id;
            return true;
        }

        std::vector<uint8_t> get_session(const std::string& client_id) override {
            if (shutting_down_.load()) return {};

            std::string filename = get_session_filename(client_id);
            std::ifstream file(filename, std::ios::binary);

            if (!file) {
                return {};
            }

            file.seekg(0, std::ios::end);
            size_t size = file.tellg();

            if (size < sizeof(uint32_t)) {
                LOG_WARN("PERSISTENCE") << "Session file too small: " << filename;
                return {};
            }

            size_t data_size = size - sizeof(uint32_t);
            file.seekg(0, std::ios::beg);

            std::vector<uint8_t> data(data_size);
            uint32_t stored_crc;

            file.read(reinterpret_cast<char*>(data.data()), data_size);
            file.read(reinterpret_cast<char*>(&stored_crc), sizeof(stored_crc));

            uint32_t calculated_crc = CRC32::calculate(data.data(), data.size());
            if (stored_crc != calculated_crc) {
                LOG_ERROR("PERSISTENCE") << "Session CRC mismatch for " << client_id;
                return {};
            }

            LOG_DEBUG("PERSISTENCE") << "Retrieved session for " << client_id;
            return data;
        }

        bool remove_session(const std::string& client_id) override {
            if (shutting_down_.load()) return false;

            try {
                std::string filename = get_session_filename(client_id);
#if HAS_FILESYSTEM
                return fs::remove(filename);
#else
                return std::remove(filename.c_str()) == 0;
#endif
            }
            catch (const std::exception& e) {
                LOG_WARN("PERSISTENCE") << "Failed to remove session: " << e.what();
                return false;
            }
        }

        void cleanup_old_messages(uint64_t older_than_timestamp) override {
            if (shutting_down_.load()) return;

            std::unique_lock<std::shared_mutex> lock(mutex_);

            std::vector<uint64_t> to_remove;

            for (const auto& [id, msg] : message_cache_) {
                if (msg.timestamp < older_than_timestamp && !msg.retain) {
                    to_remove.push_back(id);
                }
            }

            lock.unlock();

            for (uint64_t id : to_remove) {
                remove_message(id);
            }

            if (!to_remove.empty()) {
                LOG_INFO("PERSISTENCE") << "Cleaned up " << to_remove.size() << " old messages";
            }
        }

        void flush() override {
            if (!shutting_down_.load()) {
                // Nothing to flush for file-based storage
            }
        }

        size_t get_message_count() const override {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            return message_cache_.size();
        }

        size_t get_storage_size() const override {
            return total_storage_size_.load();
        }
    };

} // namespace ourmqtt

#endif // OUR_PERSISTENCE_HPP