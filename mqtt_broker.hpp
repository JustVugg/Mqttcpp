// mqtt_broker.hpp
#ifndef OUR_MQTT_BROKER_HPP
#define OUR_MQTT_BROKER_HPP

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#endif

#include "socket.hpp"
#include "tls_socket.hpp"
#include "buffer.hpp"
#include "mqtt_packet.hpp"
#include "thread_utils.hpp"
#include "time_utils.hpp"
#include "auth_manager.hpp"
#include "persistence.hpp"
#include "logger.hpp"

#include <memory>
#include <vector>
#include <map>
#include <set>
#include <queue>
#include <iostream>
#include <algorithm>
#include <atomic>
#include <regex>
#include <chrono>
#include <shared_mutex>
#include <variant>
#include <optional>
#include <functional>
#include <sstream>
#include <thread>
#include <iomanip>
#include <cstring>
#include <condition_variable>
#include <mutex>
#include <future>

namespace ourmqtt {

    class MetricsCollector;
    class MessageRouter;
    class SessionStore;

    struct BrokerConfig {
        uint16_t port = 1883;
        uint16_t tls_port = 8883;
        uint16_t ws_port = 8080;
        uint16_t wss_port = 8443;
        bool enable_tls = false;
        bool enable_websocket = false;
        TLSConfig tls_config;

        bool require_auth = false;
        std::string users_file = "mqtt_users.db";
        SecurityConfig security_config;

        bool enable_persistence = true;
        std::string persistence_path = "./mqtt_data";
        size_t persistence_flush_interval = 1000;

        size_t max_clients = 10000;
        size_t max_message_size = 256 * 1024;
        size_t max_topic_length = 65535;
        size_t max_client_id_length = 65535;
        size_t max_queued_messages = 1000;
        size_t max_inflight_messages = 20;

        uint32_t session_expiry_interval = 3600;
        bool allow_zero_length_client_id = true;

        size_t worker_threads = 4;
        size_t io_threads = 2;
        size_t max_connections_per_ip = 100;
        size_t receive_maximum = 65535;

        bool enable_sys_topics = true;
        bool enable_shared_subscriptions = true;
        bool enable_retained_messages = true;
        bool enable_bridge_mode = false;

        size_t max_publish_rate = 100;
        size_t max_subscribe_rate = 10;
        size_t max_bandwidth_per_client = 1024 * 1024;

        bool enable_clustering = false;
        std::vector<std::string> cluster_nodes;
        std::string node_id;

        bool enable_metrics = true;
        uint16_t metrics_port = 9090;
        std::string metrics_path = "/metrics";
    };

    struct WillMessage {
        std::string topic;
        std::vector<uint8_t> payload;
        QoS qos = QOS_0;
        bool retain = false;
        uint32_t delay_interval = 0;

        bool is_valid() const {
            return !topic.empty();
        }
    };

    struct SessionState {
        std::string client_id;
        std::set<std::string> subscriptions;
        std::map<std::string, QoS> subscription_qos;
        std::queue<std::pair<uint16_t, std::shared_ptr<PublishPacket>>> queued_messages;
        std::map<uint16_t, std::shared_ptr<PublishPacket>> inflight_messages;
        std::set<uint16_t> pending_pubrec;
        std::set<uint16_t> pending_pubrel;
        std::map<uint16_t, std::chrono::steady_clock::time_point> message_timestamps;
        std::chrono::system_clock::time_point created_at;
        std::chrono::system_clock::time_point expires_at;
        bool persistent = false;
    };

    class ClientSession {
    private:
        mutable std::shared_mutex mutex_;
        std::atomic<uint16_t> next_packet_id_{ 1 };
        std::atomic<bool> stopping_{ false };

    public:
        std::unique_ptr<Socket> socket;
        std::string client_id;
        std::string username;
        std::string ip_address;
        std::atomic<bool> connected{ false };
        std::atomic<bool> authenticated{ false };

        Timer last_activity;
        Timer connection_time;

        uint16_t keep_alive = 60;
        bool clean_start = true;
        uint32_t session_expiry = 0;

        SessionState state;
        WillMessage will_message;

        std::atomic<size_t> inflight_count{ 0 };
        std::atomic<size_t> queued_count{ 0 };

        std::atomic<size_t> publish_count{ 0 };
        std::atomic<size_t> bytes_sent{ 0 };
        std::atomic<size_t> bytes_received{ 0 };
        std::chrono::steady_clock::time_point rate_reset_time;

        std::atomic<uint64_t> messages_published{ 0 };
        std::atomic<uint64_t> messages_received{ 0 };

        ClientSession() : rate_reset_time(std::chrono::steady_clock::now()) {}

        ~ClientSession() {
            stopping_ = true;
            connected = false;

            if (socket) {
                socket->close();
            }
        }

        bool is_stopping() const {
            return stopping_.load();
        }

        uint16_t get_next_packet_id() {
            uint16_t id = next_packet_id_.fetch_add(1);
            if (id == 0) {
                id = next_packet_id_.fetch_add(1);
            }
            return id;
        }

        bool check_rate_limit(size_t max_rate) {
            auto now = std::chrono::steady_clock::now();
            if (now - rate_reset_time >= std::chrono::seconds(1)) {
                publish_count = 0;
                rate_reset_time = now;
            }

            if (publish_count >= max_rate) {
                return false;
            }

            publish_count++;
            return true;
        }
    };

    class MessageRouter {
    private:
        struct TopicNode {
            std::set<std::string> subscribers;
            std::map<std::string, std::unique_ptr<TopicNode>> children;
            bool has_wildcard = false;
        };

        mutable std::shared_mutex mutex_;
        std::unique_ptr<TopicNode> root_;
        std::map<std::string, std::vector<uint8_t>> retained_messages_;

    public:
        MessageRouter() : root_(std::make_unique<TopicNode>()) {}

        void add_subscription(const std::string& client_id, const std::string& topic) {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            auto parts = split_topic(topic);
            TopicNode* node = root_.get();

            for (const auto& part : parts) {
                if (part == "+" || part == "#") {
                    node->has_wildcard = true;
                }

                if (node->children.find(part) == node->children.end()) {
                    node->children[part] = std::make_unique<TopicNode>();
                }
                node = node->children[part].get();
            }

            node->subscribers.insert(client_id);
        }

        void remove_subscription(const std::string& client_id, const std::string& topic) {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            auto parts = split_topic(topic);
            TopicNode* node = root_.get();

            for (const auto& part : parts) {
                auto it = node->children.find(part);
                if (it == node->children.end()) {
                    return;
                }
                node = it->second.get();
            }

            node->subscribers.erase(client_id);
        }

        void remove_all_subscriptions(const std::string& client_id) {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            remove_client_recursive(root_.get(), client_id);
        }

        std::set<std::string> get_subscribers(const std::string& topic) const {
            std::shared_lock<std::shared_mutex> lock(mutex_);

            std::set<std::string> result;
            auto parts = split_topic(topic);
            find_subscribers_recursive(root_.get(), parts, 0, result);
            return result;
        }

        void set_retained(const std::string& topic, const std::vector<uint8_t>& payload) {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            if (payload.empty()) {
                retained_messages_.erase(topic);
            }
            else {
                retained_messages_[topic] = payload;
            }
        }

        std::vector<std::pair<std::string, std::vector<uint8_t>>> get_retained_for_pattern(
            const std::string& pattern) const {

            std::shared_lock<std::shared_mutex> lock(mutex_);
            std::vector<std::pair<std::string, std::vector<uint8_t>>> result;

            for (const auto& [topic, payload] : retained_messages_) {
                if (topic_matches(topic, pattern)) {
                    result.push_back({ topic, payload });
                }
            }

            return result;
        }

    private:
        std::vector<std::string> split_topic(const std::string& topic) const {
            std::vector<std::string> parts;
            std::stringstream ss(topic);
            std::string part;

            while (std::getline(ss, part, '/')) {
                parts.push_back(part);
            }

            return parts;
        }

        void find_subscribers_recursive(const TopicNode* node,
            const std::vector<std::string>& parts,
            size_t index,
            std::set<std::string>& result) const {
            if (index == parts.size()) {
                result.insert(node->subscribers.begin(), node->subscribers.end());

                auto it = node->children.find("#");
                if (it != node->children.end()) {
                    result.insert(it->second->subscribers.begin(),
                        it->second->subscribers.end());
                }
                return;
            }

            const std::string& part = parts[index];

            auto it = node->children.find(part);
            if (it != node->children.end()) {
                find_subscribers_recursive(it->second.get(), parts, index + 1, result);
            }

            it = node->children.find("+");
            if (it != node->children.end()) {
                find_subscribers_recursive(it->second.get(), parts, index + 1, result);
            }

            it = node->children.find("#");
            if (it != node->children.end()) {
                result.insert(it->second->subscribers.begin(),
                    it->second->subscribers.end());
            }
        }

        void remove_client_recursive(TopicNode* node, const std::string& client_id) {
            node->subscribers.erase(client_id);

            for (auto& [part, child] : node->children) {
                remove_client_recursive(child.get(), client_id);
            }
        }

        bool topic_matches(const std::string& topic, const std::string& pattern) const {
            auto topic_parts = split_topic(topic);
            auto pattern_parts = split_topic(pattern);

            size_t i = 0;
            for (; i < pattern_parts.size(); i++) {
                if (pattern_parts[i] == "#") {
                    return true;
                }

                if (i >= topic_parts.size()) {
                    return false;
                }

                if (pattern_parts[i] != "+" && pattern_parts[i] != topic_parts[i]) {
                    return false;
                }
            }

            return i == topic_parts.size();
        }
    };

    class SessionStore {
    private:
        mutable std::shared_mutex mutex_;
        std::map<std::string, std::unique_ptr<SessionState>> persistent_sessions_;
        std::unique_ptr<PersistenceStore> persistence_;

    public:
        void shutdown() {
            persistent_sessions_.clear();
            persistence_.reset();
        }

        explicit SessionStore(std::unique_ptr<PersistenceStore> persistence)
            : persistence_(std::move(persistence)) {}

        void save_session(const std::string& client_id, const SessionState& state) {
            try {
                std::unique_lock<std::shared_mutex> lock(mutex_);
                auto session = std::make_unique<SessionState>(state);
                persistent_sessions_[client_id] = std::move(session);
            }
            catch (...) {}
        }

        std::optional<SessionState> restore_session(const std::string& client_id) {
            try {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                auto it = persistent_sessions_.find(client_id);
                if (it != persistent_sessions_.end()) {
                    return *(it->second);
                }
            }
            catch (...) {}
            return std::nullopt;
        }

        void remove_session(const std::string& client_id) {
            try {
                std::unique_lock<std::shared_mutex> lock(mutex_);
                persistent_sessions_.erase(client_id);
            }
            catch (...) {}
        }
    };

    class MetricsCollector {
    private:
        struct Metrics {
            std::atomic<uint64_t> total_connections{ 0 };
            std::atomic<uint64_t> active_connections{ 0 };
            std::atomic<uint64_t> messages_published{ 0 };
            std::atomic<uint64_t> messages_delivered{ 0 };
        };

        Metrics metrics_;

    public:
        void increment(const std::string& metric, uint64_t value = 1) {
            if (metric == "active") metrics_.active_connections += value;
            else if (metric == "published") metrics_.messages_published += value;
            else if (metric == "delivered") metrics_.messages_delivered += value;
        }

        void decrement(const std::string& metric, uint64_t value = 1) {
            if (metric == "active" && metrics_.active_connections >= value) {
                metrics_.active_connections -= value;
            }
        }

        std::map<std::string, uint64_t> get_all_metrics() const {
            std::map<std::string, uint64_t> result;
            result["active_connections"] = metrics_.active_connections.load();
            result["messages_published"] = metrics_.messages_published.load();
            return result;
        }
    };

    class MqttBroker {
    private:
        BrokerConfig config_;

        std::atomic<bool> running_{ false };
        std::atomic<bool> shutting_down_{ false };
        std::atomic<bool> force_shutdown_{ false };

        std::unique_ptr<Socket> server_socket_;

        std::unique_ptr<MessageRouter> router_;
        std::unique_ptr<SessionStore> session_store_;
        std::unique_ptr<AuthManager> auth_manager_;
        std::unique_ptr<MetricsCollector> metrics_;

        mutable std::shared_mutex sessions_mutex_;
        std::map<std::string, std::shared_ptr<ClientSession>> sessions_;
        std::map<std::string, std::set<std::string>> ip_connections_;

        std::unique_ptr<ThreadPool> worker_pool_;
        std::unique_ptr<ThreadPool> io_pool_;

        std::unique_ptr<std::thread> accept_thread_;
        std::vector<std::thread> client_threads_;
        std::mutex client_threads_mutex_;

    public:
        MqttBroker(const BrokerConfig& config = {})
            : config_(config) {

            if (!SocketSystem::initialize()) {
                throw std::runtime_error("Failed to initialize socket system");
            }

            router_ = std::make_unique<MessageRouter>();
            metrics_ = std::make_unique<MetricsCollector>();

            if (config_.enable_persistence) {
                try {
                    auto persistence = std::make_unique<FilePersistence>(config_.persistence_path);
                    session_store_ = std::make_unique<SessionStore>(std::move(persistence));
                }
                catch (...) {}
            }

            if (config_.require_auth) {
                auth_manager_ = std::make_unique<AuthManager>(config_.security_config);
                if (!config_.users_file.empty()) {
                    try {
                        auth_manager_->load_users(config_.users_file);
                    }
                    catch (...) {}
                }
            }

            worker_pool_ = std::make_unique<ThreadPool>(config.worker_threads);
            io_pool_ = std::make_unique<ThreadPool>(config.io_threads);
        }

        ~MqttBroker() {
            // Segnala shutdown immediato
            force_shutdown_ = true;
            shutting_down_ = true;
            running_ = false;

#ifdef _WIN32
            // Su Windows, non fare NULLA con i socket
            // per evitare blocchi nel distruttore

            // Detach tutti i thread senza aspettare
            if (accept_thread_ && accept_thread_->joinable()) {
                accept_thread_->detach();
            }
#else
            // Su Linux/Unix, chiusura normale
            if (server_socket_) {
                try {
                    server_socket_->close();
                }
                catch (...) {}
            }

            // Su Linux possiamo provare un join con timeout
            if (accept_thread_ && accept_thread_->joinable()) {
                // Usa un future per timeout
                auto future = std::async(std::launch::async, [this]() {
                    if (accept_thread_ && accept_thread_->joinable()) {
                        accept_thread_->join();
                    }
                    });

                // Aspetta massimo 100ms
                if (future.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready) {
                    if (accept_thread_ && accept_thread_->joinable()) {
                        accept_thread_->detach();
                    }
                }
            }
#endif

            // Detach client threads (uguale per tutti)
            {
                std::lock_guard<std::mutex> lock(client_threads_mutex_);
                for (auto& t : client_threads_) {
                    if (t.joinable()) {
                        t.detach();
                    }
                }
                client_threads_.clear();
            }

            // Cleanup risorse (uguale per tutti)
            sessions_.clear();

            if (worker_pool_) {
                worker_pool_->emergency_shutdown();
            }
            if (io_pool_) {
                io_pool_->emergency_shutdown();
            }

            router_.reset();
            metrics_.reset();
            auth_manager_.reset();
            session_store_.reset();
        }

        bool start() {
            LOG_INFO("BROKER") << "Starting MQTT Broker...";

            if (!start_tcp_server()) {
                return false;
            }

            running_ = true;

            LOG_INFO("BROKER") << "MQTT Broker started on port " << config_.port;
            return true;
        }

        void stop() {
            if (!running_) return;

            LOG_INFO("BROKER") << "Stopping broker...";

            shutting_down_ = true;
            running_ = false;

#ifdef _WIN32
            // Su Windows, chiudi il socket in modo asincrono
            if (server_socket_) {
                // Usa un thread separato per chiudere il socket
                std::thread([socket = std::move(server_socket_)]() mutable {
                    if (socket) {
                        socket->close();
                    }
                }).detach();
            }
#else
            // Su Linux, chiusura normale
            if (server_socket_) {
                server_socket_->close();
            }
#endif

            // Disconnetti tutti i client
            {
                std::unique_lock<std::shared_mutex> lock(sessions_mutex_);
                for (auto& [id, session] : sessions_) {
                    if (session && session->socket) {
                        try {
                            session->socket->close();
                        }
                        catch (...) {}
                    }
                }
                sessions_.clear();
            }

#ifdef _WIN32
            // Su Windows, detach immediato
            if (accept_thread_ && accept_thread_->joinable()) {
                accept_thread_->detach();
            }
#else
            // Su Linux, prova join con timeout
            if (accept_thread_ && accept_thread_->joinable()) {
                auto future = std::async(std::launch::async, [this]() {
                    accept_thread_->join();
                    });

                if (future.wait_for(std::chrono::milliseconds(500)) != std::future_status::ready) {
                    accept_thread_->detach();
                }
            }
#endif

            // Detach client threads
            {
                std::lock_guard<std::mutex> lock(client_threads_mutex_);
                for (auto& t : client_threads_) {
                    if (t.joinable()) {
                        t.detach();
                    }
                }
                client_threads_.clear();
            }

            LOG_INFO("BROKER") << "Broker stopped";
        }

        AuthManager* get_auth_manager() { return auth_manager_.get(); }
        MetricsCollector* get_metrics() { return metrics_.get(); }

        size_t get_client_count() const {
            std::shared_lock<std::shared_mutex> lock(sessions_mutex_);
            return sessions_.size();
        }

    private:
        bool start_tcp_server() {
            server_socket_ = std::make_unique<Socket>();
            if (!server_socket_->create()) {
                LOG_ERROR("BROKER") << "Failed to create socket";
                return false;
            }

            server_socket_->set_reuse_address(true);
            server_socket_->set_receive_timeout(100);

            if (!server_socket_->bind(config_.port)) {
                LOG_ERROR("BROKER") << "Failed to bind port " << config_.port;
                return false;
            }

            if (!server_socket_->listen(128)) {
                LOG_ERROR("BROKER") << "Failed to listen";
                return false;
            }

            LOG_INFO("BROKER") << "TCP server listening on port " << config_.port;

            accept_thread_ = std::make_unique<std::thread>([this]() {
                LOG_INFO("BROKER") << "Accept thread started";

                while (!shutting_down_ && !force_shutdown_) {
                    if (!server_socket_ || !server_socket_->is_valid()) {
                        break;
                    }

                    Socket client = server_socket_->accept();

                    if (shutting_down_ || force_shutdown_) {
                        if (client.is_valid()) client.close();
                        break;
                    }

                    if (client.is_valid()) {
                        try {
                            std::string ip = client.get_peer_address();
                            handle_new_connection(std::move(client), ip);
                        }
                        catch (...) {}
                    }
                }

                LOG_INFO("BROKER") << "Accept thread ending";
                });

            return true;
        }

        void handle_new_connection(Socket client, const std::string& ip_address) {
            if (shutting_down_ || force_shutdown_) {
                client.close();
                return;
            }

            auto session = std::make_shared<ClientSession>();
            session->ip_address = ip_address;
            session->socket = std::make_unique<Socket>(std::move(client));
            session->socket->set_non_blocking(true);
            session->connected = true;

            {
                std::lock_guard<std::mutex> lock(client_threads_mutex_);

                // Pulisci thread terminati
                client_threads_.erase(
                    std::remove_if(client_threads_.begin(), client_threads_.end(),
                        [](std::thread& t) { return !t.joinable(); }),
                    client_threads_.end()
                );

                client_threads_.emplace_back([this, session]() {
                    handle_client_internal(session);
                    });
            }
        }

        void handle_client_internal(std::shared_ptr<ClientSession> session) {
            Buffer receive_buffer(65536);
            uint8_t buffer[8192];

            try {
                while (session->connected && !shutting_down_ && !force_shutdown_) {
                    session->socket->set_receive_timeout(50);

                    int received = session->socket->receive(buffer, sizeof(buffer));

                    if (received > 0) {
                        session->last_activity.reset();
                        receive_buffer.write_bytes(buffer, received);

                        while (receive_buffer.available() > 0 && !shutting_down_) {
                            if (!process_packet(session, receive_buffer)) {
                                break;
                            }
                        }
                    }
                    else if (received == 0) {
                        break;
                    }
                    else {
                        if (session->keep_alive > 0) {
                            uint64_t timeout = session->keep_alive * 1500;
                            if (session->last_activity.elapsed_ms() > timeout) {
                                break;
                            }
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    }
                }
            }
            catch (...) {}

            if (!shutting_down_ && !force_shutdown_) {
                disconnect_client(session);
            }
            else if (session->socket) {
                session->socket->close();
            }
        }

        bool process_packet(std::shared_ptr<ClientSession> session, Buffer& buffer) {
            if (shutting_down_ || force_shutdown_) return false;

            buffer.compact();

            if (buffer.available() < 2) {
                return false;
            }

            size_t start_pos = buffer.mark_read_position();

            uint8_t first_byte = buffer.peek_byte();
            PacketType type = static_cast<PacketType>(first_byte >> 4);

            if (type < CONNECT || type > DISCONNECT) {
                buffer.read_byte();
                return true;
            }

            buffer.read_byte();

            uint32_t remaining = 0;
            if (!buffer.read_variable_length(remaining)) {
                buffer.reset_read_position(start_pos);
                return false;
            }

            if (buffer.available() < remaining) {
                buffer.reset_read_position(start_pos);
                return false;
            }

            try {
                switch (type) {
                case CONNECT:
                    handle_connect(session, buffer);
                    break;
                case PUBLISH:
                    handle_publish(session, first_byte, buffer, remaining);
                    break;
                case SUBSCRIBE:
                    handle_subscribe(session, buffer, remaining);
                    break;
                case UNSUBSCRIBE:
                    handle_unsubscribe(session, buffer, remaining);
                    break;
                case PINGREQ:
                    handle_ping(session);
                    break;
                case DISCONNECT:
                    handle_disconnect(session, buffer);
                    break;
                default:
                    break;
                }
            }
            catch (...) {
                return false;
            }

            return true;
        }

        void handle_connect(std::shared_ptr<ClientSession> session, Buffer& buffer) {
            std::string protocol = buffer.read_string();
            uint8_t level = buffer.read_byte();
            uint8_t connect_flags = buffer.read_byte();
            uint16_t keep_alive = buffer.read_uint16();

            if (level >= 5) {
                uint32_t properties_len = 0;
                buffer.read_variable_length(properties_len);
                buffer.skip(properties_len);
            }

            std::string client_id = buffer.read_string();

            if (client_id.empty()) {
                client_id = generate_client_id();
            }

            bool has_will = (connect_flags & 0x04) != 0;
            if (has_will) {
                if (level >= 5) {
                    uint32_t will_properties_len = 0;
                    buffer.read_variable_length(will_properties_len);
                    buffer.skip(will_properties_len);
                }

                session->will_message.topic = buffer.read_string();
                uint16_t will_len = buffer.read_uint16();
                session->will_message.payload.resize(will_len);
                buffer.read_bytes(session->will_message.payload.data(), will_len);
            }

            bool has_username = (connect_flags & 0x80) != 0;
            bool has_password = (connect_flags & 0x40) != 0;
            std::string username, password;

            if (has_username) {
                username = buffer.read_string();
            }
            if (has_password) {
                password = buffer.read_string();
            }

            uint8_t return_code = 0;

            if (config_.require_auth && auth_manager_) {
                auto result = auth_manager_->authenticate(username, password, client_id, session->ip_address);
                if (!result.success) {
                    return_code = 0x04;
                    send_connack(session, return_code, false);
                    session->connected = false;
                    return;
                }
                session->authenticated = true;
            }

            {
                std::unique_lock<std::shared_mutex> lock(sessions_mutex_);
                session->client_id = client_id;
                session->username = username;
                session->keep_alive = keep_alive;
                sessions_[client_id] = session;
            }

            send_connack(session, return_code, false);
            metrics_->increment("active");
        }

        void handle_publish(std::shared_ptr<ClientSession> session,
            uint8_t flags, Buffer& buffer, uint32_t remaining) {

            if (config_.require_auth && !session->authenticated) {
                session->connected = false;
                return;
            }

            QoS qos = static_cast<QoS>((flags >> 1) & 0x03);
            bool retain = flags & 0x01;

            std::string topic = buffer.read_string();

            uint16_t packet_id = 0;
            if (qos > QOS_0) {
                packet_id = buffer.read_uint16();
            }

            size_t header_size = 2 + topic.length() + (qos > QOS_0 ? 2 : 0);
            size_t payload_size = remaining - header_size;

            std::vector<uint8_t> payload(payload_size);
            buffer.read_bytes(payload.data(), payload_size);

            metrics_->increment("published");

            if (retain) {
                router_->set_retained(topic, payload);
            }

            distribute_message(topic, payload, qos, retain, session->client_id);

            if (qos == QOS_1) {
                send_puback(session, packet_id);
            }
        }

        void handle_subscribe(std::shared_ptr<ClientSession> session,
            Buffer& buffer, uint32_t remaining) {

            if (config_.require_auth && !session->authenticated) {
                session->connected = false;
                return;
            }

            uint16_t packet_id = buffer.read_uint16();
            remaining -= 2;

            SubAckPacket suback;
            suback.set_packet_id(packet_id);

            while (remaining > 0) {
                std::string topic = buffer.read_string();
                uint8_t requested_qos = buffer.read_byte();

                remaining -= static_cast<uint32_t>(2 + topic.length() + 1);

                router_->add_subscription(session->client_id, topic);
                session->state.subscriptions.insert(topic);

                suback.add_return_code(requested_qos);

                send_retained_messages(session, topic, static_cast<QoS>(requested_qos));
            }

            Buffer response(1024);
            suback.serialize(response);
            send_packet(session, response);
        }

        void handle_unsubscribe(std::shared_ptr<ClientSession> session,
            Buffer& buffer, uint32_t remaining) {

            uint16_t packet_id = buffer.read_uint16();
            remaining -= 2;

            while (remaining > 0) {
                std::string topic = buffer.read_string();
                remaining -= static_cast<uint32_t>(2 + topic.length());

                router_->remove_subscription(session->client_id, topic);
                session->state.subscriptions.erase(topic);
            }

            UnsubAckPacket unsuback;
            unsuback.set_packet_id(packet_id);

            Buffer response(256);
            unsuback.serialize(response);
            send_packet(session, response);
        }

        void handle_ping(std::shared_ptr<ClientSession> session) {
            PingRespPacket pingresp;
            Buffer response(2);
            pingresp.serialize(response);
            send_packet(session, response);
        }

        void handle_disconnect(std::shared_ptr<ClientSession> session, Buffer& buffer) {
            session->will_message = WillMessage();
            session->connected = false;
        }

        void distribute_message(const std::string& topic,
            const std::vector<uint8_t>& payload,
            QoS qos, bool retain, const std::string& sender_id) {

            if (shutting_down_ || force_shutdown_) return;

            auto subscribers = router_->get_subscribers(topic);

            std::shared_lock<std::shared_mutex> lock(sessions_mutex_);

            for (const auto& subscriber_id : subscribers) {
                if (subscriber_id == sender_id) continue;

                auto it = sessions_.find(subscriber_id);
                if (it != sessions_.end() && it->second->connected) {
                    auto pub = std::make_shared<PublishPacket>();
                    pub->set_topic(topic);
                    pub->set_payload(payload.data(), payload.size());
                    pub->set_qos(QOS_0);
                    pub->set_retain(false);

                    send_publish(it->second, pub);
                    metrics_->increment("delivered");
                }
            }
        }

        void send_retained_messages(std::shared_ptr<ClientSession> session,
            const std::string& subscription, QoS max_qos) {

            auto retained = router_->get_retained_for_pattern(subscription);

            for (const auto& [topic, payload] : retained) {
                auto pub = std::make_shared<PublishPacket>();
                pub->set_topic(topic);
                pub->set_payload(payload.data(), payload.size());
                pub->set_qos(QOS_0);
                pub->set_retain(true);

                send_publish(session, pub);
            }
        }

        void send_packet(std::shared_ptr<ClientSession> session, const Buffer& buffer) {
            if (!session->connected || !session->socket) return;

            session->socket->send(buffer.data(), buffer.size());
        }

        void send_connack(std::shared_ptr<ClientSession> session,
            uint8_t return_code, bool session_present) {

            ConnAckPacket connack;
            connack.set_session_present(session_present);
            connack.set_return_code(return_code);

            Buffer response(16);
            connack.serialize(response);
            send_packet(session, response);
        }

        void send_publish(std::shared_ptr<ClientSession> session,
            std::shared_ptr<PublishPacket> pub) {

            Buffer buffer(4096);
            pub->serialize(buffer);
            send_packet(session, buffer);
        }

        void send_puback(std::shared_ptr<ClientSession> session, uint16_t packet_id) {
            PubAckPacket puback;
            puback.set_packet_id(packet_id);

            Buffer response(16);
            puback.serialize(response);
            send_packet(session, response);
        }

        std::string generate_client_id() {
            static std::atomic<uint64_t> counter(0);
            std::stringstream ss;
            ss << "auto-" << std::hex << Time::now_ms() << "-" << counter++;
            return ss.str();
        }

        void disconnect_client(std::shared_ptr<ClientSession> session) {
            if (!session || session->client_id.empty()) return;

            bool expected = true;
            if (!session->connected.compare_exchange_strong(expected, false)) {
                return;
            }

            if (session->socket) {
                session->socket->close();
            }

            router_->remove_all_subscriptions(session->client_id);

            {
                std::unique_lock<std::shared_mutex> lock(sessions_mutex_);
                sessions_.erase(session->client_id);
            }

            metrics_->decrement("active");
        }
    };

} // namespace ourmqtt

#endif // OUR_MQTT_BROKER_HPP