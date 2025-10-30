// mqtt_client.hpp
#ifndef OUR_MQTT_CLIENT_HPP
#define OUR_MQTT_CLIENT_HPP

// Configurazione per Windows
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
#include "logger.hpp"
#include <functional>
#include <map>
#include <queue>
#include <iostream>
#include <memory>
#include <atomic>
#include <future>
#include <chrono>
#include <variant>
#include <optional>
#include <shared_mutex>
#include <condition_variable>
#include <deque>
#include <unordered_set>
#include <algorithm>
#include <sstream>      // AGGIUNTO per std::stringstream
#include <thread>           // Per std::thread
#include <set>
namespace ourmqtt {

    // Forward declarations
    class MessageQueue;
    class ConnectionStateMachine;
    class ClientMetrics;
    class FlowController;

    // Event types - RINOMINATO ERROR in CLIENT_ERROR
    enum class ClientEvent {
        CONNECTED,
        DISCONNECTED,
        MESSAGE_RECEIVED,
        MESSAGE_SENT,
        SUBSCRIBE_SUCCESS,
        SUBSCRIBE_FAILURE,
        UNSUBSCRIBE_SUCCESS,
        CLIENT_ERROR,        // Cambiato da ERROR a CLIENT_ERROR
        CONNECTION_LOST,
        RECONNECTING,
        SESSION_RESUMED
    };

    // Connection states - RINOMINATO ERROR_STATE
    enum class ConnectionState {
        DISCONNECTED,
        CONNECTING,
        CONNECTED,
        DISCONNECTING,
        RECONNECTING,
        CONNECTION_ERROR     // Cambiato da ERROR_STATE a CONNECTION_ERROR
    };

    // Message for internal queue
    struct QueuedMessage {
        std::string topic;
        std::vector<uint8_t> payload;
        QoS qos;
        bool retain;
        uint16_t packet_id;
        std::chrono::steady_clock::time_point queued_at;
        std::promise<bool> promise;
        int retry_count = 0;
    };

    // Subscription request
    struct SubscriptionRequest {
        std::string topic;
        QoS qos;
        uint16_t packet_id;
        std::promise<bool> promise;
        std::chrono::steady_clock::time_point sent_at;
    };

    // Client configuration
    struct ClientConfig {
        // Basic settings
        std::string client_id;
        std::string username;
        std::string password;
        uint16_t keep_alive = 60;
        bool clean_session = true;
        uint32_t session_expiry_interval = 0; // MQTT 5.0

        // Connection settings
        bool auto_reconnect = true;
        int initial_reconnect_delay_ms = 1000;
        int max_reconnect_delay_ms = 60000;
        double reconnect_backoff_multiplier = 2.0;
        int max_reconnect_attempts = -1;
        int connection_timeout_ms = 30000;

        // TLS/SSL
        bool use_tls = false;
        TLSConfig tls_config;

        // WebSocket
        bool use_websocket = false;
        std::string websocket_path = "/mqtt";
        std::map<std::string, std::string> websocket_headers;

        // Will message
        bool has_will = false;
        std::string will_topic;
        std::vector<uint8_t> will_payload;
        QoS will_qos = QOS_0;
        bool will_retain = false;
        uint32_t will_delay_interval = 0; // MQTT 5.0

        // Performance
        size_t send_buffer_size = 65536;
        size_t receive_buffer_size = 65536;
        size_t max_queued_messages = 1000;
        size_t max_inflight_messages = 20;
        bool enable_nagle = false;

        // Features
        bool enable_persistence = false;
        std::string persistence_directory = "./mqtt_client_data";
        bool enable_metrics = true;
        bool enable_auto_subscribe = true; // Re-subscribe on reconnect

        // Rate limiting
        size_t max_publish_rate = 100; // messages per second
        size_t max_bandwidth = 1024 * 1024; // bytes per second

        // MQTT 5.0
        uint32_t receive_maximum = 65535;
        uint32_t maximum_packet_size = 268435455;
        uint16_t topic_alias_maximum = 0;
        bool request_response_info = false;
        bool request_problem_info = true;
    };

    // Client metrics
    class ClientMetrics {
    private:
        std::atomic<uint64_t> messages_sent_{ 0 };
        std::atomic<uint64_t> messages_received_{ 0 };
        std::atomic<uint64_t> bytes_sent_{ 0 };
        std::atomic<uint64_t> bytes_received_{ 0 };
        std::atomic<uint64_t> connection_attempts_{ 0 };
        std::atomic<uint64_t> connection_failures_{ 0 };
        std::atomic<uint64_t> reconnections_{ 0 };
        std::atomic<uint64_t> messages_dropped_{ 0 };
        std::atomic<uint64_t> publish_timeouts_{ 0 };
        std::chrono::steady_clock::time_point start_time_;
        std::chrono::steady_clock::time_point last_connection_time_;

    public:
        ClientMetrics() : start_time_(std::chrono::steady_clock::now()) {}

        void record_message_sent() { messages_sent_++; }
        void record_message_received() { messages_received_++; }
        void record_bytes_sent(size_t bytes) { bytes_sent_ += bytes; }
        void record_bytes_received(size_t bytes) { bytes_received_ += bytes; }
        void record_connection_attempt() { connection_attempts_++; }
        void record_connection_failure() { connection_failures_++; }
        void record_reconnection() { reconnections_++; }
        void record_message_dropped() { messages_dropped_++; }
        void record_publish_timeout() { publish_timeouts_++; }

        void record_connection_established() {
            last_connection_time_ = std::chrono::steady_clock::now();
        }

        std::map<std::string, uint64_t> get_all_metrics() const {
            std::map<std::string, uint64_t> metrics;

            metrics["messages_sent"] = messages_sent_;
            metrics["messages_received"] = messages_received_;
            metrics["bytes_sent"] = bytes_sent_;
            metrics["bytes_received"] = bytes_received_;
            metrics["connection_attempts"] = connection_attempts_;
            metrics["connection_failures"] = connection_failures_;
            metrics["reconnections"] = reconnections_;
            metrics["messages_dropped"] = messages_dropped_;
            metrics["publish_timeouts"] = publish_timeouts_;

            auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - start_time_).count();
            metrics["uptime_seconds"] = uptime;

            return metrics;
        }
    };

    // Flow controller for rate limiting and QoS management
    class FlowController {
    private:
        mutable std::shared_mutex mutex_;
        std::map<uint16_t, std::shared_ptr<QueuedMessage>> inflight_messages_;
        std::deque<std::shared_ptr<QueuedMessage>> message_queue_;
        size_t max_inflight_;
        size_t max_queued_;

        // Rate limiting
        std::chrono::steady_clock::time_point rate_window_start_;
        size_t messages_in_window_{ 0 };
        size_t max_rate_;

    public:
        FlowController(size_t max_inflight, size_t max_queued, size_t max_rate)
            : max_inflight_(max_inflight),
            max_queued_(max_queued),
            max_rate_(max_rate),
            rate_window_start_(std::chrono::steady_clock::now()) {}

        bool can_send() {
            std::shared_lock<std::shared_mutex> lock(mutex_);

            // Check rate limit
            auto now = std::chrono::steady_clock::now();
            if (now - rate_window_start_ >= std::chrono::seconds(1)) {
                messages_in_window_ = 0;
                rate_window_start_ = now;
            }

            if (messages_in_window_ >= max_rate_) {
                return false;
            }

            // Check inflight limit
            return inflight_messages_.size() < max_inflight_;
        }

        bool add_inflight(uint16_t packet_id, std::shared_ptr<QueuedMessage> msg) {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            if (inflight_messages_.size() >= max_inflight_) {
                return false;
            }

            inflight_messages_[packet_id] = msg;
            messages_in_window_++;
            return true;
        }

        void remove_inflight(uint16_t packet_id) {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            inflight_messages_.erase(packet_id);
        }

        std::shared_ptr<QueuedMessage> get_inflight(uint16_t packet_id) {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            auto it = inflight_messages_.find(packet_id);
            if (it != inflight_messages_.end()) {
                return it->second;
            }
            return nullptr;
        }

        bool queue_message(std::shared_ptr<QueuedMessage> msg) {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            if (message_queue_.size() >= max_queued_) {
                return false;
            }

            message_queue_.push_back(msg);
            return true;
        }

        std::shared_ptr<QueuedMessage> dequeue_message() {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            if (message_queue_.empty()) {
                return nullptr;
            }

            auto msg = message_queue_.front();
            message_queue_.pop_front();
            return msg;
        }

        std::vector<std::shared_ptr<QueuedMessage>> get_all_inflight() {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            std::vector<std::shared_ptr<QueuedMessage>> result;

            for (const auto& [id, msg] : inflight_messages_) {
                result.push_back(msg);
            }

            return result;
        }

        void clear() {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            inflight_messages_.clear();
            message_queue_.clear();
            messages_in_window_ = 0;
        }
    };

    // Connection state machine
    class ConnectionStateMachine {
    private:
        mutable std::shared_mutex mutex_;
        ConnectionState state_{ ConnectionState::DISCONNECTED };
        std::condition_variable_any state_cv_;

    public:
        bool transition_to(ConnectionState new_state) {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            // Validate state transition
            if (!is_valid_transition(state_, new_state)) {
                return false;
            }

            state_ = new_state;
            state_cv_.notify_all();
            return true;
        }

        ConnectionState get_state() const {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            return state_;
        }

        bool wait_for_state(ConnectionState target_state, int timeout_ms) {
            std::unique_lock<std::shared_mutex> lock(mutex_);

            return state_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                [this, target_state] { return state_ == target_state; });
        }

        bool is_connected() const {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            return state_ == ConnectionState::CONNECTED;
        }

    private:
        bool is_valid_transition(ConnectionState from, ConnectionState to) {
            switch (from) {
            case ConnectionState::DISCONNECTED:
                return to == ConnectionState::CONNECTING;

            case ConnectionState::CONNECTING:
                return to == ConnectionState::CONNECTED ||
                    to == ConnectionState::DISCONNECTED ||
                    to == ConnectionState::CONNECTION_ERROR;  // Aggiornato

            case ConnectionState::CONNECTED:
                return to == ConnectionState::DISCONNECTING ||
                    to == ConnectionState::RECONNECTING ||
                    to == ConnectionState::CONNECTION_ERROR;  // Aggiornato

            case ConnectionState::DISCONNECTING:
                return to == ConnectionState::DISCONNECTED;

            case ConnectionState::RECONNECTING:
                return to == ConnectionState::CONNECTED ||
                    to == ConnectionState::DISCONNECTED ||
                    to == ConnectionState::CONNECTION_ERROR;  // Aggiornato

            case ConnectionState::CONNECTION_ERROR:  // Aggiornato
                return to == ConnectionState::DISCONNECTED ||
                    to == ConnectionState::CONNECTING;

            default:
                return false;
            }
        }
    };

    // Main MQTT Client class
    class MqttClient {
    private:
        // Core components
        std::unique_ptr<Socket> socket_;
        ClientConfig config_;
        std::unique_ptr<ConnectionStateMachine> state_machine_;
        std::unique_ptr<ClientMetrics> metrics_;
        std::unique_ptr<FlowController> flow_controller_;

        // Connection info
        std::string host_;
        uint16_t port_;
        std::string actual_client_id_; // Server might assign one

        // Threading
        Thread receiver_thread_;
        Thread keep_alive_thread_;
        Thread reconnect_thread_;
        Thread message_processor_thread_;

        // Synchronization
        mutable std::shared_mutex socket_mutex_;
        mutable std::shared_mutex callbacks_mutex_;
        mutable std::shared_mutex pending_mutex_;

        // Packet ID management
        std::atomic<uint16_t> next_packet_id_{ 1 };

        // Callbacks
        std::function<void(ClientEvent, const std::string&)> on_event_;
        std::function<void(const std::string&, const std::vector<uint8_t>&, QoS, bool)> on_message_;
        std::map<std::string, std::function<void(const std::string&, const std::vector<uint8_t>&)>> topic_handlers_;


        // Getter utili
        const std::string& get_username() const { return config_.username; }
        bool is_clean_session() const { return config_.clean_session; }
        // Buffers
        Buffer receive_buffer_;
        Buffer send_buffer_;

        // Timers
        Timer keep_alive_timer_;
        Timer reconnect_timer_;

        // Reconnection
        std::atomic<int> reconnect_attempts_{ 0 };
        std::atomic<int> current_reconnect_delay_ms_;

        // Pending operations
        std::map<uint16_t, std::shared_ptr<SubscriptionRequest>> pending_subscribes_;
        std::map<uint16_t, std::shared_ptr<SubscriptionRequest>> pending_unsubscribes_;

        // QoS 2 state
        std::set<uint16_t> pending_pubrec_;
        std::set<uint16_t> pending_pubrel_;
        std::set<uint16_t> pending_pubcomp_;

        // Session state
        std::set<std::string> active_subscriptions_;
        std::map<std::string, QoS> subscription_qos_;

        // Shutdown flag
        std::atomic<bool> shutting_down_{ false };

    public:

        // Metodi per autenticazione e configurazione
        void set_credentials(const std::string& username, const std::string& password) {
            config_.username = username;
            config_.password = password;
        }

        void set_clean_session(bool clean) {
            config_.clean_session = clean;
        }

        void set_will(const std::string& topic, const std::string& message,
            QoS qos = QOS_0, bool retain = false) {
            config_.has_will = true;
            config_.will_topic = topic;
            config_.will_payload.assign(message.begin(), message.end());
            config_.will_qos = qos;
            config_.will_retain = retain;
        }

        MqttClient(const std::string& client_id)
            : MqttClient(ClientConfig{ client_id }) {}

        MqttClient(const ClientConfig& config)
            : config_(config),
            state_machine_(std::make_unique<ConnectionStateMachine>()),
            metrics_(std::make_unique<ClientMetrics>()),
            flow_controller_(std::make_unique<FlowController>(
                config.max_inflight_messages,
                config.max_queued_messages,
                config.max_publish_rate)),
            receive_buffer_(config.receive_buffer_size),
            send_buffer_(config.send_buffer_size),
            current_reconnect_delay_ms_(config.initial_reconnect_delay_ms) {

            if (!SocketSystem::is_initialized()) {
                if (!SocketSystem::initialize()) {
                    throw std::runtime_error("Failed to initialize socket system");
                }
            }

            // Generate client ID if empty
            if (config_.client_id.empty()) {
                config_.client_id = generate_client_id();
            }
        }

        ~MqttClient() {
            shutdown();
        }

        // Connection methods
        std::future<bool> connect_async(const std::string& host, uint16_t port) {
            auto promise = std::make_shared<std::promise<bool>>();
            auto future = promise->get_future();

            std::thread([this, host, port, promise]() {
                bool result = connect(host, port);
                promise->set_value(result);
                }).detach();

                return future;
        }

        bool connect(const std::string& host, uint16_t port, int timeout_ms = 30000) {
            if (state_machine_->get_state() != ConnectionState::DISCONNECTED) {
                LOG_WARN("CLIENT") << "Already connected or connecting";
                return false;
            }

            host_ = host;
            port_ = port;

            if (!state_machine_->transition_to(ConnectionState::CONNECTING)) {
                return false;
            }

            metrics_->record_connection_attempt();

            LOG_INFO("CLIENT") << "Connecting to " << host << ":" << port
                << (config_.use_tls ? " (TLS)" : "")
                << (config_.use_websocket ? " (WebSocket)" : "");

            try {
                // Create socket
                if (!create_and_connect_socket()) {
                    state_machine_->transition_to(ConnectionState::CONNECTION_ERROR);
                    metrics_->record_connection_failure();
                    return false;
                }

                // Send CONNECT packet
                if (!send_connect_packet()) {
                    disconnect_internal();
                    state_machine_->transition_to(ConnectionState::CONNECTION_ERROR);
                    metrics_->record_connection_failure();
                    return false;
                }

                // Wait for CONNACK
                if (!wait_for_connack(timeout_ms)) {
                    disconnect_internal();
                    state_machine_->transition_to(ConnectionState::CONNECTION_ERROR);
                    metrics_->record_connection_failure();
                    return false;
                }

                state_machine_->transition_to(ConnectionState::CONNECTED);
                metrics_->record_connection_established();

                // Start background threads
                start_background_threads();

                // Restore subscriptions if needed
                if (config_.enable_auto_subscribe && !config_.clean_session) {
                    restore_subscriptions();
                }

                // Process any queued messages
                process_queued_messages();

                // Fire connected event
                fire_event(ClientEvent::CONNECTED, "Connected successfully");

                LOG_INFO("CLIENT") << "Connected successfully";

                return true;

            }
            catch (const std::exception& e) {
                LOG_ERROR("CLIENT") << "Connection error: " << e.what();
                disconnect_internal();
                state_machine_->transition_to(ConnectionState::CONNECTION_ERROR);
                metrics_->record_connection_failure();
                return false;
            }
        }

        void disconnect(uint32_t reason_code = 0) {
            if (!state_machine_->transition_to(ConnectionState::DISCONNECTING)) {
                return;
            }

            LOG_INFO("CLIENT") << "Disconnecting...";

            // Disable auto-reconnect
            config_.auto_reconnect = false;

            // Send DISCONNECT packet
            send_disconnect_packet(reason_code);

            // Stop threads
            stop_background_threads();

            // Close socket
            disconnect_internal();

            state_machine_->transition_to(ConnectionState::DISCONNECTED);

            fire_event(ClientEvent::DISCONNECTED, "Disconnected by user");

            LOG_INFO("CLIENT") << "Disconnected";
        }

        // Publishing methods
        std::future<bool> publish_async(const std::string& topic,
            const std::string& payload,
            QoS qos = QOS_0,
            bool retain = false) {
            return publish_async(topic,
                std::vector<uint8_t>(payload.begin(), payload.end()),
                qos,
                retain);
        }

        std::future<bool> publish_async(const std::string& topic,
            const std::vector<uint8_t>& payload,
            QoS qos = QOS_0,
            bool retain = false) {
            auto msg = std::make_shared<QueuedMessage>();
            msg->topic = topic;
            msg->payload = payload;
            msg->qos = qos;
            msg->retain = retain;
            msg->queued_at = std::chrono::steady_clock::now();

            auto future = msg->promise.get_future();

            if (!state_machine_->is_connected()) {
                if (config_.enable_persistence && qos > QOS_0) {
                    // Queue for later delivery
                    if (!flow_controller_->queue_message(msg)) {
                        msg->promise.set_value(false);
                        metrics_->record_message_dropped();
                    }
                }
                else {
                    msg->promise.set_value(false);
                }
                return future;
            }

            // Try to send immediately if possible
            if (qos == QOS_0 || flow_controller_->can_send()) {
                send_publish_packet(msg);
            }
            else {
                // Queue for later
                if (!flow_controller_->queue_message(msg)) {
                    msg->promise.set_value(false);
                    metrics_->record_message_dropped();
                }
            }

            return future;
        }

        bool publish(const std::string& topic,
            const std::string& payload,
            QoS qos = QOS_0,
            bool retain = false) {
            auto future = publish_async(topic, payload, qos, retain);

            try {
                return future.get();
            }
            catch (...) {
                return false;
            }
        }

        // Subscription methods
        std::future<bool> subscribe_async(const std::string& topic, QoS qos = QOS_0) {
            auto request = std::make_shared<SubscriptionRequest>();
            request->topic = topic;
            request->qos = qos;
            request->packet_id = get_next_packet_id();
            request->sent_at = std::chrono::steady_clock::now();

            auto future = request->promise.get_future();

            if (!state_machine_->is_connected()) {
                request->promise.set_value(false);
                return future;
            }

            {
                std::unique_lock<std::shared_mutex> lock(pending_mutex_);
                pending_subscribes_[request->packet_id] = request;
            }

            SubscribePacket sub_pkt;
            sub_pkt.set_packet_id(request->packet_id);
            sub_pkt.add_subscription(topic, qos);

            Buffer buffer;
            sub_pkt.serialize(buffer);

            if (!send_packet(buffer)) {
                std::unique_lock<std::shared_mutex> lock(pending_mutex_);
                pending_subscribes_.erase(request->packet_id);
                request->promise.set_value(false);
            }

            return future;
        }

        bool subscribe(const std::string& topic, QoS qos = QOS_0) {
            auto future = subscribe_async(topic, qos);

            try {
                auto result = future.wait_for(std::chrono::seconds(10));
                if (result == std::future_status::ready) {
                    return future.get();
                }
                return false;
            }
            catch (...) {
                return false;
            }
        }

        std::future<bool> unsubscribe_async(const std::string& topic) {
            auto request = std::make_shared<SubscriptionRequest>();
            request->topic = topic;
            request->packet_id = get_next_packet_id();
            request->sent_at = std::chrono::steady_clock::now();

            auto future = request->promise.get_future();

            if (!state_machine_->is_connected()) {
                request->promise.set_value(false);
                return future;
            }

            {
                std::unique_lock<std::shared_mutex> lock(pending_mutex_);
                pending_unsubscribes_[request->packet_id] = request;
            }

            UnsubscribePacket unsub_pkt;
            unsub_pkt.set_packet_id(request->packet_id);
            unsub_pkt.add_topic(topic);

            Buffer buffer;
            unsub_pkt.serialize(buffer);

            if (!send_packet(buffer)) {
                std::unique_lock<std::shared_mutex> lock(pending_mutex_);
                pending_unsubscribes_.erase(request->packet_id);
                request->promise.set_value(false);
            }

            return future;
        }

        bool unsubscribe(const std::string& topic) {
            auto future = unsubscribe_async(topic);

            try {
                auto result = future.wait_for(std::chrono::seconds(10));
                if (result == std::future_status::ready) {
                    return future.get();
                }
                return false;
            }
            catch (...) {
                return false;
            }
        }

        // Event and message handlers
        void set_event_handler(std::function<void(ClientEvent, const std::string&)> handler) {
            std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
            on_event_ = handler;
        }

        void set_message_handler(std::function<void(const std::string&,
            const std::vector<uint8_t>&,
            QoS,
            bool)> handler) {
            std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
            on_message_ = handler;
        }

        void add_topic_handler(const std::string& topic_filter,
            std::function<void(const std::string&,
                const std::vector<uint8_t>&)> handler) {
            std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
            topic_handlers_[topic_filter] = handler;
        }

        void remove_topic_handler(const std::string& topic_filter) {
            std::unique_lock<std::shared_mutex> lock(callbacks_mutex_);
            topic_handlers_.erase(topic_filter);
        }

        // State and metrics
        bool is_connected() const {
            return state_machine_->is_connected();
        }

        ConnectionState get_connection_state() const {
            return state_machine_->get_state();
        }

        std::map<std::string, uint64_t> get_metrics() const {
            return metrics_->get_all_metrics();
        }

        const ClientConfig& get_config() const {
            return config_;
        }

        std::string get_client_id() const {
            return actual_client_id_.empty() ? config_.client_id : actual_client_id_;
        }

        std::vector<std::string> get_active_subscriptions() const {
            std::shared_lock<std::shared_mutex> lock(pending_mutex_);
            return std::vector<std::string>(active_subscriptions_.begin(),
                active_subscriptions_.end());
        }

    private:
        // Socket management
        bool create_and_connect_socket() {
            // Create appropriate socket type
            if (config_.use_tls) {
                socket_ = std::make_unique<TLSSocket>();
            }
            else {
                socket_ = std::make_unique<Socket>();
            }

            if (!socket_->create()) {
                LOG_ERROR("CLIENT") << "Failed to create socket";
                return false;
            }

            // Configure socket options BEFORE connect (but NOT non-blocking yet!)
            socket_->set_nodelay(!config_.enable_nagle);
            socket_->set_receive_buffer_size(config_.receive_buffer_size);
            socket_->set_send_buffer_size(config_.send_buffer_size);

            // Set a reasonable timeout for blocking connect
            socket_->set_receive_timeout(config_.connection_timeout_ms);
            socket_->set_send_timeout(config_.connection_timeout_ms);

            // IMPORTANT: Connect in BLOCKING mode first
            LOG_DEBUG("CLIENT") << "Connecting to " << host_ << ":" << port_ << "...";

            if (!socket_->connect(host_, port_)) {
                LOG_ERROR("CLIENT") << "Failed to connect to " << host_ << ":" << port_
                    << " - Error: " << Socket::get_last_error_string();
                return false;
            }

            LOG_DEBUG("CLIENT") << "TCP connection established";

            // NOW we can set non-blocking AFTER successful connection
            socket_->set_non_blocking(true);

            // TLS handshake if needed
            if (config_.use_tls) {
                auto tls_socket = dynamic_cast<TLSSocket*>(socket_.get());
                if (tls_socket) {
                    if (!tls_socket->enable_tls(config_.tls_config, false)) {
                        LOG_ERROR("CLIENT") << "Failed to enable TLS";
                        return false;
                    }

                    if (!tls_socket->perform_handshake(false)) {
                        LOG_ERROR("CLIENT") << "TLS handshake failed";
                        return false;
                    }

                    LOG_INFO("CLIENT") << "TLS connection established";
                    LOG_DEBUG("CLIENT") << "Cipher: " << tls_socket->get_cipher();
                    LOG_DEBUG("CLIENT") << "Peer cert: " << tls_socket->get_peer_certificate_info();
                }
            }

            // WebSocket upgrade if needed
            if (config_.use_websocket) {
                if (!perform_websocket_handshake()) {
                    LOG_ERROR("CLIENT") << "WebSocket handshake failed";
                    return false;
                }
            }

            return true;
        }

        bool perform_websocket_handshake() {
            // Generate WebSocket key
            std::string ws_key = generate_websocket_key();

            // Build HTTP upgrade request
            std::stringstream request;
            request << "GET " << config_.websocket_path << " HTTP/1.1\r\n";
            request << "Host: " << host_ << ":" << port_ << "\r\n";
            request << "Upgrade: websocket\r\n";
            request << "Connection: Upgrade\r\n";
            request << "Sec-WebSocket-Key: " << ws_key << "\r\n";
            request << "Sec-WebSocket-Version: 13\r\n";
            request << "Sec-WebSocket-Protocol: mqtt\r\n";

            // Add custom headers
            for (const auto& [key, value] : config_.websocket_headers) {
                request << key << ": " << value << "\r\n";
            }

            request << "\r\n";

            // Send request
            std::string request_str = request.str();
            if (!send_raw(request_str.data(), request_str.length())) {
                return false;
            }

            // Read response
            char response[4096];
            size_t received = 0;
            Timer timeout;

            while (received < sizeof(response) && !timeout.has_expired(5000)) {
                int r = socket_->receive(response + received,
                    sizeof(response) - received - 1);
                if (r > 0) {
                    received += r;
                    response[received] = '\0';

                    // Check if we have complete headers
                    if (strstr(response, "\r\n\r\n")) {
                        break;
                    }
                }
                Time::sleep_ms(10);
            }

            // Verify response
            if (!strstr(response, "HTTP/1.1 101")) {
                LOG_ERROR("CLIENT") << "WebSocket upgrade failed: " << response;
                return false;
            }

            LOG_INFO("CLIENT") << "WebSocket connection established";
            return true;
        }

        std::string generate_websocket_key() {
            // Generate 16 random bytes and base64 encode
            uint8_t random_bytes[16];
            for (int i = 0; i < 16; i++) {
                random_bytes[i] = rand() % 256;
            }

            // Simple base64 encoding
            static const char* b64_chars =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

            std::string result;
            for (int i = 0; i < 16; i += 3) {
                uint32_t triple = (random_bytes[i] << 16);
                if (i + 1 < 16) triple |= (random_bytes[i + 1] << 8);
                if (i + 2 < 16) triple |= random_bytes[i + 2];

                for (int j = 0; j < 4 && i + j * 3 / 4 < 16; j++) {
                    result += b64_chars[(triple >> (18 - j * 6)) & 0x3F];
                }
            }

            // Add padding
            while (result.length() % 4) {
                result += '=';
            }

            return result;
        }

        // Packet handling
        bool send_connect_packet() {
            LOG_DEBUG("CLIENT") << "Preparing CONNECT packet...";

            ConnectPacket connect_pkt;
            connect_pkt.set_client_id(config_.client_id);
            connect_pkt.set_keep_alive(config_.keep_alive);
            connect_pkt.set_clean_session(config_.clean_session);

            // Usa le credenziali dalla configurazione
            if (!config_.username.empty()) {
                connect_pkt.set_username(config_.username);
            }
            if (!config_.password.empty()) {
                connect_pkt.set_password(config_.password);
            }

            // Configura will message se presente
            if (config_.has_will) {
                connect_pkt.set_will_topic(config_.will_topic);
                connect_pkt.set_will_message(config_.will_payload.data(), config_.will_payload.size());
                connect_pkt.set_will_qos(config_.will_qos);
                connect_pkt.set_will_retain(config_.will_retain);
            }

            Buffer buffer;
            connect_pkt.serialize(buffer);

            LOG_DEBUG("CLIENT") << "Sending CONNECT packet (" << buffer.size() << " bytes)...";

            bool result = send_packet(buffer);

            if (result) {
                LOG_DEBUG("CLIENT") << "CONNECT packet sent successfully";
            }
            else {
                LOG_ERROR("CLIENT") << "Failed to send CONNECT packet";
            }

            return result;
        }

        bool wait_for_connack(int timeout_ms) {
            uint8_t buffer[256];
            size_t received = 0;
            Timer timeout;

            LOG_DEBUG("CLIENT") << "Waiting for CONNACK...";

            while (!timeout.has_expired(timeout_ms)) {
                int r = socket_->receive(buffer + received, sizeof(buffer) - received);

                if (r > 0) {
                    received += r;
                    LOG_DEBUG("CLIENT") << "Received " << r << " bytes (total: " << received << ")";

                    // Check if we have enough for CONNACK (minimum 4 bytes)
                    if (received >= 4) {
                        // Check packet type
                        uint8_t packet_type = (buffer[0] >> 4);
                        if (packet_type != CONNACK) {
                            LOG_ERROR("CLIENT") << "Expected CONNACK (0x02), got 0x"
                                << std::hex << static_cast<int>(packet_type);
                            return false;
                        }

                        // Parse CONNACK
                        uint8_t remaining_length = buffer[1];
                        if (remaining_length != 2) {
                            LOG_ERROR("CLIENT") << "Invalid CONNACK remaining length: "
                                << static_cast<int>(remaining_length);
                            return false;
                        }

                        uint8_t flags = buffer[2];
                        uint8_t return_code = buffer[3];

                        bool session_present = (flags & 0x01) != 0;

                        if (return_code != 0) {
                            handle_connack_error(return_code);
                            return false;
                        }

                        LOG_INFO("CLIENT") << "CONNACK received successfully"
                            << " (session present: "
                            << (session_present ? "yes" : "no") << ")";

                        if (session_present) {
                            fire_event(ClientEvent::SESSION_RESUMED, "Session resumed");
                        }

                        return true;
                    }
                }
                else if (r == 0) {
                    // Connection closed by peer
                    LOG_ERROR("CLIENT") << "Connection closed while waiting for CONNACK";
                    return false;
                }
                else {
                    // r < 0 - check if it's just EWOULDBLOCK
                    if (!socket_->would_block()) {
                        LOG_ERROR("CLIENT") << "Socket error while waiting for CONNACK: "
                            << Socket::get_last_error_string();
                        return false;
                    }
                    // If it's EWOULDBLOCK/EAGAIN, just continue waiting
                    }

                // Small delay to avoid busy waiting
                Time::sleep_ms(10);
                }

            LOG_ERROR("CLIENT") << "CONNACK timeout after " << timeout_ms << "ms";
            return false;
            }

        void handle_connack_error(uint8_t code) {
            const char* error_msg = "";
            switch (code) {
            case 1: error_msg = "Unacceptable protocol version"; break;
            case 2: error_msg = "Identifier rejected"; break;
            case 3: error_msg = "Server unavailable"; break;
            case 4: error_msg = "Bad username or password"; break;
            case 5: error_msg = "Not authorized"; break;
            default: error_msg = "Unknown error"; break;
            }

            LOG_ERROR("CLIENT") << "Connection refused: " << error_msg;
            fire_event(ClientEvent::CLIENT_ERROR, std::string("Connection refused: ") + error_msg);
        }

        void send_disconnect_packet(uint32_t reason_code) {
            Buffer buffer;
            buffer.write_byte(DISCONNECT << 4);

            if (reason_code != 0) {
                // MQTT 5.0 with reason code
                buffer.write_byte(1);
                buffer.write_byte(static_cast<uint8_t>(reason_code));
            }
            else {
                // MQTT 3.1.1 or normal disconnect
                buffer.write_byte(0);
            }

            send_packet(buffer);
        }

        // Background threads
        void start_background_threads() {
            // Receiver thread
            receiver_thread_.start([this](std::atomic<bool>& should_stop) {
                receiver_loop(should_stop);
                });

            // Keep-alive thread
            keep_alive_thread_.start([this](std::atomic<bool>& should_stop) {
                keep_alive_loop(should_stop);
                });

            // Message processor thread
            message_processor_thread_.start([this](std::atomic<bool>& should_stop) {
                message_processor_loop(should_stop);
                });
        }

        void stop_background_threads() {
            shutting_down_ = true;

            receiver_thread_.stop();
            keep_alive_thread_.stop();
            message_processor_thread_.stop();
            reconnect_thread_.stop();
        }

        void receiver_loop(std::atomic<bool>& should_stop) {
            uint8_t buffer[8192];

            while (!should_stop && !shutting_down_ && state_machine_->is_connected()) {
                int received = 0;

                {
                    std::shared_lock<std::shared_mutex> lock(socket_mutex_);
                    if (socket_) {
                        received = socket_->receive(buffer, sizeof(buffer));
                    }
                }

                if (received > 0) {
                    metrics_->record_bytes_received(received);
                    receive_buffer_.write_bytes(buffer, received);
                    process_received_packets();

                }
                else if (received == 0) {
                    // Connection closed
                    LOG_WARN("CLIENT") << "Connection closed by server";
                    handle_connection_lost();
                    break;

                }
                else {
                    // Check for errors
                    if (!socket_->would_block()) {
                        LOG_ERROR("CLIENT") << "Socket error";
                        handle_connection_lost();
                        break;
                    }

                    Time::sleep_ms(1);
                }
            }
        }

        void keep_alive_loop(std::atomic<bool>& should_stop) {
            Timer ping_timer;

            while (!should_stop && !shutting_down_) {
                if (state_machine_->is_connected()) {
                    if (ping_timer.elapsed_ms() > (config_.keep_alive * 1000 / 2)) {
                        send_ping();
                        ping_timer.reset();
                    }
                }

                Time::sleep_ms(1000);
            }
        }

        void message_processor_loop(std::atomic<bool>& should_stop) {
            while (!should_stop && !shutting_down_) {
                if (state_machine_->is_connected()) {
                    process_queued_messages();
                    check_timeouts();
                }

                Time::sleep_ms(100);
            }
        }

        // Packet processing
        void process_received_packets() {
            while (receive_buffer_.available() >= 2) {
                size_t start_pos = receive_buffer_.mark_read_position();

                uint8_t first_byte = receive_buffer_.peek_byte();
                PacketType type = static_cast<PacketType>(first_byte >> 4);

                if (type < CONNACK || type > DISCONNECT) {
                    LOG_WARN("CLIENT") << "Invalid packet type: " << static_cast<int>(type);
                    receive_buffer_.read_byte();
                    continue;
                }

                receive_buffer_.read_byte();

                uint32_t remaining_length = 0;
                if (!receive_buffer_.read_variable_length(remaining_length)) {
                    receive_buffer_.reset_read_position(start_pos);
                    break;
                }

                if (receive_buffer_.available() < remaining_length) {
                    receive_buffer_.reset_read_position(start_pos);
                    break;
                }

                try {
                    switch (type) {
                    case PUBLISH:
                        handle_publish(first_byte, remaining_length);
                        break;

                    case PUBACK:
                        handle_puback();
                        break;

                    case PUBREC:
                        handle_pubrec();
                        break;

                    case PUBREL:
                        handle_pubrel();
                        break;

                    case PUBCOMP:
                        handle_pubcomp();
                        break;

                    case SUBACK:
                        handle_suback(remaining_length);
                        break;

                    case UNSUBACK:
                        handle_unsuback();
                        break;

                    case PINGRESP:
                        // Ping response received
                        LOG_DEBUG("CLIENT") << "PINGRESP received";
                        break;

                    case DISCONNECT:
                        handle_disconnect_packet(remaining_length);
                        break;

                    default:
                        // Skip unknown packet
                        for (uint32_t i = 0; i < remaining_length; i++) {
                            receive_buffer_.read_byte();
                        }
                        break;
                    }
                }
                catch (const std::exception& e) {
                    LOG_ERROR("CLIENT") << "Error processing packet: " << e.what();
                    receive_buffer_.skip(remaining_length);
                }
            }

            receive_buffer_.compact();
        }

        void handle_publish(uint8_t flags, uint32_t remaining_length) {
            QoS qos = static_cast<QoS>((flags >> 1) & 0x03);
            bool retain = (flags & 0x01) != 0;
            bool dup = (flags & 0x08) != 0;

            std::string topic = receive_buffer_.read_string();

            uint16_t packet_id = 0;
            if (qos > QOS_0) {
                packet_id = receive_buffer_.read_uint16();
            }

            size_t header_size = 2 + topic.length() + (qos > QOS_0 ? 2 : 0);
            size_t payload_size = remaining_length - header_size;

            std::vector<uint8_t> payload(payload_size);
            receive_buffer_.read_bytes(payload.data(), payload_size);

            // Send acknowledgment
            if (qos == QOS_1) {
                send_puback(packet_id);
            }
            else if (qos == QOS_2) {
                send_pubrec(packet_id);
                pending_pubrec_.insert(packet_id);
            }

            metrics_->record_message_received();

            // Deliver message to handlers
            deliver_message(topic, payload, qos, retain);
        }

        void handle_puback() {
            uint16_t packet_id = receive_buffer_.read_uint16();

            LOG_DEBUG("CLIENT") << "PUBACK received for packet " << packet_id;

            auto msg = flow_controller_->get_inflight(packet_id);
            if (msg) {
                msg->promise.set_value(true);
                flow_controller_->remove_inflight(packet_id);
            }
        }

        void handle_pubrec() {
            uint16_t packet_id = receive_buffer_.read_uint16();

            LOG_DEBUG("CLIENT") << "PUBREC received for packet " << packet_id;

            // Send PUBREL
            send_pubrel(packet_id);
            pending_pubrel_.insert(packet_id);

            flow_controller_->remove_inflight(packet_id);
        }

        void handle_pubrel() {
            uint16_t packet_id = receive_buffer_.read_uint16();

            LOG_DEBUG("CLIENT") << "PUBREL received for packet " << packet_id;

            // Send PUBCOMP
            send_pubcomp(packet_id);
            pending_pubrec_.erase(packet_id);
        }

        void handle_pubcomp() {
            uint16_t packet_id = receive_buffer_.read_uint16();

            LOG_DEBUG("CLIENT") << "PUBCOMP received for packet " << packet_id;

            pending_pubrel_.erase(packet_id);

            auto msg = flow_controller_->get_inflight(packet_id);
            if (msg) {
                msg->promise.set_value(true);
            }
        }

        void handle_suback(uint32_t remaining_length) {
            uint16_t packet_id = receive_buffer_.read_uint16();
            remaining_length -= 2;

            std::vector<uint8_t> return_codes;
            while (remaining_length > 0) {
                return_codes.push_back(receive_buffer_.read_byte());
                remaining_length--;
            }

            std::shared_ptr<SubscriptionRequest> request;
            {
                std::unique_lock<std::shared_mutex> lock(pending_mutex_);
                auto it = pending_subscribes_.find(packet_id);
                if (it != pending_subscribes_.end()) {
                    request = it->second;
                    pending_subscribes_.erase(it);
                }
            }

            if (request) {
                bool success = !return_codes.empty() && return_codes[0] != 0x80;

                if (success) {
                    active_subscriptions_.insert(request->topic);
                    subscription_qos_[request->topic] = request->qos;
                    fire_event(ClientEvent::SUBSCRIBE_SUCCESS,
                        "Subscribed to " + request->topic);
                }
                else {
                    fire_event(ClientEvent::SUBSCRIBE_FAILURE,
                        "Failed to subscribe to " + request->topic);
                }

                request->promise.set_value(success);
            }
        }

        void handle_unsuback() {
            uint16_t packet_id = receive_buffer_.read_uint16();

            std::shared_ptr<SubscriptionRequest> request;
            {
                std::unique_lock<std::shared_mutex> lock(pending_mutex_);
                auto it = pending_unsubscribes_.find(packet_id);
                if (it != pending_unsubscribes_.end()) {
                    request = it->second;
                    pending_unsubscribes_.erase(it);
                }
            }

            if (request) {
                active_subscriptions_.erase(request->topic);
                subscription_qos_.erase(request->topic);
                fire_event(ClientEvent::UNSUBSCRIBE_SUCCESS,
                    "Unsubscribed from " + request->topic);
                request->promise.set_value(true);
            }
        }

        void handle_disconnect_packet(uint32_t remaining_length) {
            uint8_t reason_code = 0;

            if (remaining_length > 0) {
                reason_code = receive_buffer_.read_byte();
            }

            LOG_INFO("CLIENT") << "Server sent DISCONNECT with reason: "
                << static_cast<int>(reason_code);

            handle_connection_lost();
        }

        // Message delivery
        void deliver_message(const std::string& topic,
            const std::vector<uint8_t>& payload,
            QoS qos,
            bool retain) {

            std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);

            // Global message handler
            if (on_message_) {
                on_message_(topic, payload, qos, retain);
            }

            // Topic-specific handlers
            for (const auto& [filter, handler] : topic_handlers_) {
                if (topic_matches(topic, filter)) {
                    handler(topic, payload);
                }
            }

            fire_event(ClientEvent::MESSAGE_RECEIVED, "Message on " + topic);
        }

        bool topic_matches(const std::string& topic, const std::string& filter) {
            // Simple pattern matching for + and #
            if (filter == topic) return true;
            if (filter == "#") return true;

            // TODO: Implement full MQTT topic matching

            return false;
        }

        // Connection management
        void handle_connection_lost() {
            state_machine_->transition_to(ConnectionState::RECONNECTING);

            fire_event(ClientEvent::CONNECTION_LOST, "Connection lost");

            disconnect_internal();

            if (config_.auto_reconnect) {
                start_reconnect();
            }
            else {
                state_machine_->transition_to(ConnectionState::DISCONNECTED);
            }
        }

        void disconnect_internal() {
            {
                std::unique_lock<std::shared_mutex> lock(socket_mutex_);
                if (socket_) {
                    socket_->close();
                    socket_.reset();
                }
            }

            flow_controller_->clear();
        }

        void start_reconnect() {
            if (reconnect_thread_.is_running()) {
                return;
            }

            reconnect_thread_.start([this](std::atomic<bool>& should_stop) {
                reconnect_loop(should_stop);
                });
        }

        void reconnect_loop(std::atomic<bool>& should_stop) {
            while (!should_stop && !shutting_down_ && config_.auto_reconnect) {
                if (config_.max_reconnect_attempts > 0 &&
                    reconnect_attempts_ >= config_.max_reconnect_attempts) {
                    LOG_ERROR("CLIENT") << "Max reconnect attempts reached";
                    state_machine_->transition_to(ConnectionState::DISCONNECTED);
                    break;
                }

                reconnect_attempts_++;

                LOG_INFO("CLIENT") << "Reconnect attempt " << reconnect_attempts_
                    << " (delay: " << current_reconnect_delay_ms_ << "ms)";

                fire_event(ClientEvent::RECONNECTING,
                    "Attempt " + std::to_string(reconnect_attempts_));

                if (connect(host_, port_)) {
                    LOG_INFO("CLIENT") << "Reconnected successfully";
                    reconnect_attempts_ = 0;
                    current_reconnect_delay_ms_ = config_.initial_reconnect_delay_ms;
                    metrics_->record_reconnection();
                    break;
                }

                // Exponential backoff
                Time::sleep_ms(current_reconnect_delay_ms_);

                current_reconnect_delay_ms_ = std::min(
                    static_cast<int>(current_reconnect_delay_ms_ * config_.reconnect_backoff_multiplier),
                    config_.max_reconnect_delay_ms
                );
            }
        }

        void restore_subscriptions() {
            for (const auto& [topic, qos] : subscription_qos_) {
                subscribe_async(topic, qos);
            }
        }

        // Message queue processing
        void process_queued_messages() {
            while (flow_controller_->can_send()) {
                auto msg = flow_controller_->dequeue_message();
                if (!msg) {
                    break;
                }

                send_publish_packet(msg);
            }
        }

        void send_publish_packet(std::shared_ptr<QueuedMessage> msg) {
            PublishPacket pub_pkt;
            pub_pkt.set_topic(msg->topic);
            pub_pkt.set_payload(msg->payload.data(), msg->payload.size());
            pub_pkt.set_qos(msg->qos);
            pub_pkt.set_retain(msg->retain);

            if (msg->qos > QOS_0) {
                if (msg->packet_id == 0) {
                    msg->packet_id = get_next_packet_id();
                }
                pub_pkt.set_packet_id(msg->packet_id);
                pub_pkt.set_dup(msg->retry_count > 0);

                flow_controller_->add_inflight(msg->packet_id, msg);
            }

            Buffer buffer;
            pub_pkt.serialize(buffer);

            if (send_packet(buffer)) {
                metrics_->record_message_sent();

                if (msg->qos == QOS_0) {
                    msg->promise.set_value(true);
                }
            }
            else {
                if (msg->qos > QOS_0) {
                    flow_controller_->remove_inflight(msg->packet_id);
                }
                msg->promise.set_value(false);
            }
        }

        void check_timeouts() {
            auto now = std::chrono::steady_clock::now();

            // Check inflight messages
            auto inflight = flow_controller_->get_all_inflight();
            for (auto& msg : inflight) {
                if (now - msg->queued_at > std::chrono::seconds(30)) {
                    if (msg->retry_count < 3) {
                        msg->retry_count++;
                        send_publish_packet(msg);
                    }
                    else {
                        flow_controller_->remove_inflight(msg->packet_id);
                        msg->promise.set_value(false);
                        metrics_->record_publish_timeout();
                    }
                }
            }

            // Check pending subscriptions
            {
                std::unique_lock<std::shared_mutex> lock(pending_mutex_);
                std::vector<uint16_t> timed_out;

                for (const auto& [packet_id, request] : pending_subscribes_) {
                    if (now - request->sent_at > std::chrono::seconds(10)) {
                        timed_out.push_back(packet_id);
                    }
                }

                for (uint16_t packet_id : timed_out) {
                    auto request = pending_subscribes_[packet_id];
                    pending_subscribes_.erase(packet_id);
                    request->promise.set_value(false);
                }
            }
        }

        // Utility methods
        void send_ping() {
            PingReqPacket ping;
            Buffer buffer;
            ping.serialize(buffer);

            if (send_packet(buffer)) {
                LOG_DEBUG("CLIENT") << "PINGREQ sent";
            }
        }

        void send_puback(uint16_t packet_id) {
            PubAckPacket puback;
            puback.set_packet_id(packet_id);

            Buffer buffer;
            puback.serialize(buffer);
            send_packet(buffer);
        }

        void send_pubrec(uint16_t packet_id) {
            PubRecPacket pubrec;
            pubrec.set_packet_id(packet_id);

            Buffer buffer;
            pubrec.serialize(buffer);
            send_packet(buffer);
        }

        void send_pubrel(uint16_t packet_id) {
            PubRelPacket pubrel;
            pubrel.set_packet_id(packet_id);

            Buffer buffer;
            pubrel.serialize(buffer);
            send_packet(buffer);
        }

        void send_pubcomp(uint16_t packet_id) {
            PubCompPacket pubcomp;
            pubcomp.set_packet_id(packet_id);

            Buffer buffer;
            pubcomp.serialize(buffer);
            send_packet(buffer);
        }

        bool send_packet(const Buffer& buffer) {
            std::shared_lock<std::shared_mutex> lock(socket_mutex_);

            if (!socket_) {
                return false;
            }

            // Permetti l'invio durante CONNECTING (per CONNECT) 
            // e DISCONNECTING (per DISCONNECT)
            ConnectionState state = state_machine_->get_state();
            if (state == ConnectionState::DISCONNECTED ||
                state == ConnectionState::CONNECTION_ERROR) {
                return false;
            }

            int sent = socket_->send(buffer.data(), buffer.size());
            if (sent > 0) {
                metrics_->record_bytes_sent(sent);
                return true;
            }

            return false;
        }

        bool send_raw(const void* data, size_t len) {
            std::shared_lock<std::shared_mutex> lock(socket_mutex_);

            if (!socket_) {
                return false;
            }

            return socket_->send(data, len) > 0;
        }

        uint16_t get_next_packet_id() {
            uint16_t id = next_packet_id_.fetch_add(1);
            if (id == 0) {
                id = next_packet_id_.fetch_add(1);
            }
            return id;
        }

        std::string generate_client_id() {
            std::stringstream ss;
            ss << "mqtt_client_" << std::hex << Time::now_ms();
            return ss.str();
        }

        void fire_event(ClientEvent event, const std::string& details) {
            std::shared_lock<std::shared_mutex> lock(callbacks_mutex_);

            if (on_event_) {
                on_event_(event, details);
            }
        }

        void shutdown() {
            if (shutting_down_) {
                return;
            }

            shutting_down_ = true;

            disconnect();

            stop_background_threads();
        }
    };

} // namespace ourmqtt

#endif // OUR_MQTT_CLIENT_HPP