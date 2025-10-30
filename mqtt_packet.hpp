// mqtt_packet.hpp - Sistema Pacchetti MQTT Enterprise Production-Ready
#ifndef OUR_MQTT_PACKET_HPP
#define OUR_MQTT_PACKET_HPP

#include "buffer.hpp"
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <optional>
#include <variant>
#include <functional>
#include <stdexcept>
#include <cstring>

namespace ourmqtt {

    // MQTT Protocol versions
    enum class ProtocolVersion : uint8_t {
        MQTT_3_1 = 3,
        MQTT_3_1_1 = 4,
        MQTT_5_0 = 5
    };

    // Packet types
    enum PacketType : uint8_t {
        RESERVED_0 = 0,
        CONNECT = 1,
        CONNACK = 2,
        PUBLISH = 3,
        PUBACK = 4,
        PUBREC = 5,
        PUBREL = 6,
        PUBCOMP = 7,
        SUBSCRIBE = 8,
        SUBACK = 9,
        UNSUBSCRIBE = 10,
        UNSUBACK = 11,
        PINGREQ = 12,
        PINGRESP = 13,
        DISCONNECT = 14,
        AUTH = 15  // MQTT 5.0
    };

    // Quality of Service levels
    enum QoS : uint8_t {
        QOS_0 = 0,  // At most once
        QOS_1 = 1,  // At least once
        QOS_2 = 2   // Exactly once
    };

    // MQTT 5.0 Property identifiers
    enum class PropertyId : uint8_t {
        PAYLOAD_FORMAT_INDICATOR = 0x01,
        MESSAGE_EXPIRY_INTERVAL = 0x02,
        CONTENT_TYPE = 0x03,
        RESPONSE_TOPIC = 0x08,
        CORRELATION_DATA = 0x09,
        SUBSCRIPTION_IDENTIFIER = 0x0B,
        SESSION_EXPIRY_INTERVAL = 0x11,
        ASSIGNED_CLIENT_IDENTIFIER = 0x12,
        SERVER_KEEP_ALIVE = 0x13,
        AUTHENTICATION_METHOD = 0x15,
        AUTHENTICATION_DATA = 0x16,
        REQUEST_PROBLEM_INFORMATION = 0x17,
        WILL_DELAY_INTERVAL = 0x18,
        REQUEST_RESPONSE_INFORMATION = 0x19,
        RESPONSE_INFORMATION = 0x1A,
        SERVER_REFERENCE = 0x1C,
        REASON_STRING = 0x1F,
        RECEIVE_MAXIMUM = 0x21,
        TOPIC_ALIAS_MAXIMUM = 0x22,
        TOPIC_ALIAS = 0x23,
        MAXIMUM_QOS = 0x24,
        RETAIN_AVAILABLE = 0x25,
        USER_PROPERTY = 0x26,
        MAXIMUM_PACKET_SIZE = 0x27,
        WILDCARD_SUBSCRIPTION_AVAILABLE = 0x28,
        SUBSCRIPTION_IDENTIFIER_AVAILABLE = 0x29,
        SHARED_SUBSCRIPTION_AVAILABLE = 0x2A
    };

    // MQTT 5.0 Reason codes
    enum class ReasonCode : uint8_t {
        SUCCESS = 0x00,
        NORMAL_DISCONNECTION = 0x00,
        GRANTED_QOS_0 = 0x00,
        GRANTED_QOS_1 = 0x01,
        GRANTED_QOS_2 = 0x02,
        DISCONNECT_WITH_WILL = 0x04,
        NO_MATCHING_SUBSCRIBERS = 0x10,
        NO_SUBSCRIPTION_EXISTED = 0x11,
        CONTINUE_AUTHENTICATION = 0x18,
        RE_AUTHENTICATE = 0x19,
        UNSPECIFIED_ERROR = 0x80,
        MALFORMED_PACKET = 0x81,
        PROTOCOL_ERROR = 0x82,
        IMPLEMENTATION_SPECIFIC_ERROR = 0x83,
        UNSUPPORTED_PROTOCOL_VERSION = 0x84,
        CLIENT_IDENTIFIER_NOT_VALID = 0x85,
        BAD_USER_NAME_OR_PASSWORD = 0x86,
        NOT_AUTHORIZED = 0x87,
        SERVER_UNAVAILABLE = 0x88,
        SERVER_BUSY = 0x89,
        BANNED = 0x8A,
        SERVER_SHUTTING_DOWN = 0x8B,
        BAD_AUTHENTICATION_METHOD = 0x8C,
        KEEP_ALIVE_TIMEOUT = 0x8D,
        SESSION_TAKEN_OVER = 0x8E,
        TOPIC_FILTER_INVALID = 0x8F,
        TOPIC_NAME_INVALID = 0x90,
        PACKET_IDENTIFIER_IN_USE = 0x91,
        PACKET_IDENTIFIER_NOT_FOUND = 0x92,
        RECEIVE_MAXIMUM_EXCEEDED = 0x93,
        TOPIC_ALIAS_INVALID = 0x94,
        PACKET_TOO_LARGE = 0x95,
        MESSAGE_RATE_TOO_HIGH = 0x96,
        QUOTA_EXCEEDED = 0x97,
        ADMINISTRATIVE_ACTION = 0x98,
        PAYLOAD_FORMAT_INVALID = 0x99,
        RETAIN_NOT_SUPPORTED = 0x9A,
        QOS_NOT_SUPPORTED = 0x9B,
        USE_ANOTHER_SERVER = 0x9C,
        SERVER_MOVED = 0x9D,
        SHARED_SUBSCRIPTIONS_NOT_SUPPORTED = 0x9E,
        CONNECTION_RATE_EXCEEDED = 0x9F,
        MAXIMUM_CONNECT_TIME = 0xA0,
        SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 0xA1,
        WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED = 0xA2
    };

    // Property value types
    using PropertyValue = std::variant<
        uint8_t,
        uint16_t,
        uint32_t,
        std::string,
        std::vector<uint8_t>,
        std::pair<std::string, std::string>  // User property
    >;

    // Properties container
    class Properties {
    private:
        std::multimap<PropertyId, PropertyValue> properties_;

    public:
        void add(PropertyId id, PropertyValue value) {
            properties_.emplace(id, std::move(value));
        }

        template<typename T>
        std::optional<T> get(PropertyId id) const {
            auto it = properties_.find(id);
            if (it != properties_.end()) {
                if (auto val = std::get_if<T>(&it->second)) {
                    return *val;
                }
            }
            return std::nullopt;
        }

        template<typename T>
        std::vector<T> get_all(PropertyId id) const {
            std::vector<T> result;
            auto range = properties_.equal_range(id);
            for (auto it = range.first; it != range.second; ++it) {
                if (auto val = std::get_if<T>(&it->second)) {
                    result.push_back(*val);
                }
            }
            return result;
        }

        bool has(PropertyId id) const {
            return properties_.find(id) != properties_.end();
        }

        void clear() {
            properties_.clear();
        }

        size_t serialize_size() const {
            size_t size = 0;
            for (const auto& [id, value] : properties_) {
                size += 1; // Property ID
                size += get_property_size(value);
            }
            return size;
        }

        void serialize(Buffer& buffer) const {
            for (const auto& [id, value] : properties_) {
                buffer.write_byte(static_cast<uint8_t>(id));
                serialize_property(buffer, value);
            }
        }

        bool deserialize(Buffer& buffer, size_t properties_length) {
            size_t start_pos = buffer.position();

            while (buffer.position() - start_pos < properties_length) {
                if (buffer.available() < 1) return false;

                PropertyId id = static_cast<PropertyId>(buffer.read_byte());

                if (!deserialize_property(buffer, id)) {
                    return false;
                }
            }

            return true;
        }

    private:
        size_t get_property_size(const PropertyValue& value) const {
            return std::visit([](auto&& val) -> size_t {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, uint8_t>) {
                    return 1;
                }
                else if constexpr (std::is_same_v<T, uint16_t>) {
                    return 2;
                }
                else if constexpr (std::is_same_v<T, uint32_t>) {
                    return 4;
                }
                else if constexpr (std::is_same_v<T, std::string>) {
                    return 2 + val.length();
                }
                else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    return 2 + val.size();
                }
                else if constexpr (std::is_same_v<T, std::pair<std::string, std::string>>) {
                    return 4 + val.first.length() + val.second.length();
                }
                return 0;
                }, value);
        }

        void serialize_property(Buffer& buffer, const PropertyValue& value) const {
            std::visit([&buffer](auto&& val) {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, uint8_t>) {
                    buffer.write_byte(val);
                }
                else if constexpr (std::is_same_v<T, uint16_t>) {
                    buffer.write_uint16(val);
                }
                else if constexpr (std::is_same_v<T, uint32_t>) {
                    buffer.write_uint32(val);
                }
                else if constexpr (std::is_same_v<T, std::string>) {
                    buffer.write_string(val);
                }
                else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
                    buffer.write_uint16(val.size());
                    buffer.write_bytes(val.data(), val.size());
                }
                else if constexpr (std::is_same_v<T, std::pair<std::string, std::string>>) {
                    buffer.write_string(val.first);
                    buffer.write_string(val.second);
                }
                }, value);
        }

        bool deserialize_property(Buffer& buffer, PropertyId id) {
            switch (id) {
            case PropertyId::PAYLOAD_FORMAT_INDICATOR:
            case PropertyId::REQUEST_PROBLEM_INFORMATION:
            case PropertyId::REQUEST_RESPONSE_INFORMATION:
            case PropertyId::MAXIMUM_QOS:
            case PropertyId::RETAIN_AVAILABLE:
            case PropertyId::WILDCARD_SUBSCRIPTION_AVAILABLE:
            case PropertyId::SUBSCRIPTION_IDENTIFIER_AVAILABLE:
            case PropertyId::SHARED_SUBSCRIPTION_AVAILABLE:
                if (buffer.available() < 1) return false;
                add(id, buffer.read_byte());
                break;

            case PropertyId::SERVER_KEEP_ALIVE:
            case PropertyId::RECEIVE_MAXIMUM:
            case PropertyId::TOPIC_ALIAS_MAXIMUM:
            case PropertyId::TOPIC_ALIAS:
                if (buffer.available() < 2) return false;
                add(id, buffer.read_uint16());
                break;

            case PropertyId::MESSAGE_EXPIRY_INTERVAL:
            case PropertyId::SESSION_EXPIRY_INTERVAL:
            case PropertyId::WILL_DELAY_INTERVAL:
            case PropertyId::MAXIMUM_PACKET_SIZE:
                if (buffer.available() < 4) return false;
                add(id, buffer.read_uint32());
                break;

            case PropertyId::CONTENT_TYPE:
            case PropertyId::RESPONSE_TOPIC:
            case PropertyId::ASSIGNED_CLIENT_IDENTIFIER:
            case PropertyId::AUTHENTICATION_METHOD:
            case PropertyId::RESPONSE_INFORMATION:
            case PropertyId::SERVER_REFERENCE:
            case PropertyId::REASON_STRING:
                add(id, buffer.read_string());
                break;

            case PropertyId::CORRELATION_DATA:
            case PropertyId::AUTHENTICATION_DATA: {
                uint16_t len = buffer.read_uint16();
                std::vector<uint8_t> data(len);
                buffer.read_bytes(data.data(), len);
                add(id, data);
                break;
            }

            case PropertyId::USER_PROPERTY: {
                std::string key = buffer.read_string();
                std::string value = buffer.read_string();
                add(id, std::make_pair(key, value));
                break;
            }

            case PropertyId::SUBSCRIPTION_IDENTIFIER: {
                uint32_t value = 0;
                if (!buffer.read_variable_length(value)) return false;
                add(id, value);
                break;
            }

            default:
                return false; // Unknown property
            }

            return true;
        }
    };

    // Packet validation
    class PacketValidator {
    public:
        static bool validate_topic(const std::string& topic, bool is_filter = false) {
            if (topic.empty()) return false;
            if (topic.length() > 65535) return false;

            // Check for null characters
            if (topic.find('\0') != std::string::npos) return false;

            // Check wildcards
            size_t plus_pos = topic.find('+');
            size_t hash_pos = topic.find('#');

            if (!is_filter) {
                // Topics cannot contain wildcards
                if (plus_pos != std::string::npos || hash_pos != std::string::npos) {
                    return false;
                }
            }
            else {
                // Validate wildcard usage in filters
                if (hash_pos != std::string::npos) {
                    // # must be last character and preceded by /
                    if (hash_pos != topic.length() - 1) return false;
                    if (hash_pos > 0 && topic[hash_pos - 1] != '/') return false;
                }

                // + must be alone in its level
                size_t pos = 0;
                while ((pos = topic.find('+', pos)) != std::string::npos) {
                    bool valid = (pos == 0 || topic[pos - 1] == '/') &&
                        (pos == topic.length() - 1 || topic[pos + 1] == '/');
                    if (!valid) return false;
                    pos++;
                }
            }

            return true;
        }

        static bool validate_client_id(const std::string& client_id) {
            if (client_id.length() > 65535) return false;

            // MQTT 3.1.1 allows empty client ID with clean session
            // Characters should be 0-9, a-z, A-Z
            for (char c : client_id) {
                if (!std::isalnum(c) && c != '-' && c != '_') {
                    return false;
                }
            }

            return true;
        }

        static bool validate_qos(uint8_t qos) {
            return qos <= 2;
        }

        static bool validate_packet_id(uint16_t packet_id, QoS qos) {
            if (qos == QOS_0) {
                return packet_id == 0;
            }
            else {
                return packet_id != 0;
            }
        }
    };

    // Base packet class
    class MqttPacket {
    protected:
        PacketType type_;
        ProtocolVersion version_;
        Properties properties_;

    public:
        MqttPacket(PacketType type, uint8_t flags = 0, ProtocolVersion version = ProtocolVersion::MQTT_3_1_1)
            : type_(type), flags_(flags), version_(version) {}
        uint8_t flags_;
        virtual ~MqttPacket() = default;

        // Serialization
        virtual void serialize(Buffer& buffer) const = 0;
        virtual bool deserialize(Buffer& buffer, uint32_t remaining_length) = 0;

        // Validation
        virtual bool validate() const = 0;

        // Getters
        PacketType type() const { return type_; }
        uint8_t flags() const { return flags_; }
        ProtocolVersion version() const { return version_; }
        const Properties& properties() const { return properties_; }
        Properties& properties() { return properties_; }

        // Setters
        void set_version(ProtocolVersion v) { version_ = v; }

        // Helper methods
        static std::string type_to_string(PacketType type) {
            switch (type) {
            case CONNECT: return "CONNECT";
            case CONNACK: return "CONNACK";
            case PUBLISH: return "PUBLISH";
            case PUBACK: return "PUBACK";
            case PUBREC: return "PUBREC";
            case PUBREL: return "PUBREL";
            case PUBCOMP: return "PUBCOMP";
            case SUBSCRIBE: return "SUBSCRIBE";
            case SUBACK: return "SUBACK";
            case UNSUBSCRIBE: return "UNSUBSCRIBE";
            case UNSUBACK: return "UNSUBACK";
            case PINGREQ: return "PINGREQ";
            case PINGRESP: return "PINGRESP";
            case DISCONNECT: return "DISCONNECT";
            case AUTH: return "AUTH";
            default: return "UNKNOWN";
            }
        }

    protected:
        void write_fixed_header(Buffer& buffer, size_t remaining_length) const {
            buffer.write_byte((type_ << 4) | (flags_ & 0x0F));
            buffer.write_variable_length(remaining_length);
        }

        size_t calculate_properties_length() const {
            if (version_ < ProtocolVersion::MQTT_5_0) {
                return 0;
            }
            return properties_.serialize_size();
        }

        void write_properties(Buffer& buffer) const {
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                size_t props_len = properties_.serialize_size();
                buffer.write_variable_length(props_len);
                properties_.serialize(buffer);
            }
        }

        bool read_properties(Buffer& buffer) {
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                uint32_t props_len = 0;
                if (!buffer.read_variable_length(props_len)) {
                    return false;
                }
                return properties_.deserialize(buffer, props_len);
            }
            return true;
        }
    };

    // CONNECT packet
    class ConnectPacket : public MqttPacket {
    private:
        // Fixed header
        std::string protocol_name_ = "MQTT";
        uint8_t protocol_level_ = 4;  // MQTT 3.1.1

        // Variable header
        uint8_t connect_flags_ = 0;
        uint16_t keep_alive_ = 60;

        // Payload
        std::string client_id_;
        std::optional<std::pair<std::string, std::vector<uint8_t>>> will_message_;
        std::optional<std::string> username_;
        std::optional<std::string> password_;

        // Flags
        bool clean_session_ = true;
        QoS will_qos_ = QOS_0;
        bool will_retain_ = false;

    public:
        ConnectPacket() : MqttPacket(CONNECT) {}

        // Setters
        void set_client_id(const std::string& id) {
            client_id_ = id;
        }

        void set_credentials(const std::string& user, const std::string& pass) {
            username_ = user;
            password_ = pass;
        }

        void set_keep_alive(uint16_t seconds) {
            keep_alive_ = seconds;
        }

        void set_clean_session(bool clean) {
            clean_session_ = clean;
        }

        void set_will(const std::string& topic,
            const std::vector<uint8_t>& payload,
            QoS qos = QOS_0,
            bool retain = false) {
            will_message_ = { topic, payload };
            will_qos_ = qos;
            will_retain_ = retain;
        }

        void set_protocol_version(ProtocolVersion v) {
            version_ = v;
            switch (v) {
            case ProtocolVersion::MQTT_3_1:
                protocol_name_ = "MQIsdp";
                protocol_level_ = 3;
                break;
            case ProtocolVersion::MQTT_3_1_1:
                protocol_name_ = "MQTT";
                protocol_level_ = 4;
                break;
            case ProtocolVersion::MQTT_5_0:
                protocol_name_ = "MQTT";
                protocol_level_ = 5;
                break;
            }
        }

        // Metodi aggiuntivi per compatibilità
        void set_username(const std::string& user) {
            username_ = user;
        }

        void set_password(const std::string& pass) {
            password_ = pass;
        }

        void set_will_topic(const std::string& topic) {
            if (!will_message_.has_value()) {
                will_message_ = { topic, std::vector<uint8_t>() };
            }
            else {
                will_message_->first = topic;
            }
        }

        void set_will_message(const uint8_t* data, size_t len) {
            if (!will_message_.has_value()) {
                will_message_ = { "", std::vector<uint8_t>(data, data + len) };
            }
            else {
                will_message_->second.assign(data, data + len);
            }
        }

        void set_will_qos(QoS qos) {
            will_qos_ = qos;
        }

        void set_will_retain(bool retain) {
            will_retain_ = retain;
        }

        // Getters
        const std::string& get_client_id() const { return client_id_; }
        const std::optional<std::string>& get_username() const { return username_; }
        uint16_t get_keep_alive() const { return keep_alive_; }
        bool is_clean_session() const { return clean_session_; }

        // Getter aggiuntivi per compatibilità
        bool has_username() const { return username_.has_value(); }
        bool has_password() const { return password_.has_value(); }
        bool has_will() const { return will_message_.has_value(); }
        const std::string& get_username_value() const {
            static std::string empty;
            return username_.has_value() ? username_.value() : empty;
        }
        const std::string& get_password_value() const {
            static std::string empty;
            return password_.has_value() ? password_.value() : empty;
        }

        // Serialization
        void serialize(Buffer& buffer) const override {
            Buffer payload;

            // Protocol name and level
            payload.write_string(protocol_name_);
            payload.write_byte(protocol_level_);

            // Connect flags
            uint8_t flags = 0;
            if (clean_session_) flags |= 0x02;
            if (will_message_.has_value()) {
                flags |= 0x04;
                flags |= (will_qos_ << 3);
                if (will_retain_) flags |= 0x20;
            }
            if (username_.has_value()) flags |= 0x80;
            if (password_.has_value()) flags |= 0x40;
            payload.write_byte(flags);

            // Keep alive
            payload.write_uint16(keep_alive_);

            // Properties (MQTT 5.0)
            write_properties(payload);

            // Payload
            payload.write_string(client_id_);

            if (will_message_.has_value()) {
                // Will properties (MQTT 5.0)
                if (version_ >= ProtocolVersion::MQTT_5_0) {
                    payload.write_variable_length(0); // No will properties for now
                }
                payload.write_string(will_message_->first);
                payload.write_uint16(static_cast<uint16_t>(will_message_->second.size()));
                payload.write_bytes(will_message_->second.data(),
                    will_message_->second.size());
            }

            if (username_.has_value()) {
                payload.write_string(username_.value());
            }

            if (password_.has_value()) {
                payload.write_uint16(static_cast<uint16_t>(password_->length()));
                payload.write_bytes(reinterpret_cast<const uint8_t*>(password_->data()),
                    password_->length());
            }

            // Write to buffer
            write_fixed_header(buffer, payload.size());
            buffer.write_bytes(payload.data(), payload.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            size_t start_pos = buffer.position();

            // Protocol name
            protocol_name_ = buffer.read_string();

            // Protocol level
            protocol_level_ = buffer.read_byte();

            // Set version based on protocol level
            switch (protocol_level_) {
            case 3:
                version_ = ProtocolVersion::MQTT_3_1;
                break;
            case 4:
                version_ = ProtocolVersion::MQTT_3_1_1;
                break;
            case 5:
                version_ = ProtocolVersion::MQTT_5_0;
                break;
            default:
                return false; // Unsupported version
            }

            // Connect flags
            connect_flags_ = buffer.read_byte();
            clean_session_ = (connect_flags_ & 0x02) != 0;
            bool has_will = (connect_flags_ & 0x04) != 0;
            will_qos_ = static_cast<QoS>((connect_flags_ >> 3) & 0x03);
            will_retain_ = (connect_flags_ & 0x20) != 0;
            bool has_username = (connect_flags_ & 0x80) != 0;
            bool has_password = (connect_flags_ & 0x40) != 0;

            // Keep alive
            keep_alive_ = buffer.read_uint16();

            // Properties (MQTT 5.0)
            if (!read_properties(buffer)) {
                return false;
            }

            // Client ID
            client_id_ = buffer.read_string();

            // Will message
            if (has_will) {
                // Will properties (MQTT 5.0)
                if (version_ >= ProtocolVersion::MQTT_5_0) {
                    uint32_t will_props_len = 0;
                    buffer.read_variable_length(will_props_len);
                    buffer.skip(will_props_len); // Skip will properties for now
                }

                std::string will_topic = buffer.read_string();
                uint16_t will_len = buffer.read_uint16();
                std::vector<uint8_t> will_payload(will_len);
                buffer.read_bytes(will_payload.data(), will_len);

                will_message_ = { will_topic, will_payload };
            }

            // Username
            if (has_username) {
                username_ = buffer.read_string();
            }

            // Password
            if (has_password) {
                uint16_t pass_len = buffer.read_uint16();
                std::string pass(pass_len, '\0');
                buffer.read_bytes(reinterpret_cast<uint8_t*>(pass.data()), pass_len);
                password_ = pass;
            }

            return (buffer.position() - start_pos) == remaining_length;
        }

        bool validate() const override {
            if (!PacketValidator::validate_client_id(client_id_)) {
                return false;
            }

            if (will_message_.has_value()) {
                if (!PacketValidator::validate_topic(will_message_->first)) {
                    return false;
                }
                if (!PacketValidator::validate_qos(will_qos_)) {
                    return false;
                }
            }

            return true;
        }
    };

    // CONNACK packet
    class ConnAckPacket : public MqttPacket {
    private:
        bool session_present_ = false;
        ReasonCode reason_code_ = ReasonCode::SUCCESS;

    public:
        ConnAckPacket() : MqttPacket(CONNACK) {}

        void set_session_present(bool present) { session_present_ = present; }
        void set_reason_code(ReasonCode code) { reason_code_ = code; }
        void set_return_code(uint8_t code) { // For MQTT 3.1.1 compatibility
            reason_code_ = static_cast<ReasonCode>(code);
        }

        bool is_session_present() const { return session_present_; }
        ReasonCode get_reason_code() const { return reason_code_; }
        uint8_t get_return_code() const { return static_cast<uint8_t>(reason_code_); }

        void serialize(Buffer& buffer) const override {
            Buffer payload;

            // Acknowledge flags
            payload.write_byte(session_present_ ? 0x01 : 0x00);

            // Reason code
            payload.write_byte(static_cast<uint8_t>(reason_code_));

            // Properties (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                write_properties(payload);
            }

            write_fixed_header(buffer, payload.size());
            buffer.write_bytes(payload.data(), payload.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            if (remaining_length < 2) return false;

            session_present_ = (buffer.read_byte() & 0x01) != 0;
            reason_code_ = static_cast<ReasonCode>(buffer.read_byte());

            if (version_ >= ProtocolVersion::MQTT_5_0 && remaining_length > 2) {
                return read_properties(buffer);
            }

            return true;
        }

        bool validate() const override {
            return true;
        }
    };

    // PUBLISH packet
    class PublishPacket : public MqttPacket {
    private:
        std::string topic_;
        std::vector<uint8_t> payload_;
        uint16_t packet_id_ = 0;
        QoS qos_ = QOS_0;
        bool retain_ = false;
        bool dup_ = false;

    public:
        PublishPacket() : MqttPacket(PUBLISH) {}

        // Setters
        void set_topic(const std::string& topic) { topic_ = topic; }
        void set_payload(const uint8_t* data, size_t len) {
            payload_.assign(data, data + len);
        }
        void set_payload(const std::vector<uint8_t>& payload) {
            payload_ = payload;
        }
        void set_payload(const std::string& payload) {
            payload_.assign(payload.begin(), payload.end());
        }
        void set_qos(QoS qos) { qos_ = qos; }
        void set_packet_id(uint16_t id) { packet_id_ = id; }
        void set_retain(bool retain) { retain_ = retain; }
        void set_dup(bool dup) { dup_ = dup; }

        // Getters
        const std::string& topic() const { return topic_; }
        const std::vector<uint8_t>& payload() const { return payload_; }
        QoS qos() const { return qos_; }
        uint16_t packet_id() const { return packet_id_; }
        bool is_retain() const { return retain_; }
        bool is_dup() const { return dup_; }

        void serialize(Buffer& buffer) const override {
            Buffer variable_header;

            // Topic name
            variable_header.write_string(topic_);

            // Packet identifier (if QoS > 0)
            if (qos_ > QOS_0) {
                variable_header.write_uint16(packet_id_);
            }

            // Properties (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                write_properties(variable_header);
            }

            // Fixed header
            uint8_t first_byte = (type_ << 4);
            if (dup_) first_byte |= 0x08;
            first_byte |= (qos_ << 1);
            if (retain_) first_byte |= 0x01;

            buffer.write_byte(first_byte);
            buffer.write_variable_length(variable_header.size() + payload_.size());
            buffer.write_bytes(variable_header.data(), variable_header.size());
            buffer.write_bytes(payload_.data(), payload_.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            size_t start_pos = buffer.position();

            // Extract flags from fixed header (already read)
            dup_ = (flags_ & 0x08) != 0;
            qos_ = static_cast<QoS>((flags_ >> 1) & 0x03);
            retain_ = (flags_ & 0x01) != 0;

            // Topic name
            topic_ = buffer.read_string();

            // Packet identifier
            if (qos_ > QOS_0) {
                packet_id_ = buffer.read_uint16();
            }

            // Properties (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                if (!read_properties(buffer)) {
                    return false;
                }
            }

            // Payload (remaining bytes)
            size_t bytes_read = buffer.position() - start_pos;
            size_t payload_size = remaining_length - bytes_read;

            payload_.resize(payload_size);
            buffer.read_bytes(payload_.data(), payload_size);

            return true;
        }

        bool validate() const override {
            if (!PacketValidator::validate_topic(topic_)) {
                return false;
            }

            if (!PacketValidator::validate_qos(qos_)) {
                return false;
            }

            if (!PacketValidator::validate_packet_id(packet_id_, qos_)) {
                return false;
            }

            return true;
        }
    };

    // PUBACK packet
    class PubAckPacket : public MqttPacket {
    private:
        uint16_t packet_id_ = 0;
        ReasonCode reason_code_ = ReasonCode::SUCCESS;

    public:
        PubAckPacket() : MqttPacket(PUBACK) {}

        void set_packet_id(uint16_t id) { packet_id_ = id; }
        void set_reason_code(ReasonCode code) { reason_code_ = code; }

        uint16_t packet_id() const { return packet_id_; }
        ReasonCode reason_code() const { return reason_code_; }

        void serialize(Buffer& buffer) const override {
            Buffer payload;

            payload.write_uint16(packet_id_);

            if (version_ >= ProtocolVersion::MQTT_5_0) {
                if (reason_code_ != ReasonCode::SUCCESS || !properties_.has(PropertyId::REASON_STRING)) {
                    payload.write_byte(static_cast<uint8_t>(reason_code_));
                    write_properties(payload);
                }
            }

            write_fixed_header(buffer, payload.size());
            buffer.write_bytes(payload.data(), payload.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            if (remaining_length < 2) return false;

            packet_id_ = buffer.read_uint16();

            if (version_ >= ProtocolVersion::MQTT_5_0 && remaining_length > 2) {
                reason_code_ = static_cast<ReasonCode>(buffer.read_byte());

                if (remaining_length > 3) {
                    return read_properties(buffer);
                }
            }

            return true;
        }

        bool validate() const override {
            return packet_id_ != 0;
        }
    };

    // PUBREC packet
    class PubRecPacket : public MqttPacket {
    private:
        uint16_t packet_id_ = 0;
        ReasonCode reason_code_ = ReasonCode::SUCCESS;

    public:
        PubRecPacket() : MqttPacket(PUBREC) {}

        void set_packet_id(uint16_t id) { packet_id_ = id; }
        void set_reason_code(ReasonCode code) { reason_code_ = code; }

        uint16_t packet_id() const { return packet_id_; }

        void serialize(Buffer& buffer) const override {
            Buffer payload;

            payload.write_uint16(packet_id_);

            if (version_ >= ProtocolVersion::MQTT_5_0 && reason_code_ != ReasonCode::SUCCESS) {
                payload.write_byte(static_cast<uint8_t>(reason_code_));
                write_properties(payload);
            }

            write_fixed_header(buffer, payload.size());
            buffer.write_bytes(payload.data(), payload.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            if (remaining_length < 2) return false;

            packet_id_ = buffer.read_uint16();

            if (version_ >= ProtocolVersion::MQTT_5_0 && remaining_length > 2) {
                reason_code_ = static_cast<ReasonCode>(buffer.read_byte());

                if (remaining_length > 3) {
                    return read_properties(buffer);
                }
            }

            return true;
        }

        bool validate() const override {
            return packet_id_ != 0;
        }
    };

    // PUBREL packet
    class PubRelPacket : public MqttPacket {
    private:
        uint16_t packet_id_ = 0;
        ReasonCode reason_code_ = ReasonCode::SUCCESS;

    public:
        PubRelPacket() : MqttPacket(PUBREL, 0x02) {}

        void set_packet_id(uint16_t id) { packet_id_ = id; }
        void set_reason_code(ReasonCode code) { reason_code_ = code; }

        uint16_t packet_id() const { return packet_id_; }

        void serialize(Buffer& buffer) const override {
            Buffer payload;

            payload.write_uint16(packet_id_);

            if (version_ >= ProtocolVersion::MQTT_5_0 && reason_code_ != ReasonCode::SUCCESS) {
                payload.write_byte(static_cast<uint8_t>(reason_code_));
                write_properties(payload);
            }

            write_fixed_header(buffer, payload.size());
            buffer.write_bytes(payload.data(), payload.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            if (remaining_length < 2) return false;

            packet_id_ = buffer.read_uint16();

            if (version_ >= ProtocolVersion::MQTT_5_0 && remaining_length > 2) {
                reason_code_ = static_cast<ReasonCode>(buffer.read_byte());

                if (remaining_length > 3) {
                    return read_properties(buffer);
                }
            }

            return true;
        }

        bool validate() const override {
            return packet_id_ != 0;
        }
    };

    // PUBCOMP packet
    class PubCompPacket : public MqttPacket {
    private:
        uint16_t packet_id_ = 0;
        ReasonCode reason_code_ = ReasonCode::SUCCESS;

    public:
        PubCompPacket() : MqttPacket(PUBCOMP) {}

        void set_packet_id(uint16_t id) { packet_id_ = id; }
        void set_reason_code(ReasonCode code) { reason_code_ = code; }

        uint16_t packet_id() const { return packet_id_; }

        void serialize(Buffer& buffer) const override {
            Buffer payload;

            payload.write_uint16(packet_id_);

            if (version_ >= ProtocolVersion::MQTT_5_0 && reason_code_ != ReasonCode::SUCCESS) {
                payload.write_byte(static_cast<uint8_t>(reason_code_));
                write_properties(payload);
            }

            write_fixed_header(buffer, payload.size());
            buffer.write_bytes(payload.data(), payload.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            if (remaining_length < 2) return false;

            packet_id_ = buffer.read_uint16();

            if (version_ >= ProtocolVersion::MQTT_5_0 && remaining_length > 2) {
                reason_code_ = static_cast<ReasonCode>(buffer.read_byte());

                if (remaining_length > 3) {
                    return read_properties(buffer);
                }
            }

            return true;
        }

        bool validate() const override {
            return packet_id_ != 0;
        }
    };

    // SUBSCRIBE packet
    class SubscribePacket : public MqttPacket {
    private:
        uint16_t packet_id_ = 0;
        std::vector<std::pair<std::string, uint8_t>> subscriptions_;

    public:
        SubscribePacket() : MqttPacket(SUBSCRIBE, 0x02) {}

        void set_packet_id(uint16_t id) { packet_id_ = id; }

        void add_subscription(const std::string& topic, QoS qos) {
            uint8_t options = static_cast<uint8_t>(qos);

            if (version_ >= ProtocolVersion::MQTT_5_0) {
                // MQTT 5.0 subscription options
                // Bits 0-1: QoS
                // Bit 2: No Local
                // Bit 3: Retain As Published
                // Bits 4-5: Retain Handling
                // Bits 6-7: Reserved
            }

            subscriptions_.push_back({ topic, options });
        }

        uint16_t packet_id() const { return packet_id_; }
        const auto& subscriptions() const { return subscriptions_; }

        void serialize(Buffer& buffer) const override {
            Buffer payload;

            payload.write_uint16(packet_id_);

            // Properties (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                write_properties(payload);
            }

            // Subscription list
            for (const auto& [topic, options] : subscriptions_) {
                payload.write_string(topic);
                payload.write_byte(options);
            }

            write_fixed_header(buffer, payload.size());
            buffer.write_bytes(payload.data(), payload.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            size_t start_pos = buffer.position();

            packet_id_ = buffer.read_uint16();

            // Properties (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                if (!read_properties(buffer)) {
                    return false;
                }
            }

            // Subscriptions
            while (buffer.position() - start_pos < remaining_length) {
                std::string topic = buffer.read_string();
                uint8_t options = buffer.read_byte();
                subscriptions_.push_back({ topic, options });
            }

            return true;
        }

        bool validate() const override {
            if (packet_id_ == 0) return false;
            if (subscriptions_.empty()) return false;

            for (const auto& [topic, options] : subscriptions_) {
                if (!PacketValidator::validate_topic(topic, true)) {
                    return false;
                }

                uint8_t qos = options & 0x03;
                if (!PacketValidator::validate_qos(qos)) {
                    return false;
                }
            }

            return true;
        }
    };

    // SUBACK packet
    class SubAckPacket : public MqttPacket {
    private:
        uint16_t packet_id_ = 0;
        std::vector<ReasonCode> reason_codes_;

    public:
        SubAckPacket() : MqttPacket(SUBACK) {}

        void set_packet_id(uint16_t id) { packet_id_ = id; }

        void add_reason_code(ReasonCode code) {
            reason_codes_.push_back(code);
        }

        void add_return_code(uint8_t code) { // MQTT 3.1.1 compatibility
            reason_codes_.push_back(static_cast<ReasonCode>(code));
        }

        uint16_t packet_id() const { return packet_id_; }
        const auto& reason_codes() const { return reason_codes_; }

        void serialize(Buffer& buffer) const override {
            Buffer payload;

            payload.write_uint16(packet_id_);

            // Properties (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                write_properties(payload);
            }

            // Reason codes
            for (auto code : reason_codes_) {
                payload.write_byte(static_cast<uint8_t>(code));
            }

            write_fixed_header(buffer, payload.size());
            buffer.write_bytes(payload.data(), payload.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            size_t start_pos = buffer.position();

            packet_id_ = buffer.read_uint16();

            // Properties (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                if (!read_properties(buffer)) {
                    return false;
                }
            }

            // Reason codes
            while (buffer.position() - start_pos < remaining_length) {
                reason_codes_.push_back(static_cast<ReasonCode>(buffer.read_byte()));
            }

            return true;
        }

        bool validate() const override {
            return packet_id_ != 0 && !reason_codes_.empty();
        }
    };

    // UNSUBSCRIBE packet
    class UnsubscribePacket : public MqttPacket {
    private:
        uint16_t packet_id_ = 0;
        std::vector<std::string> topics_;

    public:
        UnsubscribePacket() : MqttPacket(UNSUBSCRIBE, 0x02) {}

        void set_packet_id(uint16_t id) { packet_id_ = id; }

        void add_topic(const std::string& topic) {
            topics_.push_back(topic);
        }

        uint16_t packet_id() const { return packet_id_; }
        const auto& topics() const { return topics_; }

        void serialize(Buffer& buffer) const override {
            Buffer payload;

            payload.write_uint16(packet_id_);

            // Properties (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                write_properties(payload);
            }

            // Topic filters
            for (const auto& topic : topics_) {
                payload.write_string(topic);
            }

            write_fixed_header(buffer, payload.size());
            buffer.write_bytes(payload.data(), payload.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            size_t start_pos = buffer.position();

            packet_id_ = buffer.read_uint16();

            // Properties (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                if (!read_properties(buffer)) {
                    return false;
                }
            }

            // Topics
            while (buffer.position() - start_pos < remaining_length) {
                topics_.push_back(buffer.read_string());
            }

            return true;
        }

        bool validate() const override {
            if (packet_id_ == 0) return false;
            if (topics_.empty()) return false;

            for (const auto& topic : topics_) {
                if (!PacketValidator::validate_topic(topic, true)) {
                    return false;
                }
            }

            return true;
        }
    };

    // UNSUBACK packet
    class UnsubAckPacket : public MqttPacket {
    private:
        uint16_t packet_id_ = 0;
        std::vector<ReasonCode> reason_codes_;

    public:
        UnsubAckPacket() : MqttPacket(UNSUBACK) {}

        void set_packet_id(uint16_t id) { packet_id_ = id; }

        void add_reason_code(ReasonCode code) {
            reason_codes_.push_back(code);
        }

        uint16_t packet_id() const { return packet_id_; }

        void serialize(Buffer& buffer) const override {
            Buffer payload;

            payload.write_uint16(packet_id_);

            // Properties and reason codes (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0) {
                write_properties(payload);

                for (auto code : reason_codes_) {
                    payload.write_byte(static_cast<uint8_t>(code));
                }
            }

            write_fixed_header(buffer, payload.size());
            buffer.write_bytes(payload.data(), payload.size());
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            size_t start_pos = buffer.position();

            packet_id_ = buffer.read_uint16();

            // Properties and reason codes (MQTT 5.0)
            if (version_ >= ProtocolVersion::MQTT_5_0 && remaining_length > 2) {
                if (!read_properties(buffer)) {
                    return false;
                }

                while (buffer.position() - start_pos < remaining_length) {
                    reason_codes_.push_back(static_cast<ReasonCode>(buffer.read_byte()));
                }
            }

            return true;
        }

        bool validate() const override {
            return packet_id_ != 0;
        }
    };

    // PINGREQ packet
    class PingReqPacket : public MqttPacket {
    public:
        PingReqPacket() : MqttPacket(PINGREQ) {}

        void serialize(Buffer& buffer) const override {
            write_fixed_header(buffer, 0);
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            return remaining_length == 0;
        }

        bool validate() const override {
            return true;
        }
    };

    // PINGRESP packet
    class PingRespPacket : public MqttPacket {
    public:
        PingRespPacket() : MqttPacket(PINGRESP) {}

        void serialize(Buffer& buffer) const override {
            write_fixed_header(buffer, 0);
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            return remaining_length == 0;
        }

        bool validate() const override {
            return true;
        }
    };

    // DISCONNECT packet
    class DisconnectPacket : public MqttPacket {
    private:
        ReasonCode reason_code_ = ReasonCode::NORMAL_DISCONNECTION;

    public:
        DisconnectPacket() : MqttPacket(DISCONNECT) {}

        void set_reason_code(ReasonCode code) { reason_code_ = code; }

        ReasonCode reason_code() const { return reason_code_; }

        void serialize(Buffer& buffer) const override {
            Buffer payload;

            if (version_ >= ProtocolVersion::MQTT_5_0) {
                if (reason_code_ != ReasonCode::NORMAL_DISCONNECTION ||
                    properties_.has(PropertyId::SESSION_EXPIRY_INTERVAL)) {
                    payload.write_byte(static_cast<uint8_t>(reason_code_));
                    write_properties(payload);
                }
            }

            write_fixed_header(buffer, payload.size());
            if (payload.size() > 0) {
                buffer.write_bytes(payload.data(), payload.size());
            }
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            if (version_ >= ProtocolVersion::MQTT_5_0 && remaining_length > 0) {
                reason_code_ = static_cast<ReasonCode>(buffer.read_byte());

                if (remaining_length > 1) {
                    return read_properties(buffer);
                }
            }

            return true;
        }

        bool validate() const override {
            return true;
        }
    };

    // AUTH packet (MQTT 5.0)
    class AuthPacket : public MqttPacket {
    private:
        ReasonCode reason_code_ = ReasonCode::SUCCESS;

    public:
        AuthPacket() : MqttPacket(AUTH) {}

        void set_reason_code(ReasonCode code) { reason_code_ = code; }

        void serialize(Buffer& buffer) const override {
            if (version_ < ProtocolVersion::MQTT_5_0) {
                throw std::runtime_error("AUTH packet is only available in MQTT 5.0");
            }

            Buffer payload;

            if (reason_code_ != ReasonCode::SUCCESS ||
                properties_.has(PropertyId::AUTHENTICATION_METHOD)) {
                payload.write_byte(static_cast<uint8_t>(reason_code_));
                write_properties(payload);
            }

            write_fixed_header(buffer, payload.size());
            if (payload.size() > 0) {
                buffer.write_bytes(payload.data(), payload.size());
            }
        }

        bool deserialize(Buffer& buffer, uint32_t remaining_length) override {
            if (version_ < ProtocolVersion::MQTT_5_0) {
                return false;
            }

            if (remaining_length > 0) {
                reason_code_ = static_cast<ReasonCode>(buffer.read_byte());

                if (remaining_length > 1) {
                    return read_properties(buffer);
                }
            }

            return true;
        }

        bool validate() const override {
            return version_ >= ProtocolVersion::MQTT_5_0;
        }
    };

    // Packet factory
    class PacketFactory {
    public:
        static std::unique_ptr<MqttPacket> create(PacketType type) {
            switch (type) {
            case CONNECT: return std::make_unique<ConnectPacket>();
            case CONNACK: return std::make_unique<ConnAckPacket>();
            case PUBLISH: return std::make_unique<PublishPacket>();
            case PUBACK: return std::make_unique<PubAckPacket>();
            case PUBREC: return std::make_unique<PubRecPacket>();
            case PUBREL: return std::make_unique<PubRelPacket>();
            case PUBCOMP: return std::make_unique<PubCompPacket>();
            case SUBSCRIBE: return std::make_unique<SubscribePacket>();
            case SUBACK: return std::make_unique<SubAckPacket>();
            case UNSUBSCRIBE: return std::make_unique<UnsubscribePacket>();
            case UNSUBACK: return std::make_unique<UnsubAckPacket>();
            case PINGREQ: return std::make_unique<PingReqPacket>();
            case PINGRESP: return std::make_unique<PingRespPacket>();
            case DISCONNECT: return std::make_unique<DisconnectPacket>();
            case AUTH: return std::make_unique<AuthPacket>();
            default: return nullptr;
            }
        }

        static std::unique_ptr<MqttPacket> parse(Buffer& buffer, ProtocolVersion version = ProtocolVersion::MQTT_3_1_1) {
            if (buffer.available() < 2) {
                return nullptr;
            }

            size_t start_pos = buffer.mark_read_position();

            uint8_t first_byte = buffer.read_byte();
            PacketType type = static_cast<PacketType>((first_byte >> 4) & 0x0F);
            uint8_t flags = first_byte & 0x0F;

            uint32_t remaining_length = 0;
            if (!buffer.read_variable_length(remaining_length)) {
                buffer.reset_read_position(start_pos);
                return nullptr;
            }

            if (buffer.available() < remaining_length) {
                buffer.reset_read_position(start_pos);
                return nullptr;
            }

            auto packet = create(type);
            if (!packet) {
                return nullptr;
            }

            packet->flags_ = flags;
            packet->set_version(version);

            if (!packet->deserialize(buffer, remaining_length)) {
                return nullptr;
            }

            if (!packet->validate()) {
                return nullptr;
            }

            return packet;
        }
    };

} // namespace ourmqtt

#endif // OUR_MQTT_PACKET_HPP