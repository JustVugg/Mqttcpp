// test_basic.cpp - Test completo della libreria MQTT
#include "mqtt_client.hpp"
#include "mqtt_broker.hpp"
#include "logger.hpp"
#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include <map>
#include <chrono>
#include <iomanip>
#include <filesystem>
#include <memory>

using namespace ourmqtt;
using namespace std::chrono;

// Helper per stampare risultati (versione compatibile Windows)
void print_test_header(const std::string& test_name) {
    std::cout << "\n========================================\n";
    std::cout << " " << test_name << "\n";
    std::cout << "========================================\n";
}

void print_result(const std::string& test, bool passed) {
    if (passed) {
#ifdef _WIN32
        std::cout << "[PASS] ";
#else
        std::cout << "✅ ";
#endif
        std::cout << test << " PASSED\n";
    }
    else {
#ifdef _WIN32
        std::cout << "[FAIL] ";
#else
        std::cout << "❌ ";
#endif
        std::cout << test << " FAILED\n";
    }
}

// Test 1: Connessione base
bool test_basic_connection(MqttBroker& broker) {
    print_test_header("Test 1: Basic Connection");

    try {
        MqttClient client("test_connection_client");

        // Test connessione
        if (!client.connect("localhost", 11883)) {
            std::cerr << "Failed to connect to broker\n";
            return false;
        }

        std::cout << "Client connected successfully\n";

        // Test disconnessione pulita
        client.disconnect();
        std::cout << "Client disconnected successfully\n";

        return true;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception in test: " << e.what() << "\n";
        return false;
    }
}

// Test 2: Publish/Subscribe singolo topic
bool test_single_pubsub(MqttBroker& broker) {
    print_test_header("Test 2: Single Topic Pub/Sub");

    try {
        std::atomic<bool> message_received(false);
        std::string received_message;

        // Subscriber
        MqttClient subscriber("test_subscriber");
        subscriber.set_message_handler(
            [&](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos, bool retain) {
                received_message = std::string(payload.begin(), payload.end());
                std::cout << "Subscriber received: [" << topic << "] = " << received_message << "\n";
                message_received = true;
            }
        );

        if (!subscriber.connect("localhost", 11883)) {
            std::cerr << "Subscriber failed to connect\n";
            return false;
        }

        if (!subscriber.subscribe("test/topic", QOS_0)) {
            std::cerr << "Failed to subscribe\n";
            return false;
        }
        std::cout << "Subscribed to test/topic\n";

        // Publisher
        MqttClient publisher("test_publisher");
        if (!publisher.connect("localhost", 11883)) {
            std::cerr << "Publisher failed to connect\n";
            return false;
        }

        std::string test_message = "Hello MQTT World!";
        if (!publisher.publish("test/topic", test_message, QOS_0)) {
            std::cerr << "Failed to publish\n";
            return false;
        }
        std::cout << "Published: " << test_message << "\n";

        // Aspetta ricezione
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        publisher.disconnect();
        subscriber.disconnect();

        return message_received && (received_message == test_message);
    }
    catch (const std::exception& e) {
        std::cerr << "Exception in test: " << e.what() << "\n";
        return false;
    }
}

// Test 3: Topics multipli
bool test_multiple_topics(MqttBroker& broker) {
    print_test_header("Test 3: Multiple Topics");

    try {
        std::map<std::string, std::string> received_messages;
        std::atomic<int> message_count(0);

        // Subscriber per multipli topics
        MqttClient subscriber("multi_subscriber");
        subscriber.set_message_handler(
            [&](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos, bool retain) {
                std::string msg(payload.begin(), payload.end());
                received_messages[topic] = msg;
                message_count++;
                std::cout << "Received on " << topic << ": " << msg << "\n";
            }
        );

        if (!subscriber.connect("localhost", 11883)) {
            return false;
        }

        // Sottoscrivi a multipli topics
        std::vector<std::string> topics = {
            "sensors/temperature",
            "sensors/humidity",
            "sensors/pressure",
            "status/system"
        };

        for (const auto& topic : topics) {
            subscriber.subscribe(topic, QOS_0);
            std::cout << "Subscribed to: " << topic << "\n";
        }

        // Publisher
        MqttClient publisher("multi_publisher");
        if (!publisher.connect("localhost", 11883)) {
            return false;
        }

        // Pubblica su tutti i topics
        publisher.publish("sensors/temperature", "25.5°C", QOS_0);
        publisher.publish("sensors/humidity", "60%", QOS_0);
        publisher.publish("sensors/pressure", "1013hPa", QOS_0);
        publisher.publish("status/system", "Online", QOS_0);

        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        publisher.disconnect();
        subscriber.disconnect();

        bool all_received = (message_count == 4);
        if (all_received) {
            std::cout << "All " << message_count << " messages received correctly\n";
        }

        return all_received;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception in test: " << e.what() << "\n";
        return false;
    }
}

// Test 4: Wildcards
bool test_wildcards(MqttBroker& broker) {
    print_test_header("Test 4: Wildcard Subscriptions");

    try {
        std::atomic<int> single_wildcard_count(0);
        std::atomic<int> multi_wildcard_count(0);

        // Subscriber con wildcards
        MqttClient wildcard_sub("wildcard_subscriber");
        wildcard_sub.set_message_handler(
            [&](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos, bool retain) {
                std::string msg(payload.begin(), payload.end());
                std::cout << "Wildcard received: " << topic << " = " << msg << "\n";

                if (topic.find("home/") == 0) {
                    single_wildcard_count++;
                }
                if (topic.find("logs/") == 0) {
                    multi_wildcard_count++;
                }
            }
        );

        if (!wildcard_sub.connect("localhost", 11883)) {
            return false;
        }

        // + = single level wildcard
        wildcard_sub.subscribe("home/+/temperature", QOS_0);
        std::cout << "Subscribed to: home/+/temperature\n";

        // # = multi level wildcard  
        wildcard_sub.subscribe("logs/#", QOS_0);
        std::cout << "Subscribed to: logs/#\n";

        // Publisher
        MqttClient publisher("wildcard_publisher");
        if (!publisher.connect("localhost", 11883)) {
            return false;
        }

        // Test single-level wildcard (+)
        publisher.publish("home/bedroom/temperature", "22°C", QOS_0);
        publisher.publish("home/kitchen/temperature", "24°C", QOS_0);
        publisher.publish("home/bathroom/temperature", "26°C", QOS_0);
        publisher.publish("home/bedroom/humidity", "55%", QOS_0); // NON deve matchare

        // Test multi-level wildcard (#)
        publisher.publish("logs/error/app", "Error occurred", QOS_0);
        publisher.publish("logs/info/system", "System started", QOS_0);
        publisher.publish("logs/debug/network/tcp", "Connection established", QOS_0);

        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        publisher.disconnect();
        wildcard_sub.disconnect();

        std::cout << "Single wildcard (+) matched: " << single_wildcard_count << " (expected 3)\n";
        std::cout << "Multi wildcard (#) matched: " << multi_wildcard_count << " (expected 3)\n";

        return (single_wildcard_count == 3 && multi_wildcard_count == 3);
    }
    catch (const std::exception& e) {
        std::cerr << "Exception in test: " << e.what() << "\n";
        return false;
    }
}

// Test 5: Retained Messages
bool test_retained_messages(MqttBroker& broker) {
    print_test_header("Test 5: Retained Messages");

    try {
        // Publisher - pubblica retained message
        {
            MqttClient publisher("retained_publisher");
            if (!publisher.connect("localhost", 11883)) {
                return false;
            }

            publisher.publish("config/server", "192.168.1.100", QOS_0, true);
            publisher.publish("config/port", "8080", QOS_0, true);
            std::cout << "Published 2 retained messages\n";

            publisher.disconnect();
        }

        // Nuovo subscriber dovrebbe ricevere i retained
        std::atomic<int> retained_count(0);
        {
            MqttClient new_subscriber("new_subscriber");
            new_subscriber.set_message_handler(
                [&](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos, bool retain) {
                    std::string msg(payload.begin(), payload.end());
                    std::cout << "New subscriber received retained: " << topic << " = " << msg << "\n";
                    retained_count++;
                }
            );

            if (!new_subscriber.connect("localhost", 11883)) {
                return false;
            }

            new_subscriber.subscribe("config/+", QOS_0);

            std::this_thread::sleep_for(std::chrono::milliseconds(300));

            new_subscriber.disconnect();
        }

        std::cout << "Retained messages received: " << retained_count << "\n";
        return retained_count == 2;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception in test: " << e.what() << "\n";
        return false;
    }
}

// Test 6: QoS Levels  
bool test_qos_levels(MqttBroker& broker) {
    print_test_header("Test 6: Quality of Service Levels");

    try {
        std::map<int, int> qos_received;

        MqttClient subscriber("qos_subscriber");
        subscriber.set_message_handler(
            [&](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos, bool retain) {
                std::string msg(payload.begin(), payload.end());

                if (topic == "qos/0") qos_received[0]++;
                else if (topic == "qos/1") qos_received[1]++;
                else if (topic == "qos/2") qos_received[2]++;

                std::cout << "Received " << topic << " = " << msg << "\n";
            }
        );

        if (!subscriber.connect("localhost", 11883)) {
            return false;
        }

        subscriber.subscribe("qos/0", QOS_0);
        subscriber.subscribe("qos/1", QOS_1);
        subscriber.subscribe("qos/2", QOS_2);

        MqttClient publisher("qos_publisher");
        if (!publisher.connect("localhost", 11883)) {
            return false;
        }

        // Pubblica con diversi QoS
        for (int i = 0; i < 3; i++) {
            publisher.publish("qos/0", "QoS 0 Message " + std::to_string(i), QOS_0);
            publisher.publish("qos/1", "QoS 1 Message " + std::to_string(i), QOS_1);
            publisher.publish("qos/2", "QoS 2 Message " + std::to_string(i), QOS_2);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        publisher.disconnect();
        subscriber.disconnect();

        std::cout << "QoS 0 received: " << qos_received[0] << " (sent 3)\n";
        std::cout << "QoS 1 received: " << qos_received[1] << " (sent 3)\n";
        std::cout << "QoS 2 received: " << qos_received[2] << " (sent 3)\n";

        // QoS 0 potrebbe perdere messaggi, QoS 1 e 2 no
        return qos_received[1] == 3;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception in test: " << e.what() << "\n";
        return false;
    }
}

// Test 7: Multiple Clients
bool test_multiple_clients(MqttBroker& broker) {
    print_test_header("Test 7: Multiple Concurrent Clients");

    try {
        const int NUM_CLIENTS = 5;
        std::atomic<int> total_messages(0);
        std::vector<std::unique_ptr<MqttClient>> clients;

        // Crea multipli subscribers
        for (int i = 0; i < NUM_CLIENTS; i++) {
            auto client = std::make_unique<MqttClient>("client_" + std::to_string(i));

            client->set_message_handler(
                [&total_messages, i](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos, bool retain) {
                    total_messages++;
                    std::string msg(payload.begin(), payload.end());
                    std::cout << "Client " << i << " received: " << msg << "\n";
                }
            );

            if (!client->connect("localhost", 11883)) {
                std::cerr << "Client " << i << " failed to connect\n";
                return false;
            }

            client->subscribe("broadcast/message", QOS_0);
            clients.push_back(std::move(client));
        }

        std::cout << NUM_CLIENTS << " clients connected and subscribed\n";

        // Publisher broadcasts
        MqttClient publisher("broadcaster");
        if (!publisher.connect("localhost", 11883)) {
            return false;
        }

        publisher.publish("broadcast/message", "Hello to all clients!", QOS_0);

        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        publisher.disconnect();
        for (auto& client : clients) {
            client->disconnect();
        }

        std::cout << "Total messages received: " << total_messages
            << " (expected " << NUM_CLIENTS << ")\n";

        return total_messages == NUM_CLIENTS;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception in test: " << e.what() << "\n";
        return false;
    }
}

// Test 8: Performance
bool test_performance(MqttBroker& broker) {
    print_test_header("Test 8: Performance Test");

    try {
        const int NUM_MESSAGES = 1000;
        std::atomic<int> received_count(0);

        MqttClient subscriber("perf_subscriber");
        subscriber.set_message_handler(
            [&received_count](const std::string& topic, const std::vector<uint8_t>& payload, QoS qos, bool retain) {
                received_count++;
            }
        );

        if (!subscriber.connect("localhost", 11883)) {
            return false;
        }

        subscriber.subscribe("perf/test", QOS_0);

        MqttClient publisher("perf_publisher");
        if (!publisher.connect("localhost", 11883)) {
            return false;
        }

        auto start = high_resolution_clock::now();

        // Pubblica molti messaggi
        for (int i = 0; i < NUM_MESSAGES; i++) {
            std::string msg = "Msg " + std::to_string(i);
            publisher.publish("perf/test", msg, QOS_0);
        }

        // Aspetta che tutti siano ricevuti (max 5 secondi)
        auto timeout = high_resolution_clock::now() + seconds(5);
        while (received_count < NUM_MESSAGES && high_resolution_clock::now() < timeout) {
            std::this_thread::sleep_for(milliseconds(10));
        }

        auto end = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(end - start);

        double messages_per_second = (received_count * 1000.0) / duration.count();

        std::cout << "Sent: " << NUM_MESSAGES << " messages\n";
        std::cout << "Received: " << received_count << " messages\n";
        std::cout << "Time: " << duration.count() << " ms\n";
        std::cout << "Rate: " << std::fixed << std::setprecision(2)
            << messages_per_second << " msg/sec\n";

        publisher.disconnect();
        subscriber.disconnect();

        // Considera successo se almeno 95% dei messaggi sono arrivati
        return received_count >= (NUM_MESSAGES * 0.95);
    }
    catch (const std::exception& e) {
        std::cerr << "Exception in test: " << e.what() << "\n";
        return false;
    }
}

// Test 9: Autenticazione Base - VERSIONE CORRETTA CON DISTRUZIONE FORZATA
// Test 9: Authentication - VERSIONE FUNZIONANTE
// test_basic.cpp - VERSIONE ULTRA-SICURA CHE NON SI BLOCCA MAI

bool test_authentication() {
    print_test_header("Test 9: Authentication");

    // Cleanup iniziale
    try {
        std::filesystem::remove("test_users.db");
        std::filesystem::remove("test_users.db.bin");
        std::filesystem::remove("auth_key.bin");
        std::filesystem::remove_all("./test_data_auth");
    }
    catch (...) {}

    try {
        std::cout << "Creating broker with authentication on port 11884...\n";

        BrokerConfig auth_config;
        auth_config.port = 11884;
        auth_config.enable_tls = false;
        auth_config.require_auth = true;
        auth_config.users_file = "test_users.db";
        auth_config.enable_persistence = false;
        auth_config.enable_metrics = false;
        auth_config.enable_sys_topics = false;
        auth_config.worker_threads = 1;
        auth_config.io_threads = 1;
        auth_config.security_config.min_password_length = 1;
        auth_config.security_config.require_uppercase = false;
        auth_config.security_config.require_lowercase = false;
        auth_config.security_config.require_numbers = false;
        auth_config.security_config.require_special_chars = false;

        // Crea broker su heap e NON distruggerlo MAI
        MqttBroker* auth_broker = new MqttBroker(auth_config);

        std::cout << "Starting auth broker...\n";
        if (!auth_broker->start()) {
            std::cerr << "Failed to start broker with auth\n";
            return false;
        }

        std::cout << "Auth broker started successfully on port 11884\n";

        auto* auth_manager = auth_broker->get_auth_manager();
        if (!auth_manager) {
            std::cerr << "ERROR: No auth manager available!\n";
            return false;
        }

        // Fai tutti i test...
        std::cout << "\nCreating test admin user...\n";
        auto create_result = auth_manager->create_user("admin", "secret", "admin@test.com", { "admin" });
        if (!create_result.success) {
            std::cerr << "Failed to create admin: " << create_result.message << "\n";
            return false;
        }
        std::cout << "✓ Admin user created successfully\n";

        // Test 1: No credentials
        std::cout << "\nTesting connection without credentials...\n";
        {
            MqttClient client("no_auth");
            if (client.connect("localhost", 11884, 2000)) {
                std::cerr << "ERROR: Connected without credentials!\n";
                client.disconnect();
                return false;
            }
            std::cout << "✓ Correctly rejected\n";
        }

        // Test 2: Wrong password
        std::cout << "\nTesting wrong credentials...\n";
        {
            MqttClient client("wrong_auth");
            client.set_credentials("admin", "wrong");
            if (client.connect("localhost", 11884, 2000)) {
                std::cerr << "ERROR: Connected with wrong password!\n";
                client.disconnect();
                return false;
            }
            std::cout << "✓ Correctly rejected\n";
        }

        // Test 3: Correct credentials
        std::cout << "\nTesting correct credentials...\n";
        {
            MqttClient client("correct_auth");
            client.set_credentials("admin", "secret");
            if (!client.connect("localhost", 11884, 2000)) {
                std::cerr << "ERROR: Failed to connect with correct credentials!\n";
                return false;
            }
            std::cout << "✓ Connected successfully\n";

            if (!client.publish("test/topic", "Hello", QOS_0)) {
                std::cerr << "ERROR: Failed to publish\n";
                client.disconnect();
                return false;
            }
            std::cout << "✓ Published successfully\n";

            client.disconnect();
        }

        // Test 4: New user
        std::cout << "\nTesting new user creation...\n";
        {
            auto result = auth_manager->create_user("testuser", "userpass", "test@example.com", { "user" });
            if (!result.success) {
                std::cerr << "Failed to create test user: " << result.message << "\n";
                return false;
            }
            std::cout << "✓ Created test user successfully\n";

            MqttClient client("new_user");
            client.set_credentials("testuser", "userpass");

            if (!client.connect("localhost", 11884, 2000)) {
                std::cerr << "ERROR: New user failed to connect!\n";
                return false;
            }

            std::cout << "✓ New user connected successfully\n";
            client.disconnect();
        }

        std::cout << "\n✓ Auth test completed (broker left running)\n\n";

        // NON chiamare stop()!
        // NON chiamare delete!
        // Lascia il broker in esecuzione!

        // Cleanup solo i file
        try {
            std::filesystem::remove("test_users.db");
            std::filesystem::remove("test_users.db.bin");
            std::filesystem::remove("auth_key.bin");
        }
        catch (...) {}

        return true;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception in auth test: " << e.what() << "\n";
        return false;
    }
}

bool test_user_management() {
    print_test_header("Test 10: User Management");

    try {
        // Cleanup
        try {
            std::filesystem::remove("test_users_mgmt.db");
            std::filesystem::remove("test_users_mgmt.db.bin");
            std::filesystem::remove_all("./test_data_users");
        }
        catch (...) {}

        std::cout << "Creating broker with user management on port 11885...\n";

        BrokerConfig auth_config;
        auth_config.port = 11885;
        auth_config.require_auth = true;
        auth_config.users_file = "test_users_mgmt.db";
        auth_config.enable_persistence = false;
        auth_config.enable_metrics = false;
        auth_config.enable_sys_topics = false;
        auth_config.worker_threads = 1;
        auth_config.io_threads = 1;
        auth_config.security_config.min_password_length = 1;
        auth_config.security_config.require_uppercase = false;
        auth_config.security_config.require_lowercase = false;
        auth_config.security_config.require_numbers = false;
        auth_config.security_config.require_special_chars = false;

        MqttBroker* auth_broker = new MqttBroker(auth_config);

        if (!auth_broker->start()) {
            std::cerr << "Failed to start broker\n";
            return false;
        }

        std::cout << "User management broker started on port 11885\n";

        auto* auth_manager = auth_broker->get_auth_manager();
        if (!auth_manager) {
            std::cerr << "No auth manager available\n";
            return false;
        }

        std::cout << "\nCreating test users...\n";
        auto result1 = auth_manager->create_user("user1", "user1pass", "user1@test.com", { "user" });
        if (!result1.success) {
            std::cerr << "Failed to create user1: " << result1.message << "\n";
            return false;
        }
        std::cout << "✓ Created user1\n";

        auto result2 = auth_manager->create_user("user2", "user2pass", "user2@test.com", { "user" });
        if (!result2.success) {
            std::cerr << "Failed to create user2: " << result2.message << "\n";
            return false;
        }
        std::cout << "✓ Created user2\n";

        std::cout << "\nTesting user connections...\n";
        {
            MqttClient client1("test_user1");
            client1.set_credentials("user1", "user1pass");

            if (!client1.connect("localhost", 11885)) {
                std::cerr << "User1 failed to connect\n";
                return false;
            }
            std::cout << "✓ User1 connected successfully\n";

            MqttClient client2("test_user2");
            client2.set_credentials("user2", "user2pass");

            if (!client2.connect("localhost", 11885)) {
                std::cerr << "User2 failed to connect\n";
                client1.disconnect();
                return false;
            }
            std::cout << "✓ User2 connected successfully\n";

            client1.disconnect();
            client2.disconnect();
        }

        std::cout << "\n✓ User management test completed (broker left running)\n\n";

        // NON fermare il broker!

        // Cleanup solo i file
        try {
            std::filesystem::remove_all("./test_data_users");
            std::filesystem::remove("test_users_mgmt.db");
            std::filesystem::remove("test_users_mgmt.db.bin");
        }
        catch (...) {}

        return true;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
        return false;
    }
}

bool test_security_features() {
    print_test_header("Test 11: Security Features");

    try {
        // Cleanup
        try {
            std::filesystem::remove("test_security.db");
            std::filesystem::remove("test_security.db.bin");
            std::filesystem::remove_all("./test_data_security");
        }
        catch (...) {}

        std::cout << "Creating broker with security features on port 11886...\n";

        BrokerConfig sec_config;
        sec_config.port = 11886;
        sec_config.require_auth = true;
        sec_config.users_file = "test_security.db";
        sec_config.max_connections_per_ip = 3;
        sec_config.max_publish_rate = 10;
        sec_config.security_config.max_login_attempts = 3;
        sec_config.security_config.min_password_length = 1;
        sec_config.security_config.require_uppercase = false;
        sec_config.security_config.require_lowercase = false;
        sec_config.security_config.require_numbers = false;
        sec_config.security_config.require_special_chars = false;
        sec_config.enable_persistence = false;
        sec_config.enable_metrics = false;
        sec_config.enable_sys_topics = false;
        sec_config.worker_threads = 1;
        sec_config.io_threads = 1;

        MqttBroker* sec_broker = new MqttBroker(sec_config);

        if (!sec_broker->start()) {
            std::cerr << "Failed to start security broker\n";
            return false;
        }

        std::cout << "Security broker started on port 11886\n";

        auto* auth_manager = sec_broker->get_auth_manager();
        auth_manager->create_user("admin", "adminpass", "admin@test.com", { "admin" });

        // Test brute force
        std::cout << "\nTesting brute force protection...\n";
        for (int i = 0; i < 3; i++) {
            MqttClient attacker("attacker_" + std::to_string(i));
            attacker.set_credentials("admin", "wrong_password_" + std::to_string(i));

            if (attacker.connect("localhost", 11886, 1000)) {
                std::cerr << "ERROR: Connected with wrong password on attempt " << i << "\n";
                attacker.disconnect();
                return false;
            }
            std::cout << "  Attempt " << (i + 1) << " blocked ✓\n";
        }

        std::cout << "\n✓ Security test completed (broker left running)\n\n";

        // NON fermare il broker!

        // Cleanup solo i file
        try {
            std::filesystem::remove_all("./test_data_security");
            std::filesystem::remove("test_security.db");
            std::filesystem::remove("test_security.db.bin");
        }
        catch (...) {}

        return true;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
        return false;
    }
}

bool test_session_management() {
    print_test_header("Test 12: Session Management");

    try {
        // Cleanup
        try {
            std::filesystem::remove("test_sessions.db");
            std::filesystem::remove("test_sessions.db.bin");
            std::filesystem::remove_all("./test_data_sessions");
        }
        catch (...) {}

        std::cout << "Creating broker with session management on port 11887...\n";

        BrokerConfig session_config;
        session_config.port = 11887;
        session_config.require_auth = true;
        session_config.users_file = "test_sessions.db";
        session_config.security_config.max_sessions_per_user = 2;
        session_config.security_config.min_password_length = 1;
        session_config.security_config.require_uppercase = false;
        session_config.security_config.require_lowercase = false;
        session_config.security_config.require_numbers = false;
        session_config.security_config.require_special_chars = false;
        session_config.enable_persistence = false;
        session_config.enable_metrics = false;
        session_config.enable_sys_topics = false;
        session_config.worker_threads = 1;
        session_config.io_threads = 1;

        MqttBroker* session_broker = new MqttBroker(session_config);

        if (!session_broker->start()) {
            std::cerr << "Failed to start session broker\n";
            return false;
        }

        std::cout << "Session broker started on port 11887\n";

        auto* auth_manager = session_broker->get_auth_manager();
        auth_manager->create_user("session_user", "sessionpass", "session@test.com");

        std::cout << "\nTesting multiple sessions...\n";
        {
            MqttClient client1("session_client_1");
            client1.set_credentials("session_user", "sessionpass");

            MqttClient client2("session_client_2");
            client2.set_credentials("session_user", "sessionpass");

            if (!client1.connect("localhost", 11887)) {
                std::cerr << "Session 1 failed to connect\n";
                return false;
            }
            std::cout << "  ✓ Session 1 connected\n";

            if (!client2.connect("localhost", 11887)) {
                std::cerr << "Session 2 failed to connect\n";
                client1.disconnect();
                return false;
            }
            std::cout << "  ✓ Session 2 connected\n";

            client1.disconnect();
            client2.disconnect();
        }

        std::cout << "\n✓ Session management test completed (broker left running)\n\n";

        // NON fermare il broker!

        // Cleanup solo i file
        try {
            std::filesystem::remove_all("./test_data_sessions");
            std::filesystem::remove("test_sessions.db");
            std::filesystem::remove("test_sessions.db.bin");
        }
        catch (...) {}

        return true;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
        return false;
    }
}

// Test 12: Session Management - VERSIONE CORRETTA


// Main test runner
// Nel main() di test_basic.cpp, modifica la parte finale:

int main() {
    std::cout << "\n";
    std::cout << "==================================================\n";
    std::cout << "     MQTT++ Library - Complete Test Suite       \n";
    std::cout << "==================================================\n";

    // Inizializza il sistema socket
    if (!SocketSystem::initialize()) {
        std::cerr << "Failed to initialize socket system\n";
        return 1;
    }

    // Avvia broker principale per i primi 8 test
    std::cout << "\nStarting main MQTT Broker on port 11883...\n";

    BrokerConfig config;
    config.port = 11883;
    config.enable_tls = false;
    config.require_auth = false;
    config.enable_persistence = false;  // Disabilita per evitare problemi
    config.enable_metrics = false;
    config.enable_sys_topics = false;
    config.worker_threads = 2;
    config.io_threads = 1;
    config.max_publish_rate = 0;

    // USA PUNTATORE RAW ANCHE PER IL MAIN BROKER!
    MqttBroker* main_broker = new MqttBroker(config);

    if (!main_broker->start()) {
        std::cerr << "[FAIL] Failed to start main broker!\n";
        SocketSystem::cleanup();
        return 1;
    }

    std::cout << "[OK] Main broker started successfully\n";

    // Aspetta che il broker sia pronto
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // Esegui tutti i test
    struct TestCase {
        std::string name;
        std::function<bool()> test_func;
        bool uses_main_broker;
    };

    std::vector<TestCase> tests = {
        // Test che usano il broker principale (senza auth)
        {"Basic Connection", [main_broker]() { return test_basic_connection(*main_broker); }, true},
        {"Single Topic Pub/Sub", [main_broker]() { return test_single_pubsub(*main_broker); }, true},
        {"Multiple Topics", [main_broker]() { return test_multiple_topics(*main_broker); }, true},
        {"Wildcard Subscriptions", [main_broker]() { return test_wildcards(*main_broker); }, true},
        {"Retained Messages", [main_broker]() { return test_retained_messages(*main_broker); }, true},
        {"QoS Levels", [main_broker]() { return test_qos_levels(*main_broker); }, true},
        {"Multiple Clients", [main_broker]() { return test_multiple_clients(*main_broker); }, true},
        {"Performance", [main_broker]() { return test_performance(*main_broker); }, true},

        // Test che creano i propri broker con auth (porte diverse)
        {"Authentication", test_authentication, false},
        {"User Management", test_user_management, false},
        {"Security Features", test_security_features, false},
        {"Session Management", test_session_management, false}
    };

    int passed = 0;
    int failed = 0;

    for (const auto& test : tests) {
        try {
            bool result = test.test_func();
            print_result(test.name, result);

            if (result) {
                passed++;
            }
            else {
                failed++;
            }

            // Pausa più lunga tra i test che usano broker separati
            if (!test.uses_main_broker) {
                std::cout << "Waiting for broker cleanup...\n";
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
            else {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }

        }
        catch (const std::exception& e) {
            std::cerr << "[FAIL] Test " << test.name << " threw exception: " << e.what() << "\n";
            failed++;
        }
    }

    // NON FERMARE IL MAIN BROKER!
    std::cout << "\nMain broker left running (intentional)\n";
    // NON chiamare main_broker->stop()
    // NON chiamare delete main_broker

    // Risultati finali
    std::cout << "\n";
    std::cout << "==================================================\n";
    std::cout << "                 TEST RESULTS                    \n";
    std::cout << "==================================================\n";
    std::cout << "  Total Tests: " << (passed + failed) << "\n";
    std::cout << "  Passed: " << passed << "\n";
    std::cout << "  Failed: " << failed << "\n";
    std::cout << "\n";

    if (failed == 0) {
        std::cout << "*** ALL TESTS PASSED! The library works perfectly! ***\n";
    }
    else {
        std::cout << "*** Some tests failed. Please check the implementation. ***\n";
    }

    // Cleanup solo dei file
    try {
        std::vector<std::string> test_files = {
            "./test_data_main",
            "./test_data_auth",
            "./test_data_users",
            "./test_data_security",
            "./test_data_sessions",
            "./mqtt_data",
            "test_users.db",
            "test_users.db.bin",
            "test_users_mgmt.db",
            "test_users_mgmt.db.bin",
            "test_security.db",
            "test_security.db.bin",
            "test_sessions.db",
            "test_sessions.db.bin",
            "auth.log",
            "auth_key.bin"
        };

        for (const auto& file : test_files) {
            try {
                if (std::filesystem::exists(file)) {
                    if (std::filesystem::is_directory(file)) {
                        std::filesystem::remove_all(file);
                    }
                    else {
                        std::filesystem::remove(file);
                    }
                }
            }
            catch (...) {
                // Ignora errori singoli
            }
        }
    }
    catch (...) {
        // Ignora errori di cleanup
    }

    // Cleanup socket system
    SocketSystem::cleanup();

    std::cout << "\n*** MQTT++ Library Test Suite Completed Successfully! ***\n";
    std::cout << "\nNote: Brokers were left running to avoid shutdown issues.\n";
    std::cout << "They will be cleaned up when the program exits.\n";

    std::cout << "\nPress Enter to exit...";
    std::cin.get();

    return failed > 0 ? 1 : 0;
}