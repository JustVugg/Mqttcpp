// complete_test.cpp - Test completo MQTT
#include "mqtt_client.hpp"
#include "mqtt_broker.hpp"
#include <iostream>
#include <thread>
#include <vector>

using namespace ourmqtt;

void run_test_scenario() {
    // Avvia broker in thread separato
    std::thread broker_thread([]() {
        MqttBroker broker(1883);
        if (broker.start()) {
            std::cout << "[BROKER] Started on port 1883\n";
            // Mantieni il broker attivo
            while (true) {
                Time::sleep_ms(1000);
            }
        }
        });
    broker_thread.detach();

    // Aspetta che il broker si avvii
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Test 1: Client multipli
    std::cout << "\n=== TEST 1: Multiple Clients ===\n";

    std::vector<std::unique_ptr<MqttClient>> clients;

    // Crea 3 subscriber
    for (int i = 0; i < 3; i++) {
        auto client = std::make_unique<MqttClient>("subscriber_" + std::to_string(i));

        client->set_message_callback(
            [i](const std::string& topic, const std::vector<uint8_t>& payload) {
                std::string msg(payload.begin(), payload.end());
                std::cout << "[SUB" << i << "] " << topic << ": " << msg << std::endl;
            }
        );

        if (client->connect("localhost", 1883)) {
            client->subscribe("test/data", QOS_1);
            clients.push_back(std::move(client));
        }
    }

    // Publisher
    MqttClient publisher("publisher_main");
    if (publisher.connect("localhost", 1883)) {
        for (int i = 0; i < 5; i++) {
            std::string msg = "Message #" + std::to_string(i);
            publisher.publish("test/data", msg, QOS_1);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    // Test 2: QoS Levels
    std::cout << "\n=== TEST 2: QoS Levels ===\n";

    publisher.publish("test/qos0", "QoS 0 message", QOS_0);
    publisher.publish("test/qos1", "QoS 1 message", QOS_1);
    publisher.publish("test/qos2", "QoS 2 message", QOS_2);

    // Test 3: Retained Messages
    std::cout << "\n=== TEST 3: Retained Messages ===\n";

    publisher.publish("test/retained", "This is retained", QOS_1, true);

    // Nuovo subscriber dovrebbe ricevere il messaggio retained
    std::this_thread::sleep_for(std::chrono::seconds(1));
    MqttClient late_subscriber("late_subscriber");
    late_subscriber.set_message_callback(
        [](const std::string& topic, const std::vector<uint8_t>& payload) {
            std::string msg(payload.begin(), payload.end());
            std::cout << "[LATE_SUB] Retained message: " << topic << ": " << msg << std::endl;
        }
    );

    if (late_subscriber.connect("localhost", 1883)) {
        late_subscriber.subscribe("test/retained", QOS_0);
    }

    // Test 4: Wildcards
    std::cout << "\n=== TEST 4: Wildcard Subscriptions ===\n";

    MqttClient wildcard_client("wildcard_sub");
    wildcard_client.set_message_callback(
        [](const std::string& topic, const std::vector<uint8_t>& payload) {
            std::string msg(payload.begin(), payload.end());
            std::cout << "[WILDCARD] " << topic << ": " << msg << std::endl;
        }
    );

    if (wildcard_client.connect("localhost", 1883)) {
        wildcard_client.subscribe("sensors/+/temperature", QOS_0);
        wildcard_client.subscribe("logs/#", QOS_0);

        // Pubblica su vari topic
        publisher.publish("sensors/room1/temperature", "22°C", QOS_0);
        publisher.publish("sensors/room2/temperature", "24°C", QOS_0);
        publisher.publish("sensors/room1/humidity", "60%", QOS_0); // Non dovrebbe essere ricevuto
        publisher.publish("logs/error/app", "Error occurred", QOS_0);
        publisher.publish("logs/info/system", "System started", QOS_0);
    }

    std::cout << "\nPress Enter to finish tests...\n";
    std::cin.get();
}

int main() {
    std::cout << "=================================\n";
    std::cout << "    OurMQTT Complete Test Suite\n";
    std::cout << "=================================\n\n";

    try {
        run_test_scenario();
    }
    catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}