// subscriber_test.cpp - Test subscriber MQTT
#include "mqtt_client.hpp"
#include <iostream>
#include <thread>
#include <atomic>

using namespace ourmqtt;

int main() {
    std::cout << "=================================\n";
    std::cout << "    OurMQTT Subscriber Test\n";
    std::cout << "=================================\n\n";

    std::atomic<bool> running(true);

    try {
        // Crea client
        MqttClient subscriber("test_subscriber_001");

        // Callback per messaggi ricevuti
        subscriber.set_message_callback(
            [](const std::string& topic, const std::vector<uint8_t>& payload) {
                std::string message(payload.begin(), payload.end());
                std::cout << "[RECEIVED] " << topic << " -> " << message << std::endl;
            }
        );

        // Callback connessione
        subscriber.set_connect_callback([]() {
            std::cout << "[SUBSCRIBER] Connected successfully!\n";
            });

        // Connetti
        std::cout << "Connecting to localhost:1883...\n";
        if (!subscriber.connect("localhost", 1883)) {
            std::cerr << "Failed to connect!\n";
            return 1;
        }

        // Sottoscrivi a vari topic
        std::cout << "\nSubscribing to topics...\n";

        // Subscription singola
        subscriber.subscribe("test/temperature", QOS_1);
        std::cout << "Subscribed to: test/temperature\n";

        // Subscription con wildcard single-level
        subscriber.subscribe("test/+", QOS_0);
        std::cout << "Subscribed to: test/+ (all test subtopics)\n";

        // Subscription con wildcard multi-level
        subscriber.subscribe("alerts/#", QOS_2);
        std::cout << "Subscribed to: alerts/# (all alerts)\n";

        std::cout << "\nWaiting for messages... Press Enter to quit\n";

        // Aspetta input per terminare
        std::cin.get();
        running = false;

        // Disconnetti
        subscriber.disconnect();

    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}