// broker_main.cpp - Main per il broker
#include "mqtt_broker.hpp"
#include <iostream>
#include <csignal>

using namespace ourmqtt;

// Broker globale per gestione segnali
MqttBroker* g_broker = nullptr;

void signal_handler(int signal) {
    if (signal == SIGINT) {
        std::cout << "\n[BROKER] Received interrupt signal\n";
        if (g_broker) {
            g_broker->stop();
        }
    }
}

int main(int argc, char* argv[]) {
    // Porta di default
    uint16_t port = 1883;

    // Parse argomenti command line
    if (argc > 1) {
        port = std::stoi(argv[1]);
    }

    std::cout << "=================================\n";
    std::cout << "    OurMQTT Broker v1.0\n";
    std::cout << "=================================\n\n";

    try {
        // Crea e avvia broker
        MqttBroker broker(port);
        g_broker = &broker;

        // Registra handler per CTRL+C
        std::signal(SIGINT, signal_handler);

        if (!broker.start()) {
            std::cerr << "Failed to start broker!\n";
            return 1;
        }

        std::cout << "\nBroker is running on port " << port << "\n";
        std::cout << "Press CTRL+C to stop...\n\n";

        // Loop principale
        while (true) {
            Time::sleep_ms(1000);

            // Opzionale: stampa statistiche periodicamente
            static int counter = 0;
            if (++counter % 30 == 0) {  // Ogni 30 secondi
                std::cout << "[INFO] Active clients: "
                    << broker.get_client_count() << "\n";
            }
        }

    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}