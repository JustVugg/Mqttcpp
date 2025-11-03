
## Mqttcpp — Modern C++ MQTT Library

**mqttcpp** is a modern, lightweight, and high-performance **MQTT library written in C++20**.
It provides both **client** and **broker** implementations and includes advanced features such as authentication, retained messages, QoS levels, user and session management, and built-in security controls.

---

### 🚀 Features

* Full MQTT 3.1.1 / 5.0 protocol support
* Integrated **broker** and **client** implementation
* **QoS 0, 1, and 2** message delivery
* **Topic wildcards** (`+` and `#`)
* **Retained messages** and persistence
* **User authentication** and access roles
* **Security features** (login limits, rate control, etc.)
* **Session handling** and concurrent client management
* **Multi-threaded** message processing
* Optional **TLS** and logging support

---

### 🧪 Test Suite

The project includes a complete test program, [`complete_test.cpp`](./complete_test.cpp), which runs a full series of integration tests covering all major features of `mqttcpp`.

#### ✅ Included Tests

| #  | Test                 | Description                                |
| -- | -------------------- | ------------------------------------------ |
| 1  | Basic Connection     | Connect and disconnect a client            |
| 2  | Single Topic Pub/Sub | Basic publish/subscribe workflow           |
| 3  | Multiple Topics      | Subscribe to and handle multiple topics    |
| 4  | Wildcards            | Verify `+` and `#` topic filters           |
| 5  | Retained Messages    | Test retained message delivery             |
| 6  | QoS Levels           | Check message reliability for QoS 0/1/2    |
| 7  | Multiple Clients     | Broadcast a message to several clients     |
| 8  | Performance          | Measure throughput with 1000+ messages     |
| 9  | Authentication       | Verify user/password login                 |
| 10 | User Management      | Create and authenticate multiple users     |
| 11 | Security Features    | Test brute-force and rate-limit protection |
| 12 | Session Management   | Validate user session limits               |

---

### 🏗️ Building the Project

#### **Requirements**

* C++20 compatible compiler (GCC ≥ 10, Clang ≥ 12, or MSVC ≥ 2019)
* CMake ≥ 3.16
* SQLite3
* OpenSSL (for TLS support)
* pthread (Linux/macOS)

#### **Build Example**

```bash
git clone https://github.com/JustVugg/Mqttcpp.git
cd Mqttcpp
mkdir build && cd build
cmake ..
make -j4
```

This will compile:

* The Mqtcpp core library
* The full integration test executable (`mqtt_tests`)

If you prefer, you can also compile it directly:

```bash
g++ complete_test.cpp -std=c++20 -pthread -lsqlite3 -lssl -lcrypto -o mqtt_tests
```

---

### ▶️ Running the Tests

Run the compiled executable:

```bash
./mqtt_tests
```

You’ll see each test section printed clearly in the console:

```
========================================
 Test 2: Single Topic Pub/Sub
========================================
Subscribed to test/topic
Published: Hello MQTT World!
✅ Single Topic Pub/Sub PASSED
```

At the end, you’ll get a summary:

```
==================================================
                 TEST RESULTS                    
==================================================
  Total Tests: 12
  Passed: 12
  Failed: 0

*** ALL TESTS PASSED! The library works perfectly! ***
```

> 🧠 **Note:** Each test uses a dedicated broker on ports **11883–11887**.
> Make sure no other MQTT services (e.g., Mosquitto) are running on these ports.

---

### ⚙️ Example Usage

#### **Start a Broker**

```cpp
#include "mqtt_broker.hpp"
using namespace mqttcpp;

int main() {
    BrokerConfig cfg;
    cfg.port = 1883;
    cfg.require_auth = false;

    MqttBroker broker(cfg);
    broker.start();

    std::cout << "Broker running on port 1883...\n";
    std::cin.get();
}
```

#### **Connect a Client**

```cpp
#include "mqtt_client.hpp"
using namespace mqttcpp;

int main() {
    MqttClient client("example_client");
    client.connect("localhost", 1883);

    client.subscribe("demo/topic", QOS_0);
    client.publish("demo/topic", "Hello MQTT!", QOS_0);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    client.disconnect();
}
```

---

### 🔒 Security & Authentication

* User database via SQLite
* Password policies (length, symbols, etc.)
* Login attempt limits per IP
* Role-based authorization
* Session caps per user
* Persistent storage and optional encryption

---

### 📈 Performance

`mqttcpp` is optimized for speed:

* Lock-free message dispatch
* Threaded network I/O
* Efficient buffering
* Minimal latency between clients

The performance test (`test 8`) demonstrates high throughput even with 1000+ messages exchanged.

---

### 🧰 Project Structure

```
mqttcpp/
├── mqtt_broker.hpp
├── mqtt_client.hpp
├── logger.hpp
├── other_source_files.cpp / .hpp
└── test_basic.cpp
```

---

### 🧑‍💻 Author & License

**Author:** JustVugg
**License:** MIT License

You are free to use, modify, and distribute this library in personal or commercial projects.

---

### ⭐ Contributing

Pull requests and issues are welcome!
If you add features or bug fixes, please include:

* Clear commit messages
* Tests for your changes
* Updated documentation if needed

---

### 💬 Feedback

If you encounter a problem or have a feature request, please open an issue on GitHub with:

* System and compiler details
* Steps to reproduce
* Any relevant output or logs
