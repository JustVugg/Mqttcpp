// time_utils.hpp - CORRETTO per Windows
#ifndef OUR_TIME_UTILS_HPP
#define OUR_TIME_UTILS_HPP

#include <chrono>
#include <cstdint>
#include <thread>
#include <string>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace ourmqtt {

    class Time {
    public:
        static uint64_t now_ms() {
            using namespace std::chrono;
            return duration_cast<milliseconds>(
                steady_clock::now().time_since_epoch()
                ).count();
        }

        static uint64_t now_us() {
            using namespace std::chrono;
            return duration_cast<microseconds>(
                steady_clock::now().time_since_epoch()
                ).count();
        }

        static void sleep_ms(uint32_t ms) {
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        }

        static std::string timestamp() {
            auto now = std::chrono::system_clock::now();
            auto time_t = std::chrono::system_clock::to_time_t(now);

            char buffer[100];

#ifdef _WIN32
            // Usa localtime_s per Windows (versione sicura)
            struct tm tm_info;
            localtime_s(&tm_info, &time_t);
            std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm_info);
#else
            // Linux/Unix usa localtime standard
            std::tm* tm = std::localtime(&time_t);
            std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", tm);
#endif

            return std::string(buffer);
        }
    };

    class Timer {
    private:
        std::chrono::steady_clock::time_point start_time_;

    public:
        Timer() : start_time_(std::chrono::steady_clock::now()) {}

        void reset() {
            start_time_ = std::chrono::steady_clock::now();
        }

        uint64_t elapsed_ms() const {
            auto now = std::chrono::steady_clock::now();
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                now - start_time_
                ).count();
        }

        uint64_t elapsed_us() const {
            auto now = std::chrono::steady_clock::now();
            return std::chrono::duration_cast<std::chrono::microseconds>(
                now - start_time_
                ).count();
        }

        bool has_expired(uint64_t timeout_ms) const {
            return elapsed_ms() >= timeout_ms;
        }
    };

    class Stopwatch {
    private:
        std::chrono::high_resolution_clock::time_point start_;
        std::chrono::high_resolution_clock::time_point lap_;

    public:
        Stopwatch() {
            start_ = lap_ = std::chrono::high_resolution_clock::now();
        }

        void start() {
            start_ = lap_ = std::chrono::high_resolution_clock::now();
        }

        double lap() {
            auto now = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(now - lap_);
            lap_ = now;
            return duration.count() / 1000.0;
        }

        double elapsed() const {
            auto now = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(now - start_);
            return duration.count() / 1000.0;
        }
    };

}
#endif