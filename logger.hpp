// logger.hpp - VERSIONE CORRETTA
#ifndef OUR_LOGGER_HPP
#define OUR_LOGGER_HPP

#include <iostream>
#include <sstream>
#include <mutex>
#include <chrono>
#include <iomanip>

namespace ourmqtt {

    class SimpleLogger {
    private:
        static std::mutex& get_mutex() {
            static std::mutex mutex_;
            return mutex_;
        }

    public:
        template<typename T>
        static void log(const std::string& level, const std::string& prefix, const T& message) {
            std::lock_guard<std::mutex> lock(get_mutex());

            auto now = std::chrono::system_clock::now();
            auto time_t = std::chrono::system_clock::to_time_t(now);

            struct tm tm_info;
#ifdef _WIN32
            localtime_s(&tm_info, &time_t);
#else
            tm_info = *std::localtime(&time_t);
#endif

            std::cout << std::put_time(&tm_info, "%H:%M:%S")
                << " [" << level << "][" << prefix << "] "
                << message
                << "\n";
            std::cout.flush();
        }
    };

    // NON definire il mutex qui - usa il pattern singleton sopra

    class LogStream {
    private:
        std::stringstream ss_;
        std::string level_;
        std::string prefix_;

    public:
        LogStream(const std::string& level, const std::string& prefix)
            : level_(level), prefix_(prefix) {}

        template<typename T>
        LogStream& operator<<(const T& value) {
            ss_ << value;
            return *this;
        }

        ~LogStream() {
            SimpleLogger::log(level_, prefix_, ss_.str());
        }
    };

#define LOG_DEBUG(prefix) ::ourmqtt::LogStream("DEBUG", prefix)
#define LOG_INFO(prefix) ::ourmqtt::LogStream("INFO", prefix)
#define LOG_WARN(prefix) ::ourmqtt::LogStream("WARN", prefix)
#define LOG_ERROR(prefix) ::ourmqtt::LogStream("ERROR", prefix)

} // namespace ourmqtt

#endif