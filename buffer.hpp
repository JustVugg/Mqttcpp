// buffer.hpp - Enterprise Production-Ready Buffer Implementation
// Version: 2.0.0
// Thread-safe, high-performance buffer with comprehensive error handling

#ifndef OURMQTT_BUFFER_HPP
#define OURMQTT_BUFFER_HPP

#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <memory>
#include <algorithm>
#include <stdexcept>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <chrono>
#include <limits>
#include <type_traits>
#include <optional>
#include <string_view>
#include <random>
#include <iostream>
#include <iomanip>
#include <cctype>
// Undefine le macro di Windows che causano conflitti
#ifdef ERROR
#undef ERROR
#endif
// Platform detection for endianness
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define OURMQTT_BIG_ENDIAN
#elif defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define OURMQTT_LITTLE_ENDIAN
#elif defined(_WIN32)
#define OURMQTT_LITTLE_ENDIAN
#else
#error "Unknown endianness"
#endif

namespace ourmqtt {

    // Forward declarations
    class ILogger;
    class IMetricsCollector;
    class IMemoryAllocator;

    // Version information
    struct BufferVersion {
        static constexpr uint32_t MAJOR = 2;
        static constexpr uint32_t MINOR = 0;
        static constexpr uint32_t PATCH = 0;
    };

    // Buffer exception hierarchy
    class BufferException : public std::exception {
    protected:
        std::string message_;
        std::string context_;
        std::chrono::system_clock::time_point timestamp_;

    public:
        BufferException(const std::string& msg, const std::string& ctx = "")
            : message_(msg), context_(ctx), timestamp_(std::chrono::system_clock::now()) {}

        const char* what() const noexcept override { return message_.c_str(); }
        const std::string& context() const noexcept { return context_; }
        std::chrono::system_clock::time_point when() const noexcept { return timestamp_; }
    };

    class BufferOverflowException : public BufferException {
    public:
        BufferOverflowException(size_t requested, size_t available)
            : BufferException(
                "Buffer overflow: requested " + std::to_string(requested) +
                " bytes, available " + std::to_string(available)) {}
    };

    class BufferUnderflowException : public BufferException {
    public:
        BufferUnderflowException(size_t requested, size_t available)
            : BufferException(
                "Buffer underflow: requested " + std::to_string(requested) +
                " bytes, available " + std::to_string(available)) {}
    };

    class BufferCorruptionException : public BufferException {
    public:
        explicit BufferCorruptionException(const std::string& details)
            : BufferException("Buffer corruption detected: " + details) {}
    };

    // Logger interface
    class ILogger {
    public:
        enum class Level {
            TRACE,
            DEBUG,
            INFO,
            WARN,
            ERROR,
            FATAL
        };

        virtual ~ILogger() = default;
        virtual void log(Level level, const std::string& message) = 0;
    };

    // Metrics collector interface
    class IMetricsCollector {
    public:
        virtual ~IMetricsCollector() = default;
        virtual void recordAllocation(size_t bytes) = 0;
        virtual void recordDeallocation(size_t bytes) = 0;
        virtual void recordReallocation(size_t old_size, size_t new_size) = 0;
        virtual void recordOperation(const std::string& op_name, std::chrono::nanoseconds duration) = 0;
    };

    // Memory allocator interface
    class IMemoryAllocator {
    public:
        virtual ~IMemoryAllocator() = default;
        virtual uint8_t* allocate(size_t size) = 0;
        virtual void deallocate(uint8_t* ptr, size_t size) = 0;
        virtual size_t max_size() const = 0;
    };

    // Default allocator using standard new/delete
    class DefaultAllocator : public IMemoryAllocator {
    public:
        uint8_t* allocate(size_t size) override {
            return new uint8_t[size];
        }

        void deallocate(uint8_t* ptr, size_t) override {
            delete[] ptr;
        }

        size_t max_size() const override {
            return std::numeric_limits<size_t>::max() / 2;
        }
    };

    // Pool allocator for better performance
    class PoolAllocator : public IMemoryAllocator {
    private:
        struct Block {
            std::unique_ptr<uint8_t[]> memory;
            size_t size;
            std::atomic<bool> in_use{ false };

            // Costruttore di default
            Block() = default;

            // Costruttore di move
            Block(Block&& other) noexcept
                : memory(std::move(other.memory)),
                size(other.size),
                in_use(other.in_use.load()) {
                other.size = 0;
            }

            // Operatore di assegnazione move
            Block& operator=(Block&& other) noexcept {
                if (this != &other) {
                    memory = std::move(other.memory);
                    size = other.size;
                    in_use = other.in_use.load();
                    other.size = 0;
                }
                return *this;
            }

            // Delete copy constructor e copy assignment
            Block(const Block&) = delete;
            Block& operator=(const Block&) = delete;
        };

        mutable std::shared_mutex mutex_;
        std::vector<Block> blocks_;
        const size_t block_size_;
        const size_t max_blocks_;
        std::atomic<size_t> allocated_count_{ 0 };
        std::atomic<size_t> pool_hits_{ 0 };
        std::atomic<size_t> pool_misses_{ 0 };

    public:
        explicit PoolAllocator(size_t block_size = 4096, size_t max_blocks = 1000)
            : block_size_(block_size), max_blocks_(max_blocks) {
            blocks_.reserve(max_blocks);
        }

        uint8_t* allocate(size_t size) override {
            // Try to find a suitable block
            {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                for (auto& block : blocks_) {
                    bool expected = false;
                    if (block.size >= size &&
                        block.in_use.compare_exchange_strong(expected, true)) {
                        pool_hits_++;
                        return block.memory.get();
                    }
                }
            }

            // Create new block if possible
            {
                std::unique_lock<std::shared_mutex> lock(mutex_);
                if (blocks_.size() < max_blocks_) {
                    size_t alloc_size = std::max(size, block_size_);

                    Block new_block;
                    new_block.memory = std::make_unique<uint8_t[]>(alloc_size);
                    new_block.size = alloc_size;
                    new_block.in_use = true;

                    blocks_.push_back(std::move(new_block));
                    allocated_count_++;
                    pool_hits_++;
                    return blocks_.back().memory.get();
                }
            }

            // Fallback to regular allocation
            pool_misses_++;
            return new uint8_t[size];
        }

        void deallocate(uint8_t* ptr, size_t) override {
            if (!ptr) return;

            std::shared_lock<std::shared_mutex> lock(mutex_);
            for (auto& block : blocks_) {
                if (block.memory.get() == ptr) {
                    block.in_use = false;
                    return;
                }
            }

            // Not from pool
            delete[] ptr;
        }

        size_t max_size() const override {
            return std::numeric_limits<size_t>::max() / 2;
        }

        // Statistics
        struct Stats {
            size_t allocated_blocks;
            size_t pool_hits;
            size_t pool_misses;
            double hit_rate;
        };

        Stats get_stats() const {
            size_t hits = pool_hits_.load();
            size_t misses = pool_misses_.load();
            size_t total = hits + misses;

            return {
                allocated_count_.load(),
                hits,
                misses,
                total > 0 ? static_cast<double>(hits) / total : 0.0
            };
        }
    };

    // Buffer configuration
    struct BufferConfig {
        size_t initial_capacity = 4096;
        size_t max_capacity = 16 * 1024 * 1024;  // 16MB default
        size_t growth_factor = 2;
        size_t max_string_length = 65535;  // Max string length for safety
        size_t max_variable_length_bytes = 4;  // Max bytes for variable length encoding
        bool thread_safe = true;
        bool zero_on_clear = true;  // Security: zero memory
        bool track_metrics = true;
        bool validate_operations = true;  // Extra validation in debug
        std::shared_ptr<IMemoryAllocator> allocator;
        std::shared_ptr<ILogger> logger;
        std::shared_ptr<IMetricsCollector> metrics;

        // Validate configuration
        void validate() const {
            if (initial_capacity == 0) {
                throw std::invalid_argument("Initial capacity must be > 0");
            }
            if (max_capacity < initial_capacity) {
                throw std::invalid_argument("Max capacity must be >= initial capacity");
            }
            if (growth_factor < 1) {
                throw std::invalid_argument("Growth factor must be >= 1");
            }
            if (max_string_length > max_capacity) {
                throw std::invalid_argument("Max string length exceeds max capacity");
            }
        }
    };

    // Main Buffer class
    class Buffer {
    private:
        // Internal state
        uint8_t* data_;
        size_t capacity_;
        size_t write_pos_;
        size_t read_pos_;
        BufferConfig config_;
        mutable std::unique_ptr<std::shared_mutex> mutex_;

        // Statistics
        mutable std::atomic<uint64_t> total_bytes_written_{ 0 };
        mutable std::atomic<uint64_t> total_bytes_read_{ 0 };
        mutable std::atomic<uint32_t> realloc_count_{ 0 };
        mutable std::atomic<uint32_t> operation_count_{ 0 };

        // Checksum for corruption detection (optional)
        mutable std::atomic<uint32_t> checksum_{ 0 };

        // Helper class for RAII-based metrics timing
        class MetricsTimer {
            IMetricsCollector* collector_;
            std::string operation_;
            std::chrono::high_resolution_clock::time_point start_;

        public:


            MetricsTimer(IMetricsCollector* collector, const std::string& op)
                : collector_(collector), operation_(op),
                start_(std::chrono::high_resolution_clock::now()) {}

            ~MetricsTimer() {
                if (collector_) {
                    auto duration = std::chrono::high_resolution_clock::now() - start_;
                    collector_->recordOperation(operation_,
                        std::chrono::duration_cast<std::chrono::nanoseconds>(duration));
                }
            }
        };

    public:
        // Constructors

        explicit Buffer(size_t initial_capacity = 4096)
            : Buffer(BufferConfig{ initial_capacity }) {}

        explicit Buffer(BufferConfig config)
            : config_(std::move(config)), capacity_(0), write_pos_(0), read_pos_(0), data_(nullptr) {

            // Validate configuration
            config_.validate();

            // Set defaults if not provided
            if (!config_.allocator) {
                config_.allocator = std::make_shared<DefaultAllocator>();
            }

            // Initialize thread safety
            if (config_.thread_safe) {
                mutex_ = std::make_unique<std::shared_mutex>();
            }

            // Allocate initial capacity
            capacity_ = config_.initial_capacity;
            data_ = config_.allocator->allocate(capacity_);

            if (!data_) {
                throw std::bad_alloc();
            }

            // Zero initialize for security
            std::memset(data_, 0, capacity_);

            // Log creation
            log(ILogger::Level::DEBUG, "Buffer created with capacity " +
                std::to_string(capacity_));

            // Record metrics
            if (config_.metrics) {
                config_.metrics->recordAllocation(capacity_);
            }
        }

        // Destructor
        ~Buffer() {
            try {
                auto lock = get_write_lock();

                // Zero memory if configured
                if (config_.zero_on_clear && data_) {
                    secure_zero_memory(data_, capacity_);
                }

                // Deallocate
                if (data_ && config_.allocator) {
                    config_.allocator->deallocate(data_, capacity_);

                    if (config_.metrics) {
                        config_.metrics->recordDeallocation(capacity_);
                    }
                }

                log(ILogger::Level::DEBUG, "Buffer destroyed");
            }
            catch (...) {
                // Don't throw from destructor
            }
        }

        // Delete copy operations
        Buffer(const Buffer&) = delete;
        Buffer& operator=(const Buffer&) = delete;

        // Move constructor
        Buffer(Buffer&& other) noexcept {
            auto lock = other.get_write_lock();

            data_ = other.data_;
            capacity_ = other.capacity_;
            write_pos_ = other.write_pos_;
            read_pos_ = other.read_pos_;
            config_ = std::move(other.config_);
            mutex_ = std::move(other.mutex_);

            total_bytes_written_ = other.total_bytes_written_.load();
            total_bytes_read_ = other.total_bytes_read_.load();
            realloc_count_ = other.realloc_count_.load();
            operation_count_ = other.operation_count_.load();
            checksum_ = other.checksum_.load();

            other.data_ = nullptr;
            other.capacity_ = 0;
            other.write_pos_ = 0;
            other.read_pos_ = 0;
        }

        // Move assignment
        Buffer& operator=(Buffer&& other) noexcept {
            if (this != &other) {
                // Clean up current buffer
                {
                    auto lock = get_write_lock();
                    if (config_.zero_on_clear && data_) {
                        secure_zero_memory(data_, capacity_);
                    }
                    if (data_ && config_.allocator) {
                        config_.allocator->deallocate(data_, capacity_);
                    }
                }

                // Move from other
                auto lock = other.get_write_lock();

                data_ = other.data_;
                capacity_ = other.capacity_;
                write_pos_ = other.write_pos_;
                read_pos_ = other.read_pos_;
                config_ = std::move(other.config_);
                mutex_ = std::move(other.mutex_);

                total_bytes_written_ = other.total_bytes_written_.load();
                total_bytes_read_ = other.total_bytes_read_.load();
                realloc_count_ = other.realloc_count_.load();
                operation_count_ = other.operation_count_.load();
                checksum_ = other.checksum_.load();

                other.data_ = nullptr;
                other.capacity_ = 0;
                other.write_pos_ = 0;
                other.read_pos_ = 0;
            }
            return *this;
        }

        // Write operations
        void write_byte(uint8_t byte) {
            MetricsTimer timer(config_.metrics.get(), "write_byte");
            auto lock = get_write_lock();

            ensure_capacity(1);
            data_[write_pos_++] = byte;
            total_bytes_written_++;
            update_checksum(byte);
            operation_count_++;
        }

        void write_uint16(uint16_t value) {
            MetricsTimer timer(config_.metrics.get(), "write_uint16");
            auto lock = get_write_lock();

            ensure_capacity(2);

            // Network byte order (big-endian)
            data_[write_pos_++] = (value >> 8) & 0xFF;
            data_[write_pos_++] = value & 0xFF;

            total_bytes_written_ += 2;
            update_checksum(value);
            operation_count_++;
        }

        void write_uint32(uint32_t value) {
            MetricsTimer timer(config_.metrics.get(), "write_uint32");
            auto lock = get_write_lock();

            ensure_capacity(4);

            // Network byte order (big-endian)
            data_[write_pos_++] = (value >> 24) & 0xFF;
            data_[write_pos_++] = (value >> 16) & 0xFF;
            data_[write_pos_++] = (value >> 8) & 0xFF;
            data_[write_pos_++] = value & 0xFF;

            total_bytes_written_ += 4;
            update_checksum(value);
            operation_count_++;
        }

        void write_uint64(uint64_t value) {
            MetricsTimer timer(config_.metrics.get(), "write_uint64");
            auto lock = get_write_lock();

            ensure_capacity(8);

            // Network byte order (big-endian)
            for (int i = 7; i >= 0; i--) {
                data_[write_pos_++] = (value >> (i * 8)) & 0xFF;
            }

            total_bytes_written_ += 8;
            update_checksum(value);
            operation_count_++;
        }

        void write_string(std::string_view str) {
            MetricsTimer timer(config_.metrics.get(), "write_string");

            if (str.length() > config_.max_string_length) {
                throw std::length_error("String length " + std::to_string(str.length()) +
                    " exceeds maximum " + std::to_string(config_.max_string_length));
            }

            write_uint16(static_cast<uint16_t>(str.length()));
            write_bytes(reinterpret_cast<const uint8_t*>(str.data()), str.length());
        }

        void write_bytes(const uint8_t* bytes, size_t len) {
            if (!bytes && len > 0) {
                throw std::invalid_argument("Null pointer with non-zero length");
            }

            if (len == 0) return;

            MetricsTimer timer(config_.metrics.get(), "write_bytes");
            auto lock = get_write_lock();

            ensure_capacity(len);
            std::memcpy(data_ + write_pos_, bytes, len);
            write_pos_ += len;
            total_bytes_written_ += len;

            // Update checksum
            for (size_t i = 0; i < len; ++i) {
                update_checksum(bytes[i]);
            }

            operation_count_++;
        }

        void write_variable_length(uint32_t value) {
            MetricsTimer timer(config_.metrics.get(), "write_variable_length");
            auto lock = get_write_lock();

            size_t bytes_written = 0;

            do {
                if (bytes_written >= config_.max_variable_length_bytes) {
                    throw std::overflow_error("Variable length encoding exceeds maximum bytes");
                }

                uint8_t byte = value & 0x7F;
                value >>= 7;

                if (value > 0) {
                    byte |= 0x80;
                }

                ensure_capacity(1);
                data_[write_pos_++] = byte;
                total_bytes_written_++;
                update_checksum(byte);
                bytes_written++;

            } while (value > 0);

            operation_count_++;
        }

        // Read operations
        uint8_t read_byte() {
            MetricsTimer timer(config_.metrics.get(), "read_byte");
            auto lock = get_read_lock();

            if (read_pos_ >= write_pos_) {
                throw BufferUnderflowException(1, write_pos_ - read_pos_);
            }

            uint8_t value = data_[read_pos_++];
            total_bytes_read_++;
            operation_count_++;

            return value;
        }

        uint8_t peek_byte() const {
            auto lock = get_read_lock();

            if (read_pos_ >= write_pos_) {
                throw BufferUnderflowException(1, write_pos_ - read_pos_);
            }

            return data_[read_pos_];
        }

        uint16_t read_uint16() {
            MetricsTimer timer(config_.metrics.get(), "read_uint16");
            auto lock = get_read_lock();

            if (read_pos_ + 2 > write_pos_) {
                throw BufferUnderflowException(2, write_pos_ - read_pos_);
            }

            uint16_t value = (static_cast<uint16_t>(data_[read_pos_]) << 8) |
                static_cast<uint16_t>(data_[read_pos_ + 1]);
            read_pos_ += 2;
            total_bytes_read_ += 2;
            operation_count_++;

            return value;
        }

        uint32_t read_uint32() {
            MetricsTimer timer(config_.metrics.get(), "read_uint32");
            auto lock = get_read_lock();

            if (read_pos_ + 4 > write_pos_) {
                throw BufferUnderflowException(4, write_pos_ - read_pos_);
            }

            uint32_t value = (static_cast<uint32_t>(data_[read_pos_]) << 24) |
                (static_cast<uint32_t>(data_[read_pos_ + 1]) << 16) |
                (static_cast<uint32_t>(data_[read_pos_ + 2]) << 8) |
                static_cast<uint32_t>(data_[read_pos_ + 3]);
            read_pos_ += 4;
            total_bytes_read_ += 4;
            operation_count_++;

            return value;
        }

        uint64_t read_uint64() {
            MetricsTimer timer(config_.metrics.get(), "read_uint64");
            auto lock = get_read_lock();

            if (read_pos_ + 8 > write_pos_) {
                throw BufferUnderflowException(8, write_pos_ - read_pos_);
            }

            uint64_t value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | data_[read_pos_++];
            }

            total_bytes_read_ += 8;
            operation_count_++;

            return value;
        }

        std::string read_string() {
            MetricsTimer timer(config_.metrics.get(), "read_string");

            uint16_t len = read_uint16();

            if (len > config_.max_string_length) {
                // Rollback the read
                read_pos_ -= 2;
                throw std::length_error("String length " + std::to_string(len) +
                    " exceeds maximum " + std::to_string(config_.max_string_length));
            }

            auto lock = get_read_lock();

            if (read_pos_ + len > write_pos_) {
                read_pos_ -= 2;  // Rollback uint16 read
                throw BufferUnderflowException(len, write_pos_ - read_pos_);
            }

            std::string result(reinterpret_cast<const char*>(data_ + read_pos_), len);
            read_pos_ += len;
            total_bytes_read_ += len;

            return result;
        }

        void read_bytes(uint8_t* dest, size_t len) {
            if (!dest && len > 0) {
                throw std::invalid_argument("Null destination with non-zero length");
            }

            if (len == 0) return;

            MetricsTimer timer(config_.metrics.get(), "read_bytes");
            auto lock = get_read_lock();

            if (read_pos_ + len > write_pos_) {
                throw BufferUnderflowException(len, write_pos_ - read_pos_);
            }

            std::memcpy(dest, data_ + read_pos_, len);
            read_pos_ += len;
            total_bytes_read_ += len;
            operation_count_++;
        }

        std::optional<uint32_t> read_variable_length() {
            MetricsTimer timer(config_.metrics.get(), "read_variable_length");
            auto lock = get_read_lock();

            uint32_t value = 0;
            uint32_t multiplier = 1;
            size_t bytes_read = 0;
            size_t saved_pos = read_pos_;
            uint8_t byte = 0;

            do {
                if (read_pos_ >= write_pos_) {
                    read_pos_ = saved_pos;
                    return std::nullopt;
                }

                if (bytes_read >= config_.max_variable_length_bytes) {
                    read_pos_ = saved_pos;
                    log(ILogger::Level::WARN, "Variable length encoding exceeds maximum bytes");
                    return std::nullopt;
                }

                byte = data_[read_pos_++];
                bytes_read++;

                value += (byte & 0x7F) * multiplier;

                if (multiplier > 128 * 128 * 128) {
                    read_pos_ = saved_pos;
                    return std::nullopt;
                }

                multiplier *= 128;

            } while ((byte & 0x80) != 0);

            total_bytes_read_ += bytes_read;
            operation_count_++;

            return value;
        }

        // Aggiungi questo overload per read_variable_length che accetta un parametro di output
        bool read_variable_length(uint32_t& value) {
            auto result = read_variable_length();
            if (result.has_value()) {
                value = result.value();
                return true;
            }
            return false;
        }

        // Mark/reset position methods
        size_t mark_read_position() const {
            auto lock = get_read_lock();
            return read_pos_;
        }

        void reset_read_position(size_t mark) {
            auto lock = get_write_lock();
            if (mark <= write_pos_) {
                read_pos_ = mark;
            }
        }

        // Position management  
        size_t position() const {
            return read_position();
        }

        size_t read_position() const {
            auto lock = get_read_lock();
            return read_pos_;
        }

        size_t write_position() const {
            auto lock = get_read_lock();
            return write_pos_;
        }

        void set_position(size_t pos) {
            set_read_position(pos);
        }

        void set_read_position(size_t pos) {
            auto lock = get_write_lock();

            if (pos > write_pos_) {
                throw std::out_of_range("Read position " + std::to_string(pos) +
                    " exceeds write position " + std::to_string(write_pos_));
            }

            read_pos_ = pos;
        }

        void set_write_position(size_t pos) {
            auto lock = get_write_lock();

            if (pos > capacity_) {
                throw std::out_of_range("Write position " + std::to_string(pos) +
                    " exceeds capacity " + std::to_string(capacity_));
            }

            write_pos_ = pos;
            if (read_pos_ > write_pos_) {
                read_pos_ = write_pos_;
            }
        }

        void skip(size_t bytes) {
            auto lock = get_write_lock();

            size_t available = write_pos_ - read_pos_;
            size_t to_skip = std::min(bytes, available);
            read_pos_ += to_skip;
            total_bytes_read_ += to_skip;

            if (to_skip < bytes) {
                log(ILogger::Level::WARN, "Skip requested " + std::to_string(bytes) +
                    " bytes but only " + std::to_string(to_skip) + " available");
            }
        }

        void rewind() {
            auto lock = get_write_lock();
            read_pos_ = 0;
        }

        // Buffer management
        size_t size() const {
            auto lock = get_read_lock();
            return write_pos_;
        }

        size_t available() const {
            auto lock = get_read_lock();
            return write_pos_ - read_pos_;
        }

        size_t capacity() const {
            auto lock = get_read_lock();
            return capacity_;
        }

        size_t remaining_capacity() const {
            auto lock = get_read_lock();
            return capacity_ - write_pos_;
        }

        bool empty() const {
            auto lock = get_read_lock();
            return read_pos_ == write_pos_;
        }

        void clear() {
            auto lock = get_write_lock();

            if (config_.zero_on_clear) {
                secure_zero_memory(data_, write_pos_);
            }

            read_pos_ = 0;
            write_pos_ = 0;
            checksum_ = 0;

            log(ILogger::Level::DEBUG, "Buffer cleared");
        }

        void compact() {
            auto lock = get_write_lock();

            if (read_pos_ > 0) {
                size_t remaining = write_pos_ - read_pos_;
                if (remaining > 0) {
                    std::memmove(data_, data_ + read_pos_, remaining);

                    // Zero the freed space for security
                    if (config_.zero_on_clear) {
                        secure_zero_memory(data_ + remaining, read_pos_);
                    }
                }
                write_pos_ = remaining;
                read_pos_ = 0;

                log(ILogger::Level::DEBUG, "Buffer compacted, " +
                    std::to_string(remaining) + " bytes remaining");
            }
        }

        void resize(size_t new_capacity) {
            auto lock = get_write_lock();

            if (new_capacity < write_pos_) {
                throw std::invalid_argument("New capacity " + std::to_string(new_capacity) +
                    " is less than current size " + std::to_string(write_pos_));
            }

            if (new_capacity > config_.max_capacity) {
                throw BufferOverflowException(new_capacity, config_.max_capacity);
            }

            if (new_capacity != capacity_) {
                reallocate(new_capacity);
            }
        }

        void shrink_to_fit() {
            auto lock = get_write_lock();

            if (write_pos_ < capacity_) {
                reallocate(std::max(write_pos_, config_.initial_capacity));
            }
        }

        // Data access
        const uint8_t* data() const {
            auto lock = get_read_lock();
            return data_;
        }

        std::pair<const uint8_t*, size_t> readable_data() const {
            auto lock = get_read_lock();
            return { data_ + read_pos_, write_pos_ - read_pos_ };
        }

        std::pair<uint8_t*, size_t> writable_space() {
            auto lock = get_write_lock();
            ensure_capacity(1);  // Ensure at least some space
            return { data_ + write_pos_, capacity_ - write_pos_ };
        }

        void advance_write_position(size_t bytes) {
            auto lock = get_write_lock();

            if (write_pos_ + bytes > capacity_) {
                throw BufferOverflowException(bytes, capacity_ - write_pos_);
            }

            write_pos_ += bytes;
            total_bytes_written_ += bytes;

            // Update checksum for the new data
            for (size_t i = write_pos_ - bytes; i < write_pos_; ++i) {
                update_checksum(data_[i]);
            }
        }

        void advance_read_position(size_t bytes) {
            auto lock = get_write_lock();

            if (read_pos_ + bytes > write_pos_) {
                throw BufferUnderflowException(bytes, write_pos_ - read_pos_);
            }

            read_pos_ += bytes;
            total_bytes_read_ += bytes;
        }

        // Validation and integrity
        bool validate_checksum() const {
            if (!config_.validate_operations) return true;

            auto lock = get_read_lock();
            uint32_t calculated = calculate_checksum(data_, write_pos_);
            return calculated == checksum_.load();
        }

        void force_validate() const {
            if (!validate_checksum()) {
                throw BufferCorruptionException("Checksum validation failed");
            }
        }

        // Statistics
        struct DetailedStats {
            uint64_t total_bytes_written;
            uint64_t total_bytes_read;
            uint32_t realloc_count;
            uint32_t operation_count;
            size_t current_capacity;
            size_t current_size;
            size_t available;
            size_t read_position;
            size_t write_position;
            double utilization_percent;
            double read_percent;
            bool checksum_valid;
        };

        DetailedStats get_detailed_stats() const {
            auto lock = get_read_lock();

            return {
                total_bytes_written_.load(),
                total_bytes_read_.load(),
                realloc_count_.load(),
                operation_count_.load(),
                capacity_,
                write_pos_,
                write_pos_ - read_pos_,
                read_pos_,
                write_pos_,
                capacity_ > 0 ? (static_cast<double>(write_pos_) / capacity_ * 100.0) : 0.0,
                write_pos_ > 0 ? (static_cast<double>(read_pos_) / write_pos_ * 100.0) : 0.0,
                validate_checksum()
            };
        }

        // Utility methods
        std::vector<uint8_t> to_vector() const {
            auto lock = get_read_lock();
            return std::vector<uint8_t>(data_ + read_pos_, data_ + write_pos_);
        }

        std::string to_string() const {
            auto lock = get_read_lock();
            return std::string(reinterpret_cast<const char*>(data_ + read_pos_),
                write_pos_ - read_pos_);
        }

        std::string to_hex_string() const {
            auto lock = get_read_lock();
            std::string result;
            result.reserve((write_pos_ - read_pos_) * 2);

            const char* hex_chars = "0123456789ABCDEF";
            for (size_t i = read_pos_; i < write_pos_; ++i) {
                result.push_back(hex_chars[(data_[i] >> 4) & 0x0F]);
                result.push_back(hex_chars[data_[i] & 0x0F]);
            }

            return result;
        }

        void from_vector(const std::vector<uint8_t>& vec) {
            clear();
            write_bytes(vec.data(), vec.size());
        }

        void from_string(std::string_view str) {
            clear();
            write_bytes(reinterpret_cast<const uint8_t*>(str.data()), str.size());
        }

        // Operators
        uint8_t operator[](size_t index) const {
            auto lock = get_read_lock();

            if (index >= write_pos_) {
                throw std::out_of_range("Index " + std::to_string(index) +
                    " out of range [0, " + std::to_string(write_pos_) + ")");
            }

            return data_[index];
        }

        // Comparison operators
        bool operator==(const Buffer& other) const {
            auto lock1 = get_read_lock();
            auto lock2 = other.get_read_lock();

            size_t this_size = write_pos_ - read_pos_;
            size_t other_size = other.write_pos_ - other.read_pos_;

            if (this_size != other_size) {
                return false;
            }

            return std::memcmp(data_ + read_pos_,
                other.data_ + other.read_pos_,
                this_size) == 0;
        }

        bool operator!=(const Buffer& other) const {
            return !(*this == other);
        }

        // Advanced features

        // Create a view of the buffer (non-owning)
        class BufferView {
            const uint8_t* data_;
            size_t size_;

        public:
            BufferView(const uint8_t* data, size_t size)
                : data_(data), size_(size) {}

            const uint8_t* data() const { return data_; }
            size_t size() const { return size_; }

            uint8_t operator[](size_t index) const {
                if (index >= size_) {
                    throw std::out_of_range("BufferView index out of range");
                }
                return data_[index];
            }
        };

        BufferView view() const {
            auto lock = get_read_lock();
            return BufferView(data_ + read_pos_, write_pos_ - read_pos_);
        }

        BufferView view(size_t offset, size_t length) const {
            auto lock = get_read_lock();

            if (offset + length > write_pos_) {
                throw std::out_of_range("View range exceeds buffer size");
            }

            return BufferView(data_ + offset, length);
        }

        // Clone a portion of the buffer
        Buffer clone() const {
            auto lock = get_read_lock();

            Buffer result(config_);
            result.write_bytes(data_ + read_pos_, write_pos_ - read_pos_);
            return result;
        }

        Buffer clone(size_t offset, size_t length) const {
            auto lock = get_read_lock();

            if (offset + length > write_pos_) {
                throw std::out_of_range("Clone range exceeds buffer size");
            }

            Buffer result(config_);
            result.write_bytes(data_ + offset, length);
            return result;
        }

    private:
        // Thread safety helpers
        std::shared_lock<std::shared_mutex> get_read_lock() const {
            if (mutex_) {
                return std::shared_lock<std::shared_mutex>(*mutex_);
            }
            return std::shared_lock<std::shared_mutex>();
        }

        std::unique_lock<std::shared_mutex> get_write_lock() const {
            if (mutex_) {
                return std::unique_lock<std::shared_mutex>(*mutex_);
            }
            return std::unique_lock<std::shared_mutex>();
        }

        // Memory management
        void ensure_capacity(size_t additional) {
            size_t required = write_pos_ + additional;

            if (required > capacity_) {
                // Calculate new capacity with growth factor
                size_t new_capacity = capacity_;

                // Use more aggressive growth for small buffers
                if (capacity_ < 1024) {
                    new_capacity = std::max(required, capacity_ * 4);
                }
                else if (capacity_ < 65536) {
                    new_capacity = std::max(required, capacity_ * 2);
                }
                else {
                    new_capacity = std::max(required, capacity_ + capacity_ / 2);
                }

                // Ensure we don't exceed max capacity
                new_capacity = std::min(new_capacity, config_.max_capacity);

                if (new_capacity < required) {
                    throw BufferOverflowException(additional, config_.max_capacity - write_pos_);
                }

                reallocate(new_capacity);
            }
        }

        void reallocate(size_t new_capacity) {
            if (new_capacity == capacity_) return;

            log(ILogger::Level::DEBUG, "Reallocating buffer from " +
                std::to_string(capacity_) + " to " + std::to_string(new_capacity));

            // Allocate new memory
            uint8_t* new_data = config_.allocator->allocate(new_capacity);
            if (!new_data) {
                throw std::bad_alloc();
            }

            // Copy existing data
            if (write_pos_ > 0) {
                std::memcpy(new_data, data_, write_pos_);
            }

            // Zero new memory for security
            if (new_capacity > write_pos_) {
                std::memset(new_data + write_pos_, 0, new_capacity - write_pos_);
            }

            // Zero old memory if required
            if (config_.zero_on_clear && data_) {
                secure_zero_memory(data_, capacity_);
            }

            // Record metrics
            if (config_.metrics) {
                config_.metrics->recordReallocation(capacity_, new_capacity);
            }

            // Deallocate old memory
            if (data_) {
                config_.allocator->deallocate(data_, capacity_);
            }

            data_ = new_data;
            capacity_ = new_capacity;
            realloc_count_++;
        }

        // Security helpers
        static void secure_zero_memory(void* ptr, size_t size) {
            if (ptr && size > 0) {
                volatile uint8_t* p = static_cast<volatile uint8_t*>(ptr);
                while (size--) {
                    *p++ = 0;
                }
            }
        }

        // Checksum calculation
        void update_checksum(uint8_t byte) {
            if (config_.validate_operations) {
                // Simple CRC-like checksum
                uint32_t crc = checksum_.load();
                crc = ((crc >> 8) | (crc << 24)) ^ byte;
                crc ^= (crc & 0xff00) << 16;
                crc ^= (crc >> 8) & 0xff00;
                crc ^= (crc & 0xff) << 8;
                checksum_ = crc;
            }
        }

        void update_checksum(uint16_t value) {
            update_checksum(static_cast<uint8_t>(value >> 8));
            update_checksum(static_cast<uint8_t>(value & 0xFF));
        }

        void update_checksum(uint32_t value) {
            update_checksum(static_cast<uint8_t>(value >> 24));
            update_checksum(static_cast<uint8_t>(value >> 16));
            update_checksum(static_cast<uint8_t>(value >> 8));
            update_checksum(static_cast<uint8_t>(value));
        }

        void update_checksum(uint64_t value) {
            for (int i = 7; i >= 0; i--) {
                update_checksum(static_cast<uint8_t>(value >> (i * 8)));
            }
        }

        static uint32_t calculate_checksum(const uint8_t* data, size_t size) {
            uint32_t crc = 0;
            for (size_t i = 0; i < size; ++i) {
                crc = ((crc >> 8) | (crc << 24)) ^ data[i];
                crc ^= (crc & 0xff00) << 16;
                crc ^= (crc >> 8) & 0xff00;
                crc ^= (crc & 0xff) << 8;
            }
            return crc;
        }

        // Logging helper
        void log(ILogger::Level level, const std::string& message) const {
            if (config_.logger) {
                config_.logger->log(level, "[Buffer] " + message);
            }
        }
    };

    // Factory functions for common buffer configurations

    // Create a high-performance buffer optimized for speed
    inline std::unique_ptr<Buffer> create_fast_buffer(size_t initial_capacity = 8192) {
        BufferConfig config;
        config.initial_capacity = initial_capacity;
        config.max_capacity = 128 * 1024 * 1024;  // 128MB
        config.growth_factor = 2;
        config.thread_safe = false;  // No thread safety for maximum speed
        config.zero_on_clear = false;  // No zeroing for speed
        config.track_metrics = false;  // No metrics overhead
        config.validate_operations = false;  // No validation overhead
        config.allocator = std::make_shared<PoolAllocator>(16384, 100);

        return std::make_unique<Buffer>(config);
    }

    // Create a secure buffer with all safety features enabled
    inline std::unique_ptr<Buffer> create_secure_buffer(size_t initial_capacity = 4096) {
        BufferConfig config;
        config.initial_capacity = initial_capacity;
        config.max_capacity = 16 * 1024 * 1024;  // 16MB
        config.growth_factor = 2;
        config.thread_safe = true;
        config.zero_on_clear = true;
        config.track_metrics = true;
        config.validate_operations = true;
        config.max_string_length = 32768;  // 32KB max string
        config.allocator = std::make_shared<DefaultAllocator>();

        return std::make_unique<Buffer>(config);
    }

    // Create a thread-safe buffer for concurrent access
    inline std::unique_ptr<Buffer> create_concurrent_buffer(size_t initial_capacity = 4096) {
        BufferConfig config;
        config.initial_capacity = initial_capacity;
        config.thread_safe = true;
        config.allocator = std::make_shared<PoolAllocator>(8192, 200);

        return std::make_unique<Buffer>(config);
    }

    // Utility functions

    // Compare two buffers efficiently
    inline bool buffer_equals(const Buffer& a, const Buffer& b) {
        return a == b;
    }

    // Create a buffer from hex string
    inline std::unique_ptr<Buffer> buffer_from_hex(const std::string& hex) {
        if (hex.length() % 2 != 0) {
            throw std::invalid_argument("Hex string must have even length");
        }

        auto buffer = std::make_unique<Buffer>(hex.length() / 2);

        for (size_t i = 0; i < hex.length(); i += 2) {
            std::string byte_str = hex.substr(i, 2);
            uint8_t byte = static_cast<uint8_t>(std::stoi(byte_str, nullptr, 16));
            buffer->write_byte(byte);
        }

        return buffer;
    }

    // XOR two buffers
    inline std::unique_ptr<Buffer> buffer_xor(const Buffer& a, const Buffer& b) {
        auto [data_a, size_a] = a.readable_data();
        auto [data_b, size_b] = b.readable_data();

        size_t min_size = std::min(size_a, size_b);
        auto result = std::make_unique<Buffer>(min_size);

        for (size_t i = 0; i < min_size; ++i) {
            result->write_byte(data_a[i] ^ data_b[i]);
        }

        return result;
    }

    // Simple compression using run-length encoding (for demonstration)
    inline std::unique_ptr<Buffer> buffer_compress_rle(const Buffer& input) {
        auto [data, size] = input.readable_data();
        auto output = std::make_unique<Buffer>(size);

        if (size == 0) return output;

        size_t i = 0;
        while (i < size) {
            uint8_t value = data[i];
            size_t count = 1;

            while (i + count < size && count < 255 && data[i + count] == value) {
                count++;
            }

            output->write_byte(static_cast<uint8_t>(count));
            output->write_byte(value);
            i += count;
        }

        output->shrink_to_fit();
        return output;
    }

    // Simple decompression using run-length encoding
    inline std::unique_ptr<Buffer> buffer_decompress_rle(const Buffer& input) {
        auto output = std::make_unique<Buffer>(input.size() * 2);  // Initial estimate

        Buffer temp = input.clone();  // Work with a copy to preserve input

        while (temp.available() >= 2) {
            uint8_t count = temp.read_byte();
            uint8_t value = temp.read_byte();

            for (uint8_t i = 0; i < count; ++i) {
                output->write_byte(value);
            }
        }

        output->shrink_to_fit();
        return output;
    }

    // Buffer pool for efficient buffer reuse
    class BufferPool {
    private:
        struct PooledBuffer {
            std::unique_ptr<Buffer> buffer;
            std::chrono::steady_clock::time_point last_used;
        };

        mutable std::mutex mutex_;
        std::vector<PooledBuffer> available_;
        std::vector<std::weak_ptr<Buffer>> in_use_;
        BufferConfig config_;
        size_t max_pool_size_;
        std::chrono::seconds max_idle_time_;
        std::atomic<size_t> total_created_{ 0 };
        std::atomic<size_t> total_reused_{ 0 };

    public:
        explicit BufferPool(
            const BufferConfig& config = BufferConfig{},
            size_t max_pool_size = 100,
            std::chrono::seconds max_idle_time = std::chrono::seconds(300))
            : config_(config),
            max_pool_size_(max_pool_size),
            max_idle_time_(max_idle_time) {

            available_.reserve(max_pool_size);
        }

        std::shared_ptr<Buffer> acquire() {
            std::lock_guard<std::mutex> lock(mutex_);

            // Clean up idle buffers
            cleanup_idle_buffers();

            // Try to reuse an existing buffer
            if (!available_.empty()) {
                auto pooled = std::move(available_.back());
                available_.pop_back();

                // Clear and reset the buffer
                pooled.buffer->clear();
                pooled.buffer->set_read_position(0);
                pooled.buffer->set_write_position(0);

                total_reused_++;

                // Create shared_ptr with custom deleter to return to pool
                auto ptr = pooled.buffer.release();
                return std::shared_ptr<Buffer>(ptr, [this](Buffer* b) {
                    return_to_pool(std::unique_ptr<Buffer>(b));
                    });
            }

            // Create new buffer
            total_created_++;
            auto buffer = std::make_unique<Buffer>(config_);
            auto ptr = buffer.release();

            return std::shared_ptr<Buffer>(ptr, [this](Buffer* b) {
                return_to_pool(std::unique_ptr<Buffer>(b));
                });
        }

        struct Stats {
            size_t total_created;
            size_t total_reused;
            size_t current_available;
            size_t current_in_use;
            double reuse_rate;
        };

        Stats get_stats() const {
            std::lock_guard<std::mutex> lock(mutex_);

            // Count active buffers
            size_t in_use = 0;
            for (const auto& weak : in_use_) {
                if (!weak.expired()) {
                    in_use++;
                }
            }

            size_t created = total_created_.load();
            size_t reused = total_reused_.load();
            size_t total = created + reused;

            return {
                created,
                reused,
                available_.size(),
                in_use,
                total > 0 ? static_cast<double>(reused) / total : 0.0
            };
        }

    private:
        void return_to_pool(std::unique_ptr<Buffer> buffer) {
            if (!buffer) return;

            std::lock_guard<std::mutex> lock(mutex_);

            // Don't exceed pool size
            if (available_.size() >= max_pool_size_) {
                return;  // Let it be destroyed
            }

            // Add back to pool
            available_.push_back({
                std::move(buffer),
                std::chrono::steady_clock::now()
                });
        }

        void cleanup_idle_buffers() {
            auto now = std::chrono::steady_clock::now();

            available_.erase(
                std::remove_if(available_.begin(), available_.end(),
                    [this, now](const PooledBuffer& pb) {
                        return (now - pb.last_used) > max_idle_time_;
                    }),
                available_.end()
                        );

            // Clean up expired weak pointers
            in_use_.erase(
                std::remove_if(in_use_.begin(), in_use_.end(),
                    [](const std::weak_ptr<Buffer>& wp) {
                        return wp.expired();
                    }),
                in_use_.end()
                        );
        }
    };

    // Global buffer pool (optional)
    inline BufferPool& get_global_buffer_pool() {
        static BufferPool pool;
        return pool;
    }

    // Convenience function to get a buffer from the global pool
    inline std::shared_ptr<Buffer> acquire_buffer() {
        return get_global_buffer_pool().acquire();
    }

    // Buffer utilities for testing and debugging
    namespace buffer_utils {

        // Generate random buffer for testing
        inline std::unique_ptr<Buffer> generate_random_buffer(size_t size) {
            auto buffer = std::make_unique<Buffer>(size);

            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(0, 255);

            for (size_t i = 0; i < size; ++i) {
                buffer->write_byte(static_cast<uint8_t>(dis(gen)));
            }

            return buffer;
        }

        // Create a pattern-filled buffer for testing
        inline std::unique_ptr<Buffer> create_pattern_buffer(size_t size, uint8_t pattern) {
            auto buffer = std::make_unique<Buffer>(size);

            for (size_t i = 0; i < size; ++i) {
                buffer->write_byte(pattern);
            }

            return buffer;
        }

        // Dump buffer contents for debugging
        inline void dump_buffer(const Buffer& buffer, std::ostream& os,
            size_t max_bytes = 256, size_t bytes_per_line = 16) {
            auto [data, size] = buffer.readable_data();
            size_t to_dump = std::min(size, max_bytes);

            os << "Buffer dump: " << size << " bytes";
            if (to_dump < size) {
                os << " (showing first " << to_dump << ")";
            }
            os << "\n";

            for (size_t i = 0; i < to_dump; i += bytes_per_line) {
                // Offset
                os << std::hex << std::setw(8) << std::setfill('0') << i << "  ";

                // Hex bytes
                for (size_t j = 0; j < bytes_per_line; ++j) {
                    if (i + j < to_dump) {
                        os << std::hex << std::setw(2) << std::setfill('0')
                            << static_cast<int>(data[i + j]) << " ";
                    }
                    else {
                        os << "   ";
                    }
                }

                os << " |";

                // ASCII representation
                for (size_t j = 0; j < bytes_per_line && i + j < to_dump; ++j) {
                    char c = static_cast<char>(data[i + j]);
                    os << (std::isprint(c) ? c : '.');
                }

                os << "|\n";
            }

            os << std::dec;  // Reset to decimal
        }

        // Calculate various checksums
        inline uint32_t calculate_crc32(const Buffer& buffer) {
            auto [data, size] = buffer.readable_data();

            uint32_t crc = 0xFFFFFFFF;
            for (size_t i = 0; i < size; ++i) {
                crc ^= data[i];
                for (int k = 0; k < 8; ++k) {
                    crc = (crc >> 1) ^ (0xEDB88320 & (0 - (crc & 1)));
                }
            }

            return ~crc;
        }

        inline uint64_t calculate_hash64(const Buffer& buffer) {
            auto [data, size] = buffer.readable_data();

            // Simple FNV-1a hash
            uint64_t hash = 0xcbf29ce484222325ULL;
            for (size_t i = 0; i < size; ++i) {
                hash ^= data[i];
                hash *= 0x100000001b3ULL;
            }

            return hash;
        }
    }

} // namespace ourmqtt

#endif // OURMQTT_BUFFER_HPP