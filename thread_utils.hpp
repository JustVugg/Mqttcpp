// thread_utils.hpp - VERSIONE SICURA SENZA ABORT
#ifndef OUR_THREAD_UTILS_HPP
#define OUR_THREAD_UTILS_HPP

#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <functional>
#include <queue>
#include <vector>
#include <memory>
#include <future>

#ifdef _WIN32
#include <windows.h>
#define SLEEP_MS(x) Sleep(x)
#else
#include <unistd.h>
#define SLEEP_MS(x) usleep((x) * 1000)
#endif

namespace ourmqtt {

    // ==================== Simple Thread Wrapper ====================
    class Thread {
    private:
        std::shared_ptr<std::thread> thread_;
        std::shared_ptr<std::atomic<bool>> should_stop_;

    public:
        Thread() : should_stop_(std::make_shared<std::atomic<bool>>(false)) {}

        ~Thread() {
            stop();
        }

        Thread(const Thread&) = delete;
        Thread& operator=(const Thread&) = delete;

        Thread(Thread&& other) noexcept = default;
        Thread& operator=(Thread&& other) noexcept = default;

        template<typename Func>
        void start(Func&& func) {
            stop(); // Stop any existing thread

            should_stop_->store(false);
            auto stop_flag = should_stop_;

            thread_ = std::make_shared<std::thread>([func = std::forward<Func>(func), stop_flag]() {
                try {
                    func(*stop_flag);
                }
                catch (...) {
                    // Ignore exceptions
                }
            });
        }

        void stop() {
            if (thread_ && thread_->joinable()) {
                should_stop_->store(true);

                // IMPORTANTE: Controlla se siamo nel thread stesso
                if (std::this_thread::get_id() != thread_->get_id()) {
                    try {
                        thread_->join();
                    }
                    catch (...) {
                        // Se join fallisce, detach
                        thread_->detach();
                    }
                }
                else {
                    // Se siamo nel thread stesso, detach invece di join
                    thread_->detach();
                }
            }
            thread_.reset();
        }

        void detach() {
            if (thread_ && thread_->joinable()) {
                thread_->detach();
            }
            thread_.reset();
        }

        void request_stop() {
            should_stop_->store(true);
        }

        bool is_running() const {
            return thread_ && thread_->joinable() && !should_stop_->load();
        }
    };

    // ==================== Simple Mutex Wrapper ====================
    class Mutex {
    private:
        mutable std::mutex mutex_;

    public:
        void lock() { mutex_.lock(); }
        void unlock() { mutex_.unlock(); }
        bool try_lock() { return mutex_.try_lock(); }
        std::mutex& native() { return mutex_; }
    };

    // ==================== Lock Guard ====================
    class LockGuard {
    private:
        Mutex& mutex_;

    public:
        explicit LockGuard(Mutex& m) : mutex_(m) {
            mutex_.lock();
        }

        ~LockGuard() {
            mutex_.unlock();
        }

        LockGuard(const LockGuard&) = delete;
        LockGuard& operator=(const LockGuard&) = delete;
    };

    // ==================== Condition Variable ====================
    class ConditionVariable {
    private:
        std::condition_variable cv_;

    public:
        void notify_one() { cv_.notify_one(); }
        void notify_all() { cv_.notify_all(); }

        template<typename Predicate>
        void wait(std::unique_lock<std::mutex>& lock, Predicate pred) {
            cv_.wait(lock, pred);
        }

        template<typename Rep, typename Period, typename Predicate>
        bool wait_for(std::unique_lock<std::mutex>& lock,
            const std::chrono::duration<Rep, Period>& rel_time,
            Predicate pred) {
            return cv_.wait_for(lock, rel_time, pred);
        }
    };

    // ==================== SAFE Thread Pool PER MQTT BROKER ====================
    class ThreadPool {
    private:
        std::vector<std::thread> workers_;
        std::queue<std::function<void()>> tasks_;
        std::mutex queue_mutex_;
        std::condition_variable condition_;
        std::atomic<bool> stop_{ false };
        std::atomic<bool> emergency_stop_{ false };

        void worker_thread() {
            while (true) {
                std::function<void()> task;

                {
                    std::unique_lock<std::mutex> lock(queue_mutex_);

                    // Controlla emergency_stop_ più frequentemente
                    condition_.wait_for(lock, std::chrono::milliseconds(50), [this] {
                        return stop_ || emergency_stop_ || !tasks_.empty();
                        });

                    // Uscita immediata in caso di emergency
                    if (emergency_stop_) {
                        return;
                    }

                    if (stop_ && tasks_.empty()) {
                        return;
                    }

                    if (!tasks_.empty()) {
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                }

                if (task && !emergency_stop_) {
                    try {
                        task();
                    }
                    catch (...) {
                        // Ignore exceptions in tasks
                    }
                }
            }
        }

    public:
        explicit ThreadPool(size_t threads = std::thread::hardware_concurrency()) {
            if (threads == 0) threads = 1;

            for (size_t i = 0; i < threads; ++i) {
                workers_.emplace_back([this] { worker_thread(); });
            }
        }

        ~ThreadPool() {
            // DISTRUTTORE SICURO: sempre detach per evitare abort
            emergency_shutdown();
        }

        template<class F>
        bool enqueue(F&& f) {
            if (emergency_stop_) return false;

            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                if (stop_ || emergency_stop_) return false;

                tasks_.emplace(std::forward<F>(f));
            }
            condition_.notify_one();
            return true;
        }

        void stop() {
            // Stop normale: segnala stop e prova a fare join con timeout breve
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                stop_ = true;
            }
            condition_.notify_all();

            // Prova join con timeout molto breve (100ms)
            auto start = std::chrono::steady_clock::now();
            bool all_joined = true;

            for (auto& worker : workers_) {
                if (worker.joinable()) {
                    // Usa un future per join con timeout
                    std::promise<void> p;
                    auto f = p.get_future();

                    std::thread joiner([&worker, &p]() {
                        try {
                            worker.join();
                            p.set_value();
                        }
                        catch (...) {
                            p.set_value();
                        }
                        });

                    if (f.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout) {
                        // Timeout - detach entrambi
                        if (worker.joinable()) worker.detach();
                        if (joiner.joinable()) joiner.detach();
                        all_joined = false;
                    }
                    else {
                        // Join completato
                        if (joiner.joinable()) joiner.join();
                    }
                }
            }

            if (!all_joined) {
                // Se non tutti i thread sono stati joined, forza emergency stop
                emergency_shutdown();
            }

            workers_.clear();
        }

        void emergency_shutdown() {
            // Shutdown di emergenza: detach immediato
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                stop_ = true;
                emergency_stop_ = true;

                // Svuota la coda
                while (!tasks_.empty()) {
                    tasks_.pop();
                }
            }
            condition_.notify_all();

            // Detach tutti i thread immediatamente
            for (auto& worker : workers_) {
                if (worker.joinable()) {
                    worker.detach();
                }
            }

            workers_.clear();
        }

        size_t queue_size() const {
            std::unique_lock<std::mutex> lock(const_cast<std::mutex&>(queue_mutex_));
            return tasks_.size();
        }

        bool is_stopping() const {
            return stop_.load() || emergency_stop_.load();
        }
    };

    // ==================== Async Task Runner ====================
    template<typename T>
    class AsyncTask {
    private:
        std::future<T> future_;

    public:
        template<typename Func>
        void run(Func&& func) {
            future_ = std::async(std::launch::async, std::forward<Func>(func));
        }

        bool is_ready() const {
            return future_.valid() &&
                future_.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        }

        T get() {
            return future_.get();
        }

        bool wait_for(std::chrono::milliseconds timeout) {
            return future_.valid() &&
                future_.wait_for(timeout) == std::future_status::ready;
        }
    };

    // ==================== Safe Detached Thread ====================
    class DetachedThread {
    private:
        std::shared_ptr<std::atomic<bool>> running_;

    public:
        DetachedThread() : running_(std::make_shared<std::atomic<bool>>(false)) {}

        template<typename Func>
        void start(Func&& func) {
            auto running = running_;
            running->store(true);

            std::thread([func = std::forward<Func>(func), running]() {
                try {
                    func();
                }
                catch (...) {
                    // Ignore exceptions
                }
                running->store(false);
            }).detach();
        }

        bool is_running() const {
            return running_->load();
        }

        void wait() const {
            while (is_running()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    };

    // ==================== Join Helper con Timeout ====================
    class JoinHelper {
    public:
        static bool join_with_timeout(std::thread& t, std::chrono::milliseconds timeout) {
            if (!t.joinable()) return true;

            if (std::this_thread::get_id() == t.get_id()) {
                // Non possiamo fare join su noi stessi
                t.detach();
                return false;
            }

            std::promise<void> p;
            auto f = p.get_future();

            std::thread joiner([&t, &p]() {
                try {
                    t.join();
                    p.set_value();
                }
                catch (...) {
                    p.set_value();
                }
                });

            bool joined = false;
            if (f.wait_for(timeout) == std::future_status::ready) {
                joined = true;
                if (joiner.joinable()) {
                    joiner.join();
                }
            }
            else {
                // Timeout - detach entrambi
                if (t.joinable()) {
                    t.detach();
                }
                if (joiner.joinable()) {
                    joiner.detach();
                }
            }

            return joined;
        }

        static void safe_detach(std::thread& t) {
            if (t.joinable()) {
                t.detach();
            }
        }
    };

    // ==================== Utility Functions ====================
    inline void sleep_ms(unsigned int milliseconds) {
        std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
    }

    inline size_t hardware_concurrency() {
        size_t n = std::thread::hardware_concurrency();
        return n > 0 ? n : 1;
    }

    inline bool is_self_join(const std::thread& t) {
        return t.get_id() == std::this_thread::get_id();
    }

    // Helper per shutdown sicuro
    inline void safe_thread_shutdown(std::thread& t, std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) {
        if (!t.joinable()) return;

        // Se siamo nel thread stesso, detach immediatamente
        if (is_self_join(t)) {
            t.detach();
            return;
        }

        // Altrimenti prova join con timeout
        if (!JoinHelper::join_with_timeout(t, timeout)) {
            // Se join fallisce, il thread è già stato detached
        }
    }

} // namespace ourmqtt

#endif // OUR_THREAD_UTILS_HPP