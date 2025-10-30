#ifndef OUR_TLS_SOCKET_HPP
#define OUR_TLS_SOCKET_HPP

// Configurazione per Windows
#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#endif

#include "socket.hpp"
#include <vector>
#include <string>
#include <memory>
#include <map>
#include <atomic>
#include <mutex>
#include <chrono>
#include <optional>
#include <functional>
#include <cstring>
#include <sstream>
#include <iomanip>
#include <algorithm>

#ifdef _WIN32
#define SECURITY_WIN32
#include <security.h>
#include <schannel.h>
#include <sspi.h>
#include <schnlsp.h>
#pragma comment(lib, "secur32.lib")
#pragma comment(lib, "crypt32.lib")
#else
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/dh.h>
#include <openssl/engine.h>
#include <openssl/conf.h>
#endif

namespace ourmqtt {

    // TLS versions
    enum class TLSVersion {
        TLS_1_0 = 0x0301,
        TLS_1_1 = 0x0302,
        TLS_1_2 = 0x0303,
        TLS_1_3 = 0x0304,
        TLS_AUTO = 0xFFFF
    };

    // Certificate verification results
    enum class CertVerifyResult {
        OK = 0,
        EXPIRED,
        NOT_YET_VALID,
        SELF_SIGNED,
        UNTRUSTED_ROOT,
        INVALID_CHAIN,
        HOSTNAME_MISMATCH,
        REVOKED,
        INVALID_PURPOSE,
        UNKNOWN_ERROR
    };

    // TLS session info
    struct TLSSessionInfo {
        std::string protocol_version;
        std::string cipher_suite;
        int key_size = 0;
        std::string peer_certificate_subject;
        std::string peer_certificate_issuer;
        std::chrono::system_clock::time_point not_before;
        std::chrono::system_clock::time_point not_after;
        std::string fingerprint_sha256;
        bool session_resumed = false;
        std::vector<std::string> san_list;
    };

    // TLS configuration
    struct TLSConfig {
        // Basic settings
        bool verify_peer = true;
        bool require_peer_cert = false;
        int verify_depth = 9;

        // Certificate and key files
        std::string ca_cert_file;
        std::string ca_cert_dir;
        std::string cert_file;
        std::string key_file;
        std::string key_password;

        // Certificate data (for in-memory certificates)
        std::string ca_cert_data;
        std::string cert_data;
        std::string key_data;

        // Advanced settings
        std::string ciphers = "HIGH:!aNULL:!MD5:!RC4:!3DES";
        std::string cipher_suites_tls13 = "TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256";
        TLSVersion min_version = TLSVersion::TLS_1_2;
        TLSVersion max_version = TLSVersion::TLS_AUTO;

        // Session management
        bool enable_session_cache = true;
        int session_cache_size = 20;
        int session_timeout = 300;

        // ALPN/SNI
        std::vector<std::string> alpn_protocols;
        std::string sni_hostname;

        // Certificate pinning
        std::vector<std::string> pinned_certificates;
        bool enable_cert_pinning = false;

        // OCSP
        bool enable_ocsp_stapling = false;
        bool require_ocsp_stapling = false;

        // Custom verification
        std::function<bool(const TLSSessionInfo&)> custom_verify_callback;

        // Performance
        bool enable_false_start = false;
        bool enable_session_tickets = true;

        // Security
        bool enable_renegotiation = false;
        bool require_sni = false;
        size_t max_cert_chain_size = 100000;

        // DH parameters
        std::string dh_params_file;
        int dh_key_size = 2048;
    };

    // TLS error information
    class TLSError {
    private:
        int error_code_;
        std::string error_message_;
        CertVerifyResult cert_verify_result_;

    public:
        TLSError() : error_code_(0), cert_verify_result_(CertVerifyResult::OK) {}

        TLSError(int code, const std::string& msg,
            CertVerifyResult cert_result = CertVerifyResult::OK)
            : error_code_(code), error_message_(msg), cert_verify_result_(cert_result) {}

        int code() const { return error_code_; }
        const std::string& message() const { return error_message_; }
        CertVerifyResult cert_result() const { return cert_verify_result_; }
        bool is_error() const { return error_code_ != 0; }

        static TLSError get_last_error() {
#ifdef _WIN32
            DWORD error = GetLastError();
            char buffer[256];
            FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM, nullptr, error,
                0, buffer, sizeof(buffer), nullptr);
            return TLSError(error, buffer);
#else
            unsigned long err = ERR_get_error();
            char buffer[256];
            ERR_error_string_n(err, buffer, sizeof(buffer));
            return TLSError(static_cast<int>(err), buffer);
#endif
        }
    };

    // TLS Socket implementation
    class TLSSocket : public Socket {
    private:
#ifdef _WIN32
        // Windows SChannel implementation
        struct SchannelContext {
            CredHandle cred_handle;
            CtxtHandle context;
            bool cred_initialized = false;
            bool context_initialized = false;
            SecPkgContext_StreamSizes stream_sizes;
            std::vector<uint8_t> recv_decrypt_buffer;
            std::vector<uint8_t> recv_encrypted_buffer;
            std::vector<uint8_t> send_buffer;
            std::vector<uint8_t> extra_data;
            PCCERT_CONTEXT client_cert = nullptr;
            HCERTSTORE cert_store = nullptr;
            bool handshake_complete = false;

            SchannelContext() {
                recv_decrypt_buffer.reserve(32768);
                recv_encrypted_buffer.reserve(32768);
                send_buffer.reserve(32768);
                ZeroMemory(&cred_handle, sizeof(cred_handle));
                ZeroMemory(&context, sizeof(context));
                ZeroMemory(&stream_sizes, sizeof(stream_sizes));
            }

            ~SchannelContext() {
                cleanup();
            }

            void cleanup() {
                if (context_initialized) {
                    DeleteSecurityContext(&context);
                    context_initialized = false;
                }
                if (cred_initialized) {
                    FreeCredentialsHandle(&cred_handle);
                    cred_initialized = false;
                }
                if (client_cert) {
                    CertFreeCertificateContext(client_cert);
                    client_cert = nullptr;
                }
                if (cert_store) {
                    CertCloseStore(cert_store, 0);
                    cert_store = nullptr;
                }
            }
        };

        std::unique_ptr<SchannelContext> schannel_;

#else
        // Linux OpenSSL implementation
        static std::once_flag openssl_init_flag_;
        static std::atomic<bool> openssl_initialized_;

        SSL_CTX* ssl_ctx_ = nullptr;
        SSL* ssl_ = nullptr;
        BIO* bio_in_ = nullptr;
        BIO* bio_out_ = nullptr;
#endif

        // Common members
        bool tls_enabled_ = false;
        bool is_server_ = false;
        TLSConfig config_;
        TLSError last_error_;
        std::optional<TLSSessionInfo> session_info_;
        mutable std::mutex tls_mutex_;

        // Statistics
        std::atomic<uint64_t> bytes_encrypted_{ 0 };
        std::atomic<uint64_t> bytes_decrypted_{ 0 };
        std::atomic<uint32_t> renegotiations_{ 0 };

    public:
        TLSSocket() : Socket() {
            initialize_tls_library();
        }

        explicit TLSSocket(Socket&& base_socket)
            : Socket(std::move(base_socket)) {
            initialize_tls_library();
        }

        virtual ~TLSSocket() override {
            cleanup_tls();
        }

        // Move constructor
        TLSSocket(TLSSocket&& other) noexcept
            : Socket(std::move(other)),
            tls_enabled_(other.tls_enabled_),
            is_server_(other.is_server_),
            config_(std::move(other.config_)),
            last_error_(std::move(other.last_error_)),
            session_info_(std::move(other.session_info_)) {

#ifdef _WIN32
            schannel_ = std::move(other.schannel_);
#else
            ssl_ctx_ = other.ssl_ctx_;
            ssl_ = other.ssl_;
            bio_in_ = other.bio_in_;
            bio_out_ = other.bio_out_;

            other.ssl_ctx_ = nullptr;
            other.ssl_ = nullptr;
            other.bio_in_ = nullptr;
            other.bio_out_ = nullptr;
#endif

            bytes_encrypted_ = other.bytes_encrypted_.load();
            bytes_decrypted_ = other.bytes_decrypted_.load();
            renegotiations_ = other.renegotiations_.load();

            other.tls_enabled_ = false;
        }

        // Move assignment
        TLSSocket& operator=(TLSSocket&& other) noexcept {
            if (this != &other) {
                cleanup_tls();
                Socket::operator=(std::move(other));

                tls_enabled_ = other.tls_enabled_;
                is_server_ = other.is_server_;
                config_ = std::move(other.config_);
                last_error_ = std::move(other.last_error_);
                session_info_ = std::move(other.session_info_);

#ifdef _WIN32
                schannel_ = std::move(other.schannel_);
#else
                ssl_ctx_ = other.ssl_ctx_;
                ssl_ = other.ssl_;
                bio_in_ = other.bio_in_;
                bio_out_ = other.bio_out_;

                other.ssl_ctx_ = nullptr;
                other.ssl_ = nullptr;
                other.bio_in_ = nullptr;
                other.bio_out_ = nullptr;
#endif

                bytes_encrypted_ = other.bytes_encrypted_.load();
                bytes_decrypted_ = other.bytes_decrypted_.load();
                renegotiations_ = other.renegotiations_.load();

                other.tls_enabled_ = false;
            }
            return *this;
        }

        // Enable TLS
        bool enable_tls(const TLSConfig& config, bool is_server = false) {
            if (!is_valid() || tls_enabled_) {
                return false;
            }

            std::lock_guard<std::mutex> lock(tls_mutex_);

            config_ = config;
            is_server_ = is_server;

#ifdef _WIN32
            return init_schannel();
#else
            return init_openssl();
#endif
        }

        // Perform TLS handshake
        bool perform_handshake(bool is_server) {
            if (!tls_enabled_) return true;

            std::lock_guard<std::mutex> lock(tls_mutex_);

#ifdef _WIN32
            return perform_schannel_handshake(is_server);
#else
            return perform_openssl_handshake(is_server);
#endif
        }

        // Send data
        virtual int send(const void* data, size_t len) override {
            return send(static_cast<const uint8_t*>(data), len);
        }

        virtual int send(const uint8_t* data, size_t len) override {
            if (!tls_enabled_) {
                return Socket::send(data, len);
            }

            std::lock_guard<std::mutex> lock(tls_mutex_);

#ifdef _WIN32
            return send_encrypted_schannel(data, len);
#else
            return send_encrypted_openssl(data, len);
#endif
        }

        // Receive data
        virtual int receive(void* buffer, size_t max_len) override {
            return receive(static_cast<uint8_t*>(buffer), max_len);
        }

        virtual int receive(uint8_t* buffer, size_t max_len) override {
            if (!tls_enabled_) {
                return Socket::receive(buffer, max_len);
            }

            std::lock_guard<std::mutex> lock(tls_mutex_);

#ifdef _WIN32
            return receive_decrypted_schannel(buffer, max_len);
#else
            return receive_decrypted_openssl(buffer, max_len);
#endif
        }

        // Get session information
        std::optional<TLSSessionInfo> get_session_info() const {
            std::lock_guard<std::mutex> lock(tls_mutex_);
            return session_info_;
        }

        // Get peer certificate
        std::string get_peer_certificate_info() const {
            auto info = get_session_info();
            if (info.has_value()) {
                return "Subject: " + info->peer_certificate_subject +
                    ", Issuer: " + info->peer_certificate_issuer;
            }
            return "No peer certificate";
        }

        // Get cipher suite
        std::string get_cipher() const {
            auto info = get_session_info();
            if (info.has_value()) {
                return info->cipher_suite;
            }
            return "No cipher";
        }

        // Check if TLS is enabled
        bool is_tls() const override {
            return tls_enabled_;
        }

        // Get last error
        const TLSError& get_last_error() const {
            return last_error_;
        }

    private:
        // Initialize TLS library
        void initialize_tls_library() {
#ifdef _WIN32
            // Windows doesn't need global initialization
#else
            std::call_once(openssl_init_flag_, []() {
                SSL_library_init();
                SSL_load_error_strings();
                OpenSSL_add_all_algorithms();
                RAND_poll();
                openssl_initialized_ = true;
                });
#endif
        }

#ifdef _WIN32
        // Windows SChannel implementation

        bool init_schannel() {
            schannel_ = std::make_unique<SchannelContext>();

            // Setup credentials
            SCHANNEL_CRED schannel_cred = { 0 };
            schannel_cred.dwVersion = SCHANNEL_CRED_VERSION;

            // Set protocol versions
            DWORD protocols = 0;
            if (config_.min_version <= TLSVersion::TLS_1_0) protocols |= SP_PROT_TLS1_0;
            if (config_.min_version <= TLSVersion::TLS_1_1) protocols |= SP_PROT_TLS1_1;
            if (config_.min_version <= TLSVersion::TLS_1_2) protocols |= SP_PROT_TLS1_2;

            if (is_server_) {
                protocols |= SP_PROT_TLS1_0_SERVER | SP_PROT_TLS1_1_SERVER |
                    SP_PROT_TLS1_2_SERVER;
            }
            else {
                protocols |= SP_PROT_TLS1_0_CLIENT | SP_PROT_TLS1_1_CLIENT |
                    SP_PROT_TLS1_2_CLIENT;
            }

            schannel_cred.grbitEnabledProtocols = protocols;

            // Set flags
            DWORD flags = SCH_USE_STRONG_CRYPTO;
            if (!config_.verify_peer) {
                flags |= SCH_CRED_MANUAL_CRED_VALIDATION;
            }
            else {
                flags |= SCH_CRED_AUTO_CRED_VALIDATION;
            }

            if (config_.enable_ocsp_stapling) {
                flags |= SCH_CRED_REVOCATION_CHECK_CHAIN;
            }

            schannel_cred.dwFlags = flags;

            // Acquire credentials
            SECURITY_STATUS status = AcquireCredentialsHandleA(
                nullptr,
                const_cast<LPSTR>(UNISP_NAME_A),
                is_server_ ? SECPKG_CRED_INBOUND : SECPKG_CRED_OUTBOUND,
                nullptr,
                &schannel_cred,
                nullptr,
                nullptr,
                &schannel_->cred_handle,
                nullptr
            );

            if (status != SEC_E_OK) {
                last_error_ = TLSError(status, "Failed to acquire credentials");
                return false;
            }

            schannel_->cred_initialized = true;
            tls_enabled_ = true;
            return true;
        }

        bool perform_schannel_handshake(bool is_server) {
            if (!schannel_ || !schannel_->cred_initialized) {
                return false;
            }

            SecBufferDesc out_buffer_desc;
            SecBuffer out_buffers[1];
            SecBufferDesc in_buffer_desc;
            SecBuffer in_buffers[2];
            DWORD context_flags;
            DWORD context_attr;
            TimeStamp expiry;
            SECURITY_STATUS status;

            // Setup output buffer
            out_buffers[0].pvBuffer = nullptr;
            out_buffers[0].BufferType = SECBUFFER_TOKEN;
            out_buffers[0].cbBuffer = 0;

            out_buffer_desc.cBuffers = 1;
            out_buffer_desc.pBuffers = out_buffers;
            out_buffer_desc.ulVersion = SECBUFFER_VERSION;

            // Setup context requirements
            context_flags = ISC_REQ_SEQUENCE_DETECT |
                ISC_REQ_REPLAY_DETECT |
                ISC_REQ_CONFIDENTIALITY |
                ISC_REQ_EXTENDED_ERROR |
                ISC_REQ_ALLOCATE_MEMORY |
                ISC_REQ_STREAM;

            if (!is_server) {
                context_flags |= ISC_REQ_MANUAL_CRED_VALIDATION;
            }

            std::vector<uint8_t> handshake_buffer;
            bool first_call = true;

            while (true) {
                if (!first_call) {
                    // Receive handshake data
                    uint8_t temp_buffer[4096];
                    int received = Socket::receive(temp_buffer, sizeof(temp_buffer));

                    if (received <= 0) {
                        last_error_ = TLSError(0, "Connection lost during handshake");
                        return false;
                    }

                    handshake_buffer.insert(handshake_buffer.end(),
                        temp_buffer, temp_buffer + received);

                    // Setup input buffers
                    in_buffers[0].pvBuffer = handshake_buffer.data();
                    in_buffers[0].cbBuffer = static_cast<ULONG>(handshake_buffer.size());
                    in_buffers[0].BufferType = SECBUFFER_TOKEN;

                    in_buffers[1].pvBuffer = nullptr;
                    in_buffers[1].cbBuffer = 0;
                    in_buffers[1].BufferType = SECBUFFER_EMPTY;

                    in_buffer_desc.cBuffers = 2;
                    in_buffer_desc.pBuffers = in_buffers;
                    in_buffer_desc.ulVersion = SECBUFFER_VERSION;
                }

                // Initialize or continue security context
                if (is_server) {
                    status = AcceptSecurityContext(
                        &schannel_->cred_handle,
                        first_call ? nullptr : &schannel_->context,
                        first_call ? nullptr : &in_buffer_desc,
                        context_flags,
                        SECURITY_NATIVE_DREP,
                        &schannel_->context,
                        &out_buffer_desc,
                        &context_attr,
                        &expiry
                    );
                }
                else {
                    SEC_CHAR* target_name = config_.sni_hostname.empty() ?
                        nullptr : const_cast<SEC_CHAR*>(config_.sni_hostname.c_str());

                    status = InitializeSecurityContextA(
                        &schannel_->cred_handle,
                        first_call ? nullptr : &schannel_->context,
                        target_name,
                        context_flags,
                        0,
                        SECURITY_NATIVE_DREP,
                        first_call ? nullptr : &in_buffer_desc,
                        0,
                        &schannel_->context,
                        &out_buffer_desc,
                        &context_attr,
                        &expiry
                    );
                }

                first_call = false;
                schannel_->context_initialized = true;

                // Send output if available
                if (out_buffers[0].cbBuffer > 0 && out_buffers[0].pvBuffer) {
                    int sent = Socket::send(
                        static_cast<uint8_t*>(out_buffers[0].pvBuffer),
                        out_buffers[0].cbBuffer
                    );

                    FreeContextBuffer(out_buffers[0].pvBuffer);

                    if (sent <= 0) {
                        last_error_ = TLSError(0, "Failed to send handshake data");
                        return false;
                    }
                }

                // Check status
                if (status == SEC_E_OK) {
                    // Handshake complete
                    schannel_->handshake_complete = true;

                    // Get stream sizes
                    QueryContextAttributes(&schannel_->context,
                        SECPKG_ATTR_STREAM_SIZES,
                        &schannel_->stream_sizes);

                    // Handle extra data
                    if (!first_call && in_buffers[1].BufferType == SECBUFFER_EXTRA) {
                        schannel_->extra_data.assign(
                            static_cast<uint8_t*>(in_buffers[1].pvBuffer),
                            static_cast<uint8_t*>(in_buffers[1].pvBuffer) + in_buffers[1].cbBuffer
                        );
                    }

                    return true;
                }
                else if (status == SEC_I_CONTINUE_NEEDED) {
                    // Continue handshake
                    if (!first_call && in_buffers[1].BufferType == SECBUFFER_EXTRA) {
                        size_t extra_size = in_buffers[1].cbBuffer;
                        size_t consumed = handshake_buffer.size() - extra_size;
                        handshake_buffer.erase(handshake_buffer.begin(),
                            handshake_buffer.begin() + consumed);
                    }
                    else {
                        handshake_buffer.clear();
                    }
                }
                else {
                    // Error
                    last_error_ = TLSError(status, "Handshake failed");
                    return false;
                }
            }
        }

        int send_encrypted_schannel(const uint8_t* data, size_t len) {
            if (!schannel_ || !schannel_->handshake_complete) {
                return -1;
            }

            size_t total_sent = 0;

            while (total_sent < len) {
                size_t chunk_size = std::min(
                    len - total_sent,
                    static_cast<size_t>(schannel_->stream_sizes.cbMaximumMessage)
                );

                // Ensure buffer is large enough
                size_t required_size = schannel_->stream_sizes.cbHeader +
                    chunk_size +
                    schannel_->stream_sizes.cbTrailer;

                if (schannel_->send_buffer.size() < required_size) {
                    schannel_->send_buffer.resize(required_size);
                }

                // Prepare buffers
                SecBuffer buffers[4];
                ZeroMemory(buffers, sizeof(buffers));

                buffers[0].pvBuffer = schannel_->send_buffer.data();
                buffers[0].cbBuffer = schannel_->stream_sizes.cbHeader;
                buffers[0].BufferType = SECBUFFER_STREAM_HEADER;

                buffers[1].pvBuffer = schannel_->send_buffer.data() +
                    schannel_->stream_sizes.cbHeader;
                buffers[1].cbBuffer = static_cast<ULONG>(chunk_size);
                buffers[1].BufferType = SECBUFFER_DATA;
                memcpy(buffers[1].pvBuffer, data + total_sent, chunk_size);

                buffers[2].pvBuffer = schannel_->send_buffer.data() +
                    schannel_->stream_sizes.cbHeader + chunk_size;
                buffers[2].cbBuffer = schannel_->stream_sizes.cbTrailer;
                buffers[2].BufferType = SECBUFFER_STREAM_TRAILER;

                buffers[3].pvBuffer = nullptr;
                buffers[3].cbBuffer = 0;
                buffers[3].BufferType = SECBUFFER_EMPTY;

                SecBufferDesc buffer_desc;
                buffer_desc.ulVersion = SECBUFFER_VERSION;
                buffer_desc.cBuffers = 4;
                buffer_desc.pBuffers = buffers;

                // Encrypt
                SECURITY_STATUS status = EncryptMessage(&schannel_->context, 0,
                    &buffer_desc, 0);

                if (status != SEC_E_OK) {
                    last_error_ = TLSError(status, "Encryption failed");
                    return total_sent > 0 ? static_cast<int>(total_sent) : -1;
                }

                // Send encrypted data
                size_t encrypted_size = buffers[0].cbBuffer +
                    buffers[1].cbBuffer +
                    buffers[2].cbBuffer;

                int sent = Socket::send(schannel_->send_buffer.data(), encrypted_size);

                if (sent <= 0) {
                    return total_sent > 0 ? static_cast<int>(total_sent) : sent;
                }

                total_sent += chunk_size;
                bytes_encrypted_ += chunk_size;
            }

            return static_cast<int>(total_sent);
        }

        int receive_decrypted_schannel(uint8_t* buffer, size_t max_len) {
            if (!schannel_ || !schannel_->handshake_complete) {
                return -1;
            }

            // Check for extra data from handshake
            if (!schannel_->extra_data.empty()) {
                size_t copy_size = std::min(max_len, schannel_->extra_data.size());
                memcpy(buffer, schannel_->extra_data.data(), copy_size);
                schannel_->extra_data.erase(schannel_->extra_data.begin(),
                    schannel_->extra_data.begin() + copy_size);
                return static_cast<int>(copy_size);
            }

            while (true) {
                // Receive encrypted data if buffer is empty
                if (schannel_->recv_encrypted_buffer.empty()) {
                    uint8_t temp_buffer[16384];
                    int received = Socket::receive(temp_buffer, sizeof(temp_buffer));

                    if (received <= 0) {
                        return received;
                    }

                    schannel_->recv_encrypted_buffer.insert(
                        schannel_->recv_encrypted_buffer.end(),
                        temp_buffer, temp_buffer + received
                    );
                }

                // Decrypt
                SecBuffer buffers[4];
                buffers[0].pvBuffer = schannel_->recv_encrypted_buffer.data();
                buffers[0].cbBuffer = static_cast<ULONG>(schannel_->recv_encrypted_buffer.size());
                buffers[0].BufferType = SECBUFFER_DATA;

                buffers[1].BufferType = SECBUFFER_EMPTY;
                buffers[2].BufferType = SECBUFFER_EMPTY;
                buffers[3].BufferType = SECBUFFER_EMPTY;

                SecBufferDesc buffer_desc;
                buffer_desc.ulVersion = SECBUFFER_VERSION;
                buffer_desc.cBuffers = 4;
                buffer_desc.pBuffers = buffers;

                SECURITY_STATUS status = DecryptMessage(&schannel_->context,
                    &buffer_desc, 0, nullptr);

                if (status == SEC_E_OK) {
                    // Find decrypted data buffer
                    SecBuffer* data_buffer = nullptr;
                    SecBuffer* extra_buffer = nullptr;

                    for (int i = 0; i < 4; i++) {
                        if (buffers[i].BufferType == SECBUFFER_DATA) {
                            data_buffer = &buffers[i];
                        }
                        else if (buffers[i].BufferType == SECBUFFER_EXTRA) {
                            extra_buffer = &buffers[i];
                        }
                    }

                    if (data_buffer && data_buffer->cbBuffer > 0) {
                        size_t copy_size = std::min(max_len,
                            static_cast<size_t>(data_buffer->cbBuffer));
                        memcpy(buffer, data_buffer->pvBuffer, copy_size);

                        bytes_decrypted_ += copy_size;

                        // Handle extra data
                        if (extra_buffer && extra_buffer->cbBuffer > 0) {
                            schannel_->recv_encrypted_buffer.assign(
                                static_cast<uint8_t*>(extra_buffer->pvBuffer),
                                static_cast<uint8_t*>(extra_buffer->pvBuffer) +
                                extra_buffer->cbBuffer
                            );
                        }
                        else {
                            schannel_->recv_encrypted_buffer.clear();
                        }

                        return static_cast<int>(copy_size);
                    }
                }
                else if (status == SEC_E_INCOMPLETE_MESSAGE) {
                    // Need more data
                    continue;
                }
                else {
                    last_error_ = TLSError(status, "Decryption failed");
                    return -1;
                }
            }
        }

#else
        // Linux OpenSSL implementation

        bool init_openssl() {
            // Create SSL context
            const SSL_METHOD* method = is_server_ ?
                TLS_server_method() : TLS_client_method();

            ssl_ctx_ = SSL_CTX_new(method);
            if (!ssl_ctx_) {
                last_error_ = TLSError::get_last_error();
                return false;
            }

            // Set protocol versions
            if (!set_protocol_versions()) {
                SSL_CTX_free(ssl_ctx_);
                ssl_ctx_ = nullptr;
                return false;
            }

            // Set cipher suites
            if (!config_.ciphers.empty()) {
                if (SSL_CTX_set_cipher_list(ssl_ctx_, config_.ciphers.c_str()) != 1) {
                    last_error_ = TLSError::get_last_error();
                    SSL_CTX_free(ssl_ctx_);
                    ssl_ctx_ = nullptr;
                    return false;
                }
            }

            // Set verification
            if (config_.verify_peer) {
                SSL_CTX_set_verify(ssl_ctx_,
                    SSL_VERIFY_PEER | (config_.require_peer_cert ? SSL_VERIFY_FAIL_IF_NO_PEER_CERT : 0),
                    nullptr);
                SSL_CTX_set_verify_depth(ssl_ctx_, config_.verify_depth);
            }
            else {
                SSL_CTX_set_verify(ssl_ctx_, SSL_VERIFY_NONE, nullptr);
            }

            // Create SSL object
            ssl_ = SSL_new(ssl_ctx_);
            if (!ssl_) {
                last_error_ = TLSError::get_last_error();
                SSL_CTX_free(ssl_ctx_);
                ssl_ctx_ = nullptr;
                return false;
            }

            // Set socket
            if (fd_ != INVALID_SOCKET_VALUE) {
                SSL_set_fd(ssl_, fd_);
            }

            // Set SNI hostname for client
            if (!is_server_ && !config_.sni_hostname.empty()) {
                SSL_set_tlsext_host_name(ssl_, config_.sni_hostname.c_str());
            }

            tls_enabled_ = true;
            return true;
        }

        bool set_protocol_versions() {
            int min_version = 0, max_version = 0;

            switch (config_.min_version) {
            case TLSVersion::TLS_1_0: min_version = TLS1_VERSION; break;
            case TLSVersion::TLS_1_1: min_version = TLS1_1_VERSION; break;
            case TLSVersion::TLS_1_2: min_version = TLS1_2_VERSION; break;
#ifdef TLS1_3_VERSION
            case TLSVersion::TLS_1_3: min_version = TLS1_3_VERSION; break;
#endif
            default: min_version = TLS1_2_VERSION;
            }

            switch (config_.max_version) {
            case TLSVersion::TLS_1_0: max_version = TLS1_VERSION; break;
            case TLSVersion::TLS_1_1: max_version = TLS1_1_VERSION; break;
            case TLSVersion::TLS_1_2: max_version = TLS1_2_VERSION; break;
#ifdef TLS1_3_VERSION
            case TLSVersion::TLS_1_3: max_version = TLS1_3_VERSION; break;
#endif
            default: max_version = 0;
            }

            if (SSL_CTX_set_min_proto_version(ssl_ctx_, min_version) != 1) {
                last_error_ = TLSError::get_last_error();
                return false;
            }

            if (max_version > 0) {
                if (SSL_CTX_set_max_proto_version(ssl_ctx_, max_version) != 1) {
                    last_error_ = TLSError::get_last_error();
                    return false;
                }
            }

            return true;
        }

        bool perform_openssl_handshake(bool is_server) {
            if (!ssl_) {
                return false;
            }

            int result;
            if (is_server) {
                SSL_set_accept_state(ssl_);
                result = SSL_accept(ssl_);
            }
            else {
                SSL_set_connect_state(ssl_);
                result = SSL_connect(ssl_);
            }

            if (result == 1) {
                // Handshake successful
                return true;
            }

            int error = SSL_get_error(ssl_, result);

            if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
                // Would block - this is normal for non-blocking sockets
                // The handshake will continue when data is available
                return true;
            }

            // Real error
            last_error_ = TLSError::get_last_error();
            return false;
        }

        int send_encrypted_openssl(const uint8_t* data, size_t len) {
            if (!ssl_) {
                return -1;
            }

            int written = SSL_write(ssl_, data, static_cast<int>(len));

            if (written > 0) {
                bytes_encrypted_ += written;
                return written;
            }

            int error = SSL_get_error(ssl_, written);

            if (error == SSL_ERROR_WANT_WRITE || error == SSL_ERROR_WANT_READ) {
                // Would block
                errno = EAGAIN;
                return -1;
            }

            last_error_ = TLSError::get_last_error();
            return -1;
        }

        int receive_decrypted_openssl(uint8_t* buffer, size_t max_len) {
            if (!ssl_) {
                return -1;
            }

            int read = SSL_read(ssl_, buffer, static_cast<int>(max_len));

            if (read > 0) {
                bytes_decrypted_ += read;
                return read;
            }

            int error = SSL_get_error(ssl_, read);

            if (error == SSL_ERROR_WANT_READ || error == SSL_ERROR_WANT_WRITE) {
                // Would block
                errno = EAGAIN;
                return -1;
            }

            if (error == SSL_ERROR_ZERO_RETURN) {
                // Connection closed
                return 0;
            }

            last_error_ = TLSError::get_last_error();
            return -1;
        }
#endif

        void cleanup_tls() {
            if (!tls_enabled_) return;

#ifdef _WIN32
            if (schannel_) {
                schannel_.reset();
            }
#else
            if (ssl_) {
                SSL_shutdown(ssl_);
                SSL_free(ssl_);
                ssl_ = nullptr;
            }

            if (ssl_ctx_) {
                SSL_CTX_free(ssl_ctx_);
                ssl_ctx_ = nullptr;
            }

            bio_in_ = nullptr;
            bio_out_ = nullptr;
#endif

            tls_enabled_ = false;
            session_info_.reset();
        }
    };

    // Static member definitions for OpenSSL
#ifndef _WIN32
    std::once_flag TLSSocket::openssl_init_flag_;
    std::atomic<bool> TLSSocket::openssl_initialized_{ false };
#endif

    // Socket factory implementation
    inline std::unique_ptr<Socket> Socket::create(bool use_tls, const TLSConfig* config) {
        if (use_tls) {
            return std::make_unique<TLSSocket>();
        }
        return std::make_unique<Socket>();
    }

} // namespace ourmqtt

#endif // OUR_TLS_SOCKET_HPP