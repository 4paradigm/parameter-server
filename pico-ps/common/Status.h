#ifndef PARADIGM4_PICO_PS_COMMON_STATUS_H
#define PARADIGM4_PICO_PS_COMMON_STATUS_H

#include <cstring>

#include <algorithm>
#include <string>

#include "pico-ps/common/core.h"

namespace paradigm4 {
namespace pico {
namespace ps {

enum class StatusCode : int32_t {
    OK,                   // 0
    INVALID_CONFIG,       // 1
    INVALID_ID,           // 2
    OUT_OF_MEMORY,        // 3
    TIMEOUT,              // 4
    SERVER_TOO_NEW_CTX,   // 5
    SERVER_TOO_OLD_CTX,   // 6
    SERVER_TOO_NEW_CTX_U, // 7
    SERVER_TOO_OLD_CTX_U, // 8
    NO_REPLICA,           // 9
    ERROR,                // 10, unclassified errors
    FATAL,                // 11, error cannot resolve
    EMPTY,
};

PICO_ENUM_SERIALIZATION(StatusCode, int32_t);

class Status {
public:
    // Create a success status.
    Status() noexcept : _code(StatusCode::OK) {}

    Status(const Status& rhs) {
        *this = rhs;
    }

    Status& operator=(const Status& rhs) {
        _code = rhs._code;
        _msg = rhs._msg;
        return *this;
    }

    Status(Status&& rhs) {
        *this = std::move(rhs);
    }

    Status& operator=(Status&& rhs) noexcept {
        _code = std::move(rhs._code);
        _msg = std::move(rhs._msg);
        return *this;
    }

    // Return a success status.
    static Status Ok() {
        return Status();
    }

    // Return error status of an appropriate type.
    static Status InvalidConfig(const std::string& msg) {
        return Status(StatusCode::INVALID_CONFIG, msg);
    }

    static Status InvalidID(const std::string& msg) {
        return Status(StatusCode::INVALID_ID, msg);
    }

    static Status OOM(const std::string& msg) {
        return Status(StatusCode::OUT_OF_MEMORY, msg);
    }

    static Status Fatal(const std::string& msg) {
        return Status(StatusCode::FATAL, msg);
    }

    static Status Error(const std::string& msg) {
        return Status(StatusCode::ERROR, msg);
    }

    static Status Timeout(const std::string& msg) {
        return Status(StatusCode::TIMEOUT, msg);
    }

    static Status ServerTooOldCtx(const std::string& msg) {
        return Status(StatusCode::SERVER_TOO_OLD_CTX, msg);
    }

    static Status ServerTooNewCtx(const std::string& msg) {
        return Status(StatusCode::SERVER_TOO_NEW_CTX, msg);
    }

    static Status ServerTooOldCtxU(const std::string& msg) {
        return Status(StatusCode::SERVER_TOO_OLD_CTX_U, msg);
    }

    static Status ServerTooNewCtxU(const std::string& msg) {
        return Status(StatusCode::SERVER_TOO_NEW_CTX_U, msg);
    }

    static Status NoReplica(const std::string& msg) {
        return Status(StatusCode::NO_REPLICA, msg);
    }

    static Status Empty(const std::string& msg) {
        return Status(StatusCode::EMPTY, msg);
    }

    operator bool() const {
        return ok();
    }

    // Returns true iff the status indicates success.
    bool ok() const {
        return (_code == StatusCode::OK);
    }

    // Returns true iff the status indicates an INVALID_CONFIG.
    bool IsInvalidConfig() const {
        return (_code == StatusCode::INVALID_CONFIG);
    }

    // Returns true iff the status indicates an INVALID_ID.
    bool IsInvalidID() const {
        return (_code == StatusCode::INVALID_ID);
    }

    // Returns true iff the status indicates an OUT_OF_MEMORY.
    bool IsOOM() const {
        return (_code == StatusCode::OUT_OF_MEMORY);
    }

    // Returns true iff the status indicates a ERROR.
    bool IsError() const {
        return (_code == StatusCode::ERROR);
    }

    // Returns true iff the status indicates a FATAL.
    bool IsFatal() const {
        return (_code == StatusCode::FATAL);
    }

    // Returns true iff the status indicates a TIMEOUT.
    bool IsTimeout() const {
        return (_code == StatusCode::TIMEOUT);
    }

    // Returns true iff the status indicates a SERVER_TOO_NEW_CTX.
    bool IsServerTooNewCtx() const {
        return (_code == StatusCode::SERVER_TOO_NEW_CTX);
    }

    // Returns true iff the status indicates a SERVER_TOO_OLD_CTX.
    bool IsServerTooOldCtx() const {
        return (_code == StatusCode::SERVER_TOO_OLD_CTX);
    }

    // Returns true iff the status indicates a SERVER_TOO_NEW_CTX_U.
    bool IsServerTooNewCtxU() const {
        return (_code == StatusCode::SERVER_TOO_NEW_CTX_U);
    }

    // Returns true iff the status indicates a SERVER_TOO_OLD_CTX_U.
    bool IsServerTooOldCtxU() const {
        return (_code == StatusCode::SERVER_TOO_OLD_CTX_U);
    }

    // Returns true iff the status indicates a NO_REPLICA.
    bool IsNoReplica() const {
        return (_code == StatusCode::NO_REPLICA);
    }

    bool IsEmpty() const {
        return (_code == StatusCode::EMPTY);
    }

    // Return a string representation of this status suitable for printing.
    // Returns the string "OK" for success.
    std::string ToString() const;

    static constexpr size_t OK_ARCHIVE_SIZE
          = sizeof(StatusCode) + sizeof(size_t) + sizeof(char);

private:
    StatusCode _code;
    std::string _msg;

    PICO_SERIALIZATION(_code, _msg);

    Status(StatusCode code, const std::string& msg) : _code(code), _msg(msg) {
        SCHECK(code != StatusCode::OK);
    }

};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_COMMON_STATUS_H
