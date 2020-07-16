#include "pico-ps/common/Status.h"

namespace paradigm4 {
namespace pico {
namespace ps {

std::string Status::ToString() const {
    if (ok()) {
        return "OK";
    } else {
        char tmp[30];
        const char* type;
        switch (_code) {
        case StatusCode::OK:
            type = "OK";
            break;
        case StatusCode::INVALID_CONFIG:
            type = "Invalid Config: ";
            break;
        case StatusCode::INVALID_ID:
            type = "Invalid ID: ";
            break;
        case StatusCode::OUT_OF_MEMORY:
            type = "Out of Memory: ";
            break;
        case StatusCode::ERROR:
            type = "Error: ";
            break;
        case StatusCode::TIMEOUT:
            type = "Timeout: ";
            break;
        case StatusCode::SERVER_TOO_NEW_CTX:
            type = "Server too new ctx: ";
            break;
        case StatusCode::SERVER_TOO_OLD_CTX:
            type = "Server too old ctx: ";
            break;
        case StatusCode::SERVER_TOO_NEW_CTX_U:
            type = "Server too new ctx: ";
            break;
        case StatusCode::SERVER_TOO_OLD_CTX_U:
            type = "Server too old ctx: ";
            break;
        case StatusCode::NO_REPLICA:
            type = "No replica";
            break;
        case StatusCode::FATAL:
            type = "Fatal: ";
            break;
        default:
            snprintf(tmp, sizeof(tmp), "Unknown code(%d): ", static_cast<int>(_code));
            type = tmp;
            break;
        }
        std::string result(type);
        result += _msg;
        return result;
    }
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
