#ifndef PARADIGM4_PICO_PS_COMMON_DEFS_H
#define PARADIGM4_PICO_PS_COMMON_DEFS_H

#include <cstddef>
#include <cstdint>

namespace paradigm4 {
namespace pico {

const char* const PSERVER_LOCK_NAME = "PSERVER_LOCK";

const char* const PSERVER_STORAGE_ID_KEY = "storage_id";
const char* const PSERVER_HANDLER_ID_KEY = "handler";

const char* const PSERVER_C2S_RPC_NAME = "pserver_c2s_rpc_api";
const char* const PSERVER_S2S_RPC_NAME = "pserver_s2s_rpc_api";

} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_COMMON_DEFS_H
