#ifndef PARADIGM4_PICO_PS_NATIVEPS_LOCALLOADHANDLER_H
#define PARADIGM4_PICO_PS_NATIVEPS_LOCALLOADHANDLER_H

#include <mutex>
#include <string>
#include <unordered_map>

#include "pico-ps/common/core.h"

#include "pico-ps/operator/operators.h"
#include "pico-ps/service/Context.h"
#include "pico-ps/storage/Storage.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class NativePS {
public:
    NativePS();

    ~NativePS();

    int32_t create_storage(const std::string& lib_name,
          const std::string& op_name,
          const Configure& conf);

    std::shared_ptr<Context>& context() {
        return _ctx;
    }

private:
    std::shared_ptr<Context> _ctx;
    int32_t _storage_num = 0;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_NATIVEPS_LOCALLOADHANDLER_H
