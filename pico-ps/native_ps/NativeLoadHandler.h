#ifndef PARADIGM4_PICO_PS_NATIVEPS_NATIVELOADHANDLER_H
#define PARADIGM4_PICO_PS_NATIVEPS_NATIVLLOADHANDLER_H

#include <cstdint>

#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "pico-ps/common/core.h"
#include "pico-ps/common/core.h"

#include "pico-ps/common/common.h"
#include "pico-ps/common/message.h"
#include "pico-ps/handler/Handler.h"
#include "pico-ps/handler/LoadHandler.h"
#include "pico-ps/operator/Operator.h"
#include "pico-ps/service/Context.h"
#include "pico-ps/native_ps/NativePS.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class NativeLoadHandler : public LoadHandler {
public:
    NativeLoadHandler(int32_t storage_id, std::shared_ptr<Operator>& op, NativePS* native_ps);

    virtual ~NativeLoadHandler();

    NativeLoadHandler(NativeLoadHandler&&) = default;
    NativeLoadHandler& operator=(NativeLoadHandler&&) = default;

    virtual void load(const URIConfig& path,
          const std::string& hadoop_bin = "",
          size_t server_concurency = 4,
          int timeout = -1) override;

    virtual Status wait_no_retry() override;

    virtual Status wait() override;

private:
    void _load(const URIConfig& uri,
          const std::vector<std::string>& files,
          size_t begin,
          size_t end);

    Storage* _st;
    TableDescriptor* _td;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_NATIVEPS_NATIVLLOADHANDLER_H
