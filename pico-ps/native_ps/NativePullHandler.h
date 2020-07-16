#ifndef PARADIGM4_PICO_PS_NATIVEPS_LOCALPULLHANDLER_H
#define PARADIGM4_PICO_PS_NATIVEPS_LOCALPULLHANDLER_H
#include <cstdint>

#include <memory>
#include <vector>

#include "pico-ps/common/core.h"

#include "pico-ps/handler/Handler.h"
#include "pico-ps/handler/PullHandler.h"
#include "pico-ps/operator/Operator.h"
#include "pico-ps/service/Context.h"
#include "pico-ps/service/TableDescriptor.h"
#include "pico-ps/storage/Storage.h"
#include "pico-ps/native_ps/NativePS.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class NativePullHandler : public PullHandler {
public:
    NativePullHandler(int32_t storage_id, std::shared_ptr<Operator>& op, NativePS* native_ps);

    virtual ~NativePullHandler();

    NativePullHandler(NativePullHandler&&) = default;
    NativePullHandler& operator=(NativePullHandler&&) = default;

    virtual Status wait_no_retry() override;

    virtual Status wait() override;

private:
    virtual void pull(const PSMessageMeta& meta,
          core::vector<std::unique_ptr<PullItems>>&& items,
          int timeout = -1) override;

    virtual Status pull_with_auto_retry(const PSMessageMeta& meta,
          core::vector<std::unique_ptr<PullItems>>&& items,
          int timeout = -1) override;

    virtual void retry(int timeout = -1) override;

    virtual void release_dealer() override;

    Storage* _st;
    TableDescriptor* _td;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_NATIVEPS_LOCALPULLHANDLER_H
