#ifndef PARADIGM4_PICO_PS_HANDLER_DUMPHANDLER_H
#define PARADIGM4_PICO_PS_HANDLER_DUMPHANDLER_H

#include <cstdint>

#include <memory>

#include "pico-ps/common/core.h"

#include "pico-ps/handler/ForEachHandler.h"
#include "pico-ps/operator/DumpOperator.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class DumpHandler : public ForEachHandler {
public:
    DumpHandler(int32_t storage_id,
          int32_t handler_id,
          std::shared_ptr<Operator>& op,
          Client* client);

    virtual ~DumpHandler();

    DumpHandler(DumpHandler&&) = default;
    DumpHandler& operator=(DumpHandler&&) = default;

    void dump(const DumpArgs& args);
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_HANDLER_DUMPHANDLER_H
