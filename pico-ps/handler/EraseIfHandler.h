#ifndef PARADIGM4_PICO_PS_HANDLER_ERASEIFHANDLER_H
#define PARADIGM4_PICO_PS_HANDLER_ERASEIFHANDLER_H

#include <cstdint>

#include <memory>

#include "pico-ps/common/core.h"

#include "pico-ps/handler/ForEachHandler.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class EraseIfHandler : public ForEachHandler {
public:
    EraseIfHandler(int32_t storage_id,
          int32_t handler_id,
          std::shared_ptr<Operator>& op,
          Client* client);

    virtual ~EraseIfHandler();

    EraseIfHandler(EraseIfHandler&&) = default;
    EraseIfHandler& operator=(EraseIfHandler&&) = default;

    template <typename CARRIEDITEM>
    void erase_if(CARRIEDITEM&& item) {
        for_each(std::forward<CARRIEDITEM>(item), RequestType::OP_ERASEIF);
    }

    void erase_if() {
        erase_if(CarriedItem());
    }
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_HANDLER_ERASEIFHANDLER_H
