#include "pico-ps/handler/EraseIfHandler.h"

namespace paradigm4 {
namespace pico {
namespace ps {

EraseIfHandler::EraseIfHandler(int32_t storage_id,
      int32_t handler_id,
      std::shared_ptr<Operator>& op,
      Client* client)
    : ForEachHandler(storage_id, handler_id, op, client) {}

EraseIfHandler::~EraseIfHandler() {}

} // namespace ps
} // namespace pico
} // namespace paradigm4
