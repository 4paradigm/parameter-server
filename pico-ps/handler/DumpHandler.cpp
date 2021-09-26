#include "pico-ps/handler/DumpHandler.h"

#include "pico-ps/common/core.h"

namespace paradigm4 {
namespace pico {
namespace ps {

DumpHandler::DumpHandler(int32_t storage_id,
      int32_t handler_id,
      std::shared_ptr<Operator>& op,
      Client* client)
    : ForEachHandler(storage_id, handler_id, op, client) {}

DumpHandler::~DumpHandler() {}

void DumpHandler::dump(const DumpArgs& args) {
    URIConfig uri(args.uri());
    if (uri.is_memory()) {
        SCHECK(FileSystem::create_output_dir(uri));
    } else {
        SCHECK(FileSystem::create_output_dir(uri.name()));
    }
    for_each(args, RequestType::OP_DUMP);
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
