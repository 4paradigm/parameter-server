#ifndef PARADIGM4_PICO_PS_HANDLER_UPDATE_CONTEXT_HANDLER_H
#define PARADIGM4_PICO_PS_HANDLER_UPDATE_CONTEXT_HANDLER_H

#include <cstdint>

#include <memory>
#include <thread>
#include <vector>

#include "pico-ps/common/core.h"

#include "pico-ps/common/common.h"
#include "pico-ps/common/message.h"
#include "pico-ps/handler/Handler.h"
#include "pico-ps/operator/UpdateContextOperator.h"
#include "pico-ps/service/Client.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class UpdateContextHandler : public Handler {
public:
    UpdateContextHandler() = default;

    UpdateContextHandler(int32_t storage_id,
          int32_t handler_id,
          std::shared_ptr<Operator>& op,
          Client* client);


    Status purge(int timeout = -1);

    virtual ~UpdateContextHandler();

    UpdateContextHandler(UpdateContextHandler&&) = default;
    UpdateContextHandler& operator=(UpdateContextHandler&&) = default;

    Status update_context(const Configure& config, int timeout = -1);

    virtual Status wait() override;
    
protected:

    Status prepare_update_ctx(TableDescriptorWriter& td, const Configure& config);

    void create_shards(TableDescriptorWriter& td, int timeout = -1);

    void shuffle(TableDescriptorWriter& td, int timeout = -1);

    void store(TableDescriptorWriter& td, int timeout = -1);

    Status commit_new_ctx_to_master(TableDescriptorWriter& td);

    void purge(TableDescriptorWriter& td, int timeout = -1);

    Status end_update_context(TableDescriptorWriter& td);

    virtual Status apply_response(PSResponse& resp, PSMessageMeta& meta) override;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_HANDLER_UPDATE_CONTEXT_HANDLER_H
