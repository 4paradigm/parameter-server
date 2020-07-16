#ifndef PARADIGM4_PICO_PS_HANDLER_LOADHANDLER_H
#define PARADIGM4_PICO_PS_HANDLER_LOADHANDLER_H

#include <cstdint>

#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "pico-ps/common/core.h"
#include "pico-ps/common/common.h"
#include "pico-ps/common/message.h"
#include "pico-ps/handler/Handler.h"
#include "pico-ps/operator/LoadOperator.h"
#include "pico-ps/service/Context.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class LoadHandler : public Handler {
public:
    LoadHandler() = default;

    LoadHandler(int32_t storage_id,
          int32_t handler_id,
          std::shared_ptr<Operator>& op,
          Client* client);

    virtual ~LoadHandler();

    LoadHandler(LoadHandler&&) = default;
    LoadHandler& operator=(LoadHandler&&) = default;

    virtual void load(const URIConfig& path,
          const std::string& hadoop_bin = "",
          size_t server_concurency = 4,
          int timeout = -1);

    virtual void restore(const URIConfig& path, bool drop = false,
        const std::string& hadoop_bin = "",
        size_t server_concurency = 4,
        int timeout = -1);

protected:
  
    Status _load();

    virtual Status wait_no_release() override;

    virtual void retry(int timeout = - 1) override;
  
    void send_request(comm_rank_t rank, const LoadArgs& args);

    struct Data{
        URIConfig path;
        std::string hadoop_bin;
        bool need_rehash = true;
        size_t server_concurency;
        bool drop = false;
        std::future<Status> future;
    };

    Data _data;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_HANDLER_LOADHANDLER_H
