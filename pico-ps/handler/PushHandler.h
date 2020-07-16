#ifndef PARADIGM4_PICO_PS_HANDLER_PUSHHANDLER_H
#define PARADIGM4_PICO_PS_HANDLER_PUSHHANDLER_H

#include <cstdint>

#include <memory>
#include <vector>

#include "pico-ps/common/core.h"
#include "pico-ps/common/ThreadSync.h"

#include "pico-ps/handler/Handler.h"
#include "pico-ps/operator/PushOperator.h"
#include "pico-ps/service/Context.h"

namespace paradigm4 {
namespace pico {
namespace ps {

struct PushRequestDataGroup {
    ThreadReduce reduce;
    uint32_t tids[GROUP_SIZE];
    RWSpinLock lock[GROUP_SIZE];
    std::unique_ptr<PushRequestData> data[GROUP_SIZE];
    pico::vector<std::unique_ptr<PushItems>> items[GROUP_SIZE];
    PushRequestDataGroup(uint32_t merge_num): reduce(merge_num) {}
};

class PushHandler : public Handler {
public:
    PushHandler(int32_t storage_id,
          int32_t handler_id,
          std::shared_ptr<Operator>& op,
          Client* client,
          std::shared_ptr<PushRequestDataGroup> group = nullptr,
          uint32_t tid = -1);

    virtual ~PushHandler();

    PushHandler(PushHandler&&) = default;
    PushHandler& operator=(PushHandler&&) = default;

    template <typename T>
    void async_push(PushItemsPackage<T>&& push_items_pkg, int timeout = -1) {
        push({_storage_id, _handler_id, -1, -1, RequestType::OP_PUSH_ASYNC},
              std::move(push_items_pkg.content()),
              timeout);
    }

    void async_push(std::unique_ptr<PushItems>&& push_items, int timeout = -1);

    template <typename KEY, typename PUSH_ARG>
    void async_push(const KEY* keys, const PUSH_ARG* push_args, size_t n, int timeout = -1) {
        async_push(
              std::make_unique<SparsePushItems<KEY, PUSH_ARG>>(keys, push_args, n), timeout);
    }

    template <typename KEY, typename PUSH_ARG>
    void async_push(std::vector<KEY>&& keys,
          std::vector<PUSH_ARG>&& push_args,
          int timeout = -1) {
        async_push(std::make_unique<SparsePushItems<KEY, PUSH_ARG>>(
                                std::move(keys), std::move(push_args)),
              timeout);
    }

    template <typename KEY, typename PUSH_ARG>
    void async_push(const std::vector<KEY>& keys,
          const std::vector<PUSH_ARG>& push_args,
          int timeout = -1) {
        async_push(
              std::make_unique<SparsePushItems<KEY, PUSH_ARG>>(keys, push_args), timeout);
    }

    template <typename T>
    void sync_push(PushItemsPackage<T>&& push_items_pkg, int timeout = -1) {
        push({_storage_id, _handler_id, -1, -1, RequestType::OP_PUSH_SYNC},
              std::move(push_items_pkg.content()),
              timeout);
    }

    void sync_push(std::unique_ptr<PushItems>&& push_items, int timeout = -1);

    template <typename KEY, typename PUSH_ARG>
    void sync_push(const KEY* keys, const PUSH_ARG* push_args, size_t n, int timeout = -1) {
        sync_push(
              std::make_unique<SparsePushItems<KEY, PUSH_ARG>>(keys, push_args, n), timeout);
    }

    template <typename KEY, typename PUSH_ARG>
    void sync_push(std::vector<KEY>&& keys,
          std::vector<PUSH_ARG>&& push_args,
          int timeout = -1) {
        sync_push(std::make_unique<SparsePushItems<KEY, PUSH_ARG>>(
                               std::move(keys), std::move(push_args)),
              timeout);
    }

    template <typename KEY, typename PUSH_ARG>
    void sync_push(const std::vector<KEY>& keys,
          const std::vector<PUSH_ARG>& push_args,
          int timeout = -1) {
        sync_push(
              std::make_unique<SparsePushItems<KEY, PUSH_ARG>>(keys, push_args), timeout);
    }

    void store(int timeout = -1);


protected:

    void push(const PSMessageMeta& meta,
          pico::vector<std::unique_ptr<PushItems>>&& items,
          int timeout = -1);

    virtual void retry(int timeout) override;
    
    virtual Status apply_response(PSResponse& resp, PSMessageMeta& meta) override;

    virtual void release_dealer() override;

    std::vector<PushRequestData*> _request_data;
    std::shared_ptr<PushRequestDataGroup> _group;
    uint32_t _tid;
}; // namespace ps

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_HANDLER_PUSHHANDLER_H
