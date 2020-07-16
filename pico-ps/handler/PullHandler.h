#ifndef PARADIGM4_PICO_PS_HANDLER_PULLHANDLER_H
#define PARADIGM4_PICO_PS_HANDLER_PULLHANDLER_H

#include <cstdint>

#include <chrono>
#include <memory>
#include <vector>

#include "pico-ps/common/core.h"
#include "pico-ps/common/ThreadSync.h"

#include "pico-ps/handler/Handler.h"
#include "pico-ps/operator/PullOperator.h"
#include "pico-ps/service/Client.h"
#include "pico-ps/service/Context.h"

namespace paradigm4 {
namespace pico {
namespace ps {

struct PullRequestDataGroup {
    ThreadReduce reduce;
    ThreadBcast bcast;
    uint32_t tids[GROUP_SIZE];
    std::unique_ptr<PullRequestData> data[GROUP_SIZE];
    Status status[GROUP_SIZE];
    PullRequestDataGroup(uint32_t merge_num): reduce(merge_num), bcast(merge_num) {}
};

class PullHandler : public Handler {
public:
    PullHandler(int32_t storage_id,
          int32_t handler_id,
          std::shared_ptr<Operator>& op,
          Client* client,
          std::shared_ptr<PullRequestDataGroup> group = nullptr,
          uint32_t tid = -1);

    virtual ~PullHandler();

    PullHandler(PullHandler&&) = default;
    PullHandler& operator=(PullHandler&&) = default;

    template <typename T>
    void pull(PullItemsPackage<T>&& pull_items_pkg, int timeout = -1) {
        pull({_storage_id, _handler_id, -1, -1, RequestType::OP_PULL},
              std::move(pull_items_pkg.content()),
              timeout);
    }

    void pull(std::unique_ptr<PullItems>&& pull_items, int timeout = -1);

    template <typename KEY, typename VALUE>
    void pull(const KEY* keys, VALUE* pull_args, size_t n, int timeout = -1) {
        pull(
              std::make_unique<SparsePullItems<KEY, VALUE>>(keys, pull_args, n), timeout);
    }

    template <typename KEY, typename VALUE>
    void pull(const std::vector<KEY>& keys, VALUE* pull_args, int timeout = -1) {
        pull(std::make_unique<SparsePullItems<KEY, VALUE>>(keys, pull_args), timeout);
    }

    template <typename KEY, typename VALUE>
    void pull(std::vector<KEY>&& keys, VALUE* pull_args, int timeout = -1) {
        pull(std::make_unique<SparsePullItems<KEY, VALUE>>(std::move(keys), pull_args),
              timeout);
    }

    template <typename T>
    Status pull_with_auto_retry(PullItemsPackage<T>&& pull_items_pkg, int timeout = -1) {
        return pull_with_auto_retry({_storage_id, _handler_id, -1, -1, RequestType::OP_PULL},
              std::move(pull_items_pkg.content()),
              timeout);
    }

    Status pull_with_auto_retry(std::unique_ptr<PullItems>&& pull_items, int timeout = -1);

    template <typename KEY, typename VALUE>
    Status
          pull_with_auto_retry(const KEY* keys, VALUE* pull_args, size_t n, int timeout = -1) {
        return pull_with_auto_retry(
              std::make_unique<SparsePullItems<KEY, VALUE>>(keys, pull_args, n), timeout);
    }

    template <typename KEY, typename VALUE>
    Status pull_with_auto_retry(const std::vector<KEY>& keys,
          VALUE* pull_args,
          int timeout = -1) {
        return pull_with_auto_retry(
              std::make_unique<SparsePullItems<KEY, VALUE>>(keys, pull_args), timeout);
    }

    template <typename KEY, typename VALUE>
    Status pull_with_auto_retry(std::vector<KEY>&& keys, VALUE* pull_args, int timeout = -1) {
        return pull_with_auto_retry(
              std::make_unique<SparsePullItems<KEY, VALUE>>(std::move(keys), pull_args),
              timeout);
    }
protected:
    virtual Status wait_no_release() override;

    virtual void retry(int timeout = -1) override;
    
    virtual Status apply_response(PSResponse& resp, PSMessageMeta& meta) override;

    virtual void pull(const PSMessageMeta& meta,
          pico::vector<std::unique_ptr<PullItems>>&& items,
          int timeout = -1);

    virtual Status pull_with_auto_retry(const PSMessageMeta& meta,
          pico::vector<std::unique_ptr<PullItems>>&& items,
          int timeout = -1);

    virtual void release_dealer() override;
    
    pico::vector<std::unique_ptr<PullItems>> _items;

    std::vector<PullRequestData*> _request_data;
    std::shared_ptr<PullRequestDataGroup> _group;
    uint32_t _tid;

//     std::chrono::high_resolution_clock::time_point _time_points;
//     std::unordered_set<comm_rank_t> _succ_flags;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_HANDLER_PULLHANDLER_H
