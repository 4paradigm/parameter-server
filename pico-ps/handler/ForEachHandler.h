#ifndef PARADIGM4_PICO_PS_HANDLER_FOREACHHANDLER_H
#define PARADIGM4_PICO_PS_HANDLER_FOREACHHANDLER_H

#include <cstdint>

#include <memory>
#include <vector>

#include "pico-ps/common/core.h"

#include "pico-ps/handler/Handler.h"
#include "pico-ps/operator/ForEachOperator.h"
#include "pico-ps/service/Context.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class ForEachHandler : public Handler {
public:
    ForEachHandler(int32_t storage_id,
          int32_t handler_id,
          std::shared_ptr<Operator>& op,
          Client* client);

    virtual ~ForEachHandler();

    ForEachHandler(ForEachHandler&&) = default;
    ForEachHandler& operator=(ForEachHandler&&) = default;

    template <typename CARRIEDITEM>
    void for_each(CARRIEDITEM&& item, RequestType req_type = RequestType::OP_FOREACH) {
        TableDescriptorReader td;
        auto st = _client->context()->GetTableDescriptorReader(_storage_id, td);
        SCHECK(st.ok()) << st.ToString();
        std::vector<PSRequest> reqs;
        static_cast<ForEachOperator*>(_op.get())->generate_request(
              item, *td.table().runtime_info, reqs);
        send(std::move(reqs), {_storage_id, _handler_id, td.table().version, -1, req_type});
        _item = std::make_unique<CarriedItem>(std::forward<CARRIEDITEM>(item));
    }

    void for_each() {
        for_each(CarriedItem(), RequestType::OP_FOREACH);
    }

    template <typename RET>
    std::unique_ptr<RET> init_result() {
        return std::unique_ptr<RET>(static_cast<RET*>(
              (static_cast<ForEachOperator*>(_op.get())->init_result_impl()).release()));
    }

    virtual Status wait_and_merge_result(ForEachResult& res);

    // 有返回值，无参数的 for_each
    template <typename RET>
    RET sync_for_each() {
        auto res = init_result<RET>();
        for_each();
        wait_and_merge_result(*res);
        return *static_cast<RET*>(res.get());
    }

    // 无返回值，无参数的 for_each
    void sync_for_each() {
        for_each();
        SCHECK(wait().ok());
    }

    // 有返回值，有参数的 for_each
    template <typename RET, typename CARRIEDITEM>
    RET sync_for_each(CARRIEDITEM&& carried) {
        auto res = init_result<RET>();
        for_each(std::forward<CARRIEDITEM>(carried));
        wait_and_merge_result(*res);
        return *static_cast<RET*>(res.get());
    }

    // 无返回值，有参数的 for_each
    template <typename CARRIEDITEM>
    void sync_for_each(CARRIEDITEM&& carried) {
        for_each(std::forward<CARRIEDITEM>(carried));
        SCHECK(wait().ok());
    }


protected:

    virtual void retry(int timeout) override;

    virtual Status apply_response(PSResponse& resp, PSMessageMeta&) override;

    virtual void release_dealer() override;
    
    ForEachResult* _for_each_result = nullptr;
    std::unique_ptr<CarriedItem> _item;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_HANDLER_FOREACHHANDLER_H
