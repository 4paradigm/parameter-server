#include "pico-ps/handler/ForEachHandler.h"

namespace paradigm4 {
namespace pico {
namespace ps {

ForEachHandler::ForEachHandler(int32_t storage_id,
      int32_t handler_id,
      std::shared_ptr<Operator>& op,
      Client* client)
    : Handler(storage_id, handler_id, op, client) {
}

ForEachHandler::~ForEachHandler() {}

void ForEachHandler::retry(int timeout) {
    TableDescriptorReader td;
    auto st = _client->context()->GetTableDescriptorReader(_storage_id, td);
    SCHECK(st.ok()) << st.ToString();
    std::vector<PSRequest> reqs;
    static_cast<ForEachOperator*>(_op.get())->generate_request(
            *_item, *td.table().runtime_info, reqs);
    _meta.ctx_ver = td.table().version;
    send(std::move(reqs), _meta, timeout); 
}

Status ForEachHandler::wait_and_merge_result(ForEachResult& res) {
    _for_each_result = &res;
    auto status = wait();
    _for_each_result = nullptr;
    return status;
}

Status ForEachHandler::apply_response(PSResponse& resp, PSMessageMeta&) {
    auto op = static_cast<ForEachOperator*>(_op.get());
    auto res = op->apply_response(resp, *_item);
    if (_for_each_result != nullptr) {
        op->merge_result_impl(*res, *_for_each_result, *_item);
    }
    return Status();
}

void ForEachHandler::release_dealer() {
    _item.reset(nullptr);
    Handler::release_dealer();
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
