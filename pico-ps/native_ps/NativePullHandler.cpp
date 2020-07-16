#include "pico-ps/native_ps/NativePullHandler.h"

namespace paradigm4 {
namespace pico {
namespace ps {

NativePullHandler::NativePullHandler(int32_t storage_id,
      std::shared_ptr<Operator>& op,
      NativePS* native_ps)
    : PullHandler(storage_id, -1, op, nullptr) {
    TableDescriptorReader td;
    auto status = native_ps->context()->GetTableDescriptorReader(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    _st = td.table().storage.get();
    _td = &(td.table());
}

NativePullHandler::~NativePullHandler() {}

void NativePullHandler::pull(const PSMessageMeta&,
      core::vector<std::unique_ptr<PullItems>>&& items,
      int) {
    std::vector<PSRequest> reqs;
    std::unique_ptr<PullRequestData> data;
    std::vector<PullRequestData*> request_data;
    auto op = static_cast<PullOperator*>(_op.get());
    op->generate_request_data(items, *_td->runtime_info, data);
    request_data.push_back(data.get());
    op->generate_request(request_data, *_td->runtime_info, reqs);
    for (auto& req : reqs) {
        PSResponse resp;
        op->apply_request(*_td->runtime_info, req, _st, resp);
        op->apply_response(resp, request_data);
    }
    op->fill_pull_items(data.get(), items);
}

Status NativePullHandler::pull_with_auto_retry(const PSMessageMeta& meta,
      core::vector<std::unique_ptr<PullItems>>&& items,
      int timeout) {
    pull(meta, std::move(items), timeout);
    return wait();
}

Status NativePullHandler::wait_no_retry() {
    return Status();
}

Status NativePullHandler::wait() {
    return Status();
}

void NativePullHandler::retry(int) {}

void NativePullHandler::release_dealer() {}

} // namespace ps
} // namespace pico
} // namespace paradigm4
