#include <algorithm>
#include "pico-ps/handler/PullHandler.h"

namespace paradigm4 {
namespace pico {
namespace ps {

std::string storage_rank_str(int32_t storage_id, comm_rank_t rank) {
    return "storage_id: " + std::to_string(storage_id) + ", rank: " + std::to_string(rank);
}

PullHandler::PullHandler(int32_t storage_id,
      int32_t handler_id,
      std::shared_ptr<Operator>& op,
      Client* client,
      std::shared_ptr<PullRequestDataGroup> group,
      uint32_t tid)
    : Handler(storage_id, handler_id, op, client) {
    _group = group;
    _tid = tid;
    if (_group == nullptr) {
        _group = std::make_shared<PullRequestDataGroup>(1);
        _tid = 0;
    }
}

PullHandler::~PullHandler() {}

void PullHandler::pull(std::unique_ptr<PullItems>&& pull_items, int timeout) {
    pico::vector<std::unique_ptr<PullItems>> pull_items_vec;
    pull_items_vec.emplace_back(std::move(pull_items));
    pull({_storage_id, _handler_id, -1, -1, RequestType::OP_PULL},
          std::move(pull_items_vec),
          timeout);
}

Status PullHandler::pull_with_auto_retry(std::unique_ptr<PullItems>&& pull_items,
      int timeout) {
    pico::vector<std::unique_ptr<PullItems>> pull_items_vec;
    pull_items_vec.emplace_back(std::move(pull_items));
    return pull_with_auto_retry({_storage_id, _handler_id, -1, -1, RequestType::OP_PULL},
          std::move(pull_items_vec),
          timeout);
}

Status PullHandler::wait_no_release() {
    auto status = Handler::wait_no_release();
    auto& group = *_group;
    for (uint32_t i = 0; i < GROUP_SIZE; ++i) {
        if (group.tids[_tid] & (1u << i)) {
            group.status[i] = status;
        }
    }
    group.bcast.bcast(_tid, group.tids[_tid]);
    status = group.status[_tid];
    if (status.ok()) {
        static_cast<PullOperator*>(_op.get())->fill_pull_items(group.data[_tid].get(), _items);
    }
    return status;
}

void PullHandler::retry(int timeout) {
    auto items = std::move(_items);
    pull(_meta, std::move(items), timeout);
    // for (auto& req : reqs) {
    //     _succ_flags.insert(req.rpc_request().head().dest_rank);
    // }
    // _time_points = std::chrono::high_resolution_clock::now();
}

Status PullHandler::apply_response(PSResponse& resp, PSMessageMeta&) {
#ifndef NDEBUG
    // auto start_time_point = std::chrono::high_resolution_clock::now();
#endif
    static_cast<PullOperator*>(_op.get())->apply_response(resp, _request_data);
    return Status();
    // auto end_time_point = std::chrono::high_resolution_clock::now();
    // auto dur = end_time_point - _time_points;
    //auto us = std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
    // comm_rank_t rank = resp.rpc_response().head().src_rank;
    //SpeedometerClient::thread_local_instance().write(
    //      storage_rank_str(_meta.sid, rank), false, us);
    // _succ_flags.erase(rank);
#ifndef NDEBUG
    //auto resp_dur = end_time_point - start_time_point;
    //auto resp_us = std::chrono::duration_cast<std::chrono::microseconds>(resp_dur).count();
    //SpeedometerClient::thread_local_instance().write(
    //      "apply response, storage_id: " + std::to_string(_meta.sid), false, resp_us);
#endif
}

void PullHandler::pull(const PSMessageMeta& meta,
      pico::vector<std::unique_ptr<PullItems>>&& items,
      int timeout) {
    // auto start_time = std::chrono::high_resolution_clock::now();
    TableDescriptorReader td;
    auto st = _client->context()->GetTableDescriptorReader(_storage_id, td);
    SCHECK(st.ok()) << st.ToString();
    static thread_local std::vector<PSRequest> reqs;
    reqs.clear();
    auto& group = *_group;
    group.reduce.add(_tid);
    _request_data.clear();
    auto op = static_cast<PullOperator*>(_op.get());
    op->generate_request_data(items, *td.table().runtime_info, group.data[_tid]);

    size_t sync_num = 1;
    size_t data_size = group.data[_tid]->item_size * group.data[_tid]->item_num;
    data_size /= std::max(1ul, td.table().runtime_info->nodes().size());
    if (data_size > 0 && data_size < 128 * 1024) {
        sync_num = 2;
    }

    group.tids[_tid] = group.reduce.reduce(_tid, sync_num);
    for (uint32_t i = 0; i < GROUP_SIZE; ++i) {
        if (group.tids[_tid] & (1u << i)) {
            _request_data.push_back(group.data[i].get());
        }
    }
    if (group.tids[_tid] != 0) {
        _send_status = op->generate_request(_request_data, *td.table().runtime_info, reqs);
    }
    send(std::move(reqs), {meta.sid, meta.hid, td.table().version, -1, meta.req_type}, timeout);
    _items = std::move(items);
#ifndef NDEBUG
    //auto gen_req_dur = std::chrono::high_resolution_clock::now() - start_time;
    //auto us = std::chrono::duration_cast<std::chrono::microseconds>(gen_req_dur).count();
    //SpeedometerClient::thread_local_instance().write(
    //      "generate request, storage_id: " + std::to_string(meta.sid), false, us);
#endif

    // std::unordered_set<comm_rank_t> rank_set;
    // for (auto& req : reqs) {
    //     rank_set.insert(req.rpc_request().head().dest_rank);
    // }

    // _time_points = start_time;
    // _succ_flags = std::move(rank_set);
}

Status PullHandler::pull_with_auto_retry(const PSMessageMeta& meta,
      pico::vector<std::unique_ptr<PullItems>>&& items,
      int timeout) {
    pull(meta, std::move(items), timeout);
    return wait();
}

void PullHandler::release_dealer() {
    _items.clear();
    Handler::release_dealer();
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
