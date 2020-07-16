#include "pico-ps/handler/PushHandler.h"

namespace paradigm4 {
namespace pico {
namespace ps {

PushHandler::PushHandler(int32_t storage_id,
      int32_t handler_id,
      std::shared_ptr<Operator>& op,
      Client* client,
      std::shared_ptr<PushRequestDataGroup> group,
      uint32_t tid)
    : Handler(storage_id, handler_id, op, client) {
    _group = group;
    _tid = tid;
    if (_group == nullptr) {
        _group = std::make_shared<PushRequestDataGroup>(1);
        _tid = 0;
    }
}

PushHandler::~PushHandler() {}

void PushHandler::async_push(std::unique_ptr<PushItems>&& push_items, int timeout) {
    pico::vector<std::unique_ptr<PushItems>> push_items_vec;
    push_items_vec.emplace_back(std::move(push_items));
    return push({_storage_id, _handler_id, -1, -1, RequestType::OP_PUSH_ASYNC},
          std::move(push_items_vec),
          timeout);
}

void PushHandler::sync_push(std::unique_ptr<PushItems>&& push_items, int timeout) {
    pico::vector<std::unique_ptr<PushItems>> push_items_vec;
    push_items_vec.emplace_back(std::move(push_items));
    return push({_storage_id, _handler_id, -1, -1, RequestType::OP_PUSH_SYNC},
          std::move(push_items_vec),
          timeout);
}

void PushHandler::store(int timeout) {
    TableDescriptorReader td;
    auto st = _client->context()->GetTableDescriptorReader(_storage_id, td);
    SCHECK(st.ok()) << st.ToString();
    static thread_local std::vector<PSRequest> reqs;
    reqs.clear();
    static_cast<PushOperator*>(_op.get())->generate_store_request(
          *td.table().runtime_info, reqs);
    send(std::move(reqs),
          {_storage_id, _handler_id, td.table().version, -1, RequestType::OP_STORE},
          timeout);
}

void PushHandler::push(const PSMessageMeta& meta,
      pico::vector<std::unique_ptr<PushItems>>&& items,
      int timeout) {
    auto& group = *_group;
    group.lock[_tid].lock();
    TableDescriptorReader td;
    auto st = _client->context()->GetTableDescriptorReader(_storage_id, td);
    SCHECK(st.ok()) << st.ToString();
    static thread_local std::vector<PSRequest> reqs;
    reqs.clear();
    group.reduce.add(_tid);
    group.items[_tid] = std::move(items);
    _request_data.clear();
    auto op = static_cast<PushOperator*>(_op.get());
    op->generate_request_data(group.items[_tid], *td.table().runtime_info, group.data[_tid]);

    size_t sync_num = 1;
    size_t data_size = group.data[_tid]->item_size * group.data[_tid]->item_num;
    data_size /= td.table().runtime_info->nodes().size();
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
        op->generate_push_request(_request_data, *td.table().runtime_info, reqs);
    }
    send(std::move(reqs), {meta.sid, meta.hid, td.table().version, -1, meta.req_type}, timeout);
}

void PushHandler::retry(int timeout) {
    TableDescriptorReader td;
    auto st = _client->context()->GetTableDescriptorReader(_storage_id, td);
    SCHECK(st.ok()) << st.ToString();
    static thread_local std::vector<PSRequest> reqs;
    reqs.clear();
    auto op = static_cast<PushOperator*>(_op.get());
    auto& group = *_group;

    switch (_meta.req_type) {
    case RequestType::OP_PUSH_ASYNC:
    case RequestType::OP_PUSH_SYNC:
        _request_data.clear();
        for (uint32_t i = 0; i < GROUP_SIZE; ++i) {
            if (group.tids[_tid] & (1u << i)) {
                op->generate_request_data(group.items[i], *td.table().runtime_info, group.data[i]);
                _request_data.push_back(group.data[i].get());
            }
        }
        op->generate_push_request(_request_data, *td.table().runtime_info, reqs);
        break;
    case RequestType::OP_STORE:
        op->generate_store_request(*td.table().runtime_info, reqs);
        break;
    default:
        SLOG(FATAL) << "irrelavent request_type: " << int(_meta.req_type);
    }
    _meta.ctx_ver = td.table().version;
    send(std::move(reqs), _meta, timeout);
}

Status PushHandler::apply_response(PSResponse& resp, PSMessageMeta&) {
    static_cast<PushOperator*>(_op.get())->apply_response(resp);
    return Status();
}

void PushHandler::release_dealer() {
    if (_meta.req_type != RequestType::OP_STORE) {
        auto& group = *_group;
        for (uint32_t i = 0; i < GROUP_SIZE; ++i) {
            if (group.tids[_tid] & (1u << i)) {
                group.lock[i].unlock();
            }
        }
    }
    Handler::release_dealer();
}


} // namespace ps
} // namespace pico
} // namespace paradigm4
