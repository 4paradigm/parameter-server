#include "pico-ps/common/DistributedAsyncReturn.h"

namespace paradigm4 {
namespace pico {
namespace ps {

DistributedAsyncReturn::DistributedAsyncReturn(RpcClient* rpc_client) {
    _rpc_client = rpc_client;
    if (_rpc_client != nullptr) {
        _dealer = _rpc_client->create_dealer();
    }
}

Status DistributedAsyncReturn::wait() {
    SCHECK(_busy);
    _busy = false;
    if (!_send_status.ok()) {
        reset_dealer();
        SLOG(WARNING) << "wait get status: " << _send_status.ToString();
        return _send_status;
    }
    for (size_t i = 0; i < _req_num; ++i) {
        RpcResponse response;
        bool ret = _dealer->recv_response(response, _timeout);
        if (ret && response.error_code() != core::ENOSUCHSERVER) {
            PSMessageMeta meta;
            PSResponse resp(std::move(response));
            auto status = check_resp_validity(resp, meta);
            if (!status.ok()) {
                reset_dealer();
                SLOG(WARNING) << "wait get status: " << status.ToString();
                return status;
            }
            status = check_rpc_error(resp);
            if (!status.ok()) {
                reset_dealer();
                SLOG(WARNING) << "wait get status: " << status.ToString();
                return status;
            }
            status = apply_response(resp, meta);
            if (!status.ok()) {
                reset_dealer();
                SLOG(WARNING) << "wait get status: " << status.ToString();
                return status;
            }
        } else {
            reset_dealer();
            auto status = Status::Timeout("");
            if (ret) {
                status = Status::Timeout("server may crash.");
            }
            SLOG(WARNING) << "wait get status: " << status.ToString();
            return status;
        }
    }
    return Status();
}


void DistributedAsyncReturn::send(std::vector<PSRequest>&& reqs, const PSMessageMeta& meta, int timeout) {
    SCHECK(!_busy);
    SCHECK(_dealer);
    _busy = true;
    _meta = meta;
    _timeout = timeout;
    _req_num = reqs.size();
    for (auto& req : reqs) {
        req << _meta;
        _dealer->send_request(std::move(req.rpc_request()));
    }
    reqs.clear();
}

void DistributedAsyncReturn::send_one_way(std::vector<PSRequest>&& reqs, const PSMessageMeta& meta) {
    SCHECK(!_busy);
    SCHECK(_dealer);
    _busy = false;
    _meta = meta;
    _timeout = -1;
    _req_num = 0;
    for (auto& req : reqs) {
        req << _meta;
        _dealer->send_request_one_way(std::move(req.rpc_request()));
    }
    reqs.clear();
}

Status DistributedAsyncReturn::check_resp_validity(PSResponse& response, PSMessageMeta& meta) {
    response.archive().read_back(&meta, sizeof(meta));
    if (_meta.sid != meta.sid) {
        SLOG(WARNING) << "got wrong storage id in response msg, expected = " << _meta.sid
                      << ", but got = " << meta.sid;
        return Status::Error("invalid meta");
    }
    if (_meta.hid != meta.hid) {
        SLOG(WARNING) << "got wrong handler id in response msg, expected = " << _meta.hid
                      << ", but got = " << meta.hid;
        return Status::Error("invalid meta");
    }
    if (_meta.req_type != meta.req_type) {
        SLOG(WARNING) << "got wrong request_type in response msg, expected = "
                      << int(_meta.req_type) << ", but got = " << int(meta.req_type);
        return Status::Error("invalid meta");
    }
    return Status();
}

Status DistributedAsyncReturn::check_rpc_error(PSResponse& resp) {
    auto err = resp.rpc_response().error_code();
    Status status;
    if (err == RpcErrorCodeType::SUCC) {
        return status;
    }
    resp >> status;
    return status;
}

void DistributedAsyncReturn::reset_dealer() {
    _dealer = _rpc_client->create_dealer();
}

Status DirectPushDistributedAsyncReturn::push(core::vector<std::unique_ptr<PushItems>>&& push_items, 
      RuntimeInfo& rt,
      const PSMessageMeta& meta,
      int timeout) {
    std::vector<PSRequest> reqs;
    _request_data.clear();
    _push_items = std::move(push_items);
    _op->generate_request_data(_push_items, rt, _data);
    _request_data.push_back(_data.get());
    _op->generate_push_request(_request_data, rt, reqs);
    send(std::move(reqs), meta, timeout);
    return wait();
}

Status DirectPushDistributedAsyncReturn::apply_response(PSResponse& resp, PSMessageMeta&) {
    _op->apply_response(resp);
    return Status();
}

Status CoordinateRestoreDistributedAsyncReturn::RestoreBatch(CoordinatedRestoreRequestItem *req_item,
                                                             CoordinatedRestoreResponseItem *resp_item,
                                                             int timeout) {
    std::vector<PSRequest> reqs;
    _op->generate_coordinated_restore_request(req_item, reqs);
    send(std::move(reqs), {req_item->storage_id, 0, -1, -1, RequestType::OP_COORDINATED_RESTORE_ITERATE}, timeout);
    _resp_item = resp_item;
    return wait();
}

Status CoordinateRestoreDistributedAsyncReturn::apply_response(PSResponse& resp, PSMessageMeta&) {
    _op->apply_coordinated_restore_response(resp, _st, _resp_item);
    return Status();
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
