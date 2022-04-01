#include <algorithm>
#include <pico-ps/common/Status.h>
#include <pico-ps/common/message.h>
#include "pico-ps/handler/UDFHandler.h"
#include "pico-ps/operator/UDFOperator.h"
#include "pico-ps/handler/Handler.h"

namespace paradigm4 {
namespace pico {
namespace ps {

void UDFHandler::call(void* param, int timeout) {
    auto op = static_cast<UDFOperatorBase*>(_op.get());
    if (!_state) {
        _state = op->create_state();
    }
    
    TableDescriptorReader td;
    Status status = _client->context()->GetTableDescriptorReader(_storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    _reqs.clear();
    _send_status = op->generate_request(param, *td.table().runtime_info.get(), _state.get(), _reqs);
    send(std::move(_reqs), {_storage_id, _handler_id, td.table().version, -1, RequestType::OP_UDF}, timeout);
}

ps::Status UDFHandler::wait() {
    ps::Status status;
    if (_param) {
        status = Handler::wait();
    } else {
        status = Handler::wait_no_retry();
    }
    _param = nullptr;
    _result = nullptr;
    return status;
};

void UDFHandler::retry(int timeout) {
    call(_param, timeout);  
}

void UDFHandler::set_wait_retry(void* param) {
    _param = param;
}

void UDFHandler::set_wait_result(void* result) {
    _result = result;
}

Status UDFHandler::apply_response(PSResponse& resp, PSMessageMeta&) {
    return static_cast<UDFOperatorBase*>(_op.get())->apply_response(resp, _state.get(), _result);
}

void UDFHandler::release_dealer() {
    _result = nullptr;
}


void UDFHandler::generate_request(void* param, UDFHandler::Request& req)const {
    auto op = static_cast<UDFOperatorBase*>(_op.get());
    if (!req._state) {
        req._state = op->create_state();
    }
    TableDescriptorReader td;
    Status status = _client->context()->GetTableDescriptorReader(_storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    req._reqs.clear();
    req._status = op->generate_request(param, *td.table().runtime_info.get(), _state.get(), req._reqs);
    req._meta = {_storage_id, _handler_id, td.table().version, -1, RequestType::OP_UDF};
}

void UDFHandler::send_request(UDFHandler::Request&& req, int timeout) {
    TableDescriptorReader td;
    Status status = _client->context()->GetTableDescriptorReader(_storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    ps::PSMessageMeta meta = {_storage_id, _handler_id, td.table().version, -1, RequestType::OP_UDF};
    if (req._meta.ctx_ver < td.table().version) {
        _send_status = ps::Status::ServerTooOldCtx("async generate request client check fail.");
    } else if (req._meta.ctx_ver > td.table().version) {
        _send_status = ps::Status::ServerTooNewCtx("async generate request client check fail.");
    }
    _state = std::move(req._state);
    send(std::move(req._reqs), meta, timeout);
}
    
} // namespace ps
} // namespace pico
} // namespace paradigm4
