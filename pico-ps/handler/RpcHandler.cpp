#include <algorithm>
#include "pico-ps/handler/RpcHandler.h"
#include "pico-ps/operator/RpcOperator.h"

namespace paradigm4 {
namespace pico {
namespace ps {


void RpcHandler::call(void* param, int timeout) {
    auto op = static_cast<RpcOperatorBase*>(_op.get());
    if (!_param) {
        _param = op->create_param();
    }
    if (!_state) {
        _state = op->create_state();
    }
    if (_param.get() != param) {
        op->copy_param(param, _param.get());
    }
    
    TableDescriptorReader td;
    Status status = _client->context()->GetTableDescriptorReader(_storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    _reqs.clear();
    _send_status = op->generate_request(param, *td.table().runtime_info.get(), _state.get(), _reqs);
    send(std::move(_reqs), {_storage_id, _handler_id, td.table().version, -1, RequestType::OP_RPC}, timeout);
}

void RpcHandler::retry(int timeout) {
    call(_param.get(), timeout);  
}

Status RpcHandler::apply_response(PSResponse& resp, PSMessageMeta&) {
    return static_cast<RpcOperatorBase*>(_op.get())->apply_response(resp, _state.get());
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
