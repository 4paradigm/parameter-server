#include "pico-ps/handler/Handler.h"

#include "pico-ps/common/core.h"

#include "pico-ps/common/defs.h"

namespace paradigm4 {
namespace pico {
namespace ps {

Handler::Handler(
        int32_t storage_id,
        int32_t handler_id,
        std::shared_ptr<Operator> op,
        Client* client)
    : DistributedAsyncReturn(client == nullptr ? nullptr : client->rpc_client()),
      _storage_id(storage_id), _handler_id(handler_id), _op(op), _client(client) {
    if (_client != nullptr) {
        _dealer = _rpc_client->create_dealer();
    }
}

Status Handler::wait_no_retry() {
    auto status = wait_no_release();
    release_dealer();
    return status;
}

static inline void reduce_time(int dur, int& timeout) {
    if (timeout != -1) {
        timeout -= dur;
        if (timeout < 0) {
            timeout = 0;
        }
    }
}

Status Handler::wait() {
    SCHECK(_busy && _client != nullptr);
    auto begin = std::chrono::high_resolution_clock::now();
    auto status = wait_no_release();
    int timeout = _timeout;
    auto dur = std::chrono::high_resolution_clock::now() - begin;
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
    reduce_time(ms, timeout);
    bool need_retry = false;
    int retry_cnt = 0;
    do {
        need_retry = false;
        if (status.IsNoReplica() || status.IsTimeout()) { 
            // 发送给某个server失败，大概率是server crash了，
            // 这时候认为server更新不及时，client重发
            if (retry_cnt > 0 && status.IsNoReplica()) {
                break;
            }
            auto begin = std::chrono::high_resolution_clock::now();
            _client->handle_timeout(_storage_id, timeout);
            auto dur = std::chrono::high_resolution_clock::now() - begin;
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
            reduce_time(ms, timeout);
            need_retry = true;
        } else if (status.IsServerTooNewCtx()) {
            auto begin = std::chrono::high_resolution_clock::now();
            _client->handle_server_too_new_ctx(_storage_id, timeout);
            auto dur = std::chrono::high_resolution_clock::now() - begin;
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
            reduce_time(ms, timeout);
            need_retry = true;
        } else if (status.IsOOM()) {
            auto begin = std::chrono::high_resolution_clock::now();
            _client->handle_out_of_memory(_storage_id, _meta, timeout);
            auto dur = std::chrono::high_resolution_clock::now() - begin;
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
            reduce_time(ms, timeout);
            need_retry = true;
        }
        if (need_retry && (timeout > 0 || timeout == -1)) {
            auto begin = std::chrono::high_resolution_clock::now();
            retry(timeout);
            ++retry_cnt;
            status = wait_no_release();
            auto dur = std::chrono::high_resolution_clock::now() - begin;
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
            reduce_time(ms, timeout);
        }
    } while (need_retry && (timeout > 0 || timeout == -1));
    release_dealer();
    return status;
}


Status Handler::wait_no_release() {
    return DistributedAsyncReturn::wait();
}

void Handler::retry(int) {
    SLOG(FATAL) << "not implement";
}

Status Handler::apply_response(PSResponse&, PSMessageMeta&) {
    return Status();
}


void Handler::release_dealer() {}

} // namespace ps
} // namespace pico
} // namespace paradigm4
