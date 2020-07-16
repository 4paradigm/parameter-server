#include "pico-ps/handler/UpdateContextHandler.h"

#include "pico-ps/common/defs.h"

namespace paradigm4 {
namespace pico {
namespace ps {

//仅支持单线程Client调用
UpdateContextHandler::UpdateContextHandler(int32_t storage_id,
      int32_t handler_id,
      std::shared_ptr<Operator>& op,
      Client* client)
    : Handler(storage_id, handler_id, op, client) {}

UpdateContextHandler::~UpdateContextHandler() {}

Status UpdateContextHandler::purge(int timeout) {
    _client->master_client()->acquire_lock(PSERVER_LOCK_NAME);
    TableDescriptorWriter td;
    auto status = _client->context()->GetTableDescriptorWriter(_storage_id, td);
    if (!status) {
        _client->master_client()->release_lock(PSERVER_LOCK_NAME);
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "table descriptor locked";
    auto ver = td.table().version;
    auto u_ver = td.table().update_version;
    status = _client->pull_context_from_master_nolock(_storage_id, td.table());
    if (!status.ok()) {
        _client->master_client()->release_lock(PSERVER_LOCK_NAME);
        return status;
    }
    if (u_ver != td.table().update_version || ver != td.table().version) {
        status = Status::Error("updated version");
        _client->master_client()->release_lock(PSERVER_LOCK_NAME);
        return status;
    }
    td.table().drop_update();
    td.table().version += 1;
    //in PSERVER_LOCK, it should be success
    SCHECK(_client->push_context_to_master_nolock(_storage_id, td.table()).ok());
    _client->master_client()->release_lock(PSERVER_LOCK_NAME);
    status = _client->require_shuffle_update_ctx_nolock(_storage_id, td.table(), timeout);
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "prepare purge";

    this->purge(td, timeout);
    status = this->wait();
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "purged";
    return status;
}

Status UpdateContextHandler::update_context(const Configure& config, int timeout) {

 ////////////////////////////////////////////////
    SLOG(INFO) << "begin update context";
    TableDescriptorWriter td;
    auto status = _client->context()->GetTableDescriptorWriter(_storage_id, td);
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "table descriptor locked";
    
    status = prepare_update_ctx(td, config);
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }

    status = _client->require_shuffle_update_ctx_nolock(_storage_id, td.table(), timeout);
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "prepare update ctx";
    
////////////////////////////////////////////////

    this->create_shards(td, timeout);
    status = this->wait();
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "created shards";
    
    this->shuffle(td, timeout);
    status = this->wait();
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "shuffled";

    this->store(td, timeout);
    status = this->wait();
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "stored";

////////////////////////////////////////////////

    status = commit_new_ctx_to_master(td);
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "commited new ctx";

    status = _client->require_shuffle_update_ctx_nolock(_storage_id, td.table(), timeout);
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "updated read only ctx";
////////////////////////////////////////////////

    this->purge(td, timeout);
    status = this->wait();
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "purged";

////////////////////////////////////////////////

    status = end_update_context(td);
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "end update context";
    status = _client->require_shuffle_update_ctx_nolock(_storage_id, td.table(), timeout);
    if (!status) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    SLOG(INFO) << "updated ctx";
    return status;
}

Status UpdateContextHandler::prepare_update_ctx(TableDescriptorWriter& td, const Configure& config) {
    _client->master_client()->acquire_lock(PSERVER_LOCK_NAME);
    auto status = _client->pull_context_from_master_nolock(_storage_id, td.table());
    if (!status.ok()) {
        _client->master_client()->release_lock(PSERVER_LOCK_NAME);
        return status;
    }
    auto& storage_status = td.table().storage_status;
    status = td.table().create_update_info(config);
    if (status.ok()) {
        Context::update_runtime_info(td.table(), _client->rpc_client()->rpc_service()->global_rank());
        storage_status = StorageStatus::RESHARD;
        //in PSERVER_LOCK, it should be success
        SCHECK(_client->push_context_to_master_nolock(_storage_id, td.table()).ok());
    }
    _client->master_client()->release_lock(PSERVER_LOCK_NAME);
    SLOG(INFO) << "new config for storage_id: " << _storage_id << " is:\n" << config.dump();
    return status;
}

void UpdateContextHandler::create_shards(TableDescriptorWriter& td, int timeout) {
    std::vector<PSRequest> reqs;
    static_cast<BaseShardUpdateContextOperator*>(_op.get())->generate_request(
          *td.table().runtime_info, *td.table().new_runtime_info, reqs);
    send(std::move(reqs), {
          _storage_id,
          _handler_id,
          td.table().version,
          td.table().update_version,
          RequestType::NEW_CTX_CREATE_SHARD,
    }, timeout);
}

void UpdateContextHandler::shuffle(TableDescriptorWriter& td, int timeout) {
    std::vector<PSRequest> reqs;
    static_cast<BaseShardUpdateContextOperator*>(_op.get())->generate_shuffle_request(
          *td.table().runtime_info, reqs);
    send(std::move(reqs), {
          _storage_id,
          _handler_id,
          td.table().version,
          td.table().update_version,
          RequestType::NEW_CTX_SHUFFLE,
    }, timeout);
}


void UpdateContextHandler::store(TableDescriptorWriter& td, int timeout) {
    std::vector<PSRequest> reqs;
    static_cast<BaseShardUpdateContextOperator*>(_op.get())->push_operator()
          ->generate_store_request(*td.table().new_runtime_info, reqs);
    send(std::move(reqs), {
          _storage_id,
          _handler_id,
          td.table().version,
          td.table().update_version,
          RequestType::NEW_CTX_STORE,
    }, timeout);
}

Status UpdateContextHandler::commit_new_ctx_to_master(TableDescriptorWriter& td) {
    _client->master_client()->acquire_lock(PSERVER_LOCK_NAME);
    auto ver = td.table().version;
    auto u_ver = td.table().update_version;
    auto status = _client->pull_context_from_master_nolock(_storage_id, td.table());
    if (!status.ok()) {
        _client->master_client()->release_lock(PSERVER_LOCK_NAME);
        return status;
    }
    if (u_ver != td.table().update_version || ver != td.table().version) {
        status = Status::Error("updated version");
        _client->master_client()->release_lock(PSERVER_LOCK_NAME);
        return status;
    }
    td.table().apply_update();
    td.table().version += 1;
    td.table().update_version += 1;
    //in PSERVER_LOCK, it should be success
    SCHECK(_client->push_context_to_master_nolock(_storage_id, td.table()).ok());
    _client->master_client()->release_lock(PSERVER_LOCK_NAME);
    return status;
}

void UpdateContextHandler::purge(TableDescriptorWriter& td, int timeout) {
    std::vector<PSRequest> reqs;
    static_cast<BaseShardUpdateContextOperator*>(_op.get())->generate_purge_request(
          *td.table().new_runtime_info, reqs);
    send(std::move(reqs), {
          _storage_id,
          _handler_id,
          td.table().version,
          td.table().update_version,
          RequestType::NEW_CTX_PURGE,
    }, timeout);
}

Status UpdateContextHandler::end_update_context(TableDescriptorWriter& td) {
    _client->master_client()->acquire_lock(PSERVER_LOCK_NAME);
    auto ver = td.table().version;
    auto u_ver = td.table().update_version;
    auto status = _client->pull_context_from_master_nolock(_storage_id, td.table());
    if (!status.ok()) {
        _client->master_client()->release_lock(PSERVER_LOCK_NAME);
        return status;
    }
    if (u_ver != td.table().update_version || ver != td.table().version) {
        status = Status::Error("updated version");
        _client->master_client()->release_lock(PSERVER_LOCK_NAME);
        return status;
    }
    td.table().storage_status = StorageStatus::NORMAL;
    td.table().version += 1;
    //in PSERVER_LOCK, it should be success
    SCHECK(_client->push_context_to_master_nolock(_storage_id, td.table()).ok());
    _client->master_client()->release_lock(PSERVER_LOCK_NAME);
    return status;
}


Status UpdateContextHandler::wait() {
    return wait_no_retry();
}

Status UpdateContextHandler::apply_response(PSResponse& resp, PSMessageMeta& meta) {
    Status status;
    switch (meta.req_type) {
    case RequestType::NEW_CTX_CREATE_SHARD:
        static_cast<BaseShardUpdateContextOperator*>(_op.get())->apply_response(resp);
        break;
    case RequestType::NEW_CTX_SHUFFLE:
        static_cast<BaseShardUpdateContextOperator*>(_op.get())->apply_response(resp);
        break;
    case RequestType::NEW_CTX_STORE:
        static_cast<BaseShardUpdateContextOperator*>(_op.get())->push_operator()->apply_response(resp);
        break;    
    case RequestType::NEW_CTX_PURGE:
        static_cast<BaseShardUpdateContextOperator*>(_op.get())->apply_response(resp);
        break;
    default:
        SLOG(FATAL) << "irrelavent request_type: " << int(meta.req_type);
    }
    return status;
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
