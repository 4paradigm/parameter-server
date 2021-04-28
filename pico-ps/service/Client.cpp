#include "pico-ps/service/Client.h"

#include "pico-ps/common/DistributedAsyncReturn.h"
#include "pico-ps/common/defs.h"
#include "pico-ps/operator/Operator.h"

namespace paradigm4 {
namespace pico {
namespace ps {

void Client::initialize(MasterClient* master_client,
      RpcClient* rpc_client,
      std::shared_ptr<Context> context) {
    SCHECK(master_client);
    _master_client = master_client;
    _rpc_client = rpc_client;
    if (context.get()) {
        _ctx = context;
    } else {
        _ctx = std::make_shared<Context>();
    }
}

void Client::finalize() {
    _ctx.reset();
    _rpc_client = nullptr;
    _master_client = nullptr;
    // pico_rpc_deregister_service(PSERVER_C2S_RPC_NAME);
}

void Client::get_pserver_list(std::vector<int>& node_id) {
    RpcServiceInfo service_info;
    _rpc_client->get_rpc_service_info(service_info);
    for (auto& server_info : service_info.servers) {
        node_id.push_back(server_info.server_id);
    }
}

void Client::get_available_pserver_list(std::vector<int>& node_id) {
    _rpc_client->get_available_servers(node_id);
}

Status Client::close_pserver(int node_id, int timeout) {
    std::vector<PSRequest> reqs;
    reqs.emplace_back(node_id);
    return default_sync_rpc(std::move(reqs), {0, 0, -1, -1, RequestType::EXIT}, timeout);
}

Status Client::query_memory_info(int node_id, NodeMemoryInfo& info, int timeout) {
    MemoryDistributedAsyncReturn ret(_rpc_client);
    ret.query_memory_info(node_id, info, timeout);
    return ret.wait();
}

Status Client::load_operator_library(const std::vector<int>& node_id,
      const std::string& path,
      int timeout) {
    std::vector<PSRequest> reqs;
    for (auto i : node_id) {
        reqs.emplace_back(i);
        reqs.back() << path;
    }
    return default_sync_rpc(std::move(reqs), 
          {0, 0, -1, -1, RequestType::OP_LOAD_LIBRARY},
          timeout);
}

Status Client::create_storage(const std::string& lib_name,
      const std::string& creator_op_name,
      const Configure& conf,
      int32_t& storage_id,
      int timeout) {
    storage_id = _master_client->generate_id(PSERVER_STORAGE_ID_KEY);
    Configure server_conf(conf.node(), creator_op_name);
    SLOG(INFO) << "storage_config:\n" << server_conf.dump();
    if (!conf.has("op_config")) {
        return Status::InvalidConfig("No field \"op_config\"");
    }
    auto op = OperatorFactory::singleton().create(lib_name, creator_op_name, conf["op_config"]);
    auto status = _ctx->CreateStorage(storage_id, op, lib_name, creator_op_name, conf);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    TableDescriptorWriter td;
    status = _ctx->GetTableDescriptorWriter(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    Context::update_runtime_info(td.table(), rpc_client()->rpc_service()->global_rank());
    auto& rt = *td.table().runtime_info;
    std::vector<PSRequest> reqs;
    for (auto& p : rt.nodes()) {
        reqs.emplace_back(p.first);
        reqs.back() << lib_name << creator_op_name << server_conf.dump();
    }
    status = default_sync_rpc(std::move(reqs),
          {storage_id, 0, -1, -1, RequestType::STORAGE_CREATE},
          timeout);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    _master_client->acquire_lock(PSERVER_LOCK_NAME);
    SCHECK(_master_client->add_context(storage_id, td.table().to_json_str()));
    _master_client->release_lock(PSERVER_LOCK_NAME);
    return status;
}

Status Client::pred_create_storage(const std::string& lib_name,
      const std::string& creator_op_name,
      const Configure& conf,
      int32_t& storage_id,
      int timeout) {
    return create_storage(lib_name, creator_op_name, conf, storage_id, timeout);
}

bool Client::initialize_storage(int32_t storage_id) {
    std::string str;
    bool ret = _master_client->get_context(storage_id, str);
    if (ret) {
        SLOG(INFO) << "initializing storage: " << storage_id << " with context:\n" << str;
        auto td = std::make_unique<TableDescriptor>();
        bool updated;
        SCHECK(td->from_json_str(str, updated, true));
        SCHECK(updated);
        _ctx->SetTableDescriptor(storage_id, std::move(td));
        _ctx->UpdateRuntimeInfo(storage_id, -1);
    }
    return ret;
}

Status Client::clear_storage(int32_t storage_id, int timeout) {
    TableDescriptorReader td;
    auto status = _ctx->GetTableDescriptorReader(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    return default_sync_rpc(gen_empty_reqs(storage_id),
          {storage_id, 0, td.table().version, -1, RequestType::STORAGE_CLEAR},
          timeout);
}

Status Client::delete_storage(int32_t storage_id, int timeout) {
    auto status = default_sync_rpc(gen_empty_reqs(storage_id),
          {storage_id, 0, -1, -1, RequestType::STORAGE_DELETE},
          timeout);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        return status;
    }
    status = _ctx->DeleteStorage(storage_id);
    SCHECK(status.ok()) << status.ToString();
    _master_client->acquire_lock(PSERVER_LOCK_NAME);
    _master_client->delete_storage(storage_id);
    _master_client->release_lock(PSERVER_LOCK_NAME);
    return status;
}

Status Client::query_storage_info(int32_t storage_id,
      std::vector<StorageStatisticInfo>* storage_stat,
      int timeout) {
    TableDescriptorReader td;
    auto status = _ctx->GetTableDescriptorReader(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    auto op = static_cast<StorageOperator*>(td.table().storage_op.get());
    StorageDistributedAsyncReturn ret(op, _rpc_client);
    status = ret.query_storage_info(storage_stat,
          *td.table().runtime_info.get(),
          {storage_id, 0, -1, -1, RequestType::STORAGE_STATISTIC},
          timeout);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
    }
    return status;
}

Status Client::register_handler(const std::string& key,
      const std::string& lib_name,
      const std::string& op_name,
      const Configure& conf,
      int32_t storage_id,
      int32_t& handler_id,
      int timeout) {
    _master_client->acquire_lock(PSERVER_LOCK_NAME);
    handler_id = _master_client->generate_id(PSERVER_HANDLER_ID_KEY);
    Configure server_conf(conf.node(), op_name);
    auto reqs = gen_empty_reqs(storage_id);
    for (auto& req : reqs) {
        req << key << lib_name << op_name << server_conf.dump();
    }
    TableDescriptorWriter td;
    auto status = _ctx->GetTableDescriptorWriter(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    auto op = OperatorFactory::singleton().create(lib_name, op_name, conf);
    SCHECK(td.table().add_handler(handler_id, op, key, lib_name, op_name, conf));
    status = default_sync_rpc(std::move(reqs),
          {storage_id, handler_id, -1, -1, RequestType::HANDLER_REGISTER},
          timeout);
    if (status.ok()) {
        status = push_context_to_master_nolock(storage_id, td.table());
    }
    _master_client->release_lock(PSERVER_LOCK_NAME);
    return status;
}

Status Client::deregister_handler(int32_t storage_id, int32_t handler_id, int timeout) {
    _master_client->acquire_lock(PSERVER_LOCK_NAME);
    auto reqs = gen_empty_reqs(storage_id);
    TableDescriptorWriter td;
    auto status = _ctx->GetTableDescriptorWriter(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    SCHECK(td.table().remove_handler(handler_id));
    status = default_sync_rpc(std::move(reqs),
          {storage_id, handler_id, -1, -1, RequestType::HANDLER_DEREGISTER},
          timeout);
    if (status.ok()) {
        status = push_context_to_master_nolock(storage_id, td.table());
    }
    _master_client->release_lock(PSERVER_LOCK_NAME);
    return status;
}

void Client::set_table_uri(int32_t storage_id, const std::string& uri) {
    {
        _master_client->acquire_lock(PSERVER_LOCK_NAME);
        TableDescriptorWriter td;
        auto status = _ctx->GetTableDescriptorWriter(storage_id, td);
        SCHECK(status.ok()) << status.ToString();
        td.table().table_uri = uri;
        status = push_context_to_master_nolock(storage_id, td.table());
        _master_client->release_lock(PSERVER_LOCK_NAME);
    }
    require_update_ctx_async(storage_id);
}

void Client::require_update_ctx_async(int32_t storage_id) {
    DefaultDistributedAsyncReturn ret(_rpc_client);
    ret.async_send_one_way(gen_empty_reqs(storage_id), 
          {storage_id, -1, -1, -1, RequestType::CTX_UPDATE});
}

Status Client::handle_timeout(int32_t storage_id, int timeout) {
    SLOG(INFO) << "handling timeout..";
    MasterUniqueLock mlock(_master_client, PSERVER_LOCK_NAME);
    TableDescriptorWriter td;
    _ctx->GetTableDescriptorWriter(storage_id, td);
    auto ret = handle_timeout_nolock(storage_id, td.table(), timeout);
    return ret;
}

Status Client::handle_server_too_new_ctx(int32_t storage_id, int) {
    SLOG(INFO) << "handling server too new ctx..";
    std::string str;
    if (!_master_client->get_context(storage_id, str)) {
        return Status::InvalidID("storage ctx not found");
    } else {
        TableDescriptor tdv;
        bool updated = false;
        SCHECK(tdv.from_json_str(str, updated)) << str;

        TableDescriptorReader td;
        auto status = _ctx->GetTableDescriptorReader(storage_id, td);
        if (!status.ok()) {
            return status;
        }
        if (td.table().version == tdv.version && 
              td.table().update_version == tdv.update_version) {
            return Status();
        }
    }
    TableDescriptorWriter td;
    auto status = _ctx->GetTableDescriptorWriter(storage_id, td);
    if (!status.ok()) {
        return status;
    }
    status = pull_context_from_master_nolock(storage_id, td.table());
    return status;
}

Status Client::handle_out_of_memory(int32_t storage_id, const PSMessageMeta& meta, int timeout) {
    SLOG(INFO) << "handling out of memory..";
    auto sleepms = 100;
    Status status;
    auto begin = std::chrono::high_resolution_clock::now();
    while (true) {
        // 可能其他handler已经更新了context, 此时如果GetWriter，这个handler可能会一直卡住，导致并发度降低
        {
            TableDescriptorReader td;
            status = _ctx->GetTableDescriptorReader(storage_id, td);
            if (td.table().version != meta.ctx_ver) {
                break;
            }
        }

        {
            std::string str;
            TableDescriptorWriter td;
            status = _ctx->GetTableDescriptorWriter(storage_id, td);
            if (!status.ok()) {
                break;
            }
            if (!_master_client->get_context(storage_id, str)) {
                status = Status::InvalidID("storage ctx not found");
                break;
            }
            Context::update_handlers_form_str(td.table(), str);
            Context::update_runtime_info(td.table(), rpc_client()->rpc_service()->global_rank());
            if (td.table().version != meta.ctx_ver) {
                SLOG(INFO) << "Get storage: " << storage_id << " ctx as:\n" << str;
                break;
            }
        }
        auto dur = std::chrono::high_resolution_clock::now() - begin;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
        if (timeout != -1) {
            if (ms >= timeout) {
                break;
            } else if (sleepms > timeout - ms) {
                sleepms = timeout - ms;
            }
        }

        // 防止server writer starvation
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepms));
        // sleepms过短可能无效
        // 大约相当于扩容的时间代价增加1.1倍
        sleepms = sleepms * 1.1;
    }
    return status;
}

Status Client::push_context_to_master_nolock(int32_t storage_id,
      TableDescriptor& td,
      int32_t base_version) {
    if (base_version != -1) {
        std::string str;
        SCHECK(_master_client->get_context(storage_id, str));
        TableDescriptor old_td;
        bool updated;
        SCHECK(old_td.from_json_str(str, updated));
        if (old_td.version != base_version) {
            return Status::Error(format_string(
                  "master version: %d, local version: %d", old_td.version, base_version));
        }
    }
    _master_client->set_context(storage_id, td.to_json_str());
    return Status();
}

Status Client::pull_context_from_master_nolock(int32_t storage_id, TableDescriptor& td) {
    std::string str;
    bool ret = _master_client->get_context(storage_id, str);
    if (ret) {
        Context::update_handlers_form_str(td, str);
        Context::update_runtime_info(td, rpc_client()->rpc_service()->global_rank());
        return Status();
    } else {
        return Status::InvalidID("storage ctx not found");
    }
}

Status Client::detect_deadnode_nolock(int32_t storage_id, TableDescriptor& td, int timeout) {
    auto status = pull_context_from_master_nolock(storage_id, td);
    if (!status.ok()) {
        return status;
    }
    std::vector<int> pserver_list, available_pserver_list;
    get_pserver_list(pserver_list);
    get_available_pserver_list(available_pserver_list);
    std::unordered_set<int> live_servers(pserver_list.begin(), pserver_list.end());
    std::unordered_set<int> live_available_servers(available_pserver_list.begin(), available_pserver_list.end());
    bool need_update = false;
    bool need_push = false;
    for (auto& it : td.node_descs) {
        if (live_servers.count(it.node_id) == 0
              && it.status != NodeStatus::DEAD) {
            SLOG(WARNING) << it.node_id << " set DEAD";
            it.status = NodeStatus::DEAD;
            need_update = true;
            need_push = true;
        }
        if (live_available_servers.count(it.node_id) == 0
              && it.status == NodeStatus::RUNNING) {
            SLOG(WARNING) << it.node_id << " set UNAVAILABALE";
            it.status = NodeStatus::UNAVAILABALE;
            need_update = true;
        }
        // 目前UNAVAILABALE一旦设置就不能恢复RUNNING，除非pull context from master。
        // 而且不能restore UNAVAILABALE节点，只能kill后变成DEAD状态再restore。
        // UNAVAILABALE状态push到master是否合理？目前有可能push到master。
    }
    if (need_update) {
        td.refresh_info();
    }
    if (need_push) {
        ++td.version;
        push_context_to_master_nolock(storage_id, td);
        return require_update_ctx_nolock(storage_id, td, timeout);
    } else {
        return Status();
    }
}

std::vector<PSRequest> Client::gen_empty_reqs(int32_t storage_id) {
    TableDescriptorReader td;
    auto status = _ctx->GetTableDescriptorReader(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    std::vector<PSRequest> reqs;
    for (auto node : td.table().nodes) {
        reqs.emplace_back(node.first);
    }
    return reqs;
}

Status Client::require_shuffle_update_ctx_nolock(int32_t storage_id,
      TableDescriptor& td,
      int timeout) {
    std::vector<PSRequest> reqs;
    std::unordered_set<int> nodes;
    for (auto item : td.runtime_info->nodes()) {
        nodes.insert(item.first);
    }
    for (auto item : td.new_runtime_info->nodes()) {
        nodes.insert(item.first);
    }
    for (auto node : nodes) {
        reqs.emplace_back(node);
    }
    return default_sync_rpc(std::move(reqs),
          {storage_id, -1, -1, -1, RequestType::CTX_UPDATE},
          timeout);
}

Status Client::require_update_ctx_nolock(int32_t storage_id,
      TableDescriptor& td,
      int timeout) {
    std::vector<PSRequest> reqs;
    for (auto node : td.nodes) {
        reqs.emplace_back(node.first);
    }
    return default_sync_rpc(std::move(reqs),
          {storage_id, -1, -1, -1, RequestType::CTX_UPDATE},
          timeout);
}

Status Client::handle_timeout_nolock(int32_t storage_id, TableDescriptor& td, int timeout) {
    auto begin = std::chrono::high_resolution_clock::now();
    auto status = detect_deadnode_nolock(storage_id, td, timeout);
    auto dur = std::chrono::high_resolution_clock::now() - begin;
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
    if (timeout != -1) {
        timeout -= ms;
        if (timeout < 0) {
            timeout = 0;
        }
    }
    if (status.IsTimeout()) {
        status = handle_timeout_nolock(storage_id, td, timeout);
    } else if (status.IsServerTooNewCtx()) {
        status = pull_context_from_master_nolock(storage_id, td);
    } else if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
    }
    return status;
}

std::string Client::get_current_ctx(int32_t storage_id) {
    TableDescriptorReader td;
    auto status = _ctx->GetTableDescriptorReader(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    return td.table().to_json_str() ;
}

Status Client::set_ctx(int32_t storage_id, const std::string& ctx) {
    master_client()->acquire_lock(PSERVER_LOCK_NAME);
    TableDescriptorWriter td;
    auto status = _ctx->GetTableDescriptorWriter(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    auto ver = td.table().version;
    auto u_ver = td.table().update_version;
    status = pull_context_from_master_nolock(storage_id, td.table());
    if (!status.ok()) {
        master_client()->release_lock(PSERVER_LOCK_NAME);
        return status;
    }
    if (u_ver != td.table().update_version || ver != td.table().version) {
        status = Status::Error("updated version");
        master_client()->release_lock(PSERVER_LOCK_NAME);
        return status;
    }
    bool updated;
    td.table().from_json_str(ctx, updated, true);
    td.table().version = ver+1;
    td.table().update_version = u_ver+1;
    status = push_context_to_master_nolock(storage_id, td.table());
    SCHECK(status.ok()) << status.ToString();
    master_client()->release_lock(PSERVER_LOCK_NAME);
    Context::update_runtime_info(td.table(), rpc_client()->rpc_service()->global_rank());
    status = require_update_ctx_nolock(storage_id, td.table());
    return status;
}

Status Client::default_sync_rpc(std::vector<PSRequest>&& reqs, const PSMessageMeta& meta, int timeout) {
    DefaultDistributedAsyncReturn ret(_rpc_client);
    return ret.default_sync_rpc(std::move(reqs), meta, timeout);
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
