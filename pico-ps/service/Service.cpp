#include "pico-ps/service/Server.h"

#include "pico-ps/common/core.h"

#include "pico-ps/common/DistributedAsyncReturn.h"
#include "pico-ps/common/defs.h"
#include "pico-ps/operator/operators.h"
#include "pico-ps/operator/RpcOperator.h"
#include "pico-ps/handler/SyncHandler.h"
#include "pico-ps/model/Model.h"
#include "pico-ps/service/coordinated_restore/CoordinatedRestoreController.h"
#include <pico-core/observability/metrics/DurationObserver.h>
#include <pico-core/observability/metrics/Metrics.h>

#ifdef USE_DCPMM
#include "pico-ps/storage/DCPmemory.h"
#endif // USE_DCPMM

namespace paradigm4 {
namespace pico {
namespace ps {

std::string PS_REQUESTS_TOTAL = "ps_requests_total";
std::string PS_REQUESTS_TOTAL_DESC = "total number of ps requests";
std::string PS_REQUESTS_SIZE_BYTES = "ps_requests_size_bytes";
std::string PS_REQUESTS_SIZE_BYTES_DESC = "request size in byte of ps requests";
std::string PS_RESPONSES_SIZE_BYTES = "ps_response_size_bytes";
std::string PS_RESPONSES_SIZE_BYTES_DESC = "response size in byte of ps responses";
std::string PS_ERRORS_TOTAL = "ps_errors_total";
std::string PS_ERRORS_TOTAL_DESC = "total number of ps errors";
std::string PS_REQUEST_DURATION_MS_BUCKET = "ps_request_duration_ms_bucket";
std::string PS_REQUEST_DURATION_MS_BUCKET_DESC = "ps request processing durations histogram in millisecond";
std::vector<double> METRICS_LATENCY_BOUNDARY = Metrics::create_general_duration_bucket();

Server::Server(const ServerConfig& lemon,
      MasterClient* master_client,
      RpcService* rpc_service,
      const std::string& hadoop_bin)
    : _c2s_thread_num(lemon.server_c2s_thread_num),
      _s2s_thread_num(lemon.server_s2s_thread_num), 
      _server_load_thread_num(lemon.server_load_thread_num),
      _server_restore_wait_timeout_second(lemon.server_restore_wait_timeout_second),
      _server_dcpmm_replace_dead_node_wait_time_second(lemon.server_dcpmm_replace_dead_node_wait_time_second),
      _load_block_size(lemon.server_load_block_size),
      _io_tg(lemon.server_load_thread_num),
      _report_interval(lemon.report_interval),
      _master_client(master_client), 
      _rpc_service(rpc_service),
      _hadoop_bin(hadoop_bin) {
    if (_server_restore_wait_timeout_second == -1) {
        _server_restore_wait_timeout_second = 2 * master_client->session_timeout_ms() / 1000;
        SLOG(INFO) << "Server restore wait time out is set as -1."
                   << " Use 2x session timeout of MasterClient as the wait time."
                   << " session timeout in second: " << master_client->session_timeout_ms() / 1000
                   << " server restore wait timeout in second: " << _server_restore_wait_timeout_second;
    }
    _c2s_thread.resize(_c2s_thread_num);
    _s2s_thread.resize(_s2s_thread_num);
    _s2s_server = _rpc_service->create_server(PSERVER_S2S_RPC_NAME);
    _node_id = _s2s_server->id();
    _s2s_client = _rpc_service->create_client(PSERVER_S2S_RPC_NAME);
    _c2s_server = _rpc_service->create_server(PSERVER_C2S_RPC_NAME, _node_id);
    _c2s_client = _rpc_service->create_client(PSERVER_C2S_RPC_NAME);
    SLOG(INFO) << "my node id is : " << _node_id;
}

void Server::set_sync_env(MasterClient* sync_master_client, RpcService* sync_rpc) {
    SCHECK(sync_master_client);
    SCHECK(sync_rpc);
    _sync_client = std::make_unique<Client>();
    _sync_rpc_client = sync_rpc->create_client(PSERVER_C2S_RPC_NAME);
    _sync_client->initialize(sync_master_client, _sync_rpc_client.get());
}

void Server::initialize() {
    for (size_t i = 0; i < _s2s_thread_num; ++i) {
        _s2s_thread[i] = std::thread(&Server::process_s2s_request, this);
    }
    for (size_t i = 0; i < _c2s_thread_num; ++i) {
        _c2s_thread[i] = std::thread(&Server::process_c2s_request, this);
    }
    if (_sync_client.get()) {
        _sync_thread = std::thread(&Server::push_sync_tables, this);
    }
    if (_report_interval != -1) {
        _self_monitor_thread = std::thread(&Server::self_monitor, this);
    }
    SLOG(INFO) << "PServer intialize finished, start to serve with node id [" << _node_id << "]";
}

void Server::finalize() {
    for (auto& th : _c2s_thread) {
        th.join();
    }

    _s2s_server->terminate();
    for (auto& th : _s2s_thread) {
        th.join();
    }

    _io_tg.terminate();
    _io_tg.join();

    //sync thread join
    if (_sync_client.get()) {
        {
            std::unique_lock<std::mutex> lock(_sync_mtx);
            _sync_exit = true;
            _sync_cond.notify_all();
        }
        if (_sync_thread.joinable())
            _sync_thread.join();
        _sync_tables.clear();
        _sync_client->finalize();
        _sync_client.reset();
        _sync_rpc_client.reset();
    }

    //self monitor thread join
    if (_self_monitor_thread.joinable()) {
        std::unique_lock<std::mutex> lock(_sync_mtx);
        _self_monitor_stop = true;
        _self_monitor_cond.notify_all();
        _self_monitor_thread.join();
    }
    _s2s_client.reset();
    _c2s_client.reset();
    _s2s_server.reset();
    _c2s_server.reset();
}

void Server::exit() {
    Client client;
    client.initialize(_master_client, _c2s_client.get());
    client.close_pserver(_c2s_server->id());
    client.finalize();
}

//void Server::initialize_storages() {
    //auto st = _master_client->get_storage_list();
     ////TODO
    //for (auto id : st) {
        //bool need_restore = initialize_storage(id).second;
        //if (need_restore) {
            //restore_storage(id);
        //}
    //}
//}

int32_t Server::_find_dead_node_and_replace(int32_t possible_dead_node_id, std::vector<int32_t>& to_restore_tables) {
    int found_dead_node_id = -1;
    _master_client->acquire_lock(PSERVER_LOCK_NAME);
    auto storage_list = _master_client->get_storage_list();
    RpcServiceInfo service_info;
    _master_client->get_rpc_service_info(
          _rpc_service->rpc_service_api(), PSERVER_C2S_RPC_NAME, service_info);
    SLOG(INFO) << "get service info : " << service_info;
    std::unordered_set<int32_t> live_servers;
    for (auto& server_info : service_info.servers) {
        live_servers.emplace(server_info.server_id);
    }

    for (auto storage_id : storage_list) {
        std::string str;
        bool ret = _master_client->get_context(storage_id, str);
        if (!ret) {
            SLOG(WARNING) << "get context error";
            continue;
        }
        auto ptd = std::make_unique<TableDescriptor>();
        auto& td = *ptd;
        bool updated;
        if (!td.from_json_str(str, updated, true)) {
            SLOG(WARNING) << "table json error:" << str;
            continue;
        }
        if (td.update_node_status(live_servers)) {
            found_dead_node_id = td.try_to_replace_one_dead_node(_node_id, possible_dead_node_id);
            _master_client->set_context(storage_id, td.to_json_str());
        }
        if (found_dead_node_id != -1) {
            SLOG(INFO) << "Dead node id : " << found_dead_node_id << " detected";
            _ctx.SetTableDescriptor(storage_id, std::move(ptd));
            to_restore_tables.emplace_back(storage_id);
        } else {
            SLOG(INFO) << "No dead node indicated by master";
        }
    }
    _master_client->release_lock(PSERVER_LOCK_NAME);
    return found_dead_node_id;
}

bool Server::test_connections() {
    RpcServiceInfo service_info;
    _master_client->get_rpc_service_info(
          _rpc_service->rpc_service_api(), PSERVER_C2S_RPC_NAME, service_info);
    SLOG(INFO) << "get service info : " << service_info;
    std::vector<int> server_node_ids;
    for (const auto& server_info : service_info.servers) {
        if (server_info.server_id != _node_id) {
            server_node_ids.emplace_back(server_info.server_id);
        }
    }
    HealthCheckDistributedAsyncReturn async_ret(_s2s_client.get());
    auto status = async_ret.health_check(server_node_ids, _server_restore_wait_timeout_second * 1000);
    if (!status.ok()) {
        SLOG(WARNING) << "Server health check failed. " << status.ToString();
        return false;
    }
    return true;
}

int32_t Server::replace_dead_node_in_loop(int looping_time_seconds, int32_t possible_dead_node_id, std::vector<int32_t>& to_restore_tables) {
    int round_wait_seconds = 5;

    auto start = std::chrono::system_clock::now();

    int remaining_seconds = looping_time_seconds;

    while (remaining_seconds > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(std::max(round_wait_seconds, remaining_seconds)));

        int32_t found_dead_node_id = _find_dead_node_and_replace(possible_dead_node_id, to_restore_tables);
        if (found_dead_node_id != -1) {
            return found_dead_node_id;
        }
        std::chrono::duration<double> elapsed_seconds = std::chrono::system_clock::now() - start;

        remaining_seconds = looping_time_seconds - std::ceil(elapsed_seconds.count());
        if (looping_time_seconds > 0) {
            SLOG(INFO) << "Checking loop remains " << looping_time_seconds << " seconds";
        }
    }
    return -1;
}

void Server::restore_storages(bool use_pmem_restore, int32_t possible_dead_node_id) {
    std::vector<int32_t> to_restore_tables;
    // NOTES(wj): 当 use_pmem_restore == false 时, possible_dead_node_id 一定是-1.
    auto found_dead_node_id = _find_dead_node_and_replace(possible_dead_node_id, to_restore_tables);
    if (found_dead_node_id == -1) {
        bool test_connection_ok = test_connections();
        if (!test_connection_ok) {
            SLOG(INFO) << "No dead node indicated by master, but pserver health check failed. "
                       << " Check in loop for " << _server_restore_wait_timeout_second << " second "
                       << " to wait server list to refresh.";
            replace_dead_node_in_loop(_server_restore_wait_timeout_second, possible_dead_node_id, to_restore_tables);
        }
        if (use_pmem_restore && to_restore_tables.empty()) {
            if (possible_dead_node_id != -1) {
                SLOG(WARNING) << "Node id " << possible_dead_node_id << " is probably not a dead node. Wait for " << _server_dcpmm_replace_dead_node_wait_time_second
                    << " seconds to use network-based restore method.";
                // 如果 dcpmm 中保存的 node_rank 不是 dead node 的话，应该替换一个其他的 dead_node 尝试以老的方式恢复。
                std::this_thread::sleep_for(std::chrono::seconds(_server_dcpmm_replace_dead_node_wait_time_second));
            }
            SLOG(INFO) << "Try to find a new dead node to replace and restore, instead of previous possible dead node id: " << possible_dead_node_id;
            found_dead_node_id = _find_dead_node_and_replace(-1, to_restore_tables);
        }
    }
    std::unordered_map<int32_t, std::unordered_set<int32_t>> retained_storage_shard;
    for (auto& storage_id : to_restore_tables) {
        TableDescriptorWriter wtd;
        auto status = _ctx.GetTableDescriptorWriter(storage_id, wtd);
        SCHECK(status.ok()) << status.ToString();
        auto& td = wtd.table();
        td.refresh_info();
        Context::initialize_storage_op(td);
        Context::update_handlers(td);
        Context::update_runtime_info(td, _node_id);
        SLOG(INFO) << "Trying to restore storage id: " << storage_id << " dead node id " << found_dead_node_id;
        td.storage = static_cast<StorageOperator*>(td.storage_op.get())
                         ->create_storage(*td.runtime_info, storage_id);
        bool check_pass = true;

        if (use_pmem_restore) {
            // 判断本地 dcpmm 与 zk 的 table 信息版本是否一致。如果不一致，清空本地 dcpmm 存储，以老的方式恢复。
            check_pass = td.storage->sanity_check(storage_id, td);
            if (!check_pass) {
                LOG(INFO) << "Version uuid check failed, local: " << td.storage->load_version_uuid() << " vs. master: " << td.version_uuid;
            }
        }
        if (td.storage->need_remote_restore() || !check_pass) {
#ifdef USE_DCPMM
            if (use_pmem_restore) {
                SLOG(WARNING) << "Storage id " << storage_id << " fallback to restore using network instead of local dcpmm.";
                DCPmemory::singleton().remove_storage(storage_id);
                td.storage.reset();
                td.storage = static_cast<StorageOperator*>(td.storage_op.get())
                                 ->create_storage(*td.runtime_info, storage_id);
            }
#endif  // USE_DCPMM
            restore_storage_by_network(storage_id, td);
            td.storage->store_version_uuid(td.version_uuid);
        }
        SLOG(INFO) << "restored storage: " << storage_id << " shards: " << td.runtime_info->local_shards_str();
        retained_storage_shard.emplace(storage_id, td.runtime_info->local_shards());
    }
#ifdef USE_DCPMM
    // 把本地上的 dcpmm 不必要的 storage 删除。
    if (use_pmem_restore) {
        DCPmemory::singleton().remove_storage_exclude(
                std::unordered_set<int32_t>(to_restore_tables.begin(), to_restore_tables.end()));
    }
#endif  // USE_DCPMM

    _master_client->acquire_lock(PSERVER_LOCK_NAME);
    for (auto& storage_id : to_restore_tables) {
        SCHECK(set_node_status_to_running(storage_id));
        SLOG(INFO) << "restored storage: " << storage_id << " node: " << _node_id << " running";
    }
    _master_client->release_lock(PSERVER_LOCK_NAME);
    SLOG(INFO) << "restore storage finished.";
}

void Server::restore_storage_by_network(int32_t storage_id, TableDescriptor& td) {
    bool restored = restore_storage_coordinated(storage_id, td);
    if (!restored) {
        SLOG(INFO) << "Failed to restore from peer. Restore from fs instead, storage id: " << storage_id;
        static_cast<StorageOperator*>(td.storage_op.get())->clear_storage(td.storage.get());
        restore_storage_from_fs(td);
    }
}

bool Server::restore_storage_coordinated(int32_t storage_id, TableDescriptor &td) {
    CoordinatedRestoreController ctl(storage_id, _load_block_size, _server_load_thread_num, _rpc_service, &td);
    ctl.start();
    ctl.join();
    return ctl.completed();
}

void Server::restore_storage_from_fs(TableDescriptor& td) {
    auto op = static_cast<RestoreOperator*>(
          static_cast<StorageOperator*>(td.storage_op.get())->restore_op());
    URIConfig uri(td.table_uri);
    std::map<std::string, std::pair<std::string, int>> param_map
          = {{core::URI_HADOOP_BIN, {_hadoop_bin, URILVL::EXTCFG}}};
    uri.replace_param(param_map);
    std::vector<std::string> files = FileSystem::get_file_list(uri.name(), _hadoop_bin);
    std::vector<std::vector<std::string>> files_per_worker(_server_load_thread_num);
    for (size_t i = 0; i < files.size(); ++i) {
        files_per_worker[i % _server_load_thread_num].emplace_back(files[i]);
    }
    std::vector<std::thread> workers;
    for (size_t i = 0; i < _server_load_thread_num; ++i) {
        workers.emplace_back(&Server::restore_storage_from_fs_worker, this, files_per_worker[i], &td, op);
    }
    for (auto& thread : workers) {
        thread.join();
    }
}

void Server::restore_storage_from_fs_worker(const std::vector<std::string>& files, TableDescriptor* td, RestoreOperator* op) {
    std::string file_name_list;
    for (const std::string& file : files) {
        file_name_list += file + "\n";
    }
    SLOG(INFO) << "Loading weight files: " << file_name_list;
    URIConfig uri(td->table_uri);
    std::map<std::string, std::pair<std::string, int>> param_map
          = {{core::URI_HADOOP_BIN, {_hadoop_bin, URILVL::EXTCFG}}};
    uri.replace_param(param_map);
    for (const std::string& file : files) {
        std::string file_uri = uri.new_uri(file);
        op->restore(URIConfig(file_uri), *(td->runtime_info), td->storage.get());
    }
}

void Server::self_monitor() {
    std::unique_lock<std::mutex> lock(_self_monitor_mtx);
    std::chrono::milliseconds dur(_report_interval);
    while (!_self_monitor_stop) {
        ShardStorageMemory::singleton().check_healthy();
        _self_monitor_cond.wait_for(lock, dur);
    }
}

void Server::process_s2s_request() {
    set_thread_name("ps_s2s");
    std::shared_ptr<Dealer> dealer = _s2s_server->create_dealer();
    RpcRequest request;
    PSMessageMeta meta;
    while (dealer->recv_request(request)) {
        PSRequest req(std::move(request));
        req.archive().read_back(&meta, sizeof(meta));
        PSResponse resp(req);
        // SLOG(INFO) << "GOT s2s request type = " << int(meta.req_type)
        //            << " sid = " << meta.sid << " src = " << request.head().src_rank
        //            << " ctx_ver = " << meta.ctx_ver << " uctx_u_ver = " << meta.ctx_u_ver;
        switch (meta.req_type) {
        case RequestType::OP_PUSH_ASYNC:
            process_s2s_async_push_request(req, meta, resp);
            break;
        case RequestType::NEW_CTX_SHUFFLE:
            process_s2s_new_ctx_shuffle_request(req, meta, resp);
            break;
        case RequestType::STORAGE_STATISTIC:
            process_query_storage_info_request(req, meta, resp);
            break;
        case RequestType::OP_COORDINATED_RESTORE_ITERATE:
            process_coordinate_restore_iterate_request(req, meta, resp);
            break;
        case RequestType::HEALTH_CHECK:
            process_health_check_request(req, meta, resp);
            break;
        default:
            SLOG(FATAL) << "irrelavent request type: " << int(meta.req_type);
        }
        //SCHECK(req.archive().is_exhausted());
        resp << meta;
        dealer->send_response(std::move(resp.rpc_response()));
    }
    dealer.reset();
}

void Server::process_c2s_request() {
    set_thread_name("ps_c2s");
    std::shared_ptr<Dealer> dealer = _c2s_server->create_dealer();

    RpcRequest request;
    PSMessageMeta meta;
    bool exit = false;

    auto send_response = [&dealer, &meta](PSResponse&& resp) {
        resp << meta;
        dealer->send_response(std::move(resp.rpc_response()));
    };
    while (dealer->recv_request(request)) {
        PSRequest req(std::move(request));
        req.archive().read_back(&meta, sizeof(meta));
        PSResponse resp(req);
        //SLOG(INFO) << " GOT c2s request type = " << int(meta.req_type)
        //            << " sid = " << meta.sid << " src = " << request.head().src_rank
        //            << " ctx_ver = " << meta.ctx_ver << " uctx_u_ver = " << meta.ctx_u_ver;
        switch (meta.req_type) {
        case RequestType::STORAGE_CREATE:
            process_create_storage_request(req, meta, resp);
            break;
        case RequestType::STORAGE_DELETE:
            process_delete_storage_request(meta, resp);
            break;
        case RequestType::STORAGE_CLEAR:
            process_clear_storage_request(meta, resp);
            break;
        case RequestType::STORAGE_STATISTIC:
            process_query_storage_info_request(req, meta, resp);
            break;
        case RequestType::HANDLER_REGISTER:
            process_register_handler_request(req, meta, resp);
            break;
        case RequestType::HANDLER_DEREGISTER:
            process_deregister_handler_request(meta, resp);
            break;
        case RequestType::OP_FOREACH:
        // intented fall through
        case RequestType::OP_ERASEIF:
        // intented fall through
        case RequestType::OP_DUMP:
            process_for_each_request(req, meta, resp);
            break;
        case RequestType::OP_LOAD:
            {
                auto m_req = std::make_shared<PSRequest>(std::move(req));
                auto func = [m_req, meta, this](int) mutable {
                    PSResponse resp(*m_req);
                    process_load_request(*m_req, meta, resp);
                    resp << meta;
                    _c2s_client->create_dealer()->send_response(std::move(resp.rpc_response()));
                };
                _io_tg.async_exec(std::move(func));
            }
            break;
        case RequestType::OP_PULL:
            process_pull_request(req, meta, resp);
            break;
        case RequestType::OP_PUSH_ASYNC:
            process_async_push_request(req, meta, resp);
            break;
        case RequestType::OP_PUSH_SYNC:
            process_sync_push_request(req, meta, resp);
            break;
        case RequestType::OP_STORE:
            process_store_request(req, meta, send_response);
            break;
        case RequestType::OP_RPC:
            process_rpc_operator(req, meta, dealer.get());
            break;
        case RequestType::OP_LOAD_LIBRARY:
            process_load_library_request(req, resp);
            break;
        case RequestType::EXIT:
            resp << Status();
            SLOG(INFO) << "Exit request received.";
            exit = true;
            break;
        case RequestType::NEW_CTX_CREATE_SHARD:
            process_new_ctx_create_shard_request(meta, resp);
            break;
        case RequestType::NEW_CTX_SHUFFLE:
            process_new_ctx_shuffle_request(req, meta, resp);
            break;
        case RequestType::NEW_CTX_STORE:
            process_new_ctx_store_request(req, meta, resp);
            break;
        case RequestType::NEW_CTX_PURGE:
            process_new_ctx_purge_request(req, meta, resp);
            ShardStorageMemory::singleton().set_healthy(true);
            break;
        case RequestType::CTX_UPDATE:
            process_ctx_update_request(meta, resp);
            break;
        case RequestType::MEMORY_INFO: {
            NodeMemoryInfo info;
            info.healthy = ShardStorageMemory::singleton().test_healthy();
            info.used_pmem = Memory::singleton().get_used_pmem();
            info.managed_vmem = Memory::singleton().get_managed_vmem();
            resp << info;
            break;
        }
        case RequestType::OP_SYNC:
            process_sync_request(req, meta, resp);
            break;
        default:
            SLOG(FATAL) << "irrelavent request type: " << int(meta.req_type);
        }

        if (meta.req_type == RequestType::OP_LOAD ||
            meta.req_type == RequestType::OP_STORE ||
            meta.req_type == RequestType::OP_RPC) {
            continue;
        }
        // SCHECK(req.archive().is_exhausted());
        resp << meta;
        //SLOG(INFO) << "send response type = " << int(meta.req_type)
        //           << " dest = "  << resp.rpc_response().head().dest_rank;
        dealer->send_response(std::move(resp.rpc_response()));
        if (exit) {
            _c2s_server->terminate();
        }
    }
    dealer.reset();
}

Status Server::create_storage(int32_t storage_id,
      const std::string& lib_name,
      const std::string& op_name,
      const std::string& config_str) {
    Configure config;
    Configure op_config;
    try {
        config.load(config_str);
        if (!config.has("op_config")) {
            return Status::InvalidConfig("No field \"op_config\"");
        }
        op_config = config["op_config"];
    } catch (const std::exception& e) {
        return Status::InvalidConfig(e.what());
    }
    auto op = OperatorFactory::singleton().create(lib_name, op_name, op_config);
    auto status = _ctx.CreateStorage(storage_id, op, lib_name, op_name, config);
    SCHECK(status.ok()) << status.ToString();
    TableDescriptorWriter td;
    status = _ctx.GetTableDescriptorWriter(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    Context::update_runtime_info(td.table(), _node_id);
    auto stop = static_cast<StorageOperator*>(op.get());
    
    td.table().storage = stop->create_storage(*td.table().runtime_info, storage_id); 
    return status;
}

void Server::process_create_storage_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    std::string lib_name, op_name, config_str;
    req >> lib_name >> op_name >> config_str;
    auto status = create_storage(meta.sid, lib_name, op_name, config_str);
    resp << status;
    BLOG(1) << "create storage with storage_id: " << meta.sid
               << " with configure: " << config_str;
}


void Server::process_clear_storage_request(const PSMessageMeta& meta, PSResponse& resp) {
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    status = check_write_ctx_version(meta, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    static_cast<StorageOperator*>(td.table().storage_op.get())
          ->clear_storage(td.table().storage.get());
    resp << status;
}

void Server::process_query_storage_info_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    auto op = static_cast<StorageOperator*>(td.table().storage_op.get());
    op->apply_query_info_request(
          *td.table().runtime_info, req, td.table().storage.get(), resp);
}

void Server::process_coordinate_restore_iterate_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    auto op = static_cast<RestoreOperator*>(
        static_cast<StorageOperator*>(td.table().storage_op.get())->restore_op());
    op->apply_coordinated_restore_request(req, td.table().storage.get(), resp);
}

void Server::process_delete_storage_request(const PSMessageMeta& meta, PSResponse& resp) {
    auto status = _ctx.DeleteStorage(meta.sid);
    SCHECK(status.ok()) << status.ToString();
    resp << status;
    //Speedometer::singleton().remove(std::to_string(meta.sid));
    del_sync_table(meta.sid);
}

void Server::process_register_handler_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    TableDescriptorWriter td;
    auto status = _ctx.GetTableDescriptorWriter(meta.sid, td);
    SCHECK(status.ok()) << status.ToString();
    std::string key, config_str, lib_name, op_name;
    req >> key >> lib_name >> op_name >> config_str;
    Configure config;
    config.load(config_str);
    try {
        config.load(config_str);
        auto op = OperatorFactory::singleton().create(lib_name, op_name, config);
        if (op != nullptr) {
            td.table().add_handler(meta.hid, op, key, lib_name, op_name, config);
            BLOG(1) << "register handler with handler id: " << meta.hid
                    << " for storage id: " << meta.sid << " with op_name: \"" << op_name
                    << "\" with configure: " << config_str;
        } else {
            status = Status::Error("No operator");
        }
    } catch (const std::exception& e) { status = Status::InvalidConfig(e.what()); }
    resp << status;
}

void Server::process_deregister_handler_request(const PSMessageMeta& meta, PSResponse& resp) {
    TableDescriptorWriter td;
    auto status = _ctx.GetTableDescriptorWriter(meta.sid, td);
    SCHECK(status.ok()) << status.ToString();
    td.table().remove_handler(meta.hid);
    SLOG(INFO) << "deregister handler with handler id: " << meta.hid
               << " for storage id: " << meta.sid;
    resp << status;
}

void Server::process_for_each_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);    
    SCHECK(status.ok()) << status.ToString();
    status = check_write_ctx_version(meta, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString() << " storage_id: " << meta.sid
                      << " client ver: " << meta.ctx_ver
                      << " server ver: " << td.table().version;
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    static_cast<ForEachOperator*>(it->second.get())
          ->apply_request(*td.table().runtime_info, req, td.table().storage.get(), resp);
}

void Server::process_pull_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    DurationObserver observer(
            metrics_histogram(PS_REQUEST_DURATION_MS_BUCKET,
                PS_REQUEST_DURATION_MS_BUCKET_DESC,
                {{"request_type", "c2s_pull"}},
                METRICS_LATENCY_BOUNDARY));
    metrics_counter(PS_REQUESTS_TOTAL,
            PS_REQUESTS_TOTAL_DESC,
            {{"request_type", "c2s_pull"}}).Increment();
    auto begin = std::chrono::high_resolution_clock::now();
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    if (!status.ok()) { //check for predictor
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        //SpeedometerClient::thread_local_instance().write(
        //        std::to_string(meta.sid), true, -1);
        metrics_counter(PS_ERRORS_TOTAL,
                PS_ERRORS_TOTAL_DESC,
                {{"request_type", "c2s_pull"}}).Increment();
        return;
    }
    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    auto op = static_cast<PullOperator*>(it->second.get());
    if (op->read_only()) {
        status = check_ctx_version(meta, td);
    } else {
        status = check_write_ctx_version(meta, td);
    }
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString() << ' ' << meta.ctx_ver << ' ' << td.table().version;
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        //SpeedometerClient::thread_local_instance().write(std::to_string(meta.sid), true, -1);
        metrics_counter(PS_ERRORS_TOTAL,
                PS_ERRORS_TOTAL_DESC,
                {{"request_type", "c2s_pull"}}).Increment();
        return;
    }

    if (!op->read_only() && td.table().storage_status == StorageStatus::RESHARD) {
        status = Status::OOM("write when update context");
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        //SpeedometerClient::thread_local_instance().write(
        //        std::to_string(meta.sid), true, -1);
        metrics_counter(PS_ERRORS_TOTAL,
                PS_ERRORS_TOTAL_DESC,
                {{"request_type", "c2s_pull"}}).Increment();
        return;
    }
    metrics_counter(PS_REQUESTS_SIZE_BYTES,
            PS_REQUESTS_SIZE_BYTES_DESC,
            {{"request_type", "c2s_pull"}}) .Increment(req.archive().length());
    op->apply_request(*td.table().runtime_info, req, td.table().storage.get(), resp);
    metrics_counter(PS_RESPONSES_SIZE_BYTES,
            PS_RESPONSES_SIZE_BYTES_DESC,
            {{"request_type", "c2s_pull"}}).Increment(resp.archive().length());
    auto dur = std::chrono::high_resolution_clock::now() - begin;
    //auto us = std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
    //SpeedometerClient::thread_local_instance().write(std::to_string(meta.sid), false, us);
}

void Server::process_sync_push_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    Storage* delta_st = nullptr;
    auto status = _ctx.CheckDeltaStorage(meta.sid, &delta_st);
    SCHECK(status.ok()) << status.ToString();
    if (delta_st == nullptr) {
        TableDescriptorWriter td;
        auto status = _ctx.GetTableDescriptorWriter(meta.sid, td);
        SCHECK(status.ok()) << status.ToString();
        status = check_write_ctx_version(meta, td);
        auto it = td.table().handlers.find(meta.hid);
        SCHECK(it != td.table().handlers.end()) << meta.hid;
        auto push_op = static_cast<PushOperator*>(it->second.get());
        if (!status.ok()) {
            SLOG(WARNING) << status.ToString() << " storage_id: " << meta.sid
                          << " client ver: " << meta.ctx_ver
                          << " server ver: " << td.table().version;
            resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
            resp << status;
            return;
        }
        if (td.table().delta_storage == nullptr) {
            td.table().delta_storage = push_op->create_delta_storage(*td.table().runtime_info);
        }
        push_op->apply_sync_push_request(
              *td.table().runtime_info, req, td.table().delta_storage.get(), resp);
    } else {
        TableDescriptorReader td;
        auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
        SCHECK(status.ok()) << status.ToString();
        status = check_write_ctx_version(meta, td);
        auto it = td.table().handlers.find(meta.hid);
        SCHECK(it != td.table().handlers.end()) << meta.hid;
        auto push_op = static_cast<PushOperator*>(it->second.get());
        if (!status.ok()) {
            SLOG(WARNING) << status.ToString() << " storage_id: " << meta.sid
                          << " client ver: " << meta.ctx_ver
                          << " server ver: " << td.table().version;
            resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
            resp << status;
            return;
        }
        push_op->apply_sync_push_request(*td.table().runtime_info, req, delta_st, resp);
    }
}

void Server::process_async_push_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    DurationObserver observer(
            metrics_histogram(PS_REQUEST_DURATION_MS_BUCKET,
                PS_REQUEST_DURATION_MS_BUCKET_DESC,
                {{"request_type", "c2s_async_push"}},
                METRICS_LATENCY_BOUNDARY));
    metrics_counter(PS_REQUESTS_TOTAL,
            PS_REQUESTS_TOTAL_DESC,
            {{"request_type", "c2s_async_push"}}).Increment();
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    SCHECK(status.ok()) << status.ToString();
    status = check_write_ctx_version(meta, td);
    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    auto push_op = static_cast<PushOperator*>(it->second.get());
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString() << " storage_id: " << meta.sid
                      << " client ver: " << meta.ctx_ver
                      << " server ver: " << td.table().version;
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        metrics_counter(PS_ERRORS_TOTAL,
                PS_ERRORS_TOTAL_DESC,
                {{"request_type", "c2s_async_push"}}).Increment();
        return;
    }

    if (push_op->sync_tables().size() > 0 && td.table().incr_storage == nullptr) {
        add_sync_table(meta.sid, td.table(), push_op);
    }
    Storage* incr_storage = nullptr;
    if (td.table().incr_storage) {
        incr_storage = td.table().incr_storage->start_write();
    }
    metrics_counter(PS_REQUESTS_SIZE_BYTES,
            PS_REQUESTS_SIZE_BYTES_DESC,
            {{"request_type", "c2s_async_push"}}).Increment(req.archive().length());
    push_op->apply_async_push_request(
                *td.table().runtime_info, req, td.table().storage.get(), 
                incr_storage, resp);
    metrics_counter(PS_RESPONSES_SIZE_BYTES,
            PS_RESPONSES_SIZE_BYTES_DESC,
            {{"request_type", "c2s_async_push"}}).Increment(resp.archive().length());
    if (td.table().incr_storage) {
        td.table().incr_storage->finish_write();
        notify_sync_table(meta.sid);
    }
}

void Server::process_store_request(PSRequest& req,
      const PSMessageMeta& meta,
      std::function<void(PSResponse&&)> send_response) {
    DurationObserver observer(
            metrics_histogram(PS_REQUEST_DURATION_MS_BUCKET,
                PS_REQUEST_DURATION_MS_BUCKET_DESC,
                {{"request_type", "c2s_store"}},
                METRICS_LATENCY_BOUNDARY));
    metrics_counter(PS_REQUESTS_TOTAL,
            PS_REQUESTS_TOTAL_DESC,
            {{"request_type", "c2s_store"}}).Increment();
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    SCHECK(status.ok()) << status.ToString();
    status = check_write_ctx_version(meta, td);
    if (!status.ok()) {
        PSResponse resp(req);
        SLOG(WARNING) << status.ToString() << " storage_id: " << meta.sid
                      << " client ver: " << meta.ctx_ver
                      << " server ver: " << td.table().version;
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        send_response(std::move(resp));
        metrics_counter(PS_ERRORS_TOTAL,
                PS_ERRORS_TOTAL_DESC,
                {{"request_type", "c2s_store"}}).Increment();
        return;
    }
    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    auto push_op = static_cast<PushOperator*>(it->second.get());
    if (push_op->sync_tables().size() > 0 && td.table().incr_storage == nullptr) {
        add_sync_table(meta.sid, td.table(), push_op);
    }
    Storage* incr_storage = nullptr;
    if (td.table().incr_storage) {
        incr_storage = td.table().incr_storage->start_write();
    }
    push_op->apply_store_request(*td.table().runtime_info,
                req,
                td.table().delta_storage.get(),
                td.table().storage.get(),
                incr_storage,
                send_response);
    if (td.table().incr_storage) {
        td.table().incr_storage->finish_write();
        notify_sync_table(meta.sid);
    }
}

void Server::process_load_library_request(PSRequest& req, PSResponse& resp) {
    std::string path;
    req >> path;
    SLOG(INFO) << "Loading \"" << path << "\"";
    bool succ = OperatorFactory::singleton().load_library(path);
    if (succ) {
        SLOG(INFO) << "Loaded \"" << path << "\"";
        resp << Status();
    } else {
        SLOG(INFO) << "Load \"" << path
                   << "\" failed, $LD_LIBRARY_PATH=" << std::getenv("LD_LIBRARY_PATH");
        resp << Status::Error("load SO failed.");
    }
}

void Server::process_rpc_operator(PSRequest& req,
      const PSMessageMeta& meta,
      Dealer* dealer) {
    DurationObserver observer(
            metrics_histogram(PS_REQUEST_DURATION_MS_BUCKET,
                PS_REQUEST_DURATION_MS_BUCKET_DESC,
                {{"request_type", "c2s_rpc_op"}},
                METRICS_LATENCY_BOUNDARY));
    metrics_counter(PS_REQUESTS_TOTAL,
            PS_REQUESTS_TOTAL_DESC,
            {{"request_type", "c2s_rpc_op"}}).Increment();
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        PSResponse resp(req);
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status << meta;
        dealer->send_response(std::move(resp.rpc_response()));
        metrics_counter(PS_ERRORS_TOTAL,
                PS_ERRORS_TOTAL_DESC,
                {{"request_type", "c2s_rpc_op"}}).Increment();
        return;
    }
    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    auto op = static_cast<RpcOperatorBase*>(it->second.get());
    if (op->read_only()) {
        status = check_ctx_version(meta, td);
    } else {
        status = check_write_ctx_version(meta, td);
    }
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString() << ' ' << meta.ctx_ver << ' ' << td.table().version;
        PSResponse resp(req);
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status << meta;
        dealer->send_response(std::move(resp.rpc_response()));
        metrics_counter(PS_ERRORS_TOTAL,
                PS_ERRORS_TOTAL_DESC,
                {{"request_type", "c2s_rpc_op"}}).Increment();
        return;
    }
    op->apply_request(meta, req, td.table(), dealer);
}

void Server::process_load_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    SCHECK(status.ok()) << status.ToString();
    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    auto op = static_cast<LoadOperator*>(it->second.get());
    op->apply_load_request(meta,*td.table().runtime_info, req, td.table().storage.get(),
        resp, td.table().version, _s2s_client.get());
}

void Server::process_health_check_request(PSRequest&,
      const PSMessageMeta&,
      PSResponse& resp) {
    Status status;
    resp << status;
    resp << _node_id;
}


void Server::process_s2s_async_push_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    DurationObserver observer(
            metrics_histogram(PS_REQUEST_DURATION_MS_BUCKET,
                PS_REQUEST_DURATION_MS_BUCKET_DESC,
                {{"request_type", "s2s_async_push"}},
                METRICS_LATENCY_BOUNDARY));
    metrics_counter(PS_REQUESTS_TOTAL,
            PS_REQUESTS_TOTAL_DESC,
            {{"request_type", "s2s_async_push"}}).Increment();
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString() << " storage_id: " << meta.sid
                      << " client ver: " << meta.ctx_ver;
        metrics_counter(PS_ERRORS_TOTAL,
                PS_ERRORS_TOTAL_DESC,
                {{"request_type", "s2s_async_push"}}).Increment();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    status = check_write_ctx_version(meta, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString() << " storage_id: " << meta.sid
                      << " client ver: " << meta.ctx_ver
                      << " server ver: " << td.table().version;
        metrics_counter(PS_ERRORS_TOTAL,
                PS_ERRORS_TOTAL_DESC,
                {{"request_type", "s2s_async_push"}}).Increment();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    std::unique_ptr<PushItems> sync_items;

    metrics_counter(PS_REQUESTS_SIZE_BYTES,
            PS_REQUESTS_SIZE_BYTES_DESC,
            {{"request_type", "s2s_async_push"}}).Increment(req.archive().length());
    static_cast<LoadOperator*>(it->second.get())->push_operator()
          ->apply_async_push_request(
                *td.table().runtime_info, req, td.table().storage.get(), nullptr, resp);
    metrics_counter(PS_RESPONSES_SIZE_BYTES,
            PS_RESPONSES_SIZE_BYTES_DESC,
            {{"request_type", "s2s_async_push"}}).Increment(resp.archive().length());
}

void Server::process_new_ctx_create_shard_request(const PSMessageMeta& meta,
      PSResponse& resp) {
    TableDescriptorWriter td;
    auto status = _ctx.GetTableDescriptorWriter(meta.sid, td);
    if (status.IsInvalidID()) {
        if (initialize_storage(meta.sid)) {
            status = _ctx.GetTableDescriptorWriter(meta.sid, td);
        }
    } 
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    status = check_ctx_version(meta, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }

    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    auto op = static_cast<BaseShardUpdateContextOperator*>(it->second.get());

    Storage* st;
    if (td.table().runtime_info->nodes().count(_node_id)) {
        st = td.table().storage.get();
#ifdef USE_DCPMM
        if (st->use_dcpmm()) {
            DCPmemory::singleton().remove_storage(-1);
            td.table().update_storage.reset(); 
        }
#endif  // USE_DCPMM
        td.table().update_storage = op->create_storage(*td.table().new_runtime_info, -1);
    } else {
        td.table().update_storage = nullptr;
        td.table().storage = op->create_storage(*td.table().new_runtime_info, meta.sid);
        st = td.table().storage.get();
    }
    op->create_new_shards(*td.table().new_runtime_info, st);
    resp << status;
}

void Server::process_new_ctx_shuffle_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    SCHECK(status.ok()) << status.ToString();
    status = check_table_version(meta, td.table());
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    auto op = static_cast<BaseShardUpdateContextOperator*>(it->second.get());
    Storage* st = td.table().storage.get();

    int32_t shard_id = 0;
    req >> shard_id;
    core::vector<std::unique_ptr<PushItems>> content;
    auto gen = op->lock_push_items_generator(
        *td.table().runtime_info, *td.table().new_runtime_info, st, shard_id);
    DirectPushDistributedAsyncReturn push(op->push_operator(), _s2s_client.get());
    while (gen->generate_push_items(content) > 0) {
        status = push.push(std::move(content),
              *td.table().new_runtime_info,
              {meta.sid,
                    meta.hid,
                    td.table().version,
                    td.table().update_version,
                    RequestType::NEW_CTX_SHUFFLE},
              10000);
        if (!status.ok()) {
            SLOG(WARNING) << status.ToString();
            resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
            resp << status;
            return;
        }
    }
    resp << status;
}

void Server::process_s2s_new_ctx_shuffle_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    SCHECK(status.ok()) << status.ToString();
    status = check_table_version(meta, td.table());
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }

    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    auto op = static_cast<BaseShardUpdateContextOperator*>(it->second.get());
    if (td.table().runtime_info->nodes().count(_node_id)) {
        op->push_operator()->apply_async_push_request(
              *td.table().new_runtime_info, req, td.table().update_storage.get(), nullptr, resp);
    } else {
        op->push_operator()->apply_async_push_request(
              *td.table().new_runtime_info, req, td.table().storage.get(), nullptr, resp);
    }
    if (resp.rpc_response().error_code() != RpcErrorCodeType::SUCC) {
        SLOG(WARNING) << "process s2s new ctx shuffle OOM";
    }
}

void Server::process_new_ctx_store_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    SCHECK(status.ok()) << status.ToString();
    status = check_table_version(meta, td.table());
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    
    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    auto op = static_cast<BaseShardUpdateContextOperator*>(it->second.get());
    Storage* st = td.table().storage.get();
    op->push_operator()->apply_store_request(*td.table().new_runtime_info,
          req,
          td.table().update_storage.get(),
          st,
          nullptr,
          [&resp](PSResponse&& resp_ret) {resp = std::move(resp_ret);});
    if (resp.rpc_response().error_code() != RpcErrorCodeType::SUCC) {
        SLOG(WARNING) << "process new ctx store OOM";
    }    
}

void Server::process_new_ctx_purge_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    //limit concurrency for memory space
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    } 
    status = check_ctx_version(meta, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }

    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    auto op = static_cast<BaseShardUpdateContextOperator*>(it->second.get());
    Storage* st = td.table().storage.get();
    op->apply_purge_request(*td.table().runtime_info, req, st, resp);
    pico_gc();
}

void Server::process_ctx_update_request(const PSMessageMeta& meta, PSResponse& resp) {
    TableDescriptorWriter old_td;
    auto status = _ctx.GetTableDescriptorWriter(meta.sid, old_td);
    if (status.IsInvalidID()) {
        if (initialize_storage(meta.sid)) {
            status = _ctx.GetTableDescriptorWriter(meta.sid, old_td);
        }
    } 
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    status = pull_context_from_master_nolock(meta.sid, old_td.table());
    resp << status;
    auto update_shards = [&](RuntimeInfo* rt, ShardStorage* st){
        auto shards = rt->local_shards();
        std::set<int32_t> shard_set(shards.begin(), shards.end());
        for(auto sid:st->shard_list()){
            if(shard_set.count(sid) == 0){
                st->erase_shard(sid);
            } else {
                shard_set.erase(sid);
            }
        }
        for(auto sid: shard_set){
            st->create_shard(sid);
        }
    };
    if(auto st = dynamic_cast<ShardStorage*>(old_td.table().storage.get())){
        update_shards(old_td.table().runtime_info.get(), st);
    }
    if(auto st = dynamic_cast<ShardStorage*>(old_td.table().delta_storage.get())){
        update_shards(old_td.table().runtime_info.get(), st);
    }
    if(auto st = dynamic_cast<ShardStorage*>(old_td.table().update_storage.get())){
        update_shards(old_td.table().new_runtime_info.get(), st);
    }
#ifdef USE_DCPMM
    Storage* st = old_td.table().storage.get();
    if (st->use_dcpmm()) {
        st->store_version_uuid(old_td.table().version_uuid);
    }
#endif // USE_DCPMM
}

void Server::process_sync_request(PSRequest& req,
      const PSMessageMeta& meta,
      PSResponse& resp) {
    TableDescriptorReader td;
    auto status = _ctx.GetTableDescriptorReader(meta.sid, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString();
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        //SpeedometerClient::thread_local_instance().write(
        //        std::to_string(meta.sid), true, -1);
        return;
    }
    SCHECK(status.ok()) << status.ToString();
    status = check_write_ctx_version(meta, td);
    if (!status.ok()) {
        SLOG(WARNING) << status.ToString() << " storage_id: " << meta.sid
                      << " client ver: " << meta.ctx_ver
                      << " server ver: " << td.table().version;
        resp.rpc_response().set_error_code(RpcErrorCodeType::ELOGICERROR);
        resp << status;
        return;
    }
    auto it = td.table().handlers.find(meta.hid);
    SCHECK(it != td.table().handlers.end()) << meta.hid;
    static_cast<SyncOperator*>(it->second.get())->apply_sync_request(
                *td.table().runtime_info, req, td.table().storage.get(), resp);
}

Status Server::pull_context_from_master_nolock(int32_t storage_id, TableDescriptor& td) {
    std::string str;
    bool ret = _master_client->get_context(storage_id, str);
    if (ret) {
        Context::update_handlers_form_str(td, str);
        Context::update_runtime_info(td, _node_id);
        return Status();
    } else {
        return Status::InvalidID("storage ctx not found");
    }
}


Status Server::check_table_version(const PSMessageMeta& meta, TableDescriptor& td) {
    if (meta.ctx_ver > td.version) {
        return Status::ServerTooOldCtx("");
    } else if (meta.ctx_ver < td.version) {
        return Status::ServerTooNewCtx("");
    } else if (meta.ctx_u_ver > td.update_version) {
        return Status::ServerTooOldCtxU("");
    } else if (meta.ctx_u_ver < td.update_version) {
        return Status::ServerTooNewCtxU("");
    } else {
        return Status();
    }
}

Status Server::check_ctx_version(const PSMessageMeta& meta, TableDescriptorWriter& td) {
    if (meta.ctx_ver > td.table().version) {
        auto status = pull_context_from_master_nolock(meta.sid, td.table());
        if (!status.ok()) {
            return status;
        }
    }
    if (meta.ctx_ver > td.table().version) {
        return Status::ServerTooOldCtx("");
    } else if (meta.ctx_ver < td.table().version) {
        return Status::ServerTooNewCtx("");
    } else {
        return Status();
    }
}

Status Server::check_ctx_version(const PSMessageMeta& meta, TableDescriptorReader& td) {
    int32_t storage_id = meta.sid;
    if (meta.ctx_ver > td.table().version) {
        td.Release();
        {
            TableDescriptorWriter tdwr;
            auto status = _ctx.GetTableDescriptorWriter(storage_id, tdwr);
            if (!status.ok()) {
                return status;
            }
            if (meta.ctx_ver > tdwr.table().version) {
                status = pull_context_from_master_nolock(storage_id, tdwr.table());
                if (!status.ok()) {
                    return status;
                }
            }
        }
        auto status = _ctx.GetTableDescriptorReader(storage_id, td);
        if (!status.ok()) {
            return status;
        }
    }
    if (meta.ctx_ver > td.table().version) {
        return Status::ServerTooOldCtx("");
    } else if (meta.ctx_ver < td.table().version) {
        return Status::ServerTooNewCtx("");
    } else {
        return Status();
    }
}

Status Server::check_write_ctx_version(const PSMessageMeta& meta, TableDescriptorReader& td) {
    auto status = check_ctx_version(meta, td);
    if (!status.ok()) {
        return status;
    }
    if (td.table().storage_status == StorageStatus::RESHARD) {
        return Status::OOM("write when update context");
    } else {
        return Status();
    }
    return status;
}

Status Server::check_write_ctx_version(const PSMessageMeta& meta, TableDescriptorWriter& td) {
    auto status = check_ctx_version(meta, td);
    if (!status.ok()) {
        return status;
    }
    if (td.table().storage_status == StorageStatus::RESHARD) {
        return Status::OOM("write when update context");
    } else {
        return Status();
    }
}


bool Server::initialize_storage(int32_t storage_id) {
    _master_client->acquire_lock(PSERVER_LOCK_NAME);
    std::string str;
    bool ret = _master_client->get_context(storage_id, str);
    if (ret) {
        SLOG(INFO) << "initializing storage: " << storage_id << " with context:\n" << str;
        auto ptd = std::make_unique<TableDescriptor>();
        auto& td = *ptd;
        bool updated;
        SCHECK(td.from_json_str(str, updated, true));
        SCHECK(updated);
        Context::initialize_storage_op(td);
        Context::update_handlers(td);

        RpcServiceInfo service_info;
        _c2s_client->get_rpc_service_info(service_info);
        std::unordered_set<int32_t> live_servers;
        for (auto& server_info : service_info.servers) {
            live_servers.emplace(server_info.server_id);
        }
        if (td.update_node_status(live_servers)) {
            _master_client->set_context(storage_id, td.to_json_str());
        }
        Context::update_runtime_info(td, _node_id);
        // add by cc pmem 接口修改 add one more para: storage_id 
        td.storage = static_cast<StorageOperator*>(td.storage_op.get())
                           ->create_storage(*td.runtime_info, storage_id);

        _ctx.SetTableDescriptor(storage_id, std::move(ptd));
        _ctx.UpdateRuntimeInfo(storage_id, _node_id);
    }
    _master_client->release_lock(PSERVER_LOCK_NAME);
    return ret;
}

bool Server::set_node_status_to_running(int32_t storage_id) {
    TableDescriptorWriter td;
    auto status = _ctx.GetTableDescriptorWriter(storage_id, td);
    SCHECK(status.ok()) << status.ToString();
    status = pull_context_from_master_nolock(storage_id, td.table());
    if (!status.ok()) {
        return false;
    }
    bool ret = td.table().set_node_status_to_running(_node_id);
    if (ret) {
        Context::update_runtime_info(td.table(), _node_id);
        _master_client->set_context(storage_id, td.table().to_json_str());
        std::vector<PSRequest> reqs;
        for (auto node : td.table().nodes) {
            if (node.first != _node_id) {
                reqs.emplace_back(node.first);
            }
        }

        auto client = _rpc_service->create_client(PSERVER_C2S_RPC_NAME);
        DefaultDistributedAsyncReturn dist_ret(client.get());
        status = dist_ret.default_sync_rpc(std::move(reqs), 
              {storage_id, -1, -1, -1, RequestType::CTX_UPDATE}, -1);
        SCHECK(status.ok()) << status;
    }
    return ret;
}

// sync table
void Server::add_sync_table(int32_t storage_id, TableDescriptor& table, PushOperator* push_op) {
    Client* client = _sync_client.get();
    SCHECK(client);
    std::unique_lock<std::mutex> lock(_sync_mtx);
    auto& sync_table = _sync_tables[storage_id];
    if (sync_table.get() == nullptr) {
        sync_table = std::make_unique<SyncTable>();
        sync_table->incr_storage = std::make_unique<IncrementStorage>(
                push_op->create_incr_storage(*table.runtime_info),
                push_op->create_incr_storage(*table.runtime_info));
        table.incr_storage = sync_table->incr_storage.get();
        for (auto& s : push_op->sync_tables()) {
            sync_table->handlers.emplace_back(std::make_unique<SyncHandler>(
                  client, s.model_name, s.table_name, s.op_key));
        }
    }
}
 
void Server::del_sync_table(int32_t storage_id) {
    std::unique_lock<std::mutex> lock(_sync_mtx);
    auto it = _sync_tables.find(storage_id);
    if (it != _sync_tables.end()) {
        SCHECK(it->second.get());
        it->second->destroyed = true;
        _sync_storages.insert(storage_id);
        _sync_cond.notify_one();
    }
}
     
void Server::notify_sync_table(int32_t storage_id) {
    std::unique_lock<std::mutex> lock(_sync_mtx);
    _sync_storages.insert(storage_id);
    _sync_cond.notify_one();
}

void Server::push_sync_tables() {
    bool run = true;
    while (run) {
        std::unordered_set<int32_t> sync_storages;
        {
            std::unique_lock<std::mutex> lock(_sync_mtx);
            _sync_cond.wait(lock, [this](){return _sync_storages.size() > 0 || _sync_exit;});
            sync_storages = std::move(_sync_storages);
            _sync_storages.clear();
            if (_sync_exit)
                run = false;
        }

        for (auto storage_id : sync_storages) {
            _sync_mtx.lock();
            auto it = _sync_tables.find(storage_id);
            if (it == _sync_tables.end()) {
                SLOG(WARNING) << "no sync storage id:" << storage_id;
                continue;
            }
            auto sync_table = it->second.get();
            bool destroyed = it->second->destroyed;
            _sync_mtx.unlock();

            auto st = sync_table->incr_storage->read_storage();
            if (st) {
                for (auto& handler : sync_table->handlers) {
                    handler->sync_pred(st, 300000); //TODO timeout
                }
                st->clear();
            }

            if (destroyed) {
                std::unique_lock<std::mutex> lock(_sync_mtx);
                _sync_tables.erase(storage_id);
            }
        }
    }
}

} // namespace ps
} // namespace pico
} // namespace paradigm4

