#ifndef PARADIGM4_PICO_PS_SERVICE_SERVER_H
#define PARADIGM4_PICO_PS_SERVICE_SERVER_H

#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include <boost/thread/shared_mutex.hpp>

#include "pico-ps/common/core.h"

#include "pico-ps/common/message.h"
#include "pico-ps/service/Context.h"
#include "pico-ps/service/Client.h"
#include "pico-ps/service/IncrementStorage.h"
#include "pico-ps/handler/SyncHandler.h"

namespace paradigm4 {
namespace pico {
namespace ps {

struct ServerConfig {
      size_t server_c2s_thread_num = 4;
      size_t server_s2s_thread_num = 4;
      size_t server_load_thread_num = 4;
      size_t server_load_block_size = 1000;
      size_t report_interval = -1;
};

/*!
 * \brief Server 负责通过调用 op 中的 apply_request() 函数处理 op 的操作。
 */
class Server {
public:
    struct SyncTable {
        std::unique_ptr<IncrementStorage> incr_storage;
        std::vector<std::unique_ptr<SyncHandler>> handlers;
        bool destroyed = false;
    };
public:
    Server(const ServerConfig& lemon,
          MasterClient* master_client,
          RpcService* rpc_service,
          const std::string& hadoop_bin = "");

    int node_id() { return _node_id; }

    void restore_storages(bool use_pmem_restore, comm_rank_t dead_rank = EMPTY_COMM_RANK);

    void set_sync_env(MasterClient* sync_master_client, RpcService* sync_rpc);

    void initialize();

    void finalize();

    void exit();

private:
    void restore_storage(int32_t storage_id, TableDescriptor& td);

    bool restore_storage_coordinated(int32_t storage_id, TableDescriptor& td);

    void restore_storage_from_fs(TableDescriptor& td);

    void restore_storage_from_fs_worker(const std::vector<std::string>& files, TableDescriptor* td, RestoreOperator* op);

    void notify_expand();

    void self_monitor();

    void process_s2s_request();

    void process_c2s_request();

    Status create_storage(int32_t storage_id,
          const std::string& lib_name,
          const std::string& op_name,
          const std::string& config_str);

    void process_create_storage_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_clear_storage_request(const PSMessageMeta& meta, PSResponse& resp);

    void process_query_storage_info_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_delete_storage_request(const PSMessageMeta& meta, PSResponse& resp);

    // no dynamic add/remove support
    void process_register_handler_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    // no dynamic add/remove support
    void process_deregister_handler_request(const PSMessageMeta& meta, PSResponse& resp);

    void process_for_each_request(PSRequest& req, const PSMessageMeta& meta, PSResponse& resp);

    void process_pull_request(PSRequest& req, const PSMessageMeta& meta, PSResponse& resp);

    void process_sync_push_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_async_push_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_store_request(PSRequest& req,
          const PSMessageMeta& meta,
          std::function<void(PSResponse&&)> send_response);

    void process_load_library_request(PSRequest& req, PSResponse& resp);

    void process_load_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_s2s_async_push_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_pred_create_storage_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_new_ctx_create_shard_request(const PSMessageMeta& meta,
          PSResponse& resp);

    void process_new_ctx_shuffle_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_s2s_new_ctx_shuffle_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_new_ctx_store_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_new_ctx_purge_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_sync_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_ctx_update_request(const PSMessageMeta& meta, PSResponse& resp);

    void process_coordinate_restore_iterate_request(PSRequest& req,
            const PSMessageMeta& meta,
            PSResponse& resp);

    // update ctx version & insert new op if not exists & delete old ops
    // used only for predictor
    Status pull_context_from_master_nolock(int32_t storage_id, TableDescriptor& td);

    Status check_table_version(const PSMessageMeta& meta, TableDescriptor& td);
    
    Status check_ctx_version(const PSMessageMeta& meta, TableDescriptorWriter& td);
    
    Status check_ctx_version(const PSMessageMeta& meta, TableDescriptorReader& td);

    Status check_write_ctx_version(const PSMessageMeta& meta, TableDescriptorWriter& td);
    
    Status check_write_ctx_version(const PSMessageMeta& meta, TableDescriptorReader& td);

    bool initialize_storage(int32_t storage_id);

    bool set_node_status_to_running(int32_t storage_id);
    
    // sync table
    void add_sync_table(int32_t storage_id, TableDescriptor& table, PushOperator* push_op);

    void del_sync_table(int32_t storage_id);

    void notify_sync_table(int32_t storage_id);

    void push_sync_tables();

    const size_t _c2s_thread_num, _s2s_thread_num, _server_load_thread_num;
    size_t _load_block_size;
    std::vector<std::thread> _c2s_thread, _s2s_thread;
    ThreadGroup _io_tg;
    int _report_interval;

    Context _ctx;
    comm_rank_t _node_id = -1;
    MasterClient* _master_client = nullptr;
    RpcService* _rpc_service = nullptr;
    std::unique_ptr<RpcServer> _c2s_server;
    std::unique_ptr<RpcServer> _s2s_server;
    std::unique_ptr<RpcClient> _c2s_client;
    std::unique_ptr<RpcClient> _s2s_client;
    std::string _hadoop_bin;
    
    std::unique_ptr<Client> _sync_client;
    std::unique_ptr<RpcClient> _sync_rpc_client;
    std::unordered_map<int32_t, std::unique_ptr<SyncTable>> _sync_tables;
    std::mutex _sync_mtx;
    std::condition_variable _sync_cond;
    std::unordered_set<int32_t> _sync_storages;
    std::thread _sync_thread;
    bool _sync_exit = false;

    std::thread _self_monitor_thread;
    std::mutex _self_monitor_mtx;
    std::condition_variable _self_monitor_cond;
    bool _self_monitor_stop = false;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_SERVICE_SERVER_H
