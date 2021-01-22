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
      // pserver 启动尝试 restore 一个 node 时，如果发现没有需要 restore 的 node，不一定是真的没有。
      // pserver live servers 是根据 zk 中保存的 rpc server 信息判断的，
      // 很多时候，pserver 重启后上一次的信息还能查询到，
      // 这里要等待一个 zk 判断 node 超时的时间，才能让新启动的 pserver 发现已经需要 restore 的 node。
      // pserver 会在 server_restore_wait_timeout_second 的时间内循环等待 master 更新 server 信息。
      // 如果设置的为 -1，server 将使用 zk session timeout 2 倍。
      int32_t server_restore_wait_timeout_second = -1;
      // 当 dcpmm 中保存的节点信息不是 dead node，server 将会以传统方式替代一个检测到的 dead_node，并恢复。
      // 在以传统方式回复之前，会等待一段时间，目的是让其他能够以 dcpmm 恢复的节点尽量以 dcpmm 方式恢复。
      size_t server_dcpmm_replace_dead_node_wait_time_second = 60;
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

    int32_t node_id() { return _node_id; }

    void restore_storages(bool use_pmem_restore, int possible_dead_node_id = -1);

    void set_sync_env(MasterClient* sync_master_client, RpcService* sync_rpc);

    void initialize();

    void finalize();

    void exit();

private:
    bool test_connections();

    void restore_storage_by_network(int32_t storage_id, TableDescriptor& td);

    bool restore_storage_coordinated(int32_t storage_id, TableDescriptor& td);

    void restore_storage_from_fs(TableDescriptor& td);

    // 返回替换掉的 dead node 的 node_id
    int32_t _find_dead_node_and_replace(int32_t possible_dead_node_id, std::vector<int32_t>& to_restore_tables);

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

    void process_rpc_operator(PSRequest& req,
          const PSMessageMeta& meta,
          Dealer* dealer);

    void process_load_library_request(PSRequest& req, PSResponse& resp);

    void process_load_request(PSRequest& req,
          const PSMessageMeta& meta,
          PSResponse& resp);

    void process_health_check_request(PSRequest& req,
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

    int32_t replace_dead_node_in_loop(int looping_time_seconds, int32_t possible_dead_node_id, std::vector<int32_t>& to_restore_tables);

    const size_t _c2s_thread_num, _s2s_thread_num, _server_load_thread_num;
    int32_t _server_restore_wait_timeout_second;
    const size_t _server_dcpmm_replace_dead_node_wait_time_second;
    size_t _load_block_size;
    std::vector<std::thread> _c2s_thread, _s2s_thread;
    ThreadGroup _io_tg;
    int _report_interval;

    Context _ctx;
    int32_t _node_id = -1;
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
