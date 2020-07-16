#ifndef PARADIGM4_PICO_PS_SERVICE_CLIENT_H
#define PARADIGM4_PICO_PS_SERVICE_CLIENT_H

#include <cstdint>

#include <chrono>
#include <string>
#include <unordered_map>

#include "pico-ps/common/core.h"

#include "pico-ps/common/message.h"
#include "pico-ps/operator/StorageOperator.h"
#include "pico-ps/service/Context.h"

namespace paradigm4 {
namespace pico {
namespace ps {

/*!
 * \brief Client 负责向 Master 申请 storage_id 与 handler_id，并发送注册 storage 与 handler
 *        的请求至 Server。
 */

class Client {
public:

    Client() {}

    void initialize(MasterClient* master_client,
          RpcClient* rpc_client,
          std::shared_ptr<Context> context = nullptr);

    void finalize();

    void get_pserver_list(std::vector<int>& node_id);

    void get_available_pserver_list(std::vector<int>& node_id);

    Status close_pserver(int node_id, int timeout = -1);

    Status query_memory_info(int node_id, NodeMemoryInfo& info, int timeout = -1);

    Status load_operator_library(const std::vector<int>& node_id,
          const std::string& path,
          int timeout = -1);

    Status create_storage(const std::string& lib_name,
          const std::string& creator_op_name,
          const Configure& conf,
          int32_t& storage_id,
          int timeout = -1);

    Status pred_create_storage(const std::string& lib_name,
          const std::string& creator_op_name,
          const Configure& conf,
          int32_t& storage_id,
          int timeout = -1);

    bool initialize_storage(int32_t storage_id);

    Status clear_storage(int32_t storage_id, int timeout = -1);

    Status delete_storage(int32_t storage_id, int timeout = -1);

    Status query_storage_info(int32_t storage_id,
          std::vector<StorageStatisticInfo>* storage_stat,
          int timeout = -1);

    Status register_handler(const std::string& key,
          const std::string& lib_name,
          const std::string& op_name,
          const Configure& conf,
          int32_t storage_id,
          int32_t& handler_id,
          int timeout = -1);

    Status deregister_handler(int32_t storage_id, int32_t handler_id, int timeout = -1);

    void set_table_uri(int32_t storage_id, const std::string& uri);

    void require_update_ctx_async(int32_t storage_id);

    Status handle_timeout(int32_t storage_id, int timeout = -1);

    Status handle_server_too_new_ctx(int32_t storage_id, int timeout = -1);

    Status handle_out_of_memory(int32_t storage_id, const PSMessageMeta& meta, int timeout = -1);

    std::shared_ptr<Context>& context() {
        return _ctx;
    }

    MasterClient* master_client() {
        return _master_client;
    }

    RpcClient* rpc_client() {
        return _rpc_client;
    }

    Status push_context_to_master_nolock(int32_t storage_id,
          TableDescriptor& td,
          int32_t base_version = -1);

    // update ctx version & insert new op if not exists & delete old ops
    // used only for predictor
    Status pull_context_from_master_nolock(int32_t storage_id, TableDescriptor& td);

    Status require_shuffle_update_ctx_nolock(int32_t storage_id,
          TableDescriptor& td,
          int timeout = -1);

    std::string get_current_ctx(int32_t storage_id);

    Status set_ctx(int32_t storage_id, const std::string& ctx) ;

private:
    /***
     * 0. pull ctx from master
     * 2. remove deadnode in ctx
     * 3. push ctx to master
     * 4. notify each server to update ctx
     */
    Status detect_deadnode_nolock(int32_t storage_id, TableDescriptor& td, int timeout = -1);

    std::vector<PSRequest> gen_empty_reqs(int32_t storage_id);

    Status require_update_ctx_nolock(int32_t storage_id,
          TableDescriptor& td,
          int timeout = -1);

    Status handle_timeout_nolock(int32_t storage_id, TableDescriptor& td, int timeout = -1);

    Status default_sync_rpc(std::vector<PSRequest>&& reqs, const PSMessageMeta& meta, int timeout = -1);

    RpcClient* _rpc_client;
    MasterClient* _master_client;
    std::shared_ptr<Context> _ctx;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_SERVICE_CLIENT_H
