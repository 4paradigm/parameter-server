#ifndef PARADIGM4_PICO_PS_SERVICE_TABLEDESCRIPOTR_H
#define PARADIGM4_PICO_PS_SERVICE_TABLEDESCRIPOTR_H

#include <functional>
#include <memory>
#include <unordered_map>

#include <boost/any.hpp>

#include "pico-ps/common/core.h"

#include "pico-ps/common/Status.h"
#include "pico-ps/storage/Storage.h"
#include "pico-ps/service/IncrementStorage.h"

namespace paradigm4 {
namespace pico {
namespace ps {

constexpr int EMPTY_COMM_RANK = -1;

class Operator;

struct OperatorDescriptor {
    std::string key;
    std::string lib_name;
    std::string op_name;
    std::string config_str;

    OperatorDescriptor();

    OperatorDescriptor(const std::string& s0,
          const std::string& s1,
          const std::string& s2,
          const std::string& s3);

    PicoJsonNode to_json_node() const;

    bool from_json_node(const PicoJsonNode& node);
};

enum class NodeStatus : int {
    RUNNING = 0,
    DEAD = 1,
    LOADING = 2,
    UNAVAILABALE = 3
};

enum class StorageStatus : int {
    NORMAL = 0,
    RESHARD = 1,
    LOADING = 2
};

struct NodeDescriptor {
    std::vector<int32_t> shard_list;
    NodeStatus status;
    int node_id;

    NodeDescriptor();

    NodeDescriptor(const Configure& config);

    PicoJsonNode to_json_node() const;

    bool from_json_node(const PicoJsonNode& node);
};

class RuntimeInfo;
struct TableDescriptor {
    //应该使用指针或引用
    TableDescriptor() = default;
    TableDescriptor(const TableDescriptor&) = delete;
    TableDescriptor& operator=(const TableDescriptor&) = delete;
    TableDescriptor(TableDescriptor&&) = delete;
    TableDescriptor& operator=(TableDescriptor&&) = delete;

    // 用于快速查找 shard 所在 node, shards 与 node_descs 中信息一致, update_shards 与
    // update_node_descs 中信息一致，其中不会包括状态为 DEAD 的 node 信息。
    std::unordered_map<int32_t, std::vector<int>> shards, update_shards;
    // 用于快速查找 node 中suo, shards 与 node_descs 中信息一致, update_shards 与
    // update_node_descs 中信息一致，其中不会包括状态为 DEAD 的 node 信息。
    std::unordered_map<int, std::vector<int32_t>> nodes, update_nodes;
    // Master 上保存的 node 描述符
    std::vector<NodeDescriptor> node_descs, update_node_descs;
    std::string table_uri = ""; // full path
    int version = 0;
    int update_version = 0;
    std::string version_uuid = "";
    StorageStatus storage_status = StorageStatus::NORMAL;

    //todo 每个push_operator对应一个delta_storage
    std::unique_ptr<Storage> storage, delta_storage;
    std::unique_ptr<Storage> update_storage;
    //update_storage用于存储shuffle时的push item，否则读写相互等待导致死锁。

    //更新增量存储
    IncrementStorage* incr_storage = nullptr;

    std::shared_ptr<Operator> storage_op;
    OperatorDescriptor storage_op_desc;

    // op_key -> handler id
    std::unordered_map<std::string, int32_t> key_to_hdl;
    // handler id -> Op desc
    std::unordered_map<int32_t, OperatorDescriptor> op_descs;
    // handler id -> Op object
    std::unordered_map<int32_t, std::shared_ptr<Operator>> handlers; // for runtime
    // 运行时信息, 运行时信息与 TableDescriptor 中信息冗余
    std::unique_ptr<RuntimeInfo> runtime_info;
    // reshard 目标状态的运行时信息, 运行时信息与 TableDescriptor 中信息冗余
    std::unique_ptr<RuntimeInfo> new_runtime_info;
    std::string table_name = "";

    Configure to_yaml_create_storage_config() const;

    bool add_handler(int32_t handler_id,
          std::shared_ptr<Operator>& op,
          const std::string& key,
          const std::string& lib_name,
          const std::string& op_name,
          const Configure& config);

    bool remove_handler(int32_t handler_id);

    std::string to_json_str() const;

    PicoJsonNode to_json_node() const;

    bool from_json_node(const PicoJsonNode& node, bool& updated, bool force_update = false);

    bool from_json_str(const std::string& str, bool& updated, bool force_update = false);

    // 根据 node descriptor 更新 shards 和 nodes 信息
    // 只有状态为 RUNNING 的 node 的信息才会被更新至 shards 和 nodes 中。
    inline void refresh_info() {
        refresh_info(node_descs, shards, nodes);
    }

    // 根据 update node descriptor 更新 update shards 和 update nodes 信息，
    // 只有状态为 RUNNING 的 update node 的信息才会被更新至 update shards 和 update nodes 中。
    inline void refresh_info_update() {
        refresh_info(update_node_descs, update_shards, update_nodes);
    }

    bool update_node_status(const std::unordered_set<int>& live_servers);

    int loading_dead_node(int my_node_id, int dead_node_id);

    // 将 node descriptor 中此 node 状态中的 LOADING 状态改为 RUNNING 状态。
    bool set_node_status_to_running(int my_node_id);

    // 用 update_node 信息替换当前 node 信息，表示 reshard 结束
    void apply_update();
    void drop_update();

    // 根据 config 中包含的 update node 配置信息更新当前的 update node 描述符。
    Status create_update_info(const Configure& config);

    void gen_new_version_uuid();

private:
    void refresh_info(std::vector<NodeDescriptor>& nds,
          std::unordered_map<int32_t, std::vector<int>>& shard_info,
          std::unordered_map<int, std::vector<int32_t>>& node_info);

    // 根据 (update) node descriptor 更新 (update) shards 和 (update) nodes 信息，
    // 状态为 RUNNING 或者 LOADING 的 node 的信息都会被更新至 shards 和 nodes 中。
    // 用于 restore 阶段
    //void refresh_info_with_loading(std::vector<NodeDescriptor>& nds,
          //std::unordered_map<int32_t, std::vector<int>>& shard_info,
          //std::unordered_map<int, std::vector<int32_t>>& node_info);
};

enum class PickAlgo {
    RANDOM,
    ROUND_ROBIN,
};

class RuntimeInfo {
public:
    RuntimeInfo(TableDescriptor& td, int my_rank, bool update = false);

    std::vector<int> pick_one_replica(PickAlgo algo);
    
    void update_nodes();

    int32_t global_shard_num() {
        return _shards->size();
    }

    const std::unordered_set<int32_t>& local_shards() {
        return _local_shards;
    }

    const std::string local_shards_str() {
        std::ostringstream oss;
        std::copy(_local_shards.begin(), _local_shards.end(), std::ostream_iterator<int32_t>(oss, " "));
        return oss.str();
    }

    const std::unordered_map<int32_t, std::vector<int>>& shards() {
        return *_shards;
    }

    const std::unordered_map<int, std::vector<int32_t>>& nodes() {
        return *_nodes;
    }

    int node_id() const {
        return _node_id;
    }

private:
    std::vector<int> random_pick();

    std::vector<int> rr_pick();

    std::unordered_map<int32_t, std::vector<int>>* _shards;
    std::unordered_map<int, std::vector<int32_t>>* _nodes;
    std::vector<NodeDescriptor>* _node_descs;
    int _node_id;
    std::unordered_set<int32_t> _local_shards;
};

class TableDescriptorReader {
public:
    TableDescriptorReader();

    TableDescriptorReader(TableDescriptor* table, RWSpinLock& mtx);

    TableDescriptorReader(const TableDescriptorReader&) = delete;
    TableDescriptorReader& operator=(const TableDescriptorReader&) = delete;

    TableDescriptorReader(TableDescriptorReader&& rhs);

    TableDescriptorReader& operator=(TableDescriptorReader&& rhs);

    ~TableDescriptorReader();

    void Release();

    TableDescriptor& table() {
        return *_table;
    }

private:
    TableDescriptor* _table = nullptr;
    boost::shared_lock<RWSpinLock>* _lock = nullptr;
};

class TableDescriptorWriter {
public:
    TableDescriptorWriter();

    TableDescriptorWriter(TableDescriptor* table, RWSpinLock& mtx);

    TableDescriptorWriter(const TableDescriptorWriter&) = delete;
    TableDescriptorWriter& operator=(const TableDescriptorWriter&) = delete;

    TableDescriptorWriter(TableDescriptorWriter&& rhs);

    TableDescriptorWriter& operator=(TableDescriptorWriter&& rhs);

    ~TableDescriptorWriter();

    void Release();

    TableDescriptor& table() {
        return *_table;
    }

private:
    TableDescriptor* _table = nullptr;
    core::lock_guard<RWSpinLock>* _lock = nullptr;
};

} // namespace ps
} // namespace pico
} // namespace paradigm4
#endif // PARADIGM4_PICO_PS_SERVICE_TABLEDESCRIPOTR_H
