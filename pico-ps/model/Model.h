#ifndef PARADIGM4_PICO_PS_MODEL_MODEL_H
#define PARADIGM4_PICO_PS_MODEL_MODEL_H

#include <string>
#include <vector>
#include <unordered_map>

#include "pico-ps/common/core.h"
#include "pico-ps/common/core.h"
#include "pico-ps/service/TableDescriptor.h"

namespace paradigm4 {
namespace pico {
namespace ps {

enum TABLE_INIT_METHOD {
    REMOTE_PS = 0,
    LOCAL_PS = 1,
    NON_PS = 2,
};

struct OperatorObject {
    std::string key;
    std::string op_name;
    std::string op_config_str;

    PicoJsonNode to_json_node() const;

    bool from_json_node(const PicoJsonNode& node);
};

struct TableObject {
    // data field
    std::string table_name;
    TABLE_INIT_METHOD init_method;
    std::string table_uri;
    int shard_num;
    int replica_num;
    int storage_id = -1;

    std::string lib_name;
    OperatorObject storage_op;
    std::unordered_map<std::string, OperatorObject> ops;

    // interface field
    bool is_remote_ps() const {
        return init_method == TABLE_INIT_METHOD::REMOTE_PS;
    }

    bool is_local_ps() const {
        return init_method == TABLE_INIT_METHOD::LOCAL_PS;
    }

    bool is_non_ps() const {
        return init_method == TABLE_INIT_METHOD::NON_PS;
    }

    PicoJsonNode to_json_node() const;

    bool from_json_node(const PicoJsonNode& node);
};

enum class ModelStatus : int {
    NORMAL = 0,
    RESTORING = 1,
    CREATING = 2,
    DELETING = 3,
    UPDATING = 4,
};

const std::string ModelStatusStr[] = {
      "NORMAL",
      "RESTORING",
      "CREATING",
      "DELETING",
      "UPDATING",
};

struct ModelObject {
    // data field
    int version = 0;
    std::string type;
    std::unordered_map<std::string, std::shared_ptr<ModelObject>> submodels;
    std::string model_config_str;
    std::unordered_map<std::string, TableObject> tables;
    int shard_num = 0;
    int replica_num = 0;
    double load_meta_time_secs = -1;

    std::string extra_info = "";

    ModelStatus status;
    comm_rank_t dealing_ctrl_rank;
    bool rollbacked = false;

    // TODO 目前先不支持progress
    //int progress = 0;

    // interface field
    std::unordered_map<std::string, TableObject*> all_tables();

    std::unordered_map<std::string, OperatorObject*> all_operators();

    std::string to_json_str() const;

    PicoJsonNode to_json_node() const;

    bool from_json_node(const PicoJsonNode& node);

    bool from_json_str(const std::string& str);
};

} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_MODEL_MODEL_H
