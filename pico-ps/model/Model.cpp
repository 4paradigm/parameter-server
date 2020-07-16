#include "pico-ps/model/Model.h"

namespace paradigm4 {
namespace pico {
namespace ps {

PicoJsonNode OperatorObject::to_json_node() const {
    PicoJsonNode node;
    node.add("key", key);
    node.add("op_name", op_name);
    node.add("op_config_str", op_config_str);
    return node;
}

bool OperatorObject::from_json_node(const PicoJsonNode& node) {
    if (node.at("key").try_as<std::string>(key) == false) {
        return false;
    }
    if (node.at("op_name").try_as<std::string>(op_name) == false) {
        return false;
    }
    if (node.at("op_config_str").try_as<std::string>(op_config_str) == false) {
        return false;
    }
    return true;
}

PicoJsonNode TableObject::to_json_node() const {
    PicoJsonNode node;
    node.add("table_name", table_name);
    node.add("init_method", int(init_method));
    node.add("table_uri", table_uri);
    node.add("shard_num", shard_num);
    node.add("replica_num", replica_num);
    node.add("storage_id", storage_id);
    node.add("lib_name", lib_name);
    node.add("storage_op", storage_op.to_json_node());
    PicoJsonNode ops_node;
    for (const auto& op : ops) {
        ops_node.add(op.first, op.second.to_json_node());
    }
    node.add("ops", ops_node);
    return node;
}

bool TableObject::from_json_node(const PicoJsonNode& node) {
    if (node.at("table_name").try_as<std::string>(table_name) == false) {
        return false;
    }
    int tmp;
    if (node.at("init_method").try_as<int>(tmp) == false) {
        return false;
    }
    init_method = TABLE_INIT_METHOD(tmp);
    if (node.at("table_uri").try_as<std::string>(table_uri) == false) {
        return false;
    }
    if (node.at("shard_num").try_as<int>(shard_num) == false) {
        return false;
    }
    if (node.at("replica_num").try_as<int>(replica_num) == false) {
        return false;
    }
    if (node.at("storage_id").try_as<int>(storage_id) == false) {
        return false;
    }
    if (node.at("lib_name").try_as<std::string>(lib_name) == false) {
        return false;
    }
    if (storage_op.from_json_node(node.at("storage_op")) == false) {
        return false;
    }
    const auto& ops_node = node.at("ops");
    for (auto it = ops_node.begin(); it != ops_node.end(); ++it) {
        OperatorObject op;
        if (op.from_json_node(it.value()) == false) {
            return false;
        }
        ops.emplace(it.key(), std::move(op));
    }
    return true;
}

std::unordered_map<std::string, TableObject*> ModelObject::all_tables() {
    std::unordered_map<std::string, TableObject*> ret;
    for (auto& submodel : submodels) {
        auto t = submodel.second->all_tables();
        for (auto& it : t) {
            SCHECK(ret.count(it.first) == 0) << it.first;
            ret.emplace(it.first, it.second);
        }
    }
    for (auto& table : tables) {
        SCHECK(ret.count(table.first) == 0) << table.first;
        ret.emplace(table.first, &(table.second));
    }
    return ret;
}

std::unordered_map<std::string, OperatorObject*> ModelObject::all_operators() {
    std::unordered_map<std::string, OperatorObject*> ret;
    for (auto& submodel : submodels) {
        auto t = submodel.second->all_operators();
        for (auto& it : t) {
            SCHECK(ret.count(it.first) == 0) << it.first;
            ret.emplace(it.first, it.second);
        }
    }
    for (auto& table : tables) {
        SCHECK(ret.count(table.second.storage_op.key) == 0);
        ret.emplace(table.second.storage_op.key, &(table.second.storage_op));
        for (auto& op : table.second.ops) {
            SCHECK(ret.count(op.first) == 0);
            ret.emplace(op.first, &(op.second));
        }
    }
    return ret;
}

std::string ModelObject::to_json_str() const {
    std::string s;
    to_json_node().save(s);
    return s;
}

PicoJsonNode ModelObject::to_json_node() const {
    PicoJsonNode node;
    node.add("version", version);
    node.add("type", type);
    node.add("model_config_str", model_config_str);
    node.add("load_meta_time", load_meta_time_secs);
    node.add("status", (int)status);
    node.add("extra_info", extra_info);
    node.add("rollbacked", (bool)rollbacked);
    node.add("dealing_ctrl_rank", dealing_ctrl_rank);
    PicoJsonNode tables_node;
    for (const auto& table : tables) {
        tables_node.add(table.first, table.second.to_json_node());
    }
    node.add("tables", tables_node);
    PicoJsonNode submodels_node;
    for (const auto& submodel : submodels) {
        submodels_node.add(submodel.first, submodel.second->to_json_node());
    }
    node.add("submodels", submodels_node);
    return node;
}

bool ModelObject::from_json_node(const PicoJsonNode& node) {
    if (node.at("version").try_as<int>(version) == false) {
        return false;
    }
    if (node.at("type").try_as<std::string>(type) == false) {
        return false;
    }
    if (node.at("model_config_str").try_as<std::string>(model_config_str) == false) {
        return false;
    }
    if (node.at("load_meta_time").try_as<double>(load_meta_time_secs) == false) {
        return false;
    }
    int _;
    if (node.at("status").try_as<int>(_) == false) {
        return false;
    } else {
        status = (ModelStatus)_;
    }
    if (node.at("dealing_ctrl_rank").try_as<comm_rank_t>(dealing_ctrl_rank) == false) {
        return false;
    }
    if (node.at("rollbacked").try_as<bool>(rollbacked) == false) {
        return false;
    }
    if (node.at("extra_info").try_as<std::string>(extra_info) == false) {
        extra_info = "";
    }
    const auto& tables_node = node.at("tables");
    for (auto it = tables_node.begin(); it != tables_node.end(); ++it) {
        TableObject table;
        if (table.from_json_node(it.value()) == false) {
            return false;
        }
        tables.emplace(it.key(), std::move(table));
    }
    const auto& submodels_node = node.at("submodels");
    for (auto it = submodels_node.begin(); it != submodels_node.end(); ++it) {
        auto submodel = std::make_shared<ModelObject>();
        if (submodel->from_json_node(it.value()) == false) {
            return false;
        }
        submodels.emplace(it.key(), std::move(submodel));
    }
    return true;
}

bool ModelObject::from_json_str(const std::string& str) {
    PicoJsonNode node;
    node.load(str);
    return from_json_node(node);
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
