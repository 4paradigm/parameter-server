#include "pico-ps/service/TableDescriptor.h"

namespace paradigm4 {
namespace pico {
namespace ps {

OperatorDescriptor::OperatorDescriptor() {}

OperatorDescriptor::OperatorDescriptor(const std::string& s0,
      const std::string& s1,
      const std::string& s2,
      const std::string& s3)
    : key(s0), lib_name(s1), op_name(s2), config_str(s3) {}

PicoJsonNode OperatorDescriptor::to_json_node() const {
    PicoJsonNode node;
    node.add("key", key);
    node.add("lib_name", lib_name);
    node.add("op_name", op_name);
    node.add("config_str", config_str);
    return node;
}

bool OperatorDescriptor::from_json_node(const PicoJsonNode& node) {
    if (node.at("key").try_as<std::string>(key) == false) {
        return false;
    }
    if (node.at("lib_name").try_as<std::string>(lib_name) == false) {
        return false;
    }
    if (node.at("op_name").try_as<std::string>(op_name) == false) {
        return false;
    }
    if (node.at("config_str").try_as<std::string>(config_str) == false) {
        return false;
    }
    return true;
}

NodeDescriptor::NodeDescriptor() {}

NodeDescriptor::NodeDescriptor(const Configure& config) {
    for (size_t i = 0; i < config["shard_list"].size(); ++i) {
        shard_list.emplace_back(config["shard_list"][i].as<int32_t>());
    }
    status = NodeStatus::RUNNING;
    node_id = config["g_rank"].as<int>();
}

PicoJsonNode NodeDescriptor::to_json_node() const {
    PicoJsonNode node;
    node.add("shard_list", PicoJsonNode(shard_list));
    node.add("status", int(status));
    node.add("g_rank", int(node_id));
    return node;
}

bool NodeDescriptor::from_json_node(const PicoJsonNode& node) {
    int tmp;
    const auto& s_list_node = node.at("shard_list");
    for (auto it = s_list_node.begin(); it != s_list_node.end(); ++it) {
        if (it->try_as<int32_t>(tmp) == false) {
            return false;
        }
        shard_list.emplace_back(tmp);
    }
    if (node.at("status").try_as<int>(tmp) == false) {
        return false;
    }
    status = NodeStatus(tmp);
    if (node.at("g_rank").try_as<int>(tmp) == false) {
        return false;
    }
    node_id = tmp;
    return true;
}

Configure TableDescriptor::to_yaml_create_storage_config() const {
    Configure conf;
    for (const auto& node : nodes) {
        YAML::Node ynode;
        for (auto id : node.second) {
            ynode["shard_list"].push_back(id);
        }
        ynode["g_rank"] = node.first;
        conf.node()["nodes"].push_back(ynode);
    }
    Configure op_config;
    op_config.load(storage_op_desc.config_str);
    conf.node()["op_config"] = op_config.node();
    return conf;
}

bool TableDescriptor::add_handler(int32_t handler_id,
      std::shared_ptr<Operator>& op,
      const std::string& key,
      const std::string& lib_name,
      const std::string& op_name,
      const Configure& config) {
    if (handlers.count(handler_id) != 0) {
        return false;
    }
    handlers[handler_id] = op;
    OperatorDescriptor opd(key, lib_name, op_name, config.dump());
    key_to_hdl[key] = handler_id;
    op_descs[handler_id] = std::move(opd);
    return true;
}

bool TableDescriptor::remove_handler(int32_t handler_id) {
    if (handlers.count(handler_id) == 0) {
        return false;
    }
    key_to_hdl.erase(op_descs[handler_id].key);
    handlers.erase(handler_id);
    op_descs.erase(handler_id);
    return true;
}

std::string TableDescriptor::to_json_str() const {
    PicoJsonNode node = to_json_node();
    std::string s;
    node.save(s);
    return s;
}

PicoJsonNode TableDescriptor::to_json_node() const {
    PicoJsonNode node;
    node.add("version", version);
    node.add("update_version", update_version);
    node.add("table_uri", table_uri);
    node.add("storage_op", storage_op_desc.to_json_node());
    PicoJsonNode hdlr_node;
    for (const auto& desc : op_descs) {
        hdlr_node.add(std::to_string(desc.first), desc.second.to_json_node());
    }
    node.add("handlers", hdlr_node);
    PicoJsonNode node_desc_node;
    for (const auto& desc : node_descs) {
        node_desc_node.push_back(desc.to_json_node());
    }
    node.add("nodes", node_desc_node);
    PicoJsonNode update_node_desc_node;
    for (const auto& desc : update_node_descs) {
        update_node_desc_node.push_back(desc.to_json_node());
    }
    node.add("update_nodes", update_node_desc_node);
    node.add("status", int(storage_status));
    node.add("table_name", table_name);
    return node;
}

bool TableDescriptor::from_json_str(const std::string& str, bool& updated, bool force_update) {
    PicoJsonNode node;
    node.load(str);
    return from_json_node(node, updated, force_update);
}

bool TableDescriptor::from_json_node(const PicoJsonNode& node,
      bool& updated,
      bool force_update) {
    updated = false;
    int p_version, u_version;
    if (node.at("version").try_as<int>(p_version) == false) {
        return false;
    }
    if (node.at("update_version").try_as<int>(u_version) == false) {
        return false;
    }
    if (p_version != version || force_update) {
        if (node.at("table_uri").try_as<std::string>(table_uri) == false) {
            return false;
        }
        if (storage_op_desc.from_json_node(node.at("storage_op")) == false) {
            return false;
        }
        const PicoJsonNode& hdls = node.at("handlers");
        for (auto it = hdls.begin(); it != hdls.end(); ++it) {
            auto id = pico_lexical_cast<int32_t>(it.key());
            if (handlers.count(id) == 0) {
                OperatorDescriptor opd;
                if (opd.from_json_node(it.value()) == false) {
                    return false;
                }
                key_to_hdl[opd.key] = id;
                op_descs.emplace(id, std::move(opd));
            }
        }
        const PicoJsonNode& node_desc_node = node.at("nodes");
        node_descs.clear();
        for (auto& it : node_desc_node) {
            NodeDescriptor nd;
            if (nd.from_json_node(it) == false) {
                return false;
            }
            node_descs.push_back(std::move(nd));
        }
        int tmp;
        if (node.at("status").try_as<int>(tmp) == false) {
            return false;
        }
        storage_status = StorageStatus(tmp);
        if (node.at("table_name").try_as<std::string>(table_name) == false) {
            return false;
        }
        refresh_info(node_descs, shards, nodes);
        if (runtime_info.get())
            runtime_info->update_nodes();
        version = p_version;
        updated = true;
    }
    if (u_version != update_version || force_update) {
        const PicoJsonNode& update_node_desc_node = node.at("update_nodes");
        update_node_descs.clear();
        for (auto& it : update_node_desc_node) {
            NodeDescriptor nd;
            if (nd.from_json_node(it) == false) {
                return false;
            }
            update_node_descs.push_back(std::move(nd));
        }
        int tmp;
        if (node.at("status").try_as<int>(tmp) == false) {
            return false;
        }
        storage_status = StorageStatus(tmp);
        refresh_info(update_node_descs, update_shards, update_nodes);
        if (new_runtime_info.get())
            new_runtime_info->update_nodes();
        update_version = u_version;
        updated = true;
    }
    return true;
}

bool TableDescriptor::update_node_status(const std::unordered_set<int>& live_servers) {
    bool ret = false;
    for (auto& it : node_descs) {
        if (live_servers.count(it.node_id) == 0) {
            it.status = NodeStatus::DEAD;
            ret = true;
        }
    }
    if (ret)
        refresh_info();
    return ret;
}

int TableDescriptor::loading_dead_node(int my_rank, int dead_rank) {
    for (auto& it : node_descs) {
        if (it.status == NodeStatus::DEAD 
                && (dead_rank == EMPTY_COMM_RANK || dead_rank == it.node_id)) {
            dead_rank = it.node_id;
            it.node_id = my_rank;
            it.status = NodeStatus::LOADING;
            version++;
            return dead_rank;         
        }
    }
    return EMPTY_COMM_RANK;
}

bool TableDescriptor::set_node_status_to_running(int my_rank) {
    for (auto& it : node_descs) {
        if (it.node_id == my_rank) {
            //TODO 目前这个check有可能挂，loading过程中，Predictor把这个节点状态更新成DEAD
            // 小概率重启ps的时候，起不起来。
            SCHECK(it.status == NodeStatus::LOADING) << it.to_json_node();
            it.status = NodeStatus::RUNNING;
            version += 1;
            refresh_info();
            if (runtime_info.get())
                runtime_info->update_nodes();
            return true;
        }
    }
    return false;
}

void TableDescriptor::apply_update() {
    std::swap(update_shards, shards);
    std::swap(update_nodes, nodes);
    std::swap(update_node_descs, node_descs);
}

void TableDescriptor::drop_update() {
    update_shards = shards;
    update_nodes = nodes;
    update_node_descs = node_descs;
}


Status TableDescriptor::create_update_info(const Configure& config) {
    if (!config.has("nodes")) {
        return Status::InvalidConfig("No field \"nodes\"");
    }
    update_shards.clear();
    update_nodes.clear();
    update_node_descs.clear();
    for (size_t i = 0; i < config["nodes"].size(); ++i) {
        update_node_descs.emplace_back(config["nodes"][i]);
    }
    refresh_info_update();
    if (new_runtime_info.get())
        new_runtime_info->update_nodes();
    update_version += 1;
    return Status();
}

void TableDescriptor::refresh_info(std::vector<NodeDescriptor>& nds,
      std::unordered_map<int32_t, std::vector<int>>& shard_info,
      std::unordered_map<int, std::vector<int32_t>>& node_info) {
    shard_info.clear();
    node_info.clear();
    int32_t max_shard_id = -1;
    for (auto& it : nds) {
        if (it.status == NodeStatus::RUNNING) {
            node_info[it.node_id] = it.shard_list;
            for (auto& s : it.shard_list) {
                shard_info[s].emplace_back(it.node_id);
            }
        } else {
            for (auto& s : it.shard_list) {
                max_shard_id = std::max(max_shard_id, s);
            }
        }
    }
    for (int32_t i = 0; i <= max_shard_id; ++i) {
        if (shard_info.count(i) == 0) {
            shard_info[i] = std::vector<int>();
        }
    }
}

//void TableDescriptor::refresh_info_with_loading(std::vector<NodeDescriptor>& nds,
      //std::unordered_map<int32_t, std::vector<int>>& shard_info,
      //std::unordered_map<int, std::vector<int32_t>>& node_info) {
    //shard_info.clear();
    //node_info.clear();
    //int32_t max_shard_id = -1;
    //for (auto& it : nds) {
        //if (it.status == NodeStatus::RUNNING || it.status == NodeStatus::LOADING) {
            //node_info[it.node_id] = it.shard_list;
            //for (auto& s : it.shard_list) {
                //shard_info[s].emplace_back(it.node_id);
            //}
        //} else {
            //for (auto& s : it.shard_list) {
                //max_shard_id = std::max(max_shard_id, s);
            //}
        //}
    //}
    //for (int32_t i = 0; i <= max_shard_id; ++i) {
        //if (shard_info.count(i) == 0) {
            //shard_info[i] = std::vector<int>();
        //}
    //}
//}

RuntimeInfo::RuntimeInfo(TableDescriptor& td,
      int node_id,
      bool update)
    : _node_id(node_id) {
    if (update) {
        _nodes = &td.update_nodes;
        _shards = &td.update_shards;
        _node_descs = &td.update_node_descs;
    } else {
        _nodes = &td.nodes;
        _shards = &td.shards;
        _node_descs = &td.node_descs;
    }
    //auto it = _nodes->find(_rank);
    //if (it != _nodes->end()) {
        //for (const auto& shard : it->second) {
            //_local_shards.emplace(shard);
        //}
    //}
    update_nodes();
}

void RuntimeInfo::update_nodes() {
    for (const auto& nd : *_node_descs) {
        if (nd.node_id == _node_id && nd.status != NodeStatus::DEAD) {
            _local_shards.insert(nd.shard_list.begin(), nd.shard_list.end());
            break;
        }
    }
}

std::vector<int> RuntimeInfo::pick_one_replica(PickAlgo algo) {
    switch (algo) {
    case PickAlgo::RANDOM:
        return random_pick();
    // case PickAlgo::ROUND_ROBIN:
    //     return rr_pick();
    default:
        SLOG(FATAL) << "Unknow Pick Algorithm: " << int(algo);
        return std::vector<int>();
    }
}

std::vector<int> RuntimeInfo::random_pick() {
    std::vector<int> ret;
    const auto& shards = *_shards;
    for (int32_t shard_id = 0; shard_id < int32_t(shards.size()); ++shard_id) {
        const auto& it = shards.find(shard_id);
        SCHECK(it != shards.end());
        if (it->second.size() == 0) {
            ret.push_back(-1);
        } else {
            size_t idx = size_t(it->second.size() * pico_real_random()) % it->second.size();
            ret.push_back(it->second[idx]);
        }
    }
    return ret;
}

std::vector<int> RuntimeInfo::rr_pick() {
    /*
    static thread_local std::vector<size_t> _pick;
    const auto& shards = *_shards;
    if (_pick.size() != shards.size()) {
        for (int32_t shard_id = 0; shard_id < int32_t(shards.size()); ++shard_id) {
            const auto& it = shards.find(shard_id);
            SCHECK(it != shards.end());
            _pick.push_back(
                  size_t(it->second.size() * pico_normal_random()) % it->second.size());
        }
    }
    std::vector<int> ret;
    for (int32_t shard_id = 0; shard_id < int32_t(shards.size()); ++shard_id) {
        const auto& it = shards.find(shard_id);
        SCHECK(it != shards.end());
        _pick[shard_id] = (_pick[shard_id] + 1) % it->second.size();
        ret.push_back(it->second[_pick[shard_id]]);
    }
    return ret;
    */
    return std::vector<int>();
}

TableDescriptorReader::TableDescriptorReader() : _table(nullptr), _lock(nullptr) {}

TableDescriptorReader::TableDescriptorReader(TableDescriptor* table, RWSpinLock& mtx)
    : _table(table), _lock(new boost::shared_lock<RWSpinLock>(mtx)) {}

TableDescriptorReader::TableDescriptorReader(TableDescriptorReader&& rhs) {
    *this = std::move(rhs);
}

TableDescriptorReader& TableDescriptorReader::operator=(TableDescriptorReader&& rhs) {
    if (_lock != nullptr) {
        delete _lock;
    }
    _table = rhs._table;
    rhs._table = nullptr;
    _lock = rhs._lock;
    rhs._lock = nullptr;
    return *this;
}

TableDescriptorReader::~TableDescriptorReader() {
    if (_lock != nullptr) {
        delete _lock;
    }
}

void TableDescriptorReader::Release() {
    if (_lock != nullptr) {
        delete _lock;
    }
    _table = nullptr;
    _lock = nullptr;
}

TableDescriptorWriter::TableDescriptorWriter() : _table(nullptr), _lock(nullptr) {}

TableDescriptorWriter::TableDescriptorWriter(TableDescriptor* table, RWSpinLock& mtx)
    : _table(table), _lock(new core::lock_guard<RWSpinLock>(mtx)) {}

TableDescriptorWriter::TableDescriptorWriter(TableDescriptorWriter&& rhs) {
    *this = std::move(rhs);
}

TableDescriptorWriter& TableDescriptorWriter::operator=(TableDescriptorWriter&& rhs) {
    if (_lock != nullptr) {
        delete _lock;
    }
    _table = rhs._table;
    rhs._table = nullptr;
    _lock = rhs._lock;
    rhs._lock = nullptr;
    return *this;
}

TableDescriptorWriter::~TableDescriptorWriter() {
    if (_lock != nullptr) {
        delete _lock;
    }
}

void TableDescriptorWriter::Release() {
    if (_lock != nullptr) {
        delete _lock;
    }
    _table = nullptr;
    _lock = nullptr;
}

} // namespace ps
} // namespace pico
} // namespace paradigm4
