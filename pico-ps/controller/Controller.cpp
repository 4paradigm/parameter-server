#include "pico-ps/controller/Controller.h"

#include "pico-ps/common/core.h"
#include "pico-ps/handler/handlers.h"

namespace paradigm4 {
namespace pico {
namespace ps {

#ifndef CONTROLLER_TRY
#define CONTROLLER_TRY() try {
#endif

#ifndef CONTROLLER_CATCH
#define CONTROLLER_CATCH(res)                                                                 \
    }                                                                                         \
    catch (CtlExpection & e) {                                                                \
        _err_str = e.what();                                                                  \
        LOG(WARNING) << _err_str; \
    }                                                                                         \
    catch (std::exception & e) {                                                              \
        _err_str = e.what();                                                                  \
        LOG(WARNING) << _err_str;\
    }                                                                                         \
    return res;
#endif

inline std::shared_ptr<Operator> create_op(const OperatorDescriptor& opd) {
    Configure config;
    if (!config.load(opd.config_str)) {
        throw CtlExpection(
              opd.lib_name + ":" + opd.op_name + " config " + opd.config_str + " error");
    }
    auto op = OperatorFactory::singleton().create(opd.lib_name, opd.op_name, config);
    if (op.get()) {
        return op;
    }
    // if (!OperatorFactory::singleton().load_library("lib" + opd.lib_name + ".so")) {
    // throw CtlExpection("load lib " + opd.lib_name + " failed");
    //}
    // auto op = OperatorFactory::singleton().create(opd.lib_name, opd.op_name, config);
    // if (op.get()) {
    // return op;
    //}
    throw CtlExpection("load op " + opd.lib_name + ":" + opd.op_name + " with config "
                       + opd.config_str + " failed");
}

Controller::Controller(MasterClient* master_client, Client* client, int ps_timeout) {
    _master_client = master_client;
    _rpc_service = client->rpc_client()->rpc_service();
    _client = client;
    _ps_timeout = ps_timeout;
}

bool Controller::shutdown_node(int node_id) {
    CONTROLLER_TRY();
    load_nodes();
    auto node = get_node(node_id);
    if (!node) {
        throw CtlExpection("node " + std::to_string(node_id) + " not exist or not running");
    }
    if (!_client->close_pserver(node_id, _ps_timeout)) {
        throw CtlExpection("shutdown node " + std::to_string(node_id) + " faield");
    }
    return true;
    CONTROLLER_CATCH(false);
}

std::string Controller::show_node(int id) {
    CONTROLLER_TRY();
    load_nodes();
    auto it = _nodes.find(id);
    if (it == _nodes.end() || !it->second.running) {
        throw CtlExpection("node not exist");
    }
    auto& node = it->second;
    std::string id_str = std::to_string(id);
    PicoJsonNode json = PicoJsonNode::object();
    json.add(id_str, PicoJsonNode::object());
    auto& node_json = json.at(id_str);
    gen_node_json(node, node_json);

    std::string s;
    json.save(s);
    return s;
    CONTROLLER_CATCH("");
}

std::string Controller::show_nodes() {
    CONTROLLER_TRY();
    load_nodes();
    PicoJsonNode json = PicoJsonNode::object();
    for (auto& it : _nodes) {
        if (!it.second.running)
            continue;
        std::string id_str = std::to_string(it.first);
        json.add(id_str, PicoJsonNode::object());
        auto& node_json = json.at(id_str);
        gen_node_simple_json(it.second, node_json);
    }

    std::string s;
    json.save(s);
    return s;
    CONTROLLER_CATCH("");
}

bool Controller::expand_nodes() {
    CONTROLLER_TRY();
    core::MasterUniqueLock lock(_master_client, "expand");
    
    auto storage_list = _client->master_client()->get_storage_list();
    for (auto& id : storage_list) {
        if (!_client->initialize_storage(id)) {
            SLOG(WARNING) << "initialize storage " << id << " failed";
            continue;
        }
        TableDescriptorReader rtd;
        auto st = _client->context()->GetTableDescriptorReader(id, rtd);
        if (st.ok()) {
            rtd.Release();
            expand_storage_nodes(id);
        } else {
            SLOG(WARNING) << st.ToString();
            continue;
        }
    }
    return true;
    CONTROLLER_CATCH(false);
}

/**********************************************************/


void Controller::load_nodes() {
    _nodes.clear();
    RpcServiceInfo service_info;
    _rpc_service->update_ctx();
    _client->rpc_client()->get_rpc_service_info(service_info);
    for (auto& server_info: service_info.servers) {
        int node_id = server_info.server_id;
        Node node;
        node.node_id = node_id;
        if (!_client->master_client()->get_comm_info(server_info.global_rank, node.info)) {
            continue;
        }
        node.running = true;
        _nodes.emplace(node_id, node);
    }
}

// TODO
// reload and check node/model

std::vector<Node*> Controller::get_running_nodes() {
    std::vector<Node*> nodes;
    for (auto it = _nodes.begin(); it != _nodes.end(); it++) {
        if (it->second.running)
            nodes.push_back(&it->second);
    }
    return nodes;
}

Node* Controller::get_node(int id) {
    auto it = _nodes.find(id);
    if (it != _nodes.end())
        return &it->second;
    return nullptr;
}

std::vector<Node*> Controller::get_nodes(const std::vector<int>& node_ids) {
    std::vector<Node*> nodes;
    for (int node_id : node_ids) {
        auto it = _nodes.find(node_id);
        if (it != _nodes.end()) {
            nodes.push_back(&it->second);
        } else {
            // TODO get node
            auto n = &(_nodes[node_id]);
            n->node_id = node_id;
            nodes.push_back(n);
        }
    }
    return nodes;
}

std::vector<Node*> Controller::get_low_load_nodes(std::vector<Node*> nodes, int n) {
    if (nodes.size() < (size_t)n) {
        throw CtlExpection("not enough nodes");
    }

    for (auto& node : nodes) {
        node->init_weight();
    }

    std::sort(nodes.begin(), nodes.end(), node_compare);
    return std::vector<Node*>(nodes.begin(), nodes.begin() + n);
}

std::vector<Node*> Controller::get_high_load_nodes(std::vector<Node*> nodes, int n) {
    if (nodes.size() < (size_t)n) {
        throw CtlExpection("not enough nodes");
    }
    for (auto& node : nodes) {
        node->init_weight();
    }
    std::sort(nodes.begin(), nodes.end(), node_compare);
    return std::vector<Node*>(nodes.rbegin(), nodes.rbegin() + n);
}


/***********************************/


void Controller::gen_table_shards_config(Configure& conf,
      const std::vector<std::vector<int>>& shards) {
    std::map<int, std::vector<int>> nodes;
    for (size_t i = 0; i < shards.size(); ++i) {
        for (auto& n : shards[i]) {
            nodes[n].push_back(i);
        }
    }
    for (auto it : nodes) {
        YAML::Node node;
        for (auto& shard : it.second) {
            node["shard_list"].push_back(shard);
        }
        node["g_rank"] = it.first;
        conf.node()["nodes"].push_back(node);
    }
}

std::vector<std::vector<int>> Controller::get_table_shards(int storage_id) {
    TableDescriptorReader rtd;
    auto st = _client->context()->GetTableDescriptorReader(storage_id, rtd);
    if (!st) {
        throw CtlExpection(st);
    }

    auto& td = rtd.table();
    std::vector<std::vector<int>> shards(td.shards.size());
    for (auto& sh : td.shards) {
        if (sh.first < 0 || sh.first >= (int)shards.size()) {
            throw CtlExpection("storage " + std::to_string(storage_id) + " shard id error");
        }
        shards[sh.first] = sh.second;
    }
    return shards;
}

std::unordered_map<int, std::vector<int32_t>> Controller::get_table_nodes(
      int storage_id) {
    TableDescriptorReader rtd;
    auto st = _client->context()->GetTableDescriptorReader(storage_id, rtd);
    if (!st) {
        throw CtlExpection(st);
    }

    auto& td = rtd.table();
    return td.nodes;
}

// void Controller::load_table(TableConfig* table_conf, const std::string& model_uri, int32_t
// storage_id) {  for (auto op : table_conf->load_op) {  int32_t handler_id;  auto st =
// _client->register_handler(  table_conf->lib_name,  op.name,  op.config,  storage_id,
// handler_id); if (!st) { throw CtlExpection(st);
//}
// auto load_op = OperatorFactory::singleton().create(table_conf->lib_name, op.name,
// op.config);  LoadHandler load_handler(storage_id, handler_id, load_op, _client->context());
// load_handler.load(model_uri);
// st = load_handler.wait();
// if (!st) {
// throw CtlExpection(st);
//}
//_client->deregister_handler(storage_id, handler_id);
//}
//}

void Controller::create_table(TableObject* table) {
    if (!table->is_remote_ps())
        return;
    int shard_num = table->shard_num;
    int replica_num = table->replica_num;

    // alloc node
    std::unordered_set<int> nodes_set;
    std::vector<std::vector<int>> shards(shard_num);
    auto running_nodes = get_running_nodes();
    if ((int)running_nodes.size() < replica_num) {
        throw CtlExpection("current running nodes:" + std::to_string(running_nodes.size())
                           + " < replica:" + std::to_string(replica_num));
    }
    for (int i = 0; i < shard_num; ++i) {
        auto nodes = get_low_load_nodes(running_nodes, replica_num);
        for (auto& node : nodes) {
            shards[i].push_back(node->node_id);
            node->shard_num++;
            nodes_set.insert(node->node_id);
        }
    }

    std::vector<int> used_nodes(nodes_set.begin(), nodes_set.end());

    // load library
    std::string library_path = "lib" + table->lib_name + ".so";
    auto st = _client->load_operator_library(used_nodes, library_path, _ps_timeout);
    if (!st)
        throw CtlExpection(st);

    // create
    Configure conf;
    gen_table_shards_config(conf, shards);
    Configure storage_op_config;
    if (!storage_op_config.load(table->storage_op.op_config_str)) {
        throw CtlExpection("load storage op " + table->storage_op.op_config_str + " error");
    }
    conf.node()["op_config"] = storage_op_config.node();

    int32_t storage_id;
    st = _client->pred_create_storage(
          table->lib_name, table->storage_op.op_name, conf, storage_id, _ps_timeout); //create_storage
    if (!st) {
        throw CtlExpection(st);
    }
    table->storage_id = storage_id;

    // register handler
    for (auto it : table->ops) {
        int32_t handler_id;
        auto& op = it.second;
        Configure op_conf;
        if (!op_conf.load(op.op_config_str)) {
            throw CtlExpection(table->table_name + " op:" + op.op_name + " config error");
        }
        auto st = _client->register_handler(op.key,
              table->lib_name,
              op.op_name,
              op_conf,
              storage_id,
              handler_id,
              _ps_timeout);
        if (!st) {
            throw CtlExpection(st);
        }
    }

    // set model uri
    _client->set_table_uri(storage_id, table->table_uri);
}

void Controller::delete_table(TableObject* table) {
    if (table->is_remote_ps() && table->storage_id >= 0) {
        if (!_client->initialize_storage(table->storage_id)) {
            SLOG(WARNING) << "initialize table " << table->table_name
                          << " storage id:" << table->storage_id << " error";
            return;
        }
        auto st = _client->delete_storage(table->storage_id, _ps_timeout);
        if (!st) {
            SLOG(WARNING) << "delete table " << table->table_name
                          << " storage id:" << table->storage_id << " error";
             //throw CtlExpection(st);
        }
    }
}

void Controller::balance_table(TableObject* table) {
    if (!table->is_remote_ps()) {
        return;
    }

    int shard_num = table->shard_num;
    int replica_num = table->replica_num;

    int32_t storage_id = table->storage_id;

    auto nodes = get_table_nodes(storage_id);
    for (auto& it : nodes) {
        auto node = get_node(it.first);
        if (node) {
            node->shard_num -= it.second.size();
        }
    }

    std::vector<std::vector<int>> shards(shard_num);
    auto running_nodes = get_running_nodes();
    if ((int)running_nodes.size() < replica_num) {
        throw CtlExpection("current running nodes:" + std::to_string(running_nodes.size())
                           + " < replica:" + std::to_string(replica_num));
    }
    for (int i = 0; i < shard_num; ++i) {
        auto nodes = get_low_load_nodes(running_nodes, replica_num);
        for (auto& node : nodes) {
            shards[i].push_back(node->node_id);
            node->shard_num++;
        }
    }

    // create
    Configure conf;
    gen_table_shards_config(conf, shards);

    update_table(table, conf);
}

void Controller::update_table_shard(TableObject* table, int shard_num) {
    if (!table->is_remote_ps())
        return;

    auto shards = get_table_shards(table->storage_id);
    int num = shards.size();
    if (num < shard_num) {
        shards.resize(shard_num);
        std::vector<Node*> running_nodes = get_running_nodes();
        // if ((int)running_nodes.size() < table->replica_num) {
        // throw CtlExpection("running nodes count:" +
        // std::to_string(running_nodes.size()) + " < replica_num:" +
        // std::to_string(table->replica_num));
        //}
        for (int i = num; i < shard_num; i++) {
            auto nodes = get_low_load_nodes(running_nodes, table->replica_num);
            for (auto& node : nodes) {
                shards[i].push_back(node->node_id);
                node->shard_num++;
            }
        }
    } else if (num > shard_num) {
        shards.resize(shard_num);
    }

    update_table_shard(table, shards);
    table->shard_num = shard_num;
}

void Controller::update_table_shard(TableObject* table,
      const std::vector<std::vector<int>>& shards) {
    Configure conf;
    gen_table_shards_config(conf, shards);
    update_table(table, conf);
}

void Controller::update_table_replica(TableObject* table, int replica_num) {
    auto shards = get_table_shards(table->storage_id);
    for (auto& shard : shards) {
        int num = shard.size();
        std::unordered_set<int> used_nodes(shard.begin(), shard.end());
        if (num < replica_num) {
            auto nodes = get_running_nodes();
            for (auto it = nodes.begin(); it != nodes.end();) {
                if (used_nodes.find((*it)->node_id) != used_nodes.end()) {
                    it = nodes.erase(it);
                } else {
                    it++;
                }
            }
            auto lnodes = get_low_load_nodes(nodes, replica_num - num);
            for (auto& n : lnodes) {
                n->shard_num++;
                shard.push_back(n->node_id);
            }
        } else if (num > replica_num) {
            auto nodes = get_nodes(shard);
            auto hnodes = get_high_load_nodes(nodes, num - replica_num);
            for (auto& n : hnodes) {
                n->shard_num--;
                if (used_nodes.erase(n->node_id) != 1) {
                    throw CtlExpection("nodes error");
                }
            }
            shard.clear();
            shard = std::vector<int>(used_nodes.begin(), used_nodes.end());
        }
    }

    update_table_shard(table, shards);
    table->replica_num = replica_num;
}

void Controller::update_table(TableObject* table, Configure& conf) {
    SLOG(INFO) << "update table " << table->table_name << ":" << table->storage_id
               << "with config " << conf.dump();
    update_table(table->storage_id, conf);
}

void Controller::update_table(int32_t storage_id, Configure& conf) {
    int32_t handler_id;
    OperatorDescriptor update_opd;
    {
        TableDescriptorReader rtd;
        auto st = _client->context()->GetTableDescriptorReader(storage_id, rtd);
        if (!st) {
            throw CtlExpection(st);
        }
        auto& td = rtd.table();
        const char* update_op_key = "update_ctx";
        auto hit = td.key_to_hdl.find(update_op_key);
        if (hit == td.key_to_hdl.end()) {
            throw CtlExpection("update contex not find handler");
        }
        handler_id = hit->second;
        auto dit = td.op_descs.find(handler_id);
        if (dit == td.op_descs.end()) {
            throw CtlExpection("update contex not find op desc");
        }
        update_opd = dit->second;
    }
    // auto oit = td.handlers.find(handler_id);
    // if (oit == td.handlers.end()) {
    // throw CtlExpection("update contex not find op");
    //}

    auto op = create_op(update_opd);
    UpdateContextHandler handler(storage_id, handler_id, op, _client);
    // TODO timeout
    auto st = handler.update_context(conf, _ps_timeout);
    if (!st) {
        throw CtlExpection(st);
    }
    //_client->deregister_handler(storage_id, handler_id, _ps_timeout);
    //
}

bool Controller::clean_unused_storage() {
    auto storage_list = _client->master_client()->get_storage_list();
    for (auto& id : storage_list) {
        if (_storages.find(id) == _storages.end()) {
            if (!_client->initialize_storage(id)) {
                SLOG(WARNING) << "initialize storage " << id << " failed";
                continue;
            } else {
                TableDescriptorReader rtd;
                auto st = _client->context()->GetTableDescriptorReader(id, rtd);
                if (!st) {
                    SLOG(WARNING) << st.ToString();
                    continue;
                }
            }
            _client->delete_storage(id, _ps_timeout);
        }
    }
    return true;
}

void Controller::expand_storage_nodes(int32_t storage_id) {
    if (!_client->initialize_storage(storage_id)) {
        SLOG(WARNING) << "storage " << storage_id << " not found";
        return;
    }
    auto shards = get_table_shards(storage_id);
    for (auto shard_nodes: shards) {
        if (shard_nodes.size() != 1) {
            SLOG(WARNING) << "ignore storage " << storage_id 
                  << ": not support replica";
            return;
        }
    }

    size_t local_shard_num = 0;
    auto nodes = get_table_nodes(storage_id);
    for (auto p: nodes) {
        if (p.second.size() > local_shard_num) {
            local_shard_num = p.second.size();
        }
    }
    if (local_shard_num == 0) {
        SLOG(INFO) << "skip empty storage " << storage_id;
        return;
    }
    for (auto p: nodes) {
        if (p.second.size() != 0 && p.second.size() != local_shard_num) {
            SLOG(WARNING) << "ignore storage " << storage_id 
                  << ": local shards inconsistent";
            return;
        }
    }

    load_nodes();
    std::set<int> new_nodes;
    for (auto p: _nodes) {
        if (!nodes.count(p.first) || nodes.at(p.first).size() == 0) {
            new_nodes.insert(p.first);
        }
    }
    for (int node_id: new_nodes) {
        for (size_t i = 0; i < local_shard_num; ++i) {
            shards.push_back({node_id});
        }
    }

    Configure conf;
    gen_table_shards_config(conf, shards);
    SLOG(INFO) << "update trainer table " << storage_id
               << " with config " << conf.dump();
    update_table(storage_id, conf);
}

// TableStat* Controller::get_table_stat(int id) {
// auto it = _table_stats.find(id);
// if (it != _table_stats.end())
// return &it->second;
// TableStat* table_stat = &_table_stats[id];

// std::vector<StorageStatisticInfo> stat_info;
//_client->query_storage_info(id, &stat_info);
// for (auto& s : stat_info) {
// auto& sh = table_stat->shards[s.shard_id];
// sh.kv_cnt = s.size;
//}

// return table_stat;
//}

// void Controlle::reset_table_stat() {

//}


const std::string MODEL_HEALTH_HEALTH="health";
const std::string MODEL_HEALTH_UNHEALTH="unhealth";
const std::string MODEL_HEALTH_UNAVAILABLE="unavailiable";


std::vector<StorageStatisticInfo> Controller::query_table_info(int id) {
    std::vector<StorageStatisticInfo> stat_info;
    {
        TableDescriptorWriter wtd;
        auto st = _client->context()->GetTableDescriptorWriter(id, wtd);
        if (!st)
            throw CtlExpection(st);
        auto& td = wtd.table();
        td.storage_op = create_op(td.storage_op_desc);
    }

    auto st = _client->query_storage_info(id, &stat_info, _ps_timeout);
    if (!st) {
        SLOG(WARNING) << st.ToString();
        // throw CtlExpection(st);
    }
    return stat_info;
}

void Controller::gen_table_json(TableObject* table, PicoJsonNode& json) {
    json.add("name", table->table_name);
    if (!table->is_remote_ps()) {
        return;
    }
    json.add("table_uri", table->table_uri);
    json.add("storage_id", table->storage_id);
    auto stat_info = query_table_info(table->storage_id);
    TableDescriptorReader rtd;
    auto st = _client->context()->GetTableDescriptorReader(table->storage_id, rtd);
    if (!st) {
        json.add("status", "storage id not exist");
        return;
    }
    auto& td = rtd.table();
    std::unordered_map<int, StorageStatisticInfo> shards_kv;
    StorageStatisticInfo total_stat;
    total_stat.item_number = 0;
    total_stat.memory_usage = 0;
    for (auto& s : stat_info) {
        total_stat.memory_usage += s.memory_usage;
        total_stat.item_number += s.item_number;
        if (!shards_kv.insert({s.shard_id, s}).second)
            continue;
    }

    json.add("shards", PicoJsonNode::object());
    auto& shards_json = json.at("shards");
    for (auto& sh : td.shards) {
        std::string id_str = std::to_string(sh.first);
        shards_json.add(id_str, PicoJsonNode::object());
        auto& shard_json = shards_json.at(id_str);
        auto it = shards_kv.find(sh.first);
        if (it != shards_kv.end()) {
            shard_json.add("item", it->second.item_number);
            shard_json.add("mem", it->second.memory_usage);
        }
        shard_json.add("node_list", sh.second);
    }
    json.add("total_item", total_stat.item_number);
    json.add("total_mem", total_stat.memory_usage);

    json.add("nodes", PicoJsonNode::array());
    auto& nodes_json = json.at("nodes");
    for (auto& it : td.node_descs) {
        nodes_json.push_back(it.to_json_node());
        auto& node_json = nodes_json.at(nodes_json.size() - 1);
        auto node = get_node(it.node_id);
        if (node) {
            node_json.add("endpoint", node->info.endpoint);
        } else {
            node_json.add("endpoint", "");
        }
    }
}

void Controller::gen_node_simple_json(Node& node, PicoJsonNode& json) {
    json.add("endpoint", node.info.endpoint);
    json.add("models_cnt", node.tables.size());
}

void Controller::gen_node_json(Node& node, PicoJsonNode& json) {
    json.add("endpoint", node.info.endpoint);
    json.add("models", PicoJsonNode::object());
    auto& models_json = json.at("models");
    for (auto& it : node.tables) {
        auto& tables = it.second;
        models_json.add(it.first, PicoJsonNode::object());
        auto& model_json = models_json.at(it.first);
        model_json.add("tables", PicoJsonNode::object());
        auto& tables_json = model_json.at("tables");

        for (auto& table : tables) {
            std::string table_str = std::to_string(table.first);
            tables_json.add(table_str, PicoJsonNode::array());
            auto& table_shard_json = tables_json.at(table_str);

            auto stat_info = query_table_info(table.first);
            for (auto& stat : stat_info) {
                SLOG(INFO) << "stat: " << stat.node_id << " shared:" << stat.shard_id;
                if (stat.node_id == node.node_id) {
                    PicoJsonNode shard_node;
                    shard_node.add("shard_id", stat.shard_id);
                    shard_node.add("item", stat.item_number);
                    shard_node.add("mem", stat.memory_usage);
                    table_shard_json.push_back(shard_node);
                }
            }
        }
    }
}

}
}
}
