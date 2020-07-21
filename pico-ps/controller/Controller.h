#ifndef PARADIGM4_PICO_PS_CONTROLLER_CONTROLLER_H
#define PARADIGM4_PICO_PS_CONTROLLER_CONTROLLER_H

#include <iostream>
#include <map>
#include <set>
#include <unordered_map>
#include <vector>


#include "pico-ps/model/Model.h"
#include "pico-ps/service/Client.h"

namespace paradigm4 {
namespace pico {
namespace ps {

class CtlExpection : public std::exception {
public:
    CtlExpection(const std::string& err_str) {
        _err_str = err_str;
    }

    CtlExpection(Status& st) {
        _err_str = st.ToString();
    }

    const char* what() const noexcept override {
        return _err_str.c_str();
    }

private:
    int _err_code = 0;
    std::string _err_str;
};

struct Node {
    int node_id = -1;
    CommInfo info;
    std::map<std::string, std::map<int32_t, std::set<int32_t>>> tables;
    int shard_num = 0;
    bool running = false;
    float weight = 0.0;

    void init_weight() {
        weight = shard_num + pico_real_random<float>();
    }
};

inline bool node_compare(const Node* n1, const Node* n2) {
    if (n1->running != n2->running)
        return n1->running;
    return n1->weight < n2->weight;
}

//只能单线程使用
class Controller {
public:
    Controller(MasterClient*, Client*, int ps_timeout);

    void set_ps_timeout(int ps_timeout) {
        _ps_timeout = ps_timeout;
    }

    bool shutdown_node(int node_id);

    std::string show_node(int id);

    std::string show_nodes();

    std::string learner_is_paused();

    bool expand_nodes();

    std::string get_last_error() {
        return _err_str;
    }

// private:

    void load_nodes();

    std::vector<Node*> get_running_nodes();

    Node* get_node(int id);

    std::vector<Node*> get_nodes(const std::vector<int>& ids);

    std::vector<Node*> get_low_load_nodes(std::vector<Node*> nodes, int n);

    std::vector<Node*> get_high_load_nodes(std::vector<Node*> nodes, int n);

    std::vector<comm_rank_t> get_learner_local_ranks();

    /**************** table storage **************/

    ModelObject deep_copy(const ModelObject& model);

    // context
    void gen_table_shards_config(Configure& conf,
          const std::vector<std::vector<int>>& shards);

    std::vector<std::vector<int>> get_table_shards(int storage_id);

    std::unordered_map<int, std::vector<int32_t>> get_table_nodes(int storage_id);

    void create_table(TableObject* table);

    void delete_table(TableObject* table);

    void balance_table(TableObject* table);

    void update_table_shard(TableObject* table, int shard_num);

    void update_table_shard(TableObject* table,
          const std::vector<std::vector<int>>& shards);

    void update_table_replica(TableObject* table, int replica_num);

    void update_table(TableObject* table, Configure& conf);

    void update_table(int32_t storage_id, Configure& conf);

    bool clean_unused_storage();

    void expand_storage_nodes(int32_t storage_id);

    /**************** show info ******************/
    void gen_table_json(TableObject* table, PicoJsonNode& json);

    std::vector<StorageStatisticInfo> query_table_info(int id);

    void gen_node_simple_json(Node& node, PicoJsonNode& json);

    void gen_node_json(Node& node, PicoJsonNode& json);

// private:
    int _ps_timeout;
    ps::Client* _client = nullptr;
    core::RpcService* _rpc_service = nullptr;
    MasterClient* _master_client = nullptr;
    // std::string _hadoop_bin;

    std::map<int, Node> _nodes;
    std::unordered_set<int> _storages;
    std::string _err_str;
};


} // namespace controller
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_CONTROLLER_CONTROLLER_H
