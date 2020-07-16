#include "pico-ps/common/core.h"

#include "pico-core/MultiProcess.h"

#include "pico-ps/test/TestOps.h"
#include "pico-ps/service/Client.h"
#include "pico-ps/service/Context.h"
#include "pico-ps/service/Server.h"

namespace paradigm4 {
namespace pico {
namespace ps {

#define PICO_LINENUM (core::format_string("%d", __LINE__))

Configure test_load_config(
      const std::vector<int>& nodes, const Configure& op_config) {
    std::vector<int32_t> shard_num;
    std::vector<int32_t> id_range({0});
    for (size_t i = 0; i < nodes.size(); ++i) {
        shard_num.push_back(i + 2);
        id_range.push_back(id_range.back() + shard_num.back());
    }
    Configure config;
    for (size_t i = 0; i < nodes.size(); ++i) {
        YAML::Node node;
        for (int32_t id = id_range[i]; id < id_range[i + 1]; ++id) {
            node["shard_list"].push_back(id);
        }
        node["g_rank"] = nodes[i];
        config.node()["nodes"].push_back(node);
    }
    config.node()["op_config"] = op_config.node();
    SLOG(INFO) << config.dump();
    return config;
}

Configure test_load_new_config(
      const std::vector<int>& nodes, const Configure& op_config) {
    std::vector<int32_t> new_shard_num;
    std::vector<int32_t> new_id_range({0});
    for (size_t i = 0; i < nodes.size(); ++i) {
        new_shard_num.push_back((i + 2) / 2);
        new_id_range.push_back(new_id_range.back() + new_shard_num.back());
    }
    Configure new_config;
    for (size_t i = 0; i < nodes.size(); ++i) {
        YAML::Node node;
        for (int32_t id = new_id_range[i]; id < new_id_range[i + 1]; ++id) {
            node["shard_list"].push_back(id);
        }
        node["g_rank"] = nodes[i];
        new_config.node()["nodes"].push_back(node);
    }
    new_config.node()["op_config"] = op_config.node();
    SLOG(INFO) << new_config.dump();
    return new_config;
}

Configure test_load_pred_config(
      const std::vector<int>& nodes, const Configure& op_config) {
    Configure pred_config;
    size_t shard_per_node = std::min(nodes.size(), size_t(3));
    for (size_t i = 0; i < nodes.size(); ++i) {
        YAML::Node node;
        for (size_t id = 0; id < shard_per_node; ++id) {
            node["shard_list"].push_back((i + id) % nodes.size());
        }
        node["g_rank"] = nodes[i];
        pred_config.node()["nodes"].push_back(node);
    }
    pred_config.node()["op_config"] = op_config.node();
    SLOG(INFO) << pred_config.dump();
    return pred_config;
}

Configure test_load_new_pred_config(
      const std::vector<int>& nodes, const Configure& op_config) {
    Configure new_pred_config;
    size_t shard_per_node = 2 * std::min(nodes.size(), size_t(3));
    for (size_t i = 0; i < nodes.size(); ++i) {
        YAML::Node node;
        for (size_t id = 0; id < shard_per_node; ++id) {
            node["shard_list"].push_back((i + id) % (2 * nodes.size()));
        }
        node["g_rank"] = nodes[i];
        new_pred_config.node()["nodes"].push_back(node);
    }
    new_pred_config.node()["op_config"] = op_config.node();
    SLOG(INFO) << new_pred_config.dump();
    return new_pred_config;
}


struct MultiProcess {
public:
    MultiProcess(size_t num_process) {
        _num_process = num_process;
        _master = std::make_unique<core::Master>("127.0.0.1");
        _master->initialize();
        _mp = std::make_unique<core::MultiProcess>(num_process, "");
        _mc = std::make_unique<core::TcpMasterClient>(_master->endpoint());
        _mc->initialize();
        core::RpcConfig rpc_config;
        rpc_config.protocol = "tcp";
        rpc_config.bind_ip = "127.0.0.1";
        rpc_config.io_thread_num = 1;
        _rpc = std::make_unique<core::RpcService>();
        _rpc->initialize(_mc.get(), rpc_config);
        _server = std::make_unique<Server>(ServerConfig(), _mc.get(), _rpc.get(), "");
        _server->initialize();

        _rpc_client = _rpc->create_client(PSERVER_C2S_RPC_NAME, num_process);
        _client = std::make_unique<Client>();
        _client->initialize(_mc.get(), _rpc_client.get());
    }

    ~MultiProcess() {
        _client->finalize();
        _client.reset();
        _rpc_client.reset();
        _server->exit();    
        _server->finalize();
        _server.reset();
        _rpc->finalize();
        _mc->finalize();
        _mp.reset();
        _master->exit();
        _master->finalize();
    }

    void barrier(const std::string& name)const {
        return _mc->barrier(name, _num_process);
    }

    void bcast(int32_t& value, comm_rank_t rank) {
        ++_bcast_id;
        std::string key = std::to_string(_bcast_id);
        if (_rpc->global_rank() == rank) {
            _mc->tree_node_add(key, std::to_string(value));
        }
        barrier("bcast" + key);
        std::string val;
        _mc->tree_node_get(key, val);
        value = std::stoi(val);
    }

    std::vector<int> pserver_list() {
        std::vector<int> list;
        for (size_t i = 0; i < _num_process; ++i) {
            list.push_back(i);
        }
        return list;
    }

    comm_rank_t rank()const {
        return _rpc->global_rank();
    }

    core::RpcService* rpc()const {
        return _rpc.get();
    }

    Client* client()const {
        return _client.get();
    }
    
    size_t _num_process = 0;
    std::unique_ptr<core::Master> _master;

    std::unique_ptr<core::MultiProcess> _mp;
    std::unique_ptr<core::RpcService> _rpc;
    std::unique_ptr<core::TcpMasterClient> _mc;
    std::unique_ptr<Server> _server;
    std::unique_ptr<core::RpcClient> _rpc_client;
    std::unique_ptr<Client> _client;
    size_t _bcast_id = 0;
};

}
}
}
