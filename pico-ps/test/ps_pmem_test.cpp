#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include <fstream>
#include <gtest/gtest.h>

#include "pico-ps/common/core.h"
#include "pico-ps/storage/DCPmemory.h"

#include "pico-ps/test/TestUtils.h"

namespace paradigm4 {
namespace pico {
namespace ps {

typedef PmemHashMapShardStorage<TestKey, 
      TestPredictorValue> TestPmemPredictorStorageType;

typedef BaseKVTextFileRestoreOperator<TestDsReader<TestKV>,
        TestKV, 
        TestKey, 
        TestPredictorValue,
        DefaultPartitioner<TestKey>, 
        TestPmemPredictorStorageType> pmem_sparse_restore_op_type;

class TestPmemPredictorRestoreOperator: public pmem_sparse_restore_op_type {
public:
    TestPmemPredictorRestoreOperator(const Configure& conf): pmem_sparse_restore_op_type(conf) {}
    virtual void deserialize_kv(TestKV& kv, 
          key_type& key, 
          value_type& value) override {
        key = kv.key;
        value.sum = kv.value.sum;
    }
};

typedef BaseKVTextFileLoadOperator<TestDsReader<TestKV>,
      TestKV,
      TestKey,
      TestPredictorValue,
      DefaultPartitioner<TestKey>,
      TestPmemPredictorStorageType> pmem_sparse_load_op_type;

class TestPmemPredictorLoadOperator : public pmem_sparse_load_op_type {
public:
    TestPmemPredictorLoadOperator(const Configure& conf): pmem_sparse_load_op_type(conf) {}
    virtual void deserialize_kv(TestKV& kv,
          key_type& key,
          value_type& push_val) override {
        key = kv.key;
        push_val.sum = kv.value.sum;
    }
};

typedef SparseTablePullOperator<TestKey,
      TestPredictorValue, int64_t, TestPullValue,
      DefaultPartitioner<TestKey>, TestPmemPredictorStorageType> pmem_sparse_predictor_pull_op_type;

class TestPmemPredictorPullOperator : public pmem_sparse_predictor_pull_op_type {
public:
    TestPmemPredictorPullOperator(const Configure& config) : pmem_sparse_predictor_pull_op_type(config) {}
    void fetch_pull_value(const key_type&, const value_type& s, pull_value_type& p) override {
        p = {s.sum};
    }
    void store_pull_value(const key_type&,
          const pull_value_type& p,
          pull_arg_type& arg) override {
        arg = p.value;
    }
};

typedef ShardStorageOperator<TestPmemPredictorStorageType, 
      TestPmemPredictorRestoreOperator> TestPmemPredictorStorageOperator;

typedef ShardUpdateContextOperator<TestKey, 
      TestPredictorValue, 
      DefaultPartitioner<TestKey>, 
      TestPmemPredictorStorageType> TestPmemPredictorUpdateContextOperator;
                                 
REGISTER_OPERATOR(TestOps, TestPmemPredictorRestoreOperator);
REGISTER_OPERATOR(TestOps, TestPmemPredictorStorageOperator);
REGISTER_OPERATOR(TestOps, TestPmemPredictorLoadOperator);
REGISTER_OPERATOR(TestOps, TestPmemPredictorPullOperator);
REGISTER_OPERATOR(TestOps, TestPmemPredictorUpdateContextOperator);

std::string pmem_pool_path = "/mnt/pmem0/ps_pmem_test";
std::string pmem_pool_storage_1_path = "/mnt/pmem0/ps_pmem_test/storage_pool_set.1";
std::string model_uri = "./.unittest_tmp/ServiceTest.0.1?format=archiveline";
std::string server_node_id_path = "./.unittest_tmp/pserver";

static constexpr size_t NUM_KEYS = 1 << 10;
static constexpr size_t NUM_REPS = 1 << 10;
static constexpr size_t META_POOL_SIZE = 100 << 20;
static constexpr size_t MAXIMUM_STORAGE_POOL_SIZE = 100 << 20;
static constexpr size_t KILL_MS = 1000;
static constexpr size_t KILL_NUM_PSERVERS = 1;
static constexpr size_t NUM_PSERVERS = 3;

void PmemHashMap_test() {
    DCPmemory::singleton().remove_storage(1);
    storage_pool_t storage_pool;
    EXPECT_TRUE(DCPmemory::singleton().get_storage_pool_or_create(1, storage_pool));
    bool healthy = true;
    PmemHashMapHandle<int, int> mp(storage_pool);
    PmemHashMapHandle<int, int>::accessor it;
    EXPECT_TRUE(safe_insert(mp, 0, 2, healthy));
    EXPECT_FALSE(!safe_insert(mp, 0, 3, healthy));
    EXPECT_FALSE(lock_find(mp, 1, &it));
    EXPECT_TRUE(lock_find(mp, 0, &it));
    EXPECT_EQ(it->second, 2);
    it->second = 3;
    EXPECT_TRUE(safe_insert(mp, 1, 4, healthy));
    EXPECT_TRUE(safe_insert(mp, 2, 5, healthy));
    EXPECT_TRUE(safe_set(mp, 3, 6, healthy));
    EXPECT_TRUE(safe_set(mp, 4, 7, healthy));
    EXPECT_FALSE(lock_find(mp, 5, &it));
    std::vector<int> A;
    for (auto& pair: mp) {
        A.push_back(pair.second);
    }
    std::sort(A.begin(), A.end());
    std::vector<int> B = {3, 4, 5, 6, 7};
    EXPECT_EQ(A, B);
    EXPECT_TRUE(safe_set(mp, 3, 6666, healthy));
    EXPECT_TRUE(lock_find(mp, 3, &it));
    EXPECT_EQ(it->second, 6666);

    // 需要把 accesor 的 lock release 才可以 erase。
    it.release();

    safe_erase(mp, 3);
    EXPECT_FALSE(lock_find(mp, 3, &it));

    A.clear();
    for (auto& pair: mp) {
        A.push_back(pair.second);
    }
    std::sort(A.begin(), A.end());
    B = {3, 4, 5, 7};
    EXPECT_EQ(A, B);
}

void PmemhashMap_councrrent_erase(PmemHashMapHandle<int, int>* mp, int key_start, int key_end) {
    for (int key = key_start; key < key_end; ++key) {
        safe_erase(*mp, key);
    }
}

void PmemHashMap_concurrent_erase_test() {
    DCPmemory::singleton().remove_storage(1);
    storage_pool_t storage_pool;
    EXPECT_TRUE(DCPmemory::singleton().get_storage_pool_or_create(1, storage_pool));
    bool healthy = true;
    PmemHashMapHandle<int, int> mp(storage_pool);

    for (int round = 0; round < 10; ++round) {
        for (int i = 0; i < 100; ++i) {
            EXPECT_TRUE(safe_insert(mp, i, i, healthy));
        }
        EXPECT_EQ(mp.size(), 100);

        std::vector<std::thread> ths;
        for (int i = 0; i < 10; ++i) {
            ths.emplace_back(PmemhashMap_councrrent_erase, &mp, i * 10, 20 + i * 10);
        }

        for (auto& th : ths) {
            th.join();
        }

        PmemHashMapHandle<int, int>::accessor it;
        for (int i = 0; i < 100; ++i) {
            EXPECT_FALSE(lock_find(mp, i, &it));
        }
        EXPECT_EQ(mp.size(), 0);
    }
}

struct TestMultiValue {
    size_t val[16];
    PICO_SERIALIZE(val);
    TestMultiValue() {
        for (auto& x: val) x = 0;
    }
};

void PmemHashMap_consistency() {
    bool healthy = true;
    Configure config;
    config.node()["pmem_create_storage"]["storage_id"] = 0;
    config.node()["pmem_create_storage"]["retain_dcpmm_data_after_deconstruction"] = 1;
    PmemHashMapShardStorage<int, TestMultiValue> storage({0}, config);
    PmemHashMapHandle<int, TestMultiValue>& mp = boost::any_cast<PmemHashMapHandle<int, TestMultiValue>&>(storage[0]->data);
    while (true) {
        if (!mp.empty()) {
            size_t i = mp.size() - 1;
            size_t key = i * 107;
            PmemHashMapHandle<int, TestMultiValue>::accessor it;
            EXPECT_TRUE(lock_find(mp, key, &it));
            ++it->second.val[0];
            while (it->second.val[1] < i * NUM_REPS) {
                lock_find(mp, i, &it);
                lock_find(mp, key, &it);
                for (size_t j = 1; j < 16; j++) {
                    it->second.val[j] += i;
                }
            }
        }
        SLOG(INFO) << mp.size();
        if (mp.size() < NUM_KEYS) {
            EXPECT_TRUE(safe_insert(mp, 107 * mp.size(), TestMultiValue(), healthy));
        } else {
            break;
        }
    }
}

size_t PmemHashMap_consistency_check() {
    Configure config;
    config.node()["pmem_create_storage"]["storage_id"] = 0;
    PmemHashMapShardStorage<int, TestMultiValue> storage({0}, config);
    PmemHashMapHandle<int, TestMultiValue>& mp = boost::any_cast<PmemHashMapHandle<int, TestMultiValue>&>(storage[0]->data);
    EXPECT_EQ(mp.size(), NUM_KEYS); 
    size_t kill_accessor = 0;
    for (auto& item: mp) {
        kill_accessor += item.second.val[0];
        for (size_t i = 1; i < 16; ++i) {
            EXPECT_EQ(item.second.val[i], item.first / 107 * NUM_REPS);
        }
    }
    return kill_accessor;
}

int PmemHashMap_consistency_exec_kill(int ms) {
    pid_t pid = fork();
    SCHECK(pid >= 0);
    if (pid == 0) {
        DCPmemory::singleton().initialize(pmem_pool_path, META_POOL_SIZE, MAXIMUM_STORAGE_POOL_SIZE);
        PmemHashMap_consistency();
        DCPmemory::singleton().finalize();
        exit(0);
    } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        SLOG(INFO) << "kill " << pid;
        kill(pid, SIGKILL);
        int status;
        pid = waitpid(pid, &status, 0);
        return status;
    }
}


class FakeRpc {
public:
    FakeRpc(std::string master_endpoint) {
        auto master_ep = master_endpoint;
        _mc = std::make_unique<TcpMasterClient>(master_ep);
        _mc->initialize();
        core::RpcConfig rpc_config;
        rpc_config.protocol = "tcp";
        rpc_config.bind_ip = "127.0.0.1";
        rpc_config.io_thread_num = 1;
        _rpc = std::make_unique<RpcService>();
        _rpc->initialize(_mc.get(), rpc_config);
    }

    ~FakeRpc() {
        _rpc->finalize();
        _mc->finalize();
    }

    RpcService* rpc() {
        return _rpc.get();
    }

    std::unique_ptr<RpcService> _rpc;
    std::unique_ptr<TcpMasterClient> _mc;
};


void touch_file(const std::string& file_path, const std::string& file_name, const std::string& content) {
    URIConfig file_path_uri(file_path);
    core::FileSystem::mkdir_p(file_path_uri);

    std::ofstream ofile(file_path + "/" + file_name);
    ofile << content;
    ofile.close();
}

void PmemHashMap_server(std::string master_endpoint, int pool_id, int round) {
    FakeRpc rpc(master_endpoint);
    DCPmemory::singleton().initialize(pmem_pool_path + std::to_string(pool_id), META_POOL_SIZE, MAXIMUM_STORAGE_POOL_SIZE);
    Server server(ServerConfig(), rpc._mc.get(), rpc._rpc.get(), "hdfs dfs");
    core::Logger::singleton().set_id(std::to_string(server.node_id()));
    auto& node_id = DCPmemory::singleton().get_meta_pool().root()->node_id;
    // if (node_id.get_ro() >= 0) {
    SLOG(INFO) << "DCPMM restoring dead node " << node_id.get_ro();
    server.restore_storages(true, node_id.get_ro());
    // }
    pmem::obj::transaction::run(DCPmemory::singleton().get_meta_pool(), [&] {
        node_id.get_rw() = server.node_id();
    });
    SLOG(INFO) << "starting node " << server.node_id();
    server.initialize();
    touch_file(server_node_id_path + "/" + std::to_string(round),
        std::to_string(server.node_id()), std::to_string(server.node_id()));
    server.finalize();
    DCPmemory::singleton().finalize();
}

struct PmemServer {
    PmemServer(std::string master_endpoint, int dead_node, int round) {
        _pid = fork();
        SCHECK(_pid >= 0);
        if (_pid == 0) {
            PmemHashMap_server(master_endpoint, dead_node, round);
            exit(0);
        }
    }

    PmemServer(const PmemServer&) = delete;
    PmemServer& operator=(const PmemServer&) = delete;
    PmemServer(PmemServer&& other) {
        _pid = other._pid;
        other._pid = 0;
    }
    PmemServer& operator=(PmemServer&& other) {
        _pid = other._pid;
        other._pid = 0;
        return *this;
    }
    ~PmemServer() {
        if (_pid > 0) {
            SLOG(INFO) << "kill " << _pid;
            kill(_pid, SIGKILL);
            int status;
            _pid = waitpid(_pid, &status, 0);
        }
    }
    pid_t _pid;
};

template<class HANDLER>
HANDLER test_make_handler(Client* client, const char* lib_name, 
      const char* op_name, const Configure& op_config, int storage_id) {
    int32_t handler_id;
    client->register_handler("", lib_name, op_name, op_config, storage_id, handler_id);
    auto op = OperatorFactory::singleton().create(lib_name, op_name, op_config);
    return HANDLER(storage_id, handler_id, op, client);
}

void wait_pserver_ready(const std::string& file_path, size_t wait_number) {
    do {
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        URIConfig uri(file_path);
        auto ls = core::FileSystem::get_file_list(uri);
        if (ls.size() == wait_number) {
            break;
        } else {
            SLOG(INFO) << "Current ready #node: " << ls.size() << ", expect: " << wait_number << ". Try again later...";
            std::this_thread::sleep_for(std::chrono::milliseconds(5000));
        }
    } while(true);
}

void PmemHashMap_pservice(std::string master_endpoint) {
    int sid = 0;
    std::vector<int> nodes;
    std::vector<PmemServer> servers;
    for (size_t i = 0; i < NUM_PSERVERS; ++i) {
        nodes.push_back(sid++);
        servers.emplace_back(master_endpoint, i, -1);
    }

    FakeRpc rpc(master_endpoint);
    std::unique_ptr<core::RpcClient> rpc_client = rpc._rpc->create_client(
          PSERVER_C2S_RPC_NAME, NUM_PSERVERS);
    std::unique_ptr<Client> client = std::make_unique<Client>();
    client->initialize(rpc._mc.get(), rpc_client.get());
    Configure op_config;
    op_config.node()["block_size"] = 100;
    op_config.node()["block_serialized_size"] = 100000;
    op_config.node()["compress_algorithm"] = "";
    int32_t storage_id;
    {
        LOG(INFO) << "Start to create in-memory storage...";
        // new_config  ---update-context-->  config
        client->create_storage("TestOps", "TestStorageOperator",
              test_load_new_config(nodes, op_config), storage_id);
        int32_t in_mem_storage_id = storage_id;
        LOG(INFO) << "Finished to create in-memory storage, id: " << in_mem_storage_id;
        auto push_handler = test_make_handler<PushHandler>(
            client.get(), "TestOps", "TestPushOperator", op_config, storage_id);
        std::vector<TestKey> keys;
        std::vector<int64_t> push_args;
        for (size_t key = 0; key < NUM_KEYS; ++key) {
            keys.emplace_back(key * 2);
            push_args.emplace_back(key);
        }
        LOG(INFO) << "Start to push key-values...";
        push_handler.async_push(keys.data(), push_args.data(), keys.size());
        push_handler.wait();
        push_handler.async_push(keys.data(), push_args.data(), keys.size());
        push_handler.wait();
        push_handler.async_push(keys.data(), push_args.data(), keys.size());
        push_handler.wait();
        LOG(INFO) << "Finished to push key-values...";

        LOG(INFO) << "Start to dump key-values...";
        auto dump_handler = test_make_handler<DumpHandler>(client.get(),
            "TestOps", "TestDumpOperator", op_config, storage_id);
        DumpArgs dump_args(model_uri, 2, "");
        dump_handler.dump(dump_args);
        dump_handler.wait();
        LOG(INFO) << "Finished to dump key-values...";

        LOG(INFO) << "Start to create dcpmm storage...";
        client->create_storage("TestOps", "TestPmemPredictorStorageOperator",
              test_load_new_config(nodes, op_config), storage_id);
        client->set_table_uri(storage_id, model_uri);
        LOG(INFO) << "Finished to create dcpmm storage...";
        LOG(INFO) << "Start to load key-values...";
        auto load_handler = test_make_handler<LoadHandler>(client.get(),
            "TestOps", "TestPmemPredictorLoadOperator", op_config, storage_id);
        load_handler.load(model_uri);
        load_handler.wait();
        LOG(INFO) << "Finished to load key-values...";
        auto pull_handler = test_make_handler<PullHandler>(client.get(),
            "TestOps", "TestPmemPredictorPullOperator", op_config, storage_id);
        for (size_t i = 0; i < NUM_KEYS; ++i) {
            keys[i] = i;
            push_args[i] = i % 2 ? 0 : i / 2 * 3;
        }
        
        std::vector<int64_t> pull_args(keys.size(), -3);
        pull_handler.pull(keys, pull_args.data());
        pull_handler.wait();
        EXPECT_EQ(pull_args, push_args);
        LOG(INFO) << "Check dcpmm storage content passed...";

        LOG(INFO) << "Start to update context";
        auto ctx_handler = test_make_handler<UpdateContextHandler>(client.get(),
            "TestOps", "TestPmemPredictorUpdateContextOperator", op_config, storage_id);
        ctx_handler.update_context(test_load_config(nodes, op_config));
        LOG(INFO) << "Finished to update context";
        pull_args = std::vector<int64_t>(keys.size(), -3);
        pull_handler.pull(keys, pull_args.data());
        pull_handler.wait();
        EXPECT_EQ(pull_args, push_args);
        LOG(INFO) << "Check dcpmm storage content after shuffle passed...";
        client->delete_storage(in_mem_storage_id);

        // 测试dead node恢复
        size_t max_round = 15;
        for (size_t k = 0; k < max_round; ++k) {
            if (k == 11) {
                // kill 某几个 pserver，测试使用网络方式恢复。
                SLOG(INFO) << "Round " << k << " start, kill last " << KILL_NUM_PSERVERS
                    << " out of all " << NUM_PSERVERS << " servers";
                for (size_t i = NUM_PSERVERS - KILL_NUM_PSERVERS; i < NUM_PSERVERS; ++i) {
                    nodes.pop_back();
                    servers.pop_back();
                    core::FileSystem::rmrf(pmem_pool_path + std::to_string(i));
                    SLOG(INFO) << "removed pool " << i;
                }
                for (size_t i = 0; i <  NUM_PSERVERS - KILL_NUM_PSERVERS; ++i) {
                    touch_file(server_node_id_path + "/" + std::to_string(k),
                        std::to_string(i), std::to_string(i));
                    ++sid;
                }
                for (size_t i = NUM_PSERVERS - KILL_NUM_PSERVERS; i < NUM_PSERVERS; ++i) {
                    nodes.push_back(sid++);
                    servers.emplace_back(master_endpoint, i, k);
                }
            } else {
                SLOG(INFO) << "Round " << k << " start, kill all servers";
                nodes.clear();
                servers.clear();
                
                if (k == 9) {
                    // 将本地 dcpmm 删除，测试通过fs方式恢复。
                    SLOG(INFO) << "At round " << k << " use network-based restore";
                    for (size_t i = 0; i < NUM_PSERVERS; ++i) {
                        core::FileSystem::rmrf(pmem_pool_path + std::to_string(i));
                    }
                }

                for (size_t i = 0; i < NUM_PSERVERS; ++i) {
                    SLOG(INFO) << "to restore pool " << i;
                    nodes.push_back(sid++);
                    servers.emplace_back(master_endpoint, i, k);
                }
            }

            wait_pserver_ready(server_node_id_path + "/" + std::to_string(k), NUM_PSERVERS);
            do {
                SLOG(INFO) << "trying pull... ";
                pull_args = std::vector<int64_t>(keys.size(), -3);
                pull_handler.pull(keys, pull_args.data(), 100);
            } while (!pull_handler.wait().ok());
            EXPECT_EQ(pull_args, push_args);
            

            if (k == 3) {
                SLOG(INFO) << "At round " << k << " try to shuffle";
                ctx_handler = test_make_handler<UpdateContextHandler>(client.get(),
                      "TestOps", "TestPmemPredictorUpdateContextOperator", op_config, storage_id);
                ctx_handler.update_context(test_load_new_pred_config(nodes, op_config));
            }

            if (k == 7) {
                SLOG(INFO) << "At round " << k << " try to shuffle";
                ctx_handler = test_make_handler<UpdateContextHandler>(client.get(),
                      "TestOps", "TestPmemPredictorUpdateContextOperator", op_config, storage_id);
                ctx_handler.update_context(test_load_pred_config(nodes, op_config));
            }
            SLOG(INFO) << "pull test round " << k << " finished";
        }

        client->clear_storage(storage_id);
        for (size_t i = 0; i < NUM_KEYS; ++i) {
            keys[i] = i;
            push_args[i] = 0;
        }
        pull_handler.pull(keys, pull_args.data(), 100);
        pull_handler.wait();
        EXPECT_EQ(pull_args, push_args);
    }
    client->finalize();
    rpc_client.reset();
};


TEST(PmemHashMapHandle, test) {
    DCPmemory::singleton().initialize(pmem_pool_path, META_POOL_SIZE, MAXIMUM_STORAGE_POOL_SIZE);
    PmemHashMap_test();
    DCPmemory::singleton().finalize();
    core::FileSystem::rmrf(pmem_pool_path);
}

TEST(PmemHashMapHandle, concurrent_erase_test) {
    DCPmemory::singleton().initialize(pmem_pool_path, META_POOL_SIZE, MAXIMUM_STORAGE_POOL_SIZE);
    PmemHashMap_concurrent_erase_test();
    DCPmemory::singleton().finalize();
    core::FileSystem::rmrf(pmem_pool_path);
}

TEST(PmemHashMapHandle, consistency) {
    size_t count = 0;
    size_t ms = rand() % KILL_MS;
    while (PmemHashMap_consistency_exec_kill(ms) != 0) {
        ms = rand() % KILL_MS;
        ++count;
    }
    DCPmemory::singleton().initialize(pmem_pool_path, META_POOL_SIZE, MAXIMUM_STORAGE_POOL_SIZE);
    size_t count2 = PmemHashMap_consistency_check();
    DCPmemory::singleton().finalize();
    SLOG(INFO) << "kill: " << count;
    SLOG(INFO) << "kill accessor: " << count2 - NUM_KEYS;
    core::FileSystem::rmrf(pmem_pool_path);
}

TEST(PmemHashMapHandle, pservice) {
    core::Master master("127.0.0.1");
    master.initialize();
    PmemHashMap_pservice(master.endpoint());
    master.exit();
    master.finalize();
    for (size_t i = 0; i < NUM_PSERVERS; ++i) {
        core::FileSystem::rmrf(pmem_pool_path + std::to_string(i));
    }
    core::FileSystem::rmrf(pmem_pool_path);
    core::FileSystem::rmrf(model_uri);
    core::FileSystem::rmrf(server_node_id_path);
}

}
} // namespace pico
} // namespace paradigm4

int main(int argc, char* argv[]) {
    google::InstallFailureSignalHandler();
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

