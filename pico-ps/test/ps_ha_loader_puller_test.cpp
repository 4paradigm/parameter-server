#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include <algorithm>
#include <chrono>
#include <deque>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "pico-ps/common/core.h"

#include "pico-ps/handler/handlers.h"

#include "pico-ps/test/TestPredictorOps.h"
#include "pico-ps/test/TestUtils.h"

namespace paradigm4 {
namespace pico {
namespace ps {

MultiProcess* mp;

namespace predictor {

void ps_ha_loader_test() {
    Client client;
    client.initialize(mp->_mc.get(), mp->_rpc_client.get());

    auto nodes = mp->pserver_list();
    std::sort(nodes.begin(), nodes.end());
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
    Configure op_config;
    op_config.node()["compress_algorithm"] = "snappy";
    config.node()["op_config"] = op_config.node();
    SLOG(INFO) << config.dump();


    client.load_operator_library(nodes, "libTestPredictorOps.so");
    int32_t storage_id;
    auto status = client.create_storage(
        "TestPredictorOps", "TestStorageOperator", config, storage_id);
    SCHECK(status.ok()) << status.ToString();

    int push_handler_id;
    status = client.register_handler("test_model_push",
        "TestPredictorOps",
        "TestPushOperator",
        op_config,
        storage_id,
        push_handler_id);
    SCHECK(status.ok()) << status.ToString();

    int32_t dump_handler_id;
    status = client.register_handler("test_model_dump",
        "TestPredictorOps",
        "TestDumpOperator",
        op_config,
        storage_id,
        dump_handler_id);
    SCHECK(status.ok()) << status.ToString();

    int32_t min_key = 0;
    int32_t max_key = 10000;

    std::vector<int32_t> keys;
    std::vector<PTestTrainValue> vals;

    for (int32_t key = min_key; key < max_key; ++key) {
        keys.emplace_back(key);
        vals.emplace_back(key + 1, key + 2);
    }

    auto push_op = OperatorFactory::singleton().create(
        "TestPredictorOps", "TestPushOperator", op_config);
    PushHandler push_handler(storage_id, push_handler_id, push_op, &client);
    push_handler.async_push(keys.data(), vals.data(), keys.size());
    ASSERT_TRUE(push_handler.wait());

    int g_secret = rand();
    std::string g_tmp_root
        = format_string("./.unittest_tmp/TableEntryTest.%d.%d", g_secret, getpid());
    DumpArgs dump_args(g_tmp_root + "?format=archiveline", 2, "");
    auto dump_op = OperatorFactory::singleton().create(
        "TestPredictorOps", "TestDumpOperator", op_config);
    DumpHandler dump_handler(storage_id, dump_handler_id, dump_op, &client);
    dump_handler.dump(dump_args);
    ASSERT_TRUE(dump_handler.wait());
    client.delete_storage(storage_id);

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

    Configure pred_op_config;
    pred_op_config.node()["compress_algorithm"] = "snappy";
    pred_op_config.node()["block_size"] = 100;

    pred_config.node()["op_config"] = pred_op_config.node();
    SLOG(INFO) << pred_config.dump();

    status = client.create_storage(
        "TestPredictorOps", "TestPredictorStorageOperator", pred_config, storage_id);
    SCHECK(status.ok()) << status.ToString();

    int load_handler_id;
    status = client.register_handler("test_model_load",
        "TestPredictorOps",
        "TestLoadOperator",
        pred_op_config,
        storage_id,
        load_handler_id);
    SCHECK(status.ok()) << status.ToString();

    int32_t pull_handler_id;
    status = client.register_handler("test_model_pull",
        "TestPredictorOps",
        "TestPullOperator",
        pred_op_config,
        storage_id,
        pull_handler_id);
    SCHECK(status.ok()) << status.ToString();

    int32_t ctx_handler_id;
    status = client.register_handler("test_model_update",
        "TestPredictorOps",
        "TestUpdateContextOperator",
        pred_op_config,
        storage_id,
        ctx_handler_id);
    ASSERT_TRUE(status.ok()) << status.ToString();

    auto load_op = OperatorFactory::singleton().create(
        "TestPredictorOps", "TestLoadOperator", pred_op_config);
    LoadHandler load_handler(storage_id, load_handler_id, load_op, &client);
    load_handler.load(URIConfig(g_tmp_root + "?format=archiveline"));
    status = load_handler.wait();
    SCHECK(status.ok()) << status.ToString();

    client.set_table_uri(storage_id, g_tmp_root + "?format=archiveline");

    auto pull_op = OperatorFactory::singleton().create(
        "TestPredictorOps", "TestPullOperator", pred_op_config);
    PullHandler pull_handler(storage_id, pull_handler_id, pull_op, &client);

    for (int i = 0; i < 5; ++i) {
        std::vector<TestPredictValue> vals(keys.size());
        pull_handler.pull(keys.data(), vals.data(), keys.size());
        ASSERT_TRUE(pull_handler.wait());

        for (size_t i = 0; i < keys.size(); ++i) {
            EXPECT_EQ(keys[i] + 2, vals[i].value);
        }
    }
    client.finalize();
}

void ps_ha_puller_test() {
    constexpr int timeout = 2000;
    Client client;
    client.initialize(mp->_mc.get(), mp->_rpc_client.get());

    int32_t min_key = 0;
    int32_t max_key = 10000;

    std::vector<PTestKey> keys;
    std::vector<PTestTrainValue> vals;

    for (int32_t key = min_key; key < max_key; ++key) {
        keys.emplace_back(key);
        vals.emplace_back(key + 1, key + 2);
    }

    Configure op_config;
    op_config.node()["compress_algorithm"] = "snappy";

    int32_t pull_handler_id = 3;
    int32_t storage_id = 1;
    SCHECK(client.initialize_storage(storage_id));
    auto pull_op = OperatorFactory::singleton().create(
        "TestPredictorOps", "TestPullOperator", op_config);
    PullHandler pull_handler(storage_id, pull_handler_id, pull_op, &client);

    for (int i = 0; i < 10; ++i) {
        SLOG(INFO) << "try to pull for " << i << " -th time";
        std::vector<TestPredictValue> vals(keys.size());
        pull_handler.pull(keys.data(), vals.data(), keys.size(), timeout);
        auto status = pull_handler.wait();
    }
}

void HA_Load_Pull_Test(size_t num_process) {
    MultiProcess forks(num_process);
    mp = &forks;
    if (0 == mp->rank()) {
        ps_ha_loader_test();
    }
    mp->barrier(PICO_LINENUM);
    ps_ha_puller_test();
}

TEST(HA_Load_Pull, Test) {
    for (size_t i = 0; i < 3; ++i) {
        HA_Load_Pull_Test(1);
        HA_Load_Pull_Test(3);
        HA_Load_Pull_Test(5);
        HA_Load_Pull_Test(8);
    }
}

}
} // namespace ps
} // namespace pico
} // namespace paradigm4

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
