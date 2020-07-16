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
#include "pico-ps/native_ps/NativeLoadHandler.h"
#include "pico-ps/native_ps/NativePS.h"
#include "pico-ps/native_ps/NativePullHandler.h"
#include "pico-ps/service/Server.h"

#include "pico-ps/test/TestPredictorOps.h"
#include "pico-ps/test/TestUtils.h"

namespace paradigm4 {
namespace pico {
namespace ps {
namespace predictor {

MultiProcess* mp;

template<class HANDLER>
HANDLER test_make_handler(const char* lib_name, 
      const char* op_name, const Configure& op_config, int storage_id) {
    int32_t handler_id;
    if (0 == mp->rank()) {
        auto st = mp->client()->register_handler(
            "", lib_name, op_name, op_config, storage_id, handler_id);
        SCHECK(st.ok()) << st.ToString();
    }
    mp->bcast(handler_id, 0);
    auto op = OperatorFactory::singleton().create(lib_name, op_name, op_config);
    return HANDLER(storage_id, handler_id, op, mp->client());
}

void test_release_handler(const Handler& handler) {
    SCHECK(mp->client()->deregister_handler(handler.storage_id(), handler.handler_id()).ok());
}

int32_t test_create_storage(
      const char* lib_name, const char* op_name, const Configure& config) {
    int32_t storage_id;
    if (0 == mp->rank()) {
        SCHECK(mp->client()->create_storage(lib_name, op_name, config, storage_id).ok());
    }
    mp->bcast(storage_id, 0);
    if (0 != mp->rank()) {
        auto op = OperatorFactory::singleton().create(lib_name, op_name, config);
        mp->client()->context().get()->CreateStorage(storage_id, op, "", "", config);
        mp->client()->context().get()->UpdateRuntimeInfo(storage_id, -1);
        TableDescriptorWriter td;
        mp->client()->context().get()->GetTableDescriptorWriter(storage_id, td);
        mp->client()->pull_context_from_master_nolock(storage_id, td.table());
    }
    SLOG(INFO) << "storage id = " << storage_id << " is created.";
    mp->barrier(PICO_LINENUM);
    return storage_id;
}

void PredictorNativePSTest_Test(size_t num_process) {
    int g_secret = rand();
    std::string g_tmp_root = format_string("./.unittest_tmp/NativePSTest.%d.%d", g_secret, getpid());

    MultiProcess forks(num_process);
    mp = &forks;
    auto nodes = mp->pserver_list();
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
    YAML::Node conf;
    config.node()["op_config"] = conf;
    SLOG(INFO) << config.dump();
    int32_t storage_id = test_create_storage("TestPredictorOps", "TestStorageOperator", config);
    
    Configure op_config;
    op_config.node()["compress_algorithm"] = "snappy";
    auto push_handler = test_make_handler<PushHandler>(
          "TestPredictorOps", "TestPushOperator", op_config, storage_id);
    auto dump_handler = test_make_handler<DumpHandler>(
          "TestPredictorOps", "TestDumpOperator", op_config, storage_id);

    int32_t min_key = mp->rank() * 1000;
    int32_t max_key = (mp->rank() + 1) * 1000;

    std::vector<PTestKey> keys;
    std::vector<PTestTrainValue> vals;

    for (int32_t key = min_key; key < max_key; ++key) {
        keys.emplace_back(key);
        vals.emplace_back(key + 1, key + 2);
    }

    std::vector<PushHandler> hdls;
    push_handler.async_push(std::make_unique<SparsePushItems<PTestKey, PTestTrainValue>>(
        keys.data(), vals.data(), keys.size()));
    ASSERT_TRUE(push_handler.wait());
    mp->barrier(PICO_LINENUM);
    if (0 == mp->rank()) {
        DumpArgs dump_args(g_tmp_root + "?format=archiveline", 2, "");
        dump_handler.dump(dump_args);
        ASSERT_TRUE(dump_handler.wait());
    }
    mp->barrier(PICO_LINENUM);

    Configure pred_config;
    size_t shard_per_node = 7;
    YAML::Node node;
    for (size_t id = 0; id < shard_per_node; ++id) {
        node["shard_list"].push_back(id);
    }
    node["g_rank"] = 0;
    pred_config.node()["nodes"].push_back(node);
    pred_config.node()["op_config"] = conf;
    SLOG(INFO) << pred_config.dump();

    Configure pred_op_config;
    pred_op_config.node()["compress_algorithm"] = "snappy";
    pred_op_config.node()["block_size"] = 100;

    NativePS native_ps;
    storage_id = native_ps.create_storage(
        "TestPredictorOps", "TestPredictorStorageOperator", pred_config);
    auto load_op = OperatorFactory::singleton().create(
        "TestPredictorOps", "TestLoadOperator", pred_op_config);
    LoadHandler* load_handler
        = new NativeLoadHandler(storage_id, load_op, &native_ps);
    load_handler->load(URIConfig(g_tmp_root + "?format=archiveline"), "", 4);
    load_handler->wait();
    auto pull_op = OperatorFactory::singleton().create(
        "TestPredictorOps", "TestPullOperator", pred_op_config);
    PullHandler* pull_handler
        = new NativePullHandler(storage_id, pull_op, &native_ps);

    for (int i = 0; i < 5; ++i) {
        std::vector<TestPredictValue> pull_vals(keys.size());
        pull_handler->pull(std::make_unique<SparsePullItems<PTestKey, TestPredictValue>>(
            keys.data(), pull_vals.data(), keys.size()));
        pull_handler->wait();

        for (size_t i = 0; i < keys.size(); ++i) {
            EXPECT_EQ(keys[i] + 2, pull_vals[i].value);
        }
    }

    delete load_handler;
    delete pull_handler;

    mp->barrier(PICO_LINENUM);

    if (0 == mp->rank()) {
        FileSystem::rmrf(g_tmp_root.c_str());
    }

    /*
    auto ctx = Lemon::singleton().client()->context();
    TableDescriptorWriter td;
    ctx->GetTableDescriptorWriter(storage_id, td);
    std::string s = td.table().to_json_str();
    SLOG(INFO) << s;
    TableDescriptor new_td;
    new_td.from_json_str(s);
    SLOG(INFO) << new_td.to_json_str();
    */
}

TEST(PredictorNativePSTest, Test) {
    PredictorNativePSTest_Test(1);
    PredictorNativePSTest_Test(3);
    PredictorNativePSTest_Test(5);
    PredictorNativePSTest_Test(8);
}

}
} // namespace ps
} // namespace pico
} // namespace paradigm4

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    for (size_t i = 0; i < 10; ++i) {
        int ret = RUN_ALL_TESTS();
        if (ret != 0) {
            return ret;
        }
    }
    return 0;
}
