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
#include "pico-ps/test/TestUtils.h"

namespace paradigm4 {
namespace pico {
namespace ps {

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

int test_query_storage_size(int32_t storage_id) {
    std::vector<StorageStatisticInfo> info;
    mp->client()->query_storage_info(storage_id, &info);
    std::sort(info.begin(), info.end());
    int size = 0;
    for (auto& item : info) {
        size += item.item_number;
    }
    return size;
}

void test_sparse(size_t num_process, const char* compress) {
    MultiProcess forks(num_process);
    mp = &forks;
    mp->barrier(PICO_LINENUM);
    auto nodes = mp->pserver_list();
    std::sort(nodes.begin(), nodes.end());

    Configure op_config;
    op_config.node()["block_size"] = 100;
    op_config.node()["block_serialized_size"] = 100000;
    op_config.node()["compress_algorithm"] = compress;
    Configure config = test_load_config(nodes, op_config);

    // async, push, push, store, dump
    int32_t storage_id = test_create_storage("TestOps", "TestStorageOperator", config);
    int32_t min_key = mp->rank() * TEST_KEY_NUM;
    int32_t max_key = (mp->rank() + 1) * TEST_KEY_NUM;

    std::vector<TestKey> keys;
    std::vector<int64_t> push_args;
    for (int32_t key = min_key; key < max_key; ++key) {
        keys.emplace_back(key);
        push_args.emplace_back(key);
    }
    auto push_handler = test_make_handler<PushHandler>(
          "TestOps", "TestPushOperator", op_config, storage_id);

    push_handler.async_push(keys.data(), push_args.data(), keys.size());
    ASSERT_TRUE(push_handler.wait());
    push_handler.sync_push(keys.data(), push_args.data(), keys.size());
    ASSERT_TRUE(push_handler.wait());
    if ((mp->rank() % 2) == 0) {
        push_handler.sync_push(keys, push_args);
        ASSERT_TRUE(push_handler.wait());
    }

    mp->barrier(PICO_LINENUM);
    auto dump_handler = test_make_handler<DumpHandler>(
          "TestOps", "TestDumpOperator", op_config, storage_id);
    int g_secret = rand();
    std::string g_tmp_root = format_string(
          "./.unittest_tmp/ServiceTest.%d.%d", g_secret, getpid());
    std::string old_ctx;
    if (0 == mp->rank()) {
        push_handler.store();
        ASSERT_TRUE(push_handler.wait());
        EXPECT_EQ(TEST_KEY_NUM * num_process, test_query_storage_size(storage_id));

        DumpArgs dump_args(g_tmp_root + "?format=archiveline", 2, "");
        dump_handler.dump(dump_args);
        ASSERT_TRUE(dump_handler.wait());
        old_ctx = mp->client()->get_current_ctx(storage_id);
        SLOG(INFO)<<old_ctx;

        test_release_handler(dump_handler);
        test_release_handler(push_handler);
        SCHECK(mp->client()->delete_storage(storage_id).ok());
    }

    mp->barrier(PICO_LINENUM);
    Configure new_config = test_load_new_config(nodes, op_config);
    storage_id = test_create_storage("TestOps", "TestStorageOperator", new_config);

    SLOG(INFO) << "storage id = " << storage_id << " is created.";
    auto load_handler = test_make_handler<LoadHandler>(
          "TestOps", "TestLoadOperator", op_config, storage_id);
    auto for_each_handler = test_make_handler<ForEachHandler>(
        "TestOps", "TestForEachOperator", op_config, storage_id);
    auto erase_if_handler = test_make_handler<EraseIfHandler>(
        "TestOps", "TestEraseIfOperator", op_config, storage_id);

    if (0 == mp->rank()) {
        // create another storage for load, pull, eraseif & foreach
        load_handler.load(URIConfig(g_tmp_root + "?format=archiveline"));
        ASSERT_TRUE(load_handler.wait());
        mp->client()->set_ctx(storage_id, old_ctx);
        SCHECK(mp->client()->clear_storage(storage_id).ok());
        EXPECT_EQ(0, test_query_storage_size(storage_id));
        load_handler.restore(URIConfig(g_tmp_root + "?format=archiveline"));
        ASSERT_TRUE(load_handler.wait());
        for_each_handler.for_each();
        ASSERT_TRUE(for_each_handler.wait());
        auto count = for_each_handler.sync_for_each<TestForEachResult>();
        EXPECT_EQ(int64_t(TEST_KEY_NUM * num_process), count.num);

        erase_if_handler.erase_if();
        ASSERT_TRUE(erase_if_handler.wait());
        count = for_each_handler.sync_for_each<TestForEachResult>();
        EXPECT_EQ(int64_t(TEST_KEY_NUM * num_process / 2), count.num);
        EXPECT_EQ(TEST_KEY_NUM * num_process / 2, test_query_storage_size(storage_id));

        test_release_handler(load_handler);
        test_release_handler(for_each_handler);
        test_release_handler(erase_if_handler);
    }

    mp->barrier(PICO_LINENUM);
    auto pull_handler = test_make_handler<PullHandler>(
        "TestOps", "TestPullOperator", op_config, storage_id);
    std::vector<int64_t> pull_args(keys.size(), -3);
    pull_handler.pull(std::make_unique<SparsePullItems<TestKey, int64_t>>(
          keys.data(), pull_args.data(), keys.size()));
    ASSERT_TRUE(pull_handler.wait());

    for (size_t i = 0; i < keys.size(); ++i) {
        int32_t rank = keys[i] / TEST_KEY_NUM;
        EXPECT_EQ(int64_t(keys[i] & 1 ? 0 : keys[i] * (2 + ((rank % 2) == 0))), pull_args[i]);
    }

    mp->barrier(PICO_LINENUM);
    Configure pred_config = test_load_pred_config(nodes, op_config);
    storage_id = test_create_storage("TestOps", "TestPredictorStorageOperator", pred_config);
    load_handler = test_make_handler<LoadHandler>(
          "TestOps", "TestPredictorLoadOperator", op_config, storage_id);
    
    if (0 == mp->rank()) {
        load_handler.load(URIConfig(g_tmp_root + "?format=archiveline"));
        ASSERT_TRUE(load_handler.wait());
        test_release_handler(load_handler);
    }
    mp->barrier(PICO_LINENUM);
    
    size_t replica_num = std::min(nodes.size(), size_t(3));
    EXPECT_EQ(int(TEST_KEY_NUM * num_process * replica_num), test_query_storage_size(storage_id));
    pull_handler =  test_make_handler<PullHandler>(
          "TestOps", "TestPredictorPullOperator", op_config, storage_id);

    for (int i = 0; i < 5; ++i) {
        std::vector<int64_t> pull_vals(keys.size());
        pull_handler.pull(keys.data(), pull_vals.data(), keys.size());
        ASSERT_TRUE(pull_handler.wait());

        for (size_t i = 0; i < keys.size(); ++i) {
            int32_t rank = keys[i] / TEST_KEY_NUM;
            EXPECT_EQ(int64_t(keys[i] * (2 + ((rank % 2) == 0))), pull_vals[i]);
        }
    }

    mp->barrier(PICO_LINENUM);
    auto ctx_handler = test_make_handler<UpdateContextHandler>(
          "TestOps", "TestPredictorUpdateContextOperator", op_config, storage_id);
    if (0 == mp->rank()) {
        Configure new_pred_config = test_load_new_pred_config(nodes, op_config);
        auto status = ctx_handler.update_context(new_pred_config);
        ASSERT_TRUE(status.ok()) << status.ToString();
        test_release_handler(ctx_handler);
    }

    for (int i = 0; i < 5; ++i) {
        std::vector<int64_t> vals(keys.size());
        ASSERT_TRUE(pull_handler.pull_with_auto_retry(keys, vals.data()));

        for (size_t i = 0; i < keys.size(); ++i) {
            int32_t rank = keys[i] / TEST_KEY_NUM;
            EXPECT_EQ(int64_t(keys[i] * (2 + ((rank % 2) == 0))), vals[i]);
        }
    }

    if (0 == mp->rank()) {
        FileSystem::rmrf(g_tmp_root.c_str());
    }
    mp->barrier(PICO_LINENUM);
}

void test_dense(size_t num_process, const char* compress) {
    MultiProcess forks(num_process);
    mp = &forks;
    auto nodes = mp->pserver_list();
    std::sort(nodes.begin(), nodes.end());

    Configure op_config;
    op_config.node()["block_size"] = 100;
    op_config.node()["compress_algorithm"] = compress;
    Configure config = test_load_config(nodes, op_config);

    typedef DensePushItems<dense_key_t, std::vector<int64_t>> dense_push_item_t;
    typedef DensePullItems<dense_key_t, std::vector<int64_t>> dense_pull_item_t;

    // async, push, push, store, dump
    int32_t storage_id = test_create_storage("TestOps", "TestDenseStorageOperator", config);
    int32_t min_key = mp->rank() * 100;
    int32_t max_key = (mp->rank() + 1) * 100;

    auto push_handler = test_make_handler<PushHandler>(
          "TestOps", "TestDensePushOperator", op_config, storage_id);
    for (int32_t key = min_key; key < max_key; ++key) {
        dense_key_t dense_key(key);
        std::vector<int64_t> push_arg(key);
        for (int32_t i = 0; i < key; i++) {
            push_arg[i] = key + i;
        }

        push_handler.async_push(
              std::make_unique<dense_push_item_t>(dense_key, &push_arg));
        ASSERT_TRUE(push_handler.wait());
        push_handler.sync_push(
              std::make_unique<dense_push_item_t>(dense_key, &push_arg));
        ASSERT_TRUE(push_handler.wait());
    }
    mp->barrier(PICO_LINENUM);
    if (0 == mp->rank()) {
        push_handler.store();
        ASSERT_TRUE(push_handler.wait());
    }
    mp->barrier(PICO_LINENUM);

    for (int32_t key = min_key; key < max_key; ++key) {
        dense_key_t dense_key(key);
        std::vector<int64_t> push_arg(key);
        for (int32_t i = 0; i < key; i++) {
            push_arg[i] = key + i;
        }
        if ((mp->rank() % 2) == 0) {
            push_handler.sync_push(
                  std::make_unique<dense_push_item_t>(dense_key, &push_arg));
            ASSERT_TRUE(push_handler.wait());
        }
    }

    auto ctx_handler = test_make_handler<UpdateContextHandler>(
          "TestOps", "TestDenseUpdateContextOperator", op_config, storage_id);
    if (0 == mp->rank()) {
        push_handler.store();
        ASSERT_TRUE(push_handler.wait());
        Configure new_config = test_load_new_config(nodes, op_config);
        auto status = ctx_handler.update_context(new_config);
        ASSERT_TRUE(status.ok()) << status.ToString();
        test_release_handler(ctx_handler);
    }

    mp->barrier(PICO_LINENUM);
    for (int32_t key = min_key; key < max_key; ++key) {
        dense_key_t dense_key(key);
        std::vector<int64_t> push_arg(key);
        for (int32_t i = 0; i < key; i++) {
            push_arg[i] = key + i;
        }

        push_handler.async_push(
              std::make_unique<dense_push_item_t>(dense_key, &push_arg));
        ASSERT_TRUE(push_handler.wait());
        push_handler.sync_push(
              std::make_unique<dense_push_item_t>(dense_key, &push_arg));
        ASSERT_TRUE(push_handler.wait());
        if ((mp->rank() % 2) == 0) {
            push_handler.sync_push(
                  std::make_unique<dense_push_item_t>(dense_key, &push_arg));
            ASSERT_TRUE(push_handler.wait());
        }
    }

    mp->barrier(PICO_LINENUM);
    auto pull_handler = test_make_handler<PullHandler>(
        "TestOps", "TestDensePullOperator", op_config, storage_id);
    for (int i = 0; i < 5; ++i) {
        for (int32_t key = min_key; key < max_key; ++key) {
            int rank = key / 100;
            dense_key_t dense_key(key);
            std::vector<int64_t> expect(key);
            for (int32_t i = 0; i < key; i++) {
                expect[i] = (key + i) * (3 + (rank % 2 == 0));
            }

            std::vector<int64_t> pull_arg(key);
            pull_handler.pull(
                  std::make_unique<dense_pull_item_t>(dense_key, &pull_arg));
            ASSERT_TRUE(pull_handler.wait());
            EXPECT_EQ(expect, pull_arg);
        }
    }

    mp->barrier(PICO_LINENUM);
    auto dump_handler = test_make_handler<DumpHandler>(
          "TestOps", "TestDenseDumpOperator", op_config, storage_id);
    int g_secret = rand();
    std::string g_tmp_root = format_string(
          "./.unittest_tmp/ServiceTest.%d.%d", g_secret, getpid());
    if (0 == mp->rank()) {
        push_handler.store();
        ASSERT_TRUE(push_handler.wait());

        DumpArgs dump_args(g_tmp_root + "?format=archiveline", 2, "");
        dump_handler.dump(dump_args);
        ASSERT_TRUE(dump_handler.wait());

        test_release_handler(dump_handler);
        test_release_handler(push_handler);
        SCHECK(mp->client()->delete_storage(storage_id).ok());
    }

    mp->barrier(PICO_LINENUM);
    storage_id = test_create_storage("TestOps", "TestDenseStorageOperator", config);

    SLOG(INFO) << "storage id = " << storage_id << " is created.";
    auto load_handler = test_make_handler<LoadHandler>(
          "TestOps", "TestDenseLoadOperator", op_config, storage_id);
    if (0 == mp->rank()) {
        // create another storage for load, pull, eraseif & foreach
        load_handler.load(URIConfig(g_tmp_root + "?format=archiveline"));
        ASSERT_TRUE(load_handler.wait());
        test_release_handler(load_handler);
    }

    mp->barrier(PICO_LINENUM);
    Configure pred_config = test_load_pred_config(nodes, op_config);
    storage_id = test_create_storage("TestOps", "TestDenseStorageOperator", pred_config);
    load_handler = test_make_handler<LoadHandler>(
          "TestOps", "TestDenseLoadOperator", op_config, storage_id);
    
    if (0 == mp->rank()) {
        load_handler.load(URIConfig(g_tmp_root + "?format=archiveline"));
        ASSERT_TRUE(load_handler.wait());
        test_release_handler(load_handler);
    }

    mp->barrier(PICO_LINENUM);
    pull_handler =  test_make_handler<PullHandler>(
          "TestOps", "TestDensePullOperator", op_config, storage_id);
    for (int i = 0; i < 5; ++i) {
        for (int32_t key = min_key; key < max_key; ++key) {
            int rank = key / 100;
            dense_key_t dense_key(key);
            std::vector<int64_t> expect(key);
            for (int32_t i = 0; i < key; i++) {
                expect[i] = (key + i) * (2 + (rank % 2 == 0)) * 2;
            }

            std::vector<int64_t> pull_arg(key);
            pull_handler.pull(
                  std::make_unique<dense_pull_item_t>(dense_key, &pull_arg));
            ASSERT_TRUE(pull_handler.wait());
            EXPECT_EQ(expect, pull_arg);
        }
    }

    mp->barrier(PICO_LINENUM);
    ctx_handler = test_make_handler<UpdateContextHandler>(
          "TestOps", "TestDenseUpdateContextOperator", op_config, storage_id);
    if (0 == mp->rank()) {
        Configure new_pred_config = test_load_new_pred_config(nodes, op_config);
        auto status = ctx_handler.update_context(new_pred_config);
        ASSERT_TRUE(status.ok()) << status.ToString();
        test_release_handler(ctx_handler);
    }

    mp->barrier(PICO_LINENUM);
    for (int i = 0; i < 5; ++i) {
        for (int32_t key = min_key; key < max_key; ++key) {
            int rank = key / 100;
            dense_key_t dense_key(key);
            std::vector<int64_t> expect(key);
            for (int32_t i = 0; i < key; i++) {
                expect[i] = (key + i) * (2 + (rank % 2 == 0)) * 2;
            }

            std::vector<int64_t> pull_arg(key);
            ASSERT_TRUE(pull_handler.pull_with_auto_retry(
                  std::make_unique<dense_pull_item_t>(dense_key, &pull_arg)));
            EXPECT_EQ(expect, pull_arg);
        }
    }

    if (0 == mp->rank()) {
        FileSystem::rmrf(g_tmp_root.c_str());
    }
    mp->barrier(PICO_LINENUM);
}


TEST(PSService, SparseTest) {
    test_sparse(1, "");
    test_sparse(3, "");
    test_sparse(5, "");
    test_sparse(8, "");
}

TEST(PSService, SparseTestCompress) {
    test_sparse(1, "snappy");
    test_sparse(3, "snappy");
    test_sparse(5, "snappy");
    test_sparse(8, "snappy");
}

TEST(PSService, DenseTest) {
    test_dense(1, "");
    test_dense(3, "");
    test_dense(5, "");
    test_dense(8, "");
}

TEST(PSService, DenseTestCompress) {
    test_dense(1, "snappy");
    test_dense(3, "snappy");
    test_dense(5, "snappy");
    test_dense(8, "snappy");
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
