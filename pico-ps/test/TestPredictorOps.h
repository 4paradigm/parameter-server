#ifndef PARADIGM4_PICO_PS_TEST_PREDICTOR_TEST_PREDICTOR_OPS_H
#define PARADIGM4_PICO_PS_TEST_PREDICTOR_TEST_PREDICTOR_OPS_H

#include "pico-ps/common/core.h"

#include "pico-ps/handler/handlers.h"
#include "pico-ps/operator/operators.h"
#include "pico-ps/storage/KVShardStorage.h"
#include "pico-ps/storage/Storage.h"
#include "pico-ps/test/TestOpsDs.h"

namespace paradigm4 {
namespace pico {
namespace ps {

struct PTestKey {
    int32_t key;
    PICO_SERIALIZE(key);
    PTestKey(int32_t key = 0): key(key) {}
    operator int32_t()const {
        return key;
    }
    static PTestKey& empty_key() {
        static PTestKey empty({-1});
         return empty;
    }
    static PTestKey& deleted_key() {
        static PTestKey deleted({-2});
        return deleted;
    }
};

struct PTestTrainValue {
    int64_t extra;
    int64_t value;
    PICO_SERIALIZE(extra, value);

    PTestTrainValue() : extra(0), value(0) {}
    PTestTrainValue(int64_t a, int64_t b) : extra(a), value(b) {}
    typedef int64_t value_type;
};

struct PTestPredictorDumpKV {
    PTestKey key;
    PTestTrainValue value;
    PICO_SERIALIZE(key, value);
    typedef PTestKey key_type;
    typedef PTestTrainValue value_type;
};

namespace predictor {
struct TestPartitioner {
    typedef PTestKey key_type;

    size_t hash(const PTestKey& key)const {
        return key;
    }
    size_t operator()(const PTestKey& key)const {
        return hash(key);
    }
    int32_t partition(const PTestKey& key, int32_t num_buckets) const {
        return key % num_buckets;
    }
};

typedef UnorderedMapShardStorage<PTestKey, PTestTrainValue, TestPartitioner> trainer_storage_type;

typedef ShardStorageOperator<trainer_storage_type> TestStorageOperator;

typedef SparseTablePushOperator<PTestKey, PTestTrainValue, PTestTrainValue,
    PTestTrainValue, TestPartitioner, trainer_storage_type, trainer_storage_type>
    sparse_push_op_type;

class TestPushOperator : public sparse_push_op_type {
public:
    typedef typename sparse_push_op_type::key_type key_type;
    typedef typename sparse_push_op_type::value_type value_type;
    typedef typename sparse_push_op_type::push_value_type push_value_type;
    typedef typename sparse_push_op_type::push_arg_type push_arg_type;

    typedef typename sparse_push_op_type::storage_type storage_type;
    typedef typename sparse_push_op_type::shard_type shard_type;
    typedef typename sparse_push_op_type::delta_storage_type delta_storage_type;
    typedef typename sparse_push_op_type::delta_shard_type delta_shard_type;

    TestPushOperator(const Configure& config) : sparse_push_op_type(config) {}

    bool init_store_value(const key_type&, const push_value_type& push_val,
        value_type& store_val) override {
        store_val = push_val;
        return true;
    }

    void apply_push_arg(const key_type&, const push_arg_type& push_arg,
        push_value_type& push_val) override {
        push_val = push_arg;
    }

    void merge_push_value(const key_type&, const push_value_type& push_val_a,
        push_value_type& push_val_b) override {
        push_val_b = push_val_a;
    }

    void store_push_value(const key_type&, const push_value_type& push_val,
        value_type& store_val) override {
        store_val = push_val;
    }
};

typedef KVTextFileDumpOperator<TestDsWriter<PTestPredictorDumpKV>, PTestPredictorDumpKV, trainer_storage_type>
    TestDumpOperator;

struct TestPredictValue {
    int64_t value = 0;
    typedef int64_t value_type;

    TestPredictValue() = default;

    TestPredictValue(int64_t value) : value(value) {}

    PICO_SERIALIZATION(value);
};

typedef UnorderedMapShardStorage<PTestKey, TestPredictValue, TestPartitioner> predict_storage_type;

class TestRestoreOperator : public BaseKVTextFileRestoreOperator<TestDsReader<PTestPredictorDumpKV>, PTestPredictorDumpKV,
                                PTestPredictorDumpKV::key_type, TestPredictValue,
                                TestPartitioner, predict_storage_type> {
public:
    TestRestoreOperator(const Configure& conf)
        : BaseKVTextFileRestoreOperator<TestDsReader<PTestPredictorDumpKV>, PTestPredictorDumpKV,
              PTestPredictorDumpKV::key_type, TestPredictValue, TestPartitioner,
              predict_storage_type>(conf) {}

    virtual void deserialize_kv(PTestPredictorDumpKV& kv,
        PTestPredictorDumpKV::key_type& key, TestPredictValue& value) override {
        key         = kv.key;
        value.value = kv.value.value;
    }
};

typedef ShardStorageOperator<predict_storage_type, TestRestoreOperator>
    TestPredictorStorageOperator;

class TestLoadOperator : public BaseKVTextFileLoadOperator<TestDsReader<PTestPredictorDumpKV>, PTestPredictorDumpKV, PTestKey,
                             TestPredictValue, TestPartitioner, predict_storage_type> {
public:
    TestLoadOperator(const Configure& conf)
        : BaseKVTextFileLoadOperator<TestDsReader<PTestPredictorDumpKV>, PTestPredictorDumpKV, PTestPredictorDumpKV::key_type,
              TestPredictValue, TestPartitioner, predict_storage_type>(conf) {}

    virtual void deserialize_kv(
        PTestPredictorDumpKV& kv, PTestKey& key, TestPredictValue& push_val) override {
        key            = kv.key;
        push_val.value = kv.value.value;
    }
};

class TestPullOperator
    : public SparseTablePullOperator<PTestKey, TestPredictValue, TestPredictValue,
          TestPredictValue, TestPartitioner, predict_storage_type> {
public:
    TestPullOperator(const Configure& conf)
        : SparseTablePullOperator<PTestKey, TestPredictValue, TestPredictValue,
              TestPredictValue, TestPartitioner, predict_storage_type>(conf) {}

    virtual void fetch_pull_value(
        const key_type&, const value_type& value, pull_value_type& pull_val) {
        pull_val = value;
    }

    /*! \brief   用户自定义函数, client 端调用*/
    virtual void store_pull_value(
        const key_type&, const pull_value_type& pull_val, pull_arg_type& pull_arg) {
        pull_arg = pull_val;
    }
};

class TestUpdateContextOperator
    : public ShardUpdateContextOperator<PTestKey, TestPredictValue, TestPartitioner,
          predict_storage_type> {
public:
    TestUpdateContextOperator(const Configure& conf)
        : ShardUpdateContextOperator<PTestKey, TestPredictValue, TestPartitioner,
              predict_storage_type>(conf) {}
};

REGISTER_OPERATOR(TestPredictorOps, TestRestoreOperator);
REGISTER_OPERATOR(TestPredictorOps, TestStorageOperator);
REGISTER_OPERATOR(TestPredictorOps, TestPredictorStorageOperator);
REGISTER_OPERATOR(TestPredictorOps, TestPushOperator);
REGISTER_OPERATOR(TestPredictorOps, TestDumpOperator);
REGISTER_OPERATOR(TestPredictorOps, TestLoadOperator);
REGISTER_OPERATOR(TestPredictorOps, TestPullOperator);
REGISTER_OPERATOR(TestPredictorOps, TestUpdateContextOperator);

} // namespace predictor
} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_TEST_PREDICTOR_TEST_PREDICTOR_OPS_H
