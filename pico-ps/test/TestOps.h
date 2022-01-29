#ifndef PARADIGM4_PICO_PS_TEST_TESTOPS_H
#define PARADIGM4_PICO_PS_TEST_TESTOPS_H

#include "pico-ps/common/core.h"

#include "pico-ps/common/dense_common.h"
#include "pico-ps/handler/handlers.h"
#include "pico-ps/operator/operators.h"
#include "pico-ps/storage/KVShardStorage.h"
#include "pico-ps/test/TestOpsDs.h"

constexpr int TEST_KEY_NUM = 100;

namespace paradigm4 {
namespace pico {
namespace ps {

struct TestKey {
    int32_t key;
    PICO_SERIALIZE(key);
    TestKey(int32_t key = 0): key(key) {}
    operator int32_t()const {
        return key;
    }
    static TestKey& empty_key() {
        static TestKey empty({-1});
         return empty;
    }
    static TestKey& deleted_key() {
        static TestKey deleted({-2});
        return deleted;
    }
};

}
}
}

namespace std {
    template<> struct hash<paradigm4::pico::ps::TestKey> {
        typedef paradigm4::pico::ps::TestKey argument_type;
        typedef std::size_t result_type;
        result_type operator()(argument_type const& s) const {
            return std::hash<int32_t>()(s.key);
        }
    };
}

namespace paradigm4 {
namespace pico {
namespace ps {

struct TestValue {
    int64_t sum = 0;
    int64_t count = 0;
    PICO_SERIALIZE(sum, count);
    TestValue() = default;
};

struct TestPushValue {
    int64_t sum = 0;
    int64_t count = 0;
    TestPushValue() = default;
    PICO_SERIALIZATION(sum, count);
};

struct TestPullValue {
    int64_t value = 0;
    PICO_SERIALIZATION(value);
};

struct TestKV {
    TestKey key;
    TestValue value;
    PICO_SERIALIZE(key, value);

    typedef TestKey key_type;
    typedef TestValue value_type;
};


typedef SparseTablePushOperator<TestKey, 
      TestValue, 
      int64_t, 
      TestPushValue> sparse_push_op_type;

class TestPushOperator : public sparse_push_op_type {
public:
    TestPushOperator(const Configure& config) : sparse_push_op_type(config) {}

    bool init_store_value(const key_type&,
          const push_value_type& push_val,
          value_type& store_val) override {
        store_val = {push_val.sum, push_val.count};
        return true;
    }

    void apply_push_arg(const key_type&,
          const push_arg_type& push_arg,
          push_value_type& push_val) override {
        push_val.sum += push_arg;
        push_val.count += 1;
    }

    void merge_push_value(const key_type&,
          const push_value_type& push_val_a,
          push_value_type& push_val_b) override {
        push_val_b.sum += push_val_a.sum;
        push_val_b.count += push_val_a.count;
    }

    void store_push_value(const key_type&,
          const push_value_type& push_val,
          value_type& store_val) override {
        store_val.sum += push_val.sum;
        store_val.count += push_val.count;
    }
};

typedef SparseTablePullOperator<TestKey,
      TestValue, 
      int64_t, 
      TestPullValue> sparse_pull_op_type;

class TestPullOperator : public sparse_pull_op_type {
public:
    TestPullOperator(const Configure& config) : sparse_pull_op_type(config) {}
    void fetch_pull_value(const key_type&, const value_type& s, pull_value_type& p) override {
        p = {s.sum};
    }
    void store_pull_value(const key_type&,
          const pull_value_type& p,
          pull_arg_type& arg) override {
        arg = p.value;
    }
};

typedef TestPushOperator::storage_type TestStorageType;
typedef KVTextFileDumpOperator<TestDsWriter<TestKV>, TestKV> TestDumpOperator;
typedef KVTextFileLoadOperator<TestDsReader<TestKV>, TestKV> TestLoadOperator;
typedef ShardStorageOperator<TestStorageType> TestStorageOperator;

class TestEraseIfOperator : public SparseTableEraseIfOperator<TestStorageType> {
public:
    TestEraseIfOperator(const Configure& conf) : SparseTableEraseIfOperator(conf) {}

    bool erase_if(const key_type& key, const value_type&) override {
        return (key % 2);
    }
};

struct TestForEachResult : ForEachResult {
    int64_t num = 0;
    int64_t sum = 0;
    int64_t count = 0;
    PICO_PS_SERIALIZATION(TestForEachResult, num, sum, count);
};

class TestForEachOperator
    : public SparseTableForEachReadOperator<TestStorageType, TestForEachResult> {
public:
    TestForEachOperator(const Configure& conf) : SparseTableForEachReadOperator(conf) {}

    std::unique_ptr<TestForEachResult> init_result() override {
        return std::make_unique<TestForEachResult>();
    }

    std::unique_ptr<TestForEachResult> for_each(const key_type& key,
          const value_type& value) override {
        int32_t rank = key / TEST_KEY_NUM;
        CHECK_EQ(int64_t(2 + ((rank % 2) == 0)), value.count);
        CHECK_EQ(int64_t(key * value.count), value.sum);
        TestForEachResult result;
        result.num = 1;
        result.sum = value.sum;
        result.count = value.count;
        return std::make_unique<TestForEachResult>(result);
    }

    void merge_result(const TestForEachResult& in, TestForEachResult& out) override {
        out.num += in.num;
        out.sum += in.sum;
        out.count += in.count;
    }
};

REGISTER_OPERATOR(TestOps, TestStorageOperator);
REGISTER_OPERATOR(TestOps, TestPushOperator);
REGISTER_OPERATOR(TestOps, TestPullOperator);
REGISTER_OPERATOR(TestOps, TestDumpOperator);
REGISTER_OPERATOR(TestOps, TestLoadOperator);
REGISTER_OPERATOR(TestOps, TestEraseIfOperator);
REGISTER_OPERATOR(TestOps, TestForEachOperator);


struct TestPredictorValue {
    int64_t sum = 0;
    PICO_SERIALIZATION(sum);
};

typedef GoogleDenseHashMapShardStorage<TestKey, 
      TestPredictorValue> TestPredictorStorageType;

typedef BaseKVTextFileRestoreOperator<TestDsReader<TestKV>,
        TestKV, 
        TestKey, 
        TestPredictorValue,
        DefaultPartitioner<TestKey>, 
        TestPredictorStorageType> sparse_restore_op_type;

class TestPredictorRestoreOperator: public sparse_restore_op_type {
public:
    TestPredictorRestoreOperator(const Configure& conf): sparse_restore_op_type(conf) {}
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
      TestPredictorStorageType> sparse_load_op_type;

class TestPredictorLoadOperator : public sparse_load_op_type {
public:
    TestPredictorLoadOperator(const Configure& conf): sparse_load_op_type(conf) {}
    virtual void deserialize_kv(TestKV& kv,
          key_type& key,
          value_type& push_val) override {
        key = kv.key;
        push_val.sum = kv.value.sum;
    }
};

typedef SparseTablePullOperator<TestKey,
      TestPredictorValue, int64_t, TestPullValue> sparse_predictor_pull_op_type;

class TestPredictorPullOperator : public sparse_predictor_pull_op_type {
public:
    TestPredictorPullOperator(const Configure& config) : sparse_predictor_pull_op_type(config) {}
    void fetch_pull_value(const key_type&, const value_type& s, pull_value_type& p) override {
        p = {s.sum};
    }
    void store_pull_value(const key_type&,
          const pull_value_type& p,
          pull_arg_type& arg) override {
        arg = p.value;
    }
};

typedef ShardStorageOperator<TestPredictorStorageType, 
      TestPredictorRestoreOperator> TestPredictorStorageOperator;

typedef ShardUpdateContextOperator<TestKey, 
      TestPredictorValue, 
      DefaultPartitioner<TestKey>, 
      TestPredictorStorageType> TestPredictorUpdateContextOperator;
                                 
REGISTER_OPERATOR(TestOps, TestPredictorRestoreOperator);
REGISTER_OPERATOR(TestOps, TestPredictorStorageOperator);
REGISTER_OPERATOR(TestOps, TestPredictorLoadOperator);
REGISTER_OPERATOR(TestOps, TestPredictorPullOperator);
REGISTER_OPERATOR(TestOps, TestPredictorUpdateContextOperator);

static constexpr size_t DENSE_BLOCK_SIZE = 5;

struct TestDensePushValue {
    std::vector<int64_t> sum;
    PICO_SERIALIZATION(sum);
    PICO_SERIALIZED_SIZE(sum);
    void operator+=(const std::vector<int64_t>& rhs) {
        if (sum.size() == 0) sum.resize(rhs.size(), 0);
        SCHECK(sum.size() == rhs.size());
        for (size_t i = 0; i < sum.size(); ++i) {
            sum[i] += rhs[i];
        }
    }
};

struct TestDenseValue {
    std::vector<int64_t> sum;
    PICO_SERIALIZE(sum);
    PICO_SERIALIZED_SIZE(sum);
    void operator+=(const std::vector<int64_t>& rhs) {
        if (sum.size() == 0) sum.resize(rhs.size(), 0);
        SCHECK(sum.size() == rhs.size());
        for (size_t i = 0; i < sum.size(); ++i) {
            sum[i] += rhs[i];
        }
    }
};

struct TestDenseKV {
    dense_slice_key_t key;
    TestDenseValue value;
    PICO_SERIALIZE(key, value);

    typedef dense_slice_key_t key_type;
    typedef TestDenseValue value_type;
};

struct TestValuePartitioner: DenseValuePartitioner<dense_slice_key_t, std::vector<int64_t>> {
    PartitionResult partition(dense_key_t, value_type& origin) {
        std::vector<value_type> res;
        std::vector<size_t> serialized_size;
        size_t n = origin.size();
        for (size_t l = 0; l < n; l += DENSE_BLOCK_SIZE) {
            size_t r = std::min(l + DENSE_BLOCK_SIZE, n);
            res.emplace_back(origin.begin() + l, origin.begin() + r);
            serialized_size.push_back(sizeof(size_t) + (r - l) * sizeof(int64_t));
        }
        return {serialized_size, res};
    }
};

struct TestValueMerger : DenseValueMerger<dense_slice_key_t, std::vector<int64_t>> {
    size_t slice_number(dense_key_t, value_type& val) {
        return (val.size() + DENSE_BLOCK_SIZE - 1) / DENSE_BLOCK_SIZE;
    }
    void merge(dense_slice_key_t, value_type& merged, 
          size_t, value_type& slice, size_t idx) {
        for (size_t i = 0; i < slice.size(); ++i) {
            merged[idx * DENSE_BLOCK_SIZE + i] = slice[i];
        }
    }
    size_t serialized_size(dense_slice_key_t, value_type& val) {
        return sizeof(size_t) + val.size() * sizeof(int64_t);
    }
};


typedef GoogleDenseHashMapShardStorage<dense_slice_key_t, 
      TestDenseValue, dense_slice_key_hasher_t> TestDenseStorageType;
typedef GoogleDenseHashMapShardStorage<dense_slice_key_t, 
      TestDensePushValue, dense_slice_key_hasher_t> TestDenseDeltaStorageType;

typedef DenseTablePushOperator<dense_slice_key_t,
      TestDenseValue, 
      std::vector<int64_t>, 
      TestDensePushValue,
      DefaultPartitioner<dense_slice_key_t>, 
      TestValuePartitioner,
      TestDenseStorageType, 
      TestDenseDeltaStorageType> dense_push_op_type;

class TestDensePushOperator : public dense_push_op_type {
public:
    TestDensePushOperator(const Configure& config) : dense_push_op_type(config) {}

    bool init_store_value(const slice_key_type&,
          const push_value_type& push_val,
          value_type& store_val) override {
        store_val = {push_val.sum};
        return true;
    }

    void apply_push_arg(const slice_key_type&,
          const push_arg_type& push_arg,
          push_value_type& push_val) override {
        push_val = {push_arg};
    }

    void merge_push_value(const slice_key_type&,
          const push_value_type& push_val_a,
          push_value_type& push_val_b) override {
        push_val_b += push_val_a.sum;
    }

    void store_push_value(const slice_key_type&,
          const push_value_type& push_val,
          value_type& store_val) override {
        store_val += push_val.sum;
    }
};

typedef DenseTablePullOperator<dense_slice_key_t,
      TestDenseValue, 
      std::vector<int64_t>, 
      TestDensePushValue,
      DefaultPartitioner<dense_slice_key_t>, 
      TestValueMerger,
      TestDenseStorageType> dense_pull_op_type;

class TestDensePullOperator : public dense_pull_op_type {
public:
    TestDensePullOperator(const Configure& config): dense_pull_op_type(config) {}
    void fetch_pull_value(const slice_key_type&, 
          const value_type& s, 
          pull_value_type& p) override {
        p = {s.sum};
    }

    void store_pull_value(const slice_key_type&,
          pull_value_type& p,
          pull_arg_type& arg) override {
        arg = {p.sum};
    }
};

typedef KVTextFileRestoreOperator<TestDsReader<TestDenseKV>,
      TestDenseKV,
      DefaultPartitioner<dense_slice_key_t>,
      TestDenseStorageType> TestDenseRestoreOperator;

typedef ShardStorageOperator<TestDenseStorageType, 
      TestDenseRestoreOperator> TestDenseStorageOperator;

typedef KVTextFileDumpOperator<TestDsWriter<TestDenseKV>,
      TestDenseKV, 
      TestDenseStorageType> TestDenseDumpOperator;

typedef KVTextFileLoadOperator<TestDsReader<TestDenseKV>,
      TestDenseKV, 
      DefaultPartitioner<dense_slice_key_t>, 
      TestDenseStorageType> TestDenseLoadOperator;

typedef ShardUpdateContextOperator<dense_slice_key_t, 
      TestDenseValue, 
      DefaultPartitioner<dense_slice_key_t>, 
      TestDenseStorageType> TestDenseUpdateContextOperator;


REGISTER_OPERATOR(TestOps, TestDenseRestoreOperator);
REGISTER_OPERATOR(TestOps, TestDenseStorageOperator);
REGISTER_OPERATOR(TestOps, TestDensePushOperator);
REGISTER_OPERATOR(TestOps, TestDensePullOperator);
REGISTER_OPERATOR(TestOps, TestDenseDumpOperator);
REGISTER_OPERATOR(TestOps, TestDenseLoadOperator);
REGISTER_OPERATOR(TestOps, TestDenseUpdateContextOperator);


template <class T>
class TestBadAllocator : public std::allocator<T> {
public:
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef T& reference;
    typedef const T& const_reference;
    typedef T value_type;
    template <class U>
    struct rebind {
        typedef TestBadAllocator<U> other;
    };

    TestBadAllocator(Storage* st = nullptr) : std::allocator<T>() {(void)(st);}

    TestBadAllocator(const TestBadAllocator& other) : std::allocator<T>(other) {}

    template <class U>
    TestBadAllocator(const TestBadAllocator<U>& other) : std::allocator<T>(other) {}

    ~TestBadAllocator() {}

    pointer allocate(size_type num, const void* hint = 0) {
        static std::atomic<int> atom;
        //num == 32 when initialize google dense hash map
        if (num > 32) {
            atom.fetch_add(1);
            if (atom.load() % 2 != 0) {
                std::__throw_bad_alloc();
            }            
        }
        return std::allocator<T>::allocate(num, hint);
    }
    
    void deallocate(pointer p, size_type num) {
        std::allocator<T>::deallocate(p, num);
    }
};


} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_TEST_TESTOPS_H
