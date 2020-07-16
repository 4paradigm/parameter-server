#ifndef PARADIGM4_PICO_PS_OPERATOR_STORAGE_OPERATOR_H
#define PARADIGM4_PICO_PS_OPERATOR_STORAGE_OPERATOR_H

#include <memory>
#include <vector>

#include "pico-ps/common/core.h"
#include "pico-ps/common/core.h"

#include "pico-ps/common/common.h"
#include "pico-ps/common/message.h"
#include "pico-ps/operator/Operator.h"
#include "pico-ps/operator/RestoreOperator.h"
#include "pico-ps/storage/Storage.h"

namespace paradigm4 {
namespace pico {
namespace ps {

/*!
 * \brief  每个 shard 的统计信息，
 *         item_number 表示有多少元素
 *         memory_usage 表示元素占用内存
 *         node_id 表示属于哪个 node
 *         shard_id 表示 shard id
 */
struct StorageStatisticInfo : Object {
    size_t item_number;
    size_t memory_usage;
    int32_t node_id;
    int32_t shard_id;

    bool operator<(const StorageStatisticInfo& rhs) {
        return (node_id < rhs.node_id) || (node_id == rhs.node_id && shard_id < rhs.shard_id);
    }

    PICO_SERIALIZATION(item_number, memory_usage, node_id, shard_id);
};

struct NodeMemoryInfo {
    bool healthy;
    size_t used_pmem;
    size_t managed_vmem;
    PICO_SERIALIZATION(healthy, used_pmem, managed_vmem);
};

/*!  \brief Storage 类定义作用于 Storage 的创建、清除内容与查询元信息的操作。 */
class StorageOperator : public Operator {
public:
    StorageOperator(const Configure& conf) : Operator(conf) {}

    virtual ~StorageOperator() {}

    StorageOperator(StorageOperator&&) = default;
    StorageOperator& operator=(StorageOperator&&) = default;

    /*!  \brief Storage 的创建操作。 */
    //add by cc pmem 接口修改 add one more para: storage_id 
    virtual std::unique_ptr<Storage> create_storage(RuntimeInfo&, int32_t) = 0;
    //virtual std::unique_ptr<Storage> create_storage(RuntimeInfo&) = 0;

    /*!  \brief Storage 的清除内容操作。 */
    virtual void clear_storage(Storage*) = 0;

    /*!  \brief 产生查询 Storage 元信息的 PSRequest。client 端使用。 */
    virtual void generate_query_info_request(RuntimeInfo&, std::vector<PSRequest>&) = 0;

    /*!  \brief  根据 Storage 元信息的 PSRequest 生成对应的 PSResponse。server 端使用。 */
    virtual void apply_query_info_request(RuntimeInfo&, PSRequest&, Storage*, PSResponse&) = 0;

    /*!  \brief  应用 server 返回的查询结果 PSResponse。client 端使用。 */
    virtual void apply_query_info_response(PSResponse&, std::vector<StorageStatisticInfo>&)
          = 0;

    virtual Operator* restore_op() = 0;
};

template <typename STORAGE, typename RESTORE_OP = RestoreOperator>
class ShardStorageOperator: public StorageOperator {
public:
    typedef STORAGE storage_type;
    typedef typename storage_type::shard_type shard_type;

    /*!
     * \brief 使用 storage config 初始化本Operator.
     *        Config 需要包括的内容域有
     *           1. node_num: parameter server 节点数;
     *           2. node_id: 当前 parameter server 节点号;
     *           3. shard_num: 当前 parameter server 节点中 shard 的数目;
     *        initialize_shard_storage_info() 定义在 pico-ps/common/common.h 中
     *        此函数将 _storage_info 与 _node_info 的内容填写。
     *        NodeInfo 定义在 pico-ps/common/common.h 中
     *        StorageInfo 定义在 pico-ps/common/common.h 中
     */
    // CreateSparseTableOperator use the STORAGE CONFIG to intialize directly rather than an
    // operator config
    ShardStorageOperator(const Configure& conf)
         : StorageOperator(conf), _restore_op(conf) {}

    virtual ~ShardStorageOperator() {}

    ShardStorageOperator(ShardStorageOperator&&) = default;
    ShardStorageOperator& operator=(ShardStorageOperator&&) = default;

    //add by cc pmem 接口修改 add one more para: storage_id 
    virtual std::unique_ptr<Storage> create_storage(RuntimeInfo& rt, int32_t storage_id) override { 
        _config.node()["pmem_create_storage"]["storage_id"] = storage_id;
        return std::make_unique<storage_type>(rt.local_shards(), _config);
    }

    virtual void clear_storage(Storage* st) override {
        static_cast<storage_type*>(st)->clear();
    }

    virtual void generate_query_info_request(RuntimeInfo& rt,
          std::vector<PSRequest>& reqs) override {
        reqs.reserve(rt.nodes().size());
        for (auto& node : rt.nodes()) {
            reqs.emplace_back(node.first);
        }
    }

    virtual void apply_query_info_request(RuntimeInfo& rt,
          PSRequest& req,
          Storage* storage,
          PSResponse& resp_ret) override {
        std::vector<StorageStatisticInfo> info;
        info.reserve(rt.local_shards().size());
        auto shard_storage = static_cast<storage_type*>(storage);
        for (auto shard_id : rt.local_shards()) {
            info.emplace_back();
            info.back().node_id = rt.node_id();
            info.back().shard_id = shard_id;
            info.back().item_number = shard_storage->shard_size(shard_id);
            info.back().memory_usage = shard_storage->shard_memory_usage(shard_id);
        }

        PSResponse resp(req);
        resp << info;
        resp_ret = std::move(resp);
    }

    virtual void apply_query_info_response(PSResponse& resp,
          std::vector<StorageStatisticInfo>& info_vec) override {
        std::vector<StorageStatisticInfo> info;
        resp >> info;
        for (auto& item : info) {
            info_vec.push_back(item);
        }
    }

    virtual Operator* restore_op() override {
        return &_restore_op;
    }

protected:
    RESTORE_OP _restore_op;
};


} // namespace ps
} // namespace pico
} // namespace paradigm4

#endif // PARADIGM4_PICO_PS_OPERATOR_STORAGE_OPERATOR_H
